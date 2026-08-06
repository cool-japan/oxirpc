//! End-to-end tests for the native HTTP/3 (gRPC-over-QUIC) transport.
//!
//! These spin up a real QUIC loopback server (`ServerEndpoint` bound to
//! `127.0.0.1:0`) running a `NativeServiceRegistry`, and drive it with the
//! native [`H3Channel`] client. They exercise:
//!
//! - unary RPC (`Health/Check`)
//! - server-streaming RPC (`Health/Watch`)
//! - client-streaming / bidi RPC (a custom echo service)
//! - deadline / timeout handling
//! - graceful shutdown
//! - ALPN-mismatch rejection (a client offering `h2` must be refused)
//!
//! Requires the `http3`, `native`, `health`, and `client` cargo features.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body::Body as _;
use prost::Message as _;
use rustls::RootCertStore;

use oxirpc_client::native_channel::h3::H3ChannelBuilder;
use oxirpc_core::wire::{
    bidi_sequential_response, body_channel, encode_grpc_message, grpc_response_headers, NativeBody,
};
use oxirpc_core::OxiRpcError;
use oxirpc_health::HealthBuilder;
use oxirpc_server::{NativeServiceRegistry, ServerBuilder, ServerEndpoint, TransportConfig};

use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

// ── cert / config helpers ─────────────────────────────────────────────────────

/// Generate a self-signed Ed25519 cert for `localhost` and return
/// `(cert_pem, key_pem, cert_der)`. Ed25519 is the cert type proven to complete
/// the OxiQUIC handshake with the pure-Rust crypto provider.
fn localhost_cert() -> (String, String, Vec<u8>) {
    let ck = oxitls_rcgen::generate_self_signed_ed25519(&["localhost"])
        .expect("generate self-signed ed25519 cert");
    let key_pem = pkcs8_der_to_pem(&ck.pkcs8_der);
    (ck.cert_pem, key_pem, ck.cert_der)
}

/// Build a QUIC-capable client config trusting `cert_der`, with the given ALPN.
fn client_config(cert_der: &[u8], alpn: &[&[u8]]) -> Arc<rustls::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
        .expect("trust self-signed cert");
    let mut cfg = oxirpc_core::tls::client_config_h3(roots).expect("client_config_h3");
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(cfg)
}

/// Bind a loopback HTTP/3 server endpoint from the given PEM cert + key.
async fn bind_server(cert_pem: &str, key_pem: &str) -> (ServerEndpoint, std::net::SocketAddr) {
    let server_cfg =
        oxirpc_core::tls::server_config_h3_arc(cert_pem.as_bytes(), key_pem.as_bytes())
            .expect("server_config_h3");
    let endpoint = ServerEndpoint::bind(
        "127.0.0.1:0".parse().expect("addr"),
        server_cfg,
        TransportConfig::default(),
    )
    .await
    .expect("bind h3 server endpoint");
    let addr = endpoint.local_addr().expect("local_addr");
    (endpoint, addr)
}

/// Build a plain unary gRPC request over h3 for `path` carrying `msg`.
///
/// The authority in the URI is cosmetic — `H3Channel` rewrites it to the dialled
/// endpoint — so a fixed `localhost` authority is used for readability.
fn unary_request<M: prost::Message>(path: &str, msg: &M) -> http::Request<NativeBody> {
    let framed = encode_grpc_message(msg).expect("encode grpc message");
    http::Request::builder()
        .method("POST")
        .uri(format!("https://localhost{path}"))
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(NativeBody::once(framed))
        .expect("build request")
}

/// Collect all data frames of a `NativeBody`, returning concatenated payloads
/// and any terminal error.
async fn collect_payloads(body: NativeBody) -> Result<Vec<Bytes>, OxiRpcError> {
    let mut out = Vec::new();
    let mut body = std::pin::pin!(body);
    loop {
        match std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if frame.is_data() {
                    if let Ok(d) = frame.into_data() {
                        if !d.is_empty() {
                            out.push(d);
                        }
                    }
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    Ok(out)
}

// ── custom bidi echo native service ───────────────────────────────────────────

/// A tiny native gRPC service exposing `/test.Echo/BidiEcho`, which echoes each
/// `HealthCheckRequest` back as a `HealthCheckResponse` whose `status` is the
/// byte length of the request's `service` field. Exercises full-duplex streaming.
#[derive(Clone)]
struct EchoService;

impl tonic::server::NamedService for EchoService {
    const NAME: &'static str = "test.Echo";
}

impl<B> tower::Service<http::Request<B>> for EchoService
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<OxiRpcError> + Send,
{
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let path = req.uri().path().to_owned();
        Box::pin(async move {
            if path != "/test.Echo/BidiEcho" {
                let body = oxirpc_core::wire::error_response_body(12, "unknown method");
                let mut resp = http::Response::new(body);
                for (k, v) in &grpc_response_headers() {
                    resp.headers_mut().insert(k.clone(), v.clone());
                }
                return Ok(resp);
            }
            let body = bidi_sequential_response::<_, HealthCheckRequest, HealthCheckResponse, _>(
                req.into_body(),
                |r| {
                    Ok(HealthCheckResponse {
                        status: r.service.len() as i32,
                    })
                },
            );
            let mut resp = http::Response::new(body);
            for (k, v) in &grpc_response_headers() {
                resp.headers_mut().insert(k.clone(), v.clone());
            }
            Ok(resp)
        })
    }
}

// ── custom bad-content-type native service ────────────────────────────────────

/// A tiny native gRPC service that always responds with a non-gRPC
/// `content-type` (`text/html`), simulating a reverse-proxy error page or a
/// misconfigured backend reachable at the same QUIC endpoint. Used to prove
/// the H3 client rejects such a response instead of feeding it to the frame
/// decoder.
#[derive(Clone)]
struct WrongContentTypeService;

impl tonic::server::NamedService for WrongContentTypeService {
    const NAME: &'static str = "test.WrongContentType";
}

impl<B> tower::Service<http::Request<B>> for WrongContentTypeService
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<OxiRpcError> + Send,
{
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: http::Request<B>) -> Self::Future {
        Box::pin(async move {
            let body = NativeBody::once(Bytes::from_static(b"<html>not gRPC</html>"));
            let mut resp = http::Response::new(body);
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("text/html"),
            );
            Ok(resp)
        })
    }
}

// ── Test 1: unary Health/Check ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn h3_unary_check() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);

    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
            .ok();
    });

    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h3"]))
        .build()
        .expect("build h3 channel");

    let req = unary_request(
        "/grpc.health.v1.Health/Check",
        &HealthCheckRequest {
            service: String::new(),
        },
    );
    let resp = tokio::time::timeout(Duration::from_secs(10), channel.call(req))
        .await
        .expect("call within 10s")
        .expect("unary check must succeed");
    assert_eq!(resp.status(), 200);

    let payloads = collect_payloads(resp.into_body())
        .await
        .expect("collect body");
    let all: Vec<u8> = payloads.into_iter().flat_map(|b| b.to_vec()).collect();
    let decoded = HealthCheckResponse::decode(all.as_slice()).expect("decode response");
    assert_eq!(decoded.status, 1, "expected SERVING (1)");

    server.abort();
}

// ── Test 2: server-streaming Health/Watch ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn h3_server_streaming_watch() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("watch.Target").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);

    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
            .ok();
    });

    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h3"]))
        .build()
        .expect("build h3 channel");

    let req = unary_request(
        "/grpc.health.v1.Health/Watch",
        &HealthCheckRequest {
            service: "watch.Target".to_string(),
        },
    );
    let resp = tokio::time::timeout(Duration::from_secs(10), channel.call(req))
        .await
        .expect("call within 10s")
        .expect("watch stream open must succeed");
    assert_eq!(resp.status(), 200);

    // Read just the first streamed message payload.
    let mut body = std::pin::pin!(resp.into_body());
    let first = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
                Some(Ok(frame)) if frame.is_data() => {
                    if let Ok(d) = frame.into_data() {
                        if !d.is_empty() {
                            return Some(d);
                        }
                    }
                }
                Some(Ok(_)) => continue,
                _ => return None,
            }
        }
    })
    .await
    .expect("first watch message within 5s")
    .expect("stream must yield a message");

    let decoded = HealthCheckResponse::decode(first.as_ref()).expect("decode watch message");
    assert_eq!(
        decoded.status, 1,
        "expected SERVING (1) in first Watch update"
    );

    server.abort();
}

// ── Test 3: client-streaming / bidi echo ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn h3_bidi_echo() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let registry = NativeServiceRegistry::new().add_service(EchoService);
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
            .ok();
    });

    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h3"]))
        .build()
        .expect("build h3 channel");

    // Build a streaming request body carrying three messages.
    let inputs = ["a", "bb", "ccc"];
    let (tx, req_body) = body_channel(8);
    let send_task = tokio::spawn(async move {
        for s in inputs {
            let framed = encode_grpc_message(&HealthCheckRequest {
                service: s.to_string(),
            })
            .expect("encode");
            if tx.send_data(framed).await.is_err() {
                return;
            }
        }
        // Dropping tx closes the request stream (half-close).
    });

    let req = http::Request::builder()
        .method("POST")
        .uri("https://localhost/test.Echo/BidiEcho")
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(req_body)
        .expect("build request");

    let resp = tokio::time::timeout(Duration::from_secs(10), channel.call(req))
        .await
        .expect("call within 10s")
        .expect("bidi echo must succeed");
    assert_eq!(resp.status(), 200);

    let payloads = collect_payloads(resp.into_body())
        .await
        .expect("collect body");
    send_task.await.ok();

    // Each response is a separate gRPC frame → one payload per input.
    let statuses: Vec<i32> = payloads
        .iter()
        .map(|p| {
            HealthCheckResponse::decode(p.as_ref())
                .expect("decode echo")
                .status
        })
        .collect();
    assert_eq!(
        statuses,
        vec![1, 2, 3],
        "echo must return the byte length of each request's service field"
    );

    server.abort();
}

// ── Test 4: deadline / timeout ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn h3_deadline_expires() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
            .ok();
    });

    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h3"]))
        .build()
        .expect("build h3 channel");
    // Warm the connection so the timeout is measured against the RPC, not dialing.
    channel.ready().await.expect("connection ready");

    let req = unary_request(
        "/grpc.health.v1.Health/Check",
        &HealthCheckRequest {
            service: String::new(),
        },
    );
    let expired = Instant::now() - Duration::from_secs(1);
    let err = channel
        .call_with_deadline(req, expired)
        .await
        .expect_err("expired deadline must fail");
    assert!(
        matches!(err, OxiRpcError::Timeout),
        "expected Timeout, got {err:?}"
    );

    server.abort();
}

// ── Test 5: graceful shutdown ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn h3_graceful_shutdown() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, Some(shutdown_rx))
            .await
            .ok();
    });

    // Verify the server is up first.
    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h3"]))
        .build()
        .expect("build h3 channel");
    let req = unary_request(
        "/grpc.health.v1.Health/Check",
        &HealthCheckRequest {
            service: String::new(),
        },
    );
    let resp = tokio::time::timeout(Duration::from_secs(10), channel.call(req))
        .await
        .expect("call within 10s")
        .expect("check before shutdown");
    assert_eq!(resp.status(), 200);
    drop(collect_payloads(resp.into_body()).await);

    // Fire the shutdown signal; the serve task must return promptly.
    shutdown_tx.send(()).ok();
    let done = tokio::time::timeout(Duration::from_secs(5), server).await;
    assert!(
        done.is_ok(),
        "server must exit within 5s of shutdown signal"
    );
}

// ── Test 6: ALPN mismatch is rejected ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn h3_alpn_mismatch_rejected() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
            .ok();
    });

    // Client offers only "h2"; the h3 server advertises only "h3" → the QUIC/TLS
    // handshake must fail to agree on an application protocol. A short idle
    // timeout bounds how long the failed handshake lingers before erroring.
    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h2"]))
        .transport(TransportConfig::default().idle_timeout(Duration::from_secs(3)))
        .build()
        .expect("build h3 channel");

    let req = unary_request(
        "/grpc.health.v1.Health/Check",
        &HealthCheckRequest {
            service: String::new(),
        },
    );
    let err = tokio::time::timeout(Duration::from_secs(20), channel.call(req))
        .await
        .expect("call within 20s")
        .expect_err("ALPN mismatch must be rejected");
    assert!(
        matches!(err, OxiRpcError::Transport(_)),
        "expected Transport error on ALPN mismatch, got {err:?}"
    );

    server.abort();
}

// ── Test 7: response content-type validation ──────────────────────────────────

/// Regression test: an H3 response whose `content-type` is not a gRPC variant
/// must be surfaced as a clear [`OxiRpcError::Transport`] naming the actual
/// content-type, rather than being fed into the frame decoder.
#[tokio::test(flavor = "multi_thread")]
async fn h3_response_content_type_rejected() {
    let (cert_pem, key_pem, cert_der) = localhost_cert();
    let (endpoint, addr) = bind_server(&cert_pem, &key_pem).await;

    let registry = NativeServiceRegistry::new().add_service(WrongContentTypeService);
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
            .ok();
    });

    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(client_config(&cert_der, &[b"h3"]))
        .build()
        .expect("build h3 channel");

    let req = unary_request(
        "/test.WrongContentType/Anything",
        &HealthCheckRequest {
            service: String::new(),
        },
    );
    let err = tokio::time::timeout(Duration::from_secs(10), channel.call(req))
        .await
        .expect("call within 10s")
        .expect_err("a text/html response must be rejected");

    match err {
        OxiRpcError::Transport(msg) => {
            assert!(
                msg.contains("text/html"),
                "error must name the actual content-type, got: {msg}"
            );
        }
        other => panic!("expected Transport error, got {other:?}"),
    }

    server.abort();
}

// ── PKCS#8 DER → PEM helper (test-only) ───────────────────────────────────────

/// Wrap PKCS#8 DER key bytes in a `PRIVATE KEY` PEM envelope.
fn pkcs8_der_to_pem(der: &[u8]) -> String {
    let b64 = base64_encode(der);
    let mut out = String::from("-----BEGIN PRIVATE KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        out.push('\n');
    }
    out.push_str("-----END PRIVATE KEY-----\n");
    out
}

/// Standard RFC 4648 base64 encoder (test-only; avoids a dependency).
fn base64_encode(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[((n >> 18) & 0x3f) as usize] as char);
        out.push(T[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            T[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}
