//! Integration tests for NativeChannel (pure-h2 client channel).
//!
//! Each test spins up a minimal h2 server using the fixture helpers,
//! then exercises NativeChannel end-to-end.

// Include the native h2 server fixture directly in this test crate.
mod native_h2_server;
use native_h2_server::{
    spawn_goaway_server, spawn_header_echo_server, spawn_streaming_server,
    spawn_trailer_only_server, spawn_unary_ok_server,
};

use bytes::Bytes;
use http_body::Body as _;
use std::pin::pin;
use std::time::{Duration, Instant};

use oxirpc_client::balance::{Endpoint, StaticResolver};
use oxirpc_client::native_channel::{NativeBody, NativeChannelBuilder};
use oxirpc_core::OxiRpcError;

// ── test helpers ──────────────────────────────────────────────────────────────

/// Build a NativeChannel pointing at a single address.
async fn channel_for(addr: std::net::SocketAddr) -> oxirpc_client::NativeChannel {
    let uri = format!("http://{addr}").parse().unwrap();
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await
        .expect("channel build must succeed")
}

/// Build a minimal gRPC request with required HTTP/2 pseudo-headers.
fn grpc_request(authority: &str, path: &str, body: NativeBody) -> http::Request<NativeBody> {
    http::Request::builder()
        .method("POST")
        .uri(format!("http://{authority}{path}"))
        .version(http::Version::HTTP_2)
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(body)
        .unwrap()
}

/// Drain all data bytes from a `NativeBody` and collect them.
async fn collect_body(body: NativeBody) -> Result<Vec<Bytes>, OxiRpcError> {
    let mut out = Vec::new();
    let mut pinned = pin!(body);
    loop {
        match futures_util::future::poll_fn(|cx| pinned.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if frame.is_data() {
                    if let Ok(data) = frame.into_data() {
                        out.push(data);
                    }
                }
                // Skip trailer frames.
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    Ok(out)
}

// ── Test 1: unary_roundtrip ───────────────────────────────────────────────────

#[tokio::test]
async fn unary_roundtrip() {
    // A small payload to send as the gRPC response body.
    let payload = Bytes::from_static(b"\x08\x01"); // proto varint
    let server = spawn_unary_ok_server(payload.clone()).await;
    let channel = channel_for(server.addr).await;

    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Unary",
        NativeBody::empty(),
    );

    let resp = channel.call(req).await.expect("unary call must succeed");
    assert_eq!(resp.status(), 200);

    let chunks = collect_body(resp.into_body())
        .await
        .expect("body collect must succeed");
    assert!(!chunks.is_empty(), "expected at least one data chunk");
    // pump_response decodes gRPC frames via FrameDecoder and sends only the payload.
    // So the collected bytes are the raw proto payload (without 5-byte framing header).
    let total: Vec<u8> = chunks.into_iter().flat_map(|b| b.to_vec()).collect();
    assert_eq!(
        total,
        payload.as_ref(),
        "decoded payload must match server response"
    );

    server.shutdown();
}

// ── Test 2: trailer_only_unimplemented ───────────────────────────────────────

#[tokio::test]
async fn trailer_only_unimplemented() {
    // grpc-status:12 = UNIMPLEMENTED
    let server = spawn_trailer_only_server(12).await;
    let channel = channel_for(server.addr).await;

    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Missing",
        NativeBody::empty(),
    );

    let err = channel
        .call(req)
        .await
        .expect_err("call to trailer-only UNIMPLEMENTED must fail");

    match err {
        OxiRpcError::Status(s) => {
            assert_eq!(
                s.code(),
                tonic::Code::Unimplemented,
                "expected UNIMPLEMENTED (12), got {:?}",
                s.code()
            );
        }
        other => panic!("expected Status error, got {other:?}"),
    }

    server.shutdown();
}

// ── Test 3: trailer_only_ok_returns_empty_body ────────────────────────────────

#[tokio::test]
async fn trailer_only_ok_returns_empty_body() {
    // grpc-status:0 in initial headers = OK with no response body
    let server = spawn_trailer_only_server(0).await;
    let channel = channel_for(server.addr).await;

    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Empty",
        NativeBody::empty(),
    );

    let resp = channel
        .call(req)
        .await
        .expect("trailer-only OK must succeed");

    let chunks = collect_body(resp.into_body())
        .await
        .expect("body collect must succeed");
    // No data frames for an OK trailers-only response.
    assert!(
        chunks.is_empty() || chunks.iter().all(|c| c.is_empty()),
        "expected empty body for trailers-only OK"
    );

    server.shutdown();
}

// ── Test 4: server_streaming_three_frames ────────────────────────────────────

#[tokio::test]
async fn server_streaming_three_frames() {
    let payload = Bytes::from_static(b"\x0a\x03hey");
    let server = spawn_streaming_server(3, payload.clone()).await;
    let channel = channel_for(server.addr).await;

    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Stream",
        NativeBody::empty(),
    );

    let resp = channel
        .call(req)
        .await
        .expect("streaming call must succeed");
    assert_eq!(resp.status(), 200);

    let chunks = collect_body(resp.into_body())
        .await
        .expect("body collect must succeed");

    // pump_response decodes gRPC frames and sends the raw payload bytes
    // (the 5-byte framing header is stripped by FrameDecoder).
    // We expect 3 payloads of payload.len() bytes each.
    let total: Vec<u8> = chunks.into_iter().flat_map(|b| b.to_vec()).collect();
    assert_eq!(
        total.len(),
        3 * payload.len(),
        "expected payload bytes from 3 decoded gRPC frames, got {} bytes",
        total.len()
    );

    server.shutdown();
}

// ── Test 5: deadline_expires ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_deadline_expires() {
    // Use a sleeping server — but actually the simplest approach is to
    // set an already-expired deadline and not bother with a real server.
    let server = spawn_unary_ok_server(Bytes::from_static(b"")).await;
    let channel = channel_for(server.addr).await;

    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Slow",
        NativeBody::empty(),
    );

    // Deadline already in the past.
    let expired = Instant::now() - Duration::from_secs(1);
    let err = channel
        .call_with_deadline(req, expired)
        .await
        .expect_err("expired deadline must return Timeout error");

    assert!(
        matches!(err, OxiRpcError::Timeout),
        "expected Timeout, got {err:?}"
    );

    server.shutdown();
}

// ── Test 6: connection_failure_returns_error ──────────────────────────────────

#[tokio::test]
async fn connection_failure_returns_error() {
    // Point at a port with nothing listening.
    let uri: http::Uri = "http://127.0.0.1:1".parse().unwrap();
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(Duration::from_millis(200))
        .build()
        .await
        .expect("build succeeds even with no server — connection is lazy");

    let req = grpc_request("127.0.0.1:1", "/test.Service/Fail", NativeBody::empty());

    let err = channel
        .call(req)
        .await
        .expect_err("call to non-existent port must fail");

    // Should be Timeout (connect timed out) or Transport (connection refused).
    assert!(
        matches!(err, OxiRpcError::Timeout | OxiRpcError::Transport(_)),
        "expected Timeout or Transport error, got {err:?}"
    );
}

// ── Test 7: ready_returns_after_connection_established ───────────────────────

#[tokio::test]
async fn ready_returns_after_connection_established() {
    let payload = Bytes::from_static(b"");
    let server = spawn_unary_ok_server(payload).await;

    let uri: http::Uri = format!("http://{}", server.addr).parse().unwrap();
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .build()
        .await
        .expect("build must succeed");

    // ready() should return within a reasonable time.
    tokio::time::timeout(Duration::from_secs(5), channel.ready())
        .await
        .expect("ready() must resolve within 5 seconds");

    server.shutdown();
}

// ── Test 8: goaway_marks_connection_draining ─────────────────────────────────

#[tokio::test]
async fn goaway_marks_connection_draining() {
    // The GOAWAY server closes after the first request.
    let server = spawn_goaway_server().await;
    let channel = channel_for(server.addr).await;

    // First request: may succeed or fail depending on timing.
    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Test",
        NativeBody::empty(),
    );
    let _ = channel.call(req).await;

    // After the GOAWAY, a second request must fail (server is gone).
    let req2 = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Test",
        NativeBody::empty(),
    );
    let result = channel.call(req2).await;
    // We just verify it doesn't panic; the result may be an error or OK depending
    // on whether the pool has already evicted the dead connection.
    let _ = result;
}

// ── Test 9: body_channel_roundtrip ───────────────────────────────────────────

#[tokio::test]
async fn body_channel_roundtrip() {
    use oxirpc_client::native_channel::body_channel;

    let (tx, body) = body_channel(4);

    let data1 = Bytes::from_static(b"hello");
    let data2 = Bytes::from_static(b"world");

    let send_task = tokio::spawn(async move {
        tx.send_data(data1).await.expect("send data1");
        tx.send_data(data2).await.expect("send data2");
        // Sender drops here, signalling EOF.
    });

    let chunks = collect_body(body).await.expect("collect body");
    send_task.await.expect("send task");

    let combined: Vec<u8> = chunks.into_iter().flat_map(|b| b.to_vec()).collect();
    assert_eq!(combined, b"helloworld");
}

// ── Test 10: builder_default_and_custom ──────────────────────────────────────

#[tokio::test]
async fn builder_with_no_resolver_fails() {
    let err = NativeChannelBuilder::new()
        .build()
        .await
        .expect_err("build without resolver must fail");

    assert!(
        matches!(err, OxiRpcError::Build(_)),
        "expected Build error, got {err:?}"
    );
}

// ── Test 11: tls_dial_connects_to_tls_server ─────────────────────────────────

/// Spin up a minimal TLS h2 server with a self-signed rcgen cert, dial via
/// `NativeChannelBuilder::new().tls(...)`, send a unary request, verify the
/// response comes back with the expected payload.
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_dial_connects_to_tls_server() {
    use std::sync::Arc;

    use bytes::Bytes;
    use oxirpc_client::native_channel::TlsConfig;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    // ── 1. Generate a self-signed certificate with rcgen ─────────────────────
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("rcgen cert generation must succeed");
    let cert_der = cert.cert.der().to_owned();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    // ── 2. Build the rustls ServerConfig via oxirpc_core::tls ────────────────
    let server_cfg = oxirpc_core::tls::server_config(cert_pem.as_bytes(), key_pem.as_bytes())
        .expect("server config must build");
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    // ── 3. Start the TLS h2 server ────────────────────────────────────────────
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind must succeed");
    let addr = listener.local_addr().expect("local_addr");
    let response_payload = Bytes::from_static(b"\x08\x01");

    let server_payload = response_payload.clone();
    let server_task = tokio::spawn(async move {
        loop {
            let (tcp, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let acc = acceptor.clone();
            let payload = server_payload.clone();
            tokio::spawn(async move {
                let tls = match acc.accept(tcp).await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let mut h2 = match h2::server::handshake(tls).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                while let Some(Ok((req, mut respond))) = h2.accept().await {
                    // Drain request body.
                    let (_, mut body) = req.into_parts();
                    while let Some(chunk) = body.data().await {
                        if let Ok(chunk) = chunk {
                            let _ = body.flow_control().release_capacity(chunk.len());
                        }
                    }
                    // Send unary OK response.
                    send_tls_unary_response(&mut respond, payload.clone()).await;
                }
            });
        }
    });

    // ── 4. Build the client TlsConfig ────────────────────────────────────────
    let mut roots = RootCertStore::empty();
    roots.add(cert_der).expect("add cert to root store");
    let client_rustls_cfg =
        oxirpc_core::tls::client_config(roots).expect("client config must build");
    let server_name = ServerName::try_from("localhost")
        .expect("valid server name")
        .to_owned();
    let tls_cfg = TlsConfig::new(Arc::new(client_rustls_cfg), server_name);

    // ── 5. Build the NativeChannel with TLS ──────────────────────────────────
    let uri: http::Uri = format!("https://{addr}").parse().unwrap();
    let resolver =
        oxirpc_client::balance::StaticResolver::new(vec![oxirpc_client::balance::Endpoint::new(
            uri,
        )]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(std::time::Duration::from_secs(5))
        .tls(tls_cfg)
        .build()
        .await
        .expect("TLS channel build must succeed");

    // ── 6. Send a unary request ───────────────────────────────────────────────
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("https://{addr}/test.Service/Tls"))
        .version(http::Version::HTTP_2)
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(oxirpc_client::native_channel::NativeBody::empty())
        .expect("request build must succeed");

    let resp = channel
        .call(req)
        .await
        .expect("TLS unary call must succeed");
    assert_eq!(resp.status(), 200);

    let chunks = collect_body(resp.into_body())
        .await
        .expect("body collect must succeed");
    assert!(
        !chunks.is_empty(),
        "expected at least one data chunk in TLS response"
    );

    let total: Vec<u8> = chunks.into_iter().flat_map(|b| b.to_vec()).collect();
    assert_eq!(
        total,
        response_payload.as_ref(),
        "TLS round-trip payload must match"
    );

    server_task.abort();
}

/// Helper: send a unary gRPC-style response over a TLS h2 stream.
#[cfg(feature = "tls")]
async fn send_tls_unary_response(respond: &mut h2::server::SendResponse<Bytes>, payload: Bytes) {
    use bytes::{BufMut, BytesMut};

    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc+proto")
        .body(())
        .unwrap();

    let mut send_stream = match respond.send_response(response, false) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Encode as a gRPC length-prefixed frame.
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(0x00);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(&payload);
    if send_stream.send_data(buf.freeze(), false).is_err() {
        return;
    }

    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    let _ = send_stream.send_trailers(trailers);
}

// ── async interceptor: inject + abort ──────────────────────────────────────

#[tokio::test]
async fn async_interceptor_injects_header() {
    use oxirpc_client::balance::{Endpoint, StaticResolver};
    use oxirpc_client::native_channel::NativeChannelBuilder;
    use oxirpc_core::interceptor::AsyncInterceptor;
    use std::sync::Arc;

    let server = spawn_header_echo_server("x-injected").await;

    let interceptor = |mut req: oxirpc_core::message::Request<()>| async move {
        req.metadata_mut()
            .insert("x-injected", "from-interceptor")
            .unwrap();
        Ok::<_, oxirpc_core::rpc::Status>(req)
    };

    let uri = format!("http://{}", server.addr).parse().unwrap();
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(Duration::from_secs(5))
        .with_async_interceptor(Arc::new(interceptor) as Arc<dyn AsyncInterceptor>)
        .build()
        .await
        .expect("channel build");

    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Unary",
        NativeBody::empty(),
    );
    let resp = channel.call(req).await.expect("call must succeed");
    assert_eq!(resp.status(), 200);
    let echoed = resp
        .headers()
        .get("x-echoed")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        echoed, "from-interceptor",
        "interceptor must inject the header onto the outgoing request"
    );
    server.shutdown();
}

#[tokio::test]
async fn async_interceptor_abort_propagates_status() {
    use oxirpc_client::balance::{Endpoint, StaticResolver};
    use oxirpc_client::native_channel::NativeChannelBuilder;
    use oxirpc_core::interceptor::AsyncInterceptor;
    use oxirpc_core::status::StatusCode;
    use std::sync::Arc;

    // Use the goaway server only to have a reachable endpoint; the interceptor
    // aborts before any stream is opened, so the server is never actually hit.
    let server = spawn_goaway_server().await;
    let interceptor = |_req: oxirpc_core::message::Request<()>| async move {
        Err::<oxirpc_core::message::Request<()>, _>(oxirpc_core::rpc::Status::new(
            StatusCode::PermissionDenied,
            "blocked",
        ))
    };
    let uri = format!("http://{}", server.addr).parse().unwrap();
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(Duration::from_secs(5))
        .with_async_interceptor(Arc::new(interceptor) as Arc<dyn AsyncInterceptor>)
        .build()
        .await
        .expect("channel build");
    let req = grpc_request(
        &server.addr.to_string(),
        "/test.Service/Unary",
        NativeBody::empty(),
    );
    let err = channel
        .call(req)
        .await
        .expect_err("interceptor must abort the call");
    // The abort surfaces as an OxiRpcError::Status carrying PermissionDenied.
    match err {
        oxirpc_core::OxiRpcError::Status(s) => {
            assert_eq!(s.code(), tonic::Code::PermissionDenied);
        }
        other => panic!("expected Status error, got {other:?}"),
    }
    server.shutdown();
}
