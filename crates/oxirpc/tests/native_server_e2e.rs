//! End-to-end integration tests for the native HTTP/2 server transport.
//!
//! These tests prove that `NativeServiceRegistry` + `serve_native_registry_with_listener`
//! (and related helpers) correctly accepts gRPC connections and returns valid
//! gRPC responses — without going through tonic's transport layer in the
//! hot path.
//!
//! All tests use `127.0.0.1:0` (OS-assigned port) to avoid port conflicts.
//!
//! Requires the `native` and `health` cargo features.

use oxirpc_health::HealthBuilder;
use oxirpc_server::NativeServiceRegistry;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Bind an OS-assigned listener and return the `(listener, addr)` pair.
async fn bind_random() -> (tokio::net::TcpListener, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

/// Connect a tonic channel to `addr` (plaintext).
async fn connect_channel(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect channel")
}

/// Call `grpc.health.v1.Health/Check` with the given service name and
/// return the raw status integer from the response.
async fn health_check(channel: tonic::transport::Channel, service: impl Into<String>) -> i32 {
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: service.into(),
        })
        .await
        .expect("health check RPC")
        .into_inner();
    resp.status
}

// ---------------------------------------------------------------------------
// Test 1: native_server_unary_roundtrip
//
// Spin up a NativeHealthService on the native transport, connect via tonic
// Channel, call Check RPC, assert SERVING.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn native_server_unary_roundtrip() {
    // Build health service + handle.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    // Bind OS-assigned port.
    let (listener, addr) = bind_random().await;

    // Spawn the native server via NativeServiceRegistry (Phase 4.1).
    let server_handle = tokio::spawn(async move {
        oxirpc_server::ServerBuilder::new()
            .serve_native_registry_with_listener(
                listener,
                NativeServiceRegistry::new().add_service(native_svc),
            )
            .await
            .ok();
    });

    // Give the server a moment to start handling.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Connect and call.
    let channel = connect_channel(addr).await;
    let status = health_check(channel, "").await;

    // ServingStatus::Serving = 1 in the proto enum.
    assert_eq!(status, 1, "expected SERVING (1), got {status}");

    // Abort the server (no clean shutdown signal in this variant).
    server_handle.abort();
    server_handle.await.ok();
}

// ---------------------------------------------------------------------------
// Test 2: native_server_multi_service
//
// Register TWO distinct services on the same native server:
//   1. NativeHealthService   → path prefix /grpc.health.v1.Health/
//   2. NativeReflectionServiceV1 → path prefix /grpc.reflection.v1.ServerReflection/
//
// Both are registered via chained `add_service` calls on `NativeServiceRegistry`,
// exercising the native registry dispatch path (Phase 4.1).
//
// Assertions:
//   • Health Check succeeds (SERVING) → proves the Health route fires.
//   • Reflection ServerReflectionInfo with ListServices returns a response
//     without an unimplemented / not-found error → proves the Reflection
//     route fires through `Routes`.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn native_server_multi_service() {
    use oxirpc_reflect::ReflectionBuilder;
    use tokio_stream::iter as stream_iter;
    use tonic_reflection::pb::v1::{
        server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
        ServerReflectionRequest, FILE_DESCRIPTOR_SET,
    };

    // ── Service 1: health ──────────────────────────────────────────────────
    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("svc.Multi").await;

    // ── Service 2: reflection (v1, self-describing via its own FDS) ────────
    let reflection_svc = ReflectionBuilder::new()
        .register_file_descriptor_set(FILE_DESCRIPTOR_SET.to_vec())
        .build_native_v1();

    // ── Bind + start server with BOTH services via NativeServiceRegistry ──────
    let (listener, addr) = bind_random().await;

    let server_handle = tokio::spawn(async move {
        oxirpc_server::ServerBuilder::new()
            .serve_native_registry_with_listener(
                listener,
                NativeServiceRegistry::new()
                    .add_service(health_svc) // Route 1: /grpc.health.v1.Health/
                    .add_service(reflection_svc), // Route 2: /grpc.reflection.v1.ServerReflection/
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let channel = connect_channel(addr).await;

    // ── Assert 1: Health route fires ───────────────────────────────────────
    let status = health_check(channel.clone(), "svc.Multi").await;
    assert_eq!(status, 1, "svc.Multi should be SERVING (1), got {status}");

    // ── Assert 2: Reflection route fires ──────────────────────────────────
    let mut reflect_client = ServerReflectionClient::new(channel);

    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };

    let mut response_stream = reflect_client
        .server_reflection_info(stream_iter(vec![request]))
        .await
        .expect("server_reflection_info RPC must succeed")
        .into_inner();

    let first_reply = response_stream
        .message()
        .await
        .expect("stream recv must not error")
        .expect("stream must yield at least one response");

    // The reply must be a ListServicesResponse — not an error.
    match first_reply.message_response {
        Some(MessageResponse::ListServicesResponse(_)) => {
            // Expected: reflection route is live and answered correctly.
        }
        other => panic!("expected ListServicesResponse from reflection service, got: {other:?}"),
    }

    server_handle.abort();
    server_handle.await.ok();
}

// ---------------------------------------------------------------------------
// Test 3: native_server_graceful_shutdown
//
// Start a native server, trigger a shutdown signal, verify it exits cleanly
// within a reasonable timeout.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn native_server_graceful_shutdown() {
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    let (listener, addr) = bind_random().await;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        oxirpc_server::ServerBuilder::new()
            .serve_native_registry_with_listener_shutdown(
                listener,
                NativeServiceRegistry::new().add_service(native_svc),
                async move {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Verify it's up.
    let channel = connect_channel(addr).await;
    let status = health_check(channel, "").await;
    assert_eq!(status, 1, "server should be SERVING before shutdown");

    // Trigger shutdown.
    shutdown_tx.send(()).ok();

    // Wait for the server task to finish — it should exit cleanly.
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
    assert!(
        result.is_ok(),
        "server task should have exited within 2 s after shutdown signal"
    );
}

// ---------------------------------------------------------------------------
// Test 4: native_server_tls_roundtrip
//
// Same as test 1 but with rcgen-generated self-signed cert + TLS on both
// server and client.  Gated on the `tls` feature.
// ---------------------------------------------------------------------------

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn native_server_tls_roundtrip() {
    use oxirpc_client::tls_connector::PureRustTlsConnector;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;

    // 1. Generate self-signed cert.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen self-signed cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.signing_key.serialize_pem().into_bytes();

    // 2. Build ServerConfig via oxirpc_core::tls.
    let server_cfg = oxirpc_core::tls::server_config(&cert_pem, &key_pem).expect("server_config");

    // 3. Build ClientConfig that trusts the self-signed cert.
    let cert_der: Vec<u8> = {
        let mut rdr = std::io::BufReader::new(cert_pem.as_slice());
        let der = rustls_pemfile::certs(&mut rdr)
            .next()
            .expect("at least one cert")
            .expect("cert DER")
            .to_vec();
        der
    };
    let mut roots = RootCertStore::empty();
    roots
        .add(rustls_pki_types::CertificateDer::from(cert_der))
        .expect("add root cert");
    let client_cfg = oxirpc_core::tls::client_config(roots).expect("client_config");

    // 4. Build health service + handle.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    // 5. Bind OS-assigned port.
    let (listener, addr) = bind_random().await;

    // 6. Spawn the native TLS server via NativeServiceRegistry (Phase 4.1).
    let server_handle = tokio::spawn(async move {
        oxirpc_server::ServerBuilder::new()
            .tls(server_cfg)
            .serve_native_registry_with_listener(
                listener,
                NativeServiceRegistry::new().add_service(native_svc),
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // 7. Connect using PureRustTlsConnector.
    let server_name = ServerName::try_from("localhost").expect("server name");
    let connector = PureRustTlsConnector::new(client_cfg, server_name);
    let channel =
        tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{}", addr.port()))
            .expect("endpoint URI")
            .connect_with_connector(connector)
            .await
            .expect("TLS connect");

    // 8. Call Check RPC.
    let status = health_check(channel, "").await;
    assert_eq!(status, 1, "expected SERVING (1) over TLS, got {status}");

    server_handle.abort();
    server_handle.await.ok();
}
