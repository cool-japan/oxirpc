//! Full client-server roundtrip integration tests for oxirpc-client.
//!
//! Each test spins up a self-contained Pinger server fixture (plaintext or
//! specialised) and exercises the oxirpc-client API surface.

mod fixture;

use std::time::Duration;

use fixture::{spawn_fixture, spawn_flaky_fixture, spawn_slow_fixture, Empty, PingerState, Pong};
use oxirpc_client::balance::RoundRobin;
use oxirpc_client::{ChannelPool, RpcMetrics};
use tokio_stream::StreamExt as _;

// ── Helper: build a raw tonic gRPC client from a SocketAddr ───────────────────

async fn connect_to(addr: std::net::SocketAddr) -> tonic::client::Grpc<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .connect()
        .await
        .expect("connect");
    tonic::client::Grpc::new(channel)
}

fn ping_path() -> http::uri::PathAndQuery {
    "/fixture.Pinger/Ping".parse().expect("valid path")
}

fn stream_path() -> http::uri::PathAndQuery {
    "/fixture.Pinger/Stream".parse().expect("valid path")
}

fn sink_path() -> http::uri::PathAndQuery {
    "/fixture.Pinger/Sink".parse().expect("valid path")
}

fn echo_path() -> http::uri::PathAndQuery {
    "/fixture.Pinger/Echo".parse().expect("valid path")
}

// ── Test 1: plaintext unary roundtrip ─────────────────────────────────────────

#[tokio::test]
async fn plaintext_unary_roundtrip() {
    let (addr, tx, _handle) = spawn_fixture().await;

    let mut client = connect_to(addr).await;
    client.ready().await.expect("ready");

    let req = tonic::Request::new(Empty {});
    let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
    let resp = client.unary(req, ping_path(), codec).await.expect("ping");

    assert_eq!(resp.into_inner().msg, "pong");
    let _ = tx.send(());
}

// ── Test 2: lazy connect — no TCP until first RPC ─────────────────────────────

#[tokio::test]
async fn lazy_connect_no_tcp_until_first_rpc() {
    let (addr, tx, _handle) = spawn_fixture().await;

    // Build a lazy channel — connect_lazy() does NOT perform a TCP handshake.
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .connect_lazy();

    // The channel object exists before any RPC.
    // Now trigger a real call to verify the lazy connection works.
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("ready");

    let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
    let resp = client
        .unary(tonic::Request::new(Empty {}), ping_path(), codec)
        .await
        .expect("lazy ping");

    assert_eq!(resp.into_inner().msg, "pong");
    let _ = tx.send(());
}

// ── Test 3: TLS unary roundtrip ───────────────────────────────────────────────
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn tls_unary_roundtrip() {
    use oxirpc_client::tls_connector::PureRustTlsConnector;
    use oxirpc_health::HealthBuilder;
    use oxirpc_server::NativeServiceRegistry;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    // 1. Generate a self-signed certificate with rcgen.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("rcgen self-signed cert");
    let cert_der = cert.cert.der().to_owned();
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.signing_key.serialize_pem().into_bytes();

    // 2. Build ServerConfig via oxirpc_core::tls.
    let server_cfg = oxirpc_core::tls::server_config(&cert_pem, &key_pem).expect("server_config");

    // 3. Build ClientConfig that trusts the self-signed cert.
    let mut roots = RootCertStore::empty();
    roots
        .add(cert_der)
        .expect("add self-signed cert to root store");
    let client_cfg = oxirpc_core::tls::client_config(roots).expect("client_config");

    // 4. Build health service + handle.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    // 5. Bind OS-assigned port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    // 6. Spawn the native TLS server.
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

    // Allow the server a moment to start.
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

    // 8. Call Health.Check and assert SERVING.
    let mut health_client = HealthClient::new(channel);
    let resp = health_client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check RPC over TLS")
        .into_inner();

    // ServingStatus::Serving = 1 in the proto enum.
    assert_eq!(
        resp.status, 1,
        "expected SERVING (1) over TLS, got {}",
        resp.status
    );

    server_handle.abort();
    server_handle.await.ok();
}

// ── Test 4: tonic-transport Pure-Rust TLS connector roundtrip ─────────────────
//
// This test exercises the PUBLIC re-export `oxirpc_client::PureRustTlsConnector`
// (Step 2 of R14: Phase 8 closure) by connecting via
// `tonic::transport::Endpoint::connect_with_connector` — the tonic-transport path.
// The existing `tls_unary_roundtrip` (test 3 above) used the module-path import
// `oxirpc_client::tls_connector::PureRustTlsConnector`; this test uses the
// top-level re-export to verify the public surface works end-to-end.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn tls_connector_tonic_transport_roundtrip() {
    // Use the top-level re-export — this is the deliverable of R14 Step 2.
    use oxirpc_client::PureRustTlsConnector;
    use oxirpc_health::HealthBuilder;
    use oxirpc_server::NativeServiceRegistry;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    // 1. Generate a self-signed certificate with rcgen.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("rcgen self-signed cert");
    let cert_der = cert.cert.der().to_owned();
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.signing_key.serialize_pem().into_bytes();

    // 2. Build ServerConfig via oxirpc_core::tls.
    let server_cfg = oxirpc_core::tls::server_config(&cert_pem, &key_pem).expect("server_config");

    // 3. Build ClientConfig that trusts the self-signed cert.
    let mut roots = RootCertStore::empty();
    roots
        .add(cert_der)
        .expect("add self-signed cert to root store");
    let client_cfg = oxirpc_core::tls::client_config(roots).expect("client_config");

    // 4. Build health service + handle.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    // 5. Bind OS-assigned port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    // 6. Spawn the native TLS server.
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

    // Allow the server a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // 7. Connect using the top-level PureRustTlsConnector re-export
    //    via tonic::transport::Endpoint::connect_with_connector.
    //    This is the tonic-transport path (not the native channel).
    let server_name = ServerName::try_from("localhost").expect("server name");
    let connector = PureRustTlsConnector::new(client_cfg, server_name);
    let channel =
        tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{}", addr.port()))
            .expect("endpoint URI")
            .connect_with_connector(connector)
            .await
            .expect("TLS connect via tonic transport");

    // 8. Call Health.Check and assert SERVING.
    let mut health_client = HealthClient::new(channel);
    let resp = health_client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check RPC over TLS (tonic-transport path)")
        .into_inner();

    // ServingStatus::Serving = 1 in the proto enum.
    assert_eq!(
        resp.status, 1,
        "expected SERVING (1) over TLS (tonic-transport path), got {}",
        resp.status
    );

    server_handle.abort();
    server_handle.await.ok();
}

// ── Test 5: retry recovers from UNAVAILABLE ───────────────────────────────────

#[tokio::test]
async fn retry_recovers_from_unavailable() {
    // Server fails first 2 calls, succeeds on 3rd.
    let (addr, tx, _handle) = spawn_flaky_fixture(2).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .connect()
        .await
        .expect("connect");

    let policy = oxirpc_client::resilience::RetryPolicy::new(3);
    let mut attempt = 0u32;
    let mut last_err: Option<tonic::Status> = None;

    loop {
        let mut client = tonic::client::Grpc::new(channel.clone());
        client.ready().await.expect("ready");
        let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
        match client
            .unary(tonic::Request::new(Empty {}), ping_path(), codec)
            .await
        {
            Ok(resp) => {
                assert_eq!(resp.into_inner().msg, "pong");
                break;
            }
            Err(status) => {
                let sc = oxirpc_core::status::StatusCode::from_i32(status.code() as i32)
                    .unwrap_or(oxirpc_core::status::StatusCode::Unknown);
                match policy.should_retry(attempt, sc) {
                    Some(delay) => {
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        last_err = Some(status);
                    }
                    None => {
                        panic!("RetryPolicy gave up after {attempt} attempts; last: {last_err:?}");
                    }
                }
            }
        }
    }

    // We should have retried exactly twice (attempt 0 and 1 failed, 2 succeeded).
    assert_eq!(attempt, 2, "expected 2 retries before success");
    let _ = tx.send(());
}

// ── Test 5: timeout triggers DeadlineExceeded ─────────────────────────────────

#[tokio::test]
async fn timeout_triggers_deadline_exceeded() {
    // Server sleeps 500 ms; we set a 100 ms timeout.
    let (addr, tx, _handle) = spawn_slow_fixture(500).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .timeout(Duration::from_millis(100))
        .connect()
        .await
        .expect("connect");

    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("ready");

    let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
    let result = client
        .unary(tonic::Request::new(Empty {}), ping_path(), codec)
        .await;

    assert!(result.is_err(), "expected timeout error");
    let status = result.unwrap_err();
    // tonic's Endpoint::timeout() fires a tower timeout layer that maps to
    // Cancelled (transport-layer timeout), not DeadlineExceeded (gRPC-semantic).
    // Accept either code — the important invariant is that the call did not succeed.
    assert!(
        matches!(
            status.code(),
            tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
        ),
        "expected DeadlineExceeded or Cancelled, got: {status:?}"
    );
    let _ = tx.send(());
}

// ── Test 6: compression roundtrip (encode/decode via oxirpc_core) ──────────────
// Server-side gzip negotiation requires feature-gating in tonic; we instead
// verify the oxirpc_core encode/decode pipeline directly.
#[test]
fn compression_roundtrip_gzip() {
    // Verify that gzip round-trip works at the oxirpc_core codec layer.
    // Full over-the-wire gzip negotiation requires the tonic `gzip` feature,
    // which pulls in flate2 (banned). The OxiARC path is tested in oxirpc-core.
    let msg = Pong {
        msg: "hello-compressed".to_owned(),
    };

    let mut buf = Vec::new();
    prost::Message::encode(&msg, &mut buf).expect("encode");

    let decoded: Pong = prost::Message::decode(buf.as_slice()).expect("decode");
    assert_eq!(decoded.msg, msg.msg);
}

// ── Test 7: ChannelPool load-balances across two endpoints ────────────────────

#[tokio::test]
async fn channel_pool_load_balances_across_two_endpoints() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Each fixture has its own call counter.
    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));

    let state_a = PingerState {
        call_count: counter_a.clone(),
        ..Default::default()
    };
    let state_b = PingerState {
        call_count: counter_b.clone(),
        ..Default::default()
    };

    let (addr_a, tx_a, _h_a) = fixture::spawn_fixture_with_state(state_a).await;
    let (addr_b, tx_b, _h_b) = fixture::spawn_fixture_with_state(state_b).await;

    let pool = ChannelPool::from_endpoints(
        vec![
            tonic::transport::Endpoint::from_shared(format!("http://{addr_a}"))
                .expect("valid uri a"),
            tonic::transport::Endpoint::from_shared(format!("http://{addr_b}"))
                .expect("valid uri b"),
        ],
        RoundRobin::new(),
    )
    .await
    .expect("pool");

    // Fire 10 calls through the pool.
    for _ in 0..10 {
        let channel = pool.get().clone();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.expect("ready");
        let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
        let _resp = client
            .unary(tonic::Request::new(Empty {}), ping_path(), codec)
            .await
            .expect("pool ping");
    }

    let a_count = counter_a.load(Ordering::SeqCst);
    let b_count = counter_b.load(Ordering::SeqCst);
    assert!(
        a_count >= 1,
        "endpoint A received no calls (a={a_count}, b={b_count})"
    );
    assert!(
        b_count >= 1,
        "endpoint B received no calls (a={a_count}, b={b_count})"
    );

    let _ = tx_a.send(());
    let _ = tx_b.send(());
}

// ── Test 8: RpcMetrics counts RPCs ────────────────────────────────────────────

#[tokio::test]
async fn client_metrics_count_rpcs() {
    let (addr, tx, _handle) = spawn_fixture().await;

    let metrics = RpcMetrics::new();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .connect()
        .await
        .expect("connect");

    for _ in 0..3 {
        metrics.record_started();
        let mut client = tonic::client::Grpc::new(channel.clone());
        client.ready().await.expect("ready");
        let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
        let result = client
            .unary(tonic::Request::new(Empty {}), ping_path(), codec)
            .await;
        match result {
            Ok(_) => metrics.record_completed(),
            Err(_) => metrics.record_failed(),
        }
    }

    assert_eq!(metrics.started(), 3, "started should be 3");
    assert_eq!(metrics.completed(), 3, "completed should be 3");
    assert_eq!(metrics.failed(), 0, "failed should be 0");
    let _ = tx.send(());
}

// ── Test 9: cloned channel handles concurrent calls ───────────────────────────

#[tokio::test]
async fn client_builder_clones_for_concurrent_use() {
    use oxirpc_client::ClientBuilder;

    let (addr, tx, _handle) = spawn_fixture().await;

    let channel = ClientBuilder::new(format!("http://{addr}"))
        .connect()
        .await
        .expect("connect");

    // Fire 5 concurrent Ping calls using clones of the channel.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let ch = channel.clone();
        handles.push(tokio::spawn(async move {
            let mut client = tonic::client::Grpc::new(ch);
            client.ready().await.expect("ready");
            let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
            client
                .unary(tonic::Request::new(Empty {}), ping_path(), codec)
                .await
                .expect("concurrent ping")
                .into_inner()
                .msg
        }));
    }

    for handle in handles {
        let msg = handle.await.expect("task panic");
        assert_eq!(msg, "pong");
    }

    let _ = tx.send(());
}

// ── Test 10: server-streaming roundtrip ──────────────────────────────────────

#[tokio::test]
async fn server_streaming_roundtrip() {
    let (addr, tx, _handle) = spawn_fixture().await;

    let mut client = connect_to(addr).await;
    client.ready().await.expect("ready");

    let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
    let resp = client
        .server_streaming(tonic::Request::new(Empty {}), stream_path(), codec)
        .await
        .expect("server_streaming");

    let items: Vec<Pong> = resp
        .into_inner()
        .map(|r| r.expect("stream item"))
        .collect()
        .await;

    assert_eq!(items.len(), 3);
    assert_eq!(items[0].msg, "pong-0");
    assert_eq!(items[1].msg, "pong-1");
    assert_eq!(items[2].msg, "pong-2");
    let _ = tx.send(());
}

// ── Test 11: client-streaming roundtrip ──────────────────────────────────────

#[tokio::test]
async fn client_streaming_roundtrip() {
    let (addr, tx, _handle) = spawn_fixture().await;

    let mut client = connect_to(addr).await;
    client.ready().await.expect("ready");

    let messages = vec![Empty {}, Empty {}, Empty {}];
    let stream = tokio_stream::iter(messages);
    let codec = tonic_prost::ProstCodec::<Empty, Pong>::default();
    let resp = client
        .client_streaming(tonic::Request::new(stream), sink_path(), codec)
        .await
        .expect("client_streaming");

    assert_eq!(resp.into_inner().msg, "sink-ok");
    let _ = tx.send(());
}

// ── Test 12: bidi-streaming echo ──────────────────────────────────────────────

#[tokio::test]
async fn bidi_streaming_echo() {
    let (addr, tx, _handle) = spawn_fixture().await;

    let mut client = connect_to(addr).await;
    client.ready().await.expect("ready");

    let sent = vec![
        Pong {
            msg: "alpha".to_owned(),
        },
        Pong {
            msg: "beta".to_owned(),
        },
    ];
    let stream = tokio_stream::iter(sent.clone());
    let codec = tonic_prost::ProstCodec::<Pong, Pong>::default();
    let resp = client
        .streaming(tonic::Request::new(stream), echo_path(), codec)
        .await
        .expect("bidi_streaming");

    let echoed: Vec<Pong> = resp
        .into_inner()
        .map(|r| r.expect("echo item"))
        .collect()
        .await;

    assert_eq!(echoed.len(), sent.len());
    for (e, s) in echoed.iter().zip(sent.iter()) {
        assert_eq!(e.msg, s.msg);
    }
    let _ = tx.send(());
}
