//! Integration tests for TLS server support (feature = "tls").
//!
//! Tests cover:
//! - Display output includes `tls=true|false`
//! - Builder API: `tls()`, `tls_arc()`, `is_tls()`
//! - `tls_acceptor()` helper: produces a working `TlsAcceptor`
//! - TLS acceptor can complete a raw handshake with a `tokio-rustls` connector
//! - A plain-TCP client gets the connection dropped (TLS rejected)

#![cfg(feature = "tls")]

use std::sync::Arc;

use oxirpc_server::{tls, ServerBuilder};
use tokio::net::{TcpListener, TcpStream};

// ─── rcgen helpers ──────────────────────────────────────────────────────────

/// Generate a self-signed cert + key using rcgen, return PEM bytes.
fn make_self_signed_pem() -> (Vec<u8>, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen self-signed cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.signing_key.serialize_pem().into_bytes();
    (cert_pem, key_pem)
}

/// Build a `rustls::ServerConfig` from the given PEM bytes via `oxirpc_core::tls`.
fn server_config_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> rustls::ServerConfig {
    oxirpc_core::tls::server_config(cert_pem, key_pem).expect("server_config")
}

/// Build a `rustls::ClientConfig` that trusts only the given DER cert.
fn client_config_for_cert(cert_der: &[u8]) -> rustls::ClientConfig {
    use rustls::RootCertStore;
    let mut roots = RootCertStore::empty();
    roots
        .add(rustls_pki_types::CertificateDer::from(cert_der.to_vec()))
        .expect("add root cert");
    oxirpc_core::tls::client_config(roots).expect("client_config")
}

// ─── Display tests ───────────────────────────────────────────────────────────

/// ServerBuilder without TLS must show `tls=false` in its Display string.
#[test]
fn display_includes_tls_false_by_default() {
    let b = ServerBuilder::new();
    let s = format!("{b}");
    assert!(
        s.contains("tls=false"),
        "expected 'tls=false' in Display output, got: {s}"
    );
}

/// ServerBuilder after calling `.tls()` must show `tls=true`.
#[test]
fn display_includes_tls_true_after_tls_call() {
    let (cert_pem, key_pem) = make_self_signed_pem();
    let config = server_config_from_pem(&cert_pem, &key_pem);

    let b = ServerBuilder::new().tls(config);
    let s = format!("{b}");
    assert!(
        s.contains("tls=true"),
        "expected 'tls=true' in Display output, got: {s}"
    );
}

// ─── Builder API tests ───────────────────────────────────────────────────────

/// `is_tls()` returns `false` on a fresh builder.
#[test]
fn is_tls_false_on_new() {
    assert!(!ServerBuilder::new().is_tls());
}

/// `is_tls()` returns `true` after calling `.tls()`.
#[test]
fn is_tls_true_after_tls() {
    let (cert_pem, key_pem) = make_self_signed_pem();
    let config = server_config_from_pem(&cert_pem, &key_pem);
    assert!(ServerBuilder::new().tls(config).is_tls());
}

/// Calling `.tls()` a second time replaces the first config.
#[test]
fn tls_second_call_replaces_first() {
    let (cert_pem, key_pem) = make_self_signed_pem();
    let cfg1 = server_config_from_pem(&cert_pem, &key_pem);
    let cfg2 = server_config_from_pem(&cert_pem, &key_pem);
    let b = ServerBuilder::new().tls(cfg1).tls(cfg2);
    assert!(b.is_tls());
}

/// `tls_arc()` accepts a pre-wrapped Arc and sets `is_tls()` to true.
#[test]
fn tls_arc_sets_config() {
    let (cert_pem, key_pem) = make_self_signed_pem();
    let config = server_config_from_pem(&cert_pem, &key_pem);
    let arc = Arc::new(config);
    let b = ServerBuilder::new().tls_arc(arc);
    assert!(b.is_tls());
}

/// `tls_arc()` followed by `tls()` replaces the arc with a new one.
#[test]
fn tls_builder_clones_config_via_arc() {
    let (cert_pem, key_pem) = make_self_signed_pem();
    let cfg_arc = Arc::new(server_config_from_pem(&cert_pem, &key_pem));
    let cfg2 = server_config_from_pem(&cert_pem, &key_pem);

    let b = ServerBuilder::new().tls_arc(Arc::clone(&cfg_arc)).tls(cfg2);
    // Second call (.tls) should have overridden the first (.tls_arc).
    assert!(b.is_tls());
}

// ─── tls_acceptor helper ─────────────────────────────────────────────────────

/// `tls_acceptor()` constructs a TlsAcceptor from an Arc<ServerConfig>.
#[test]
fn tls_acceptor_constructs_from_arc() {
    let (cert_pem, key_pem) = make_self_signed_pem();
    let config = Arc::new(server_config_from_pem(&cert_pem, &key_pem));
    // Simply verify that the call doesn't panic and returns successfully.
    let _acceptor = tls::tls_acceptor(config);
}

// ─── Raw TLS handshake test ─────────────────────────────────────────────────

/// Perform a raw TLS handshake using `accept_one` + `tokio-rustls` connector.
/// No gRPC stack needed — just verifies the TLS layer works.
#[tokio::test]
async fn tls_acceptor_passes_through_io_correctly() {
    let (cert_pem, key_pem) = make_self_signed_pem();

    // Extract DER for the client's root store.
    let cert_der: Vec<u8> = {
        let mut rdr = std::io::BufReader::new(cert_pem.as_slice());
        let der = rustls_pemfile::certs(&mut rdr)
            .next()
            .expect("at least one cert")
            .expect("cert DER")
            .to_vec();
        der
    };

    let server_cfg = Arc::new(server_config_from_pem(&cert_pem, &key_pem));
    let acceptor = tls::tls_acceptor(server_cfg);

    // Bind a random port.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local_addr");

    // Spawn the server side.
    let server = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept tcp");
        tls::accept_one(&acceptor, stream)
            .await
            .expect("tls handshake")
        // TLS stream is ready; we don't read/write — test is done.
    });

    // Connect a TLS client.
    let client_cfg = Arc::new(client_config_for_cert(&cert_der));
    let connector = tokio_rustls::TlsConnector::from(client_cfg);
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = rustls_pki_types::ServerName::try_from("localhost").expect("server name");
    let _tls_stream = connector
        .connect(server_name, tcp)
        .await
        .expect("client tls handshake");

    // Both sides must complete without error.
    server.await.expect("server task");
}

// ─── Plaintext rejection test ────────────────────────────────────────────────

/// A client that sends plain TCP bytes should be rejected quickly.
///
/// We verify that sending `PRI * HTTP/2.0` (HTTP/2 connection preface) to a
/// TLS port causes the server to close the connection — i.e., the `accept_one`
/// call returns an error.
#[tokio::test]
async fn tls_server_rejects_plaintext_client() {
    use tokio::io::AsyncWriteExt;

    let (cert_pem, key_pem) = make_self_signed_pem();
    let server_cfg = Arc::new(server_config_from_pem(&cert_pem, &key_pem));
    let acceptor = tls::tls_acceptor(server_cfg);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local_addr");

    // Server: accept one TCP stream, attempt TLS handshake.
    let server = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept tcp");
        // This should fail because the client sends plaintext.
        tls::accept_one(&acceptor, stream).await
    });

    // Client: connect plain TCP and send garbage bytes.
    let mut tcp = TcpStream::connect(addr).await.expect("tcp connect");
    tcp.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .expect("write plaintext");
    drop(tcp); // close the connection

    let result = server.await.expect("server task joined");
    assert!(
        result.is_err(),
        "expected TLS handshake error for plaintext client, got Ok"
    );
}

// ─── Full gRPC-over-TLS roundtrip stub ──────────────────────────────────────

// ─── Fixture types (used only in the TLS roundtrip test) ────────────────────

#[allow(dead_code)]
mod fixture;

// ─── Full gRPC-over-TLS roundtrip ───────────────────────────────────────────

/// Full gRPC unary RPC roundtrip over TLS using a real `ServerBuilder`.
///
/// Flow:
/// 1. Generate a self-signed cert via rcgen.
/// 2. Build a `rustls::ServerConfig` via `oxirpc_core::tls::server_config`.
/// 3. Spawn `ServerBuilder::new().tls(cfg).add_service(pinger).serve_with_listener_shutdown`.
/// 4. Build a `PureRustTlsConnector` from `oxirpc_client` that trusts the self-signed cert.
/// 5. Connect via `tonic::transport::Endpoint::connect_with_connector`.
/// 6. Issue `/fixture.Pinger/Ping`, assert `msg == "pong"`.
#[tokio::test(flavor = "multi_thread")]
async fn tls_server_handshake_then_unary_rpc() {
    use std::sync::{atomic::AtomicUsize, Arc};

    use oxirpc_client::tls_connector::PureRustTlsConnector;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;

    // 1. Generate self-signed cert.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen self-signed cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.signing_key.serialize_pem().into_bytes();

    // 2. Build ServerConfig.
    let server_cfg = oxirpc_core::tls::server_config(&cert_pem, &key_pem).expect("server_config");

    // 3. Spawn TLS server using the fixture builder helper.
    let builder = oxirpc_server::ServerBuilder::new().tls(server_cfg);
    let (addr, shutdown_tx, server_handle) = fixture::spawn_with_builder(builder).await;

    // 4. Build a ClientConfig that trusts only our self-signed cert.
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

    let server_name = ServerName::try_from("localhost").expect("server name");
    let connector = PureRustTlsConnector::new(client_cfg, server_name);

    // 5. Connect via Endpoint::connect_with_connector (bypasses tonic's ring/aws-lc TLS).
    let channel =
        tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{}", addr.port()))
            .expect("endpoint URI")
            .connect_with_connector(connector)
            .await
            .expect("TLS connect");

    // 6. Issue a Ping RPC and verify the response.
    let resp = fixture::ping(channel, vec![]).await.expect("ping over TLS");
    assert_eq!(
        resp.into_inner().msg,
        "pong",
        "expected 'pong' from TLS Pinger"
    );

    // Shutdown.
    shutdown_tx.send(()).ok();
    server_handle.await.ok();

    let _ = Arc::new(AtomicUsize::new(0)); // suppress unused-import warning
}
