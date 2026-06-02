//! Tests for RpcMetrics counters and Pure-Rust TLS connector construction.

// ─── RpcMetrics ───────────────────────────────────────────────────────────────

#[test]
fn rpc_metrics_starts_at_zero() {
    use oxirpc_client::RpcMetrics;
    let m = RpcMetrics::new();
    assert_eq!(m.started(), 0);
    assert_eq!(m.completed(), 0);
    assert_eq!(m.failed(), 0);
}

#[test]
fn rpc_metrics_counting() {
    use oxirpc_client::RpcMetrics;
    let m = RpcMetrics::new();
    m.record_started();
    m.record_started();
    m.record_completed();
    assert_eq!(m.started(), 2);
    assert_eq!(m.completed(), 1);
    assert_eq!(m.failed(), 0);
}

#[test]
fn rpc_metrics_failed_counting() {
    use oxirpc_client::RpcMetrics;
    let m = RpcMetrics::new();
    m.record_started();
    m.record_failed();
    assert_eq!(m.started(), 1);
    assert_eq!(m.completed(), 0);
    assert_eq!(m.failed(), 1);
}

#[test]
fn rpc_metrics_clone_shared() {
    use oxirpc_client::RpcMetrics;
    let m = RpcMetrics::new();
    let m2 = m.clone();
    m.record_started();
    // Clones share the same Arc-backed counters.
    assert_eq!(m2.started(), 1, "cloned metrics should share arc counters");
}

#[test]
fn rpc_metrics_all_counters_independent() {
    use oxirpc_client::RpcMetrics;
    let m = RpcMetrics::new();
    for _ in 0..5 {
        m.record_started();
    }
    for _ in 0..3 {
        m.record_completed();
    }
    for _ in 0..2 {
        m.record_failed();
    }
    assert_eq!(m.started(), 5);
    assert_eq!(m.completed(), 3);
    assert_eq!(m.failed(), 2);
}

#[test]
fn rpc_metrics_default_is_zero() {
    use oxirpc_client::RpcMetrics;
    let m = RpcMetrics::default();
    assert_eq!(m.started(), 0);
    assert_eq!(m.completed(), 0);
    assert_eq!(m.failed(), 0);
}

// ─── ClientBuilder::with_metrics ─────────────────────────────────────────────

#[test]
fn client_builder_with_metrics_compiles() {
    use oxirpc_client::{ClientBuilder, RpcMetrics};
    let m = RpcMetrics::new();
    let _builder = ClientBuilder::new("http://127.0.0.1:50051").with_metrics(m);
}

// ─── PureRustTlsConnector ─────────────────────────────────────────────────────

#[cfg(feature = "tls")]
#[test]
fn pure_rust_tls_connector_constructs() {
    use oxirpc_client::tls_connector::PureRustTlsConnector;
    use oxirpc_core::tls::client_config;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;

    let config = client_config(RootCertStore::empty()).expect("client_config with empty roots");
    let server_name = ServerName::try_from("example.com").expect("valid DNS name");
    // Verify it constructs without panicking.
    let _connector = PureRustTlsConnector::new(config, server_name);
}

#[cfg(feature = "tls")]
#[test]
fn pure_rust_tls_connector_is_clone() {
    use oxirpc_client::tls_connector::PureRustTlsConnector;
    use oxirpc_core::tls::client_config;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;

    let config = client_config(RootCertStore::empty()).expect("client_config with empty roots");
    let server_name = ServerName::try_from("grpc.example.com").expect("valid DNS name");
    let connector = PureRustTlsConnector::new(config, server_name);
    let _cloned = connector.clone();
}

#[cfg(feature = "tls")]
#[test]
fn client_builder_tls_method_compiles() {
    use oxirpc_client::ClientBuilder;
    use oxirpc_core::tls::client_config;
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;

    let config = client_config(RootCertStore::empty()).expect("client_config with empty roots");
    let server_name = ServerName::try_from("example.com").expect("valid DNS name");
    // Verify the builder accepts a TLS config.
    let _builder = ClientBuilder::new("https://example.com:443").tls(config, server_name);
}
