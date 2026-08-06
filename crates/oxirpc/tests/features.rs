//! Feature-flag smoke tests for the `oxirpc` facade crate.
//!
//! Each test checks that the types/functions declared in a feature's doc
//! are actually accessible with that feature enabled.  The `core_types_*`
//! tests require only the default (empty) feature set.

// ─── default features (always compiled) ─────────────────────────────────────

#[test]
fn core_types_accessible() {
    let _code = oxirpc::Code::Ok;
    let _status = oxirpc::Status::ok("ok");
    let _error = oxirpc::OxiRpcError::Timeout;
}

#[test]
fn version_accessible() {
    let v = oxirpc::version();
    assert!(!v.is_empty(), "version() must return a non-empty string");
}

#[test]
fn prelude_exports_accessible() {
    use oxirpc::prelude::*;
    let _ok = StatusCode::Ok;
    let _v = version();
}

// ─── client feature ──────────────────────────────────────────────────────────

#[cfg(feature = "client")]
#[test]
fn client_builder_accessible() {
    let _ = oxirpc::ClientBuilder::new("http://localhost:50051");
}

// ─── server feature ──────────────────────────────────────────────────────────

#[cfg(feature = "server")]
#[test]
fn server_builder_accessible() {
    let _ = oxirpc::ServerBuilder::new();
}

// ─── reflect feature ─────────────────────────────────────────────────────────

#[cfg(feature = "reflect")]
#[test]
fn reflect_types_accessible() {
    let _ = oxirpc::reflect::ReflectionBuilder::new();
}

// ─── health feature ──────────────────────────────────────────────────────────

#[cfg(feature = "health")]
#[test]
fn health_types_accessible() {
    let _ = oxirpc::health::HealthBuilder::new();
}

// ─── web feature ─────────────────────────────────────────────────────────────

#[cfg(feature = "web")]
#[test]
fn web_grpc_layer_accessible() {
    let _ = oxirpc::web::grpc_web_layer();
}

// ─── tls feature ─────────────────────────────────────────────────────────────

#[cfg(feature = "tls")]
#[test]
fn tls_module_accessible() {
    // Verify the tls module is reachable; full construction is in tls_interop.rs.
    use rustls::RootCertStore;
    let roots = RootCertStore::empty();
    let cfg = oxirpc::tls::client_config(roots);
    assert!(
        cfg.is_ok(),
        "tls::client_config with empty roots should succeed"
    );
}

// ─── http3 feature ───────────────────────────────────────────────────────────

#[cfg(feature = "http3")]
#[test]
fn http3_module_accessible() {
    use rustls::RootCertStore;

    // The h3 TLS config helpers must be reachable and advertise the "h3" ALPN.
    let roots = RootCertStore::empty();
    let cfg = oxirpc::http3::client_config_h3(roots).expect("client_config_h3 must build");
    assert_eq!(
        cfg.alpn_protocols,
        vec![b"h3".to_vec()],
        "h3 client config must advertise the \"h3\" ALPN"
    );

    // The client channel builder types are reachable and enforce required fields.
    let err = oxirpc::http3::H3ChannelBuilder::new()
        .build()
        .expect_err("H3ChannelBuilder without addr/server_name/tls must fail");
    assert!(matches!(err, oxirpc::OxiRpcError::Build(_)), "got {err:?}");

    // Server entry points are reachable as items (signature reference only).
    let _serve = oxirpc::http3::serve_native_h3_with_service::<
        oxirpc_server::RegistryService,
        oxirpc_core::wire::NativeBody,
    >;
    let _bind = oxirpc::http3::bind_h3_endpoint;
}

// ─── compression feature ─────────────────────────────────────────────────────

#[cfg(feature = "compression")]
#[test]
fn compression_module_accessible() {
    use oxirpc::compression::OxiArcGzip;
    let data = b"hello gRPC compression";
    let compressed = OxiArcGzip::compress(data).expect("compress");
    let decompressed = OxiArcGzip::decompress(&compressed).expect("decompress");
    assert_eq!(decompressed.as_slice(), data.as_slice());
}
