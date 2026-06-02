//! End-to-end TLS configuration tests for OxiRPC M2.
//!
//! Validates that `oxirpc::tls::{client_config, server_config}` produce valid
//! `rustls::ClientConfig` / `rustls::ServerConfig` objects with `h2` ALPN set,
//! using the Pure-Rust OxiTLS RustCrypto provider.
//!
//! Full tonic round-trip TLS wiring is deferred to M3 (tonic 0.14's TLS API
//! requires `tls-ring`/`tls-aws-lc` features which pull FFI crates onto normal
//! edges; see open question #5 in TODO.md).
//!
//! This entire test module is compiled only when the `tls` feature is enabled.
#![cfg(feature = "tls")]

use std::io::BufReader;

use rustls::RootCertStore;
use rustls_pemfile::certs;

/// Helper: generate a self-signed certificate for `localhost` using rcgen.
/// Returns `(cert_pem, key_pem)` as owned `String`.
fn localhost_cert() -> (String, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen::generate_simple_self_signed");
    (cert.cert.pem(), cert.signing_key.serialize_pem())
}

// ─── client_config tests ────────────────────────────────────────────────────

#[test]
fn client_config_empty_roots_succeeds() {
    let roots = RootCertStore::empty();
    let cfg = oxirpc::tls::client_config(roots).expect("client_config with empty roots");
    // Must have h2 ALPN
    assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
}

#[test]
fn client_config_has_h2_alpn() {
    let roots = RootCertStore::empty();
    let cfg = oxirpc::tls::client_config(roots).expect("client_config");
    assert!(
        cfg.alpn_protocols.contains(&b"h2".to_vec()),
        "expected h2 ALPN to be present, got {:?}",
        cfg.alpn_protocols
    );
}

#[test]
fn client_config_with_self_signed_root() {
    let (cert_pem, _key_pem) = localhost_cert();

    // Load the self-signed cert into a RootCertStore
    let mut roots = RootCertStore::empty();
    let cert_ders: Vec<_> = certs(&mut BufReader::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .expect("parse cert DERs");
    for der in cert_ders {
        roots.add(der).expect("add root");
    }

    let cfg = oxirpc::tls::client_config(roots).expect("client_config with self-signed root");
    assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
}

// ─── server_config tests ────────────────────────────────────────────────────

#[test]
fn server_config_has_h2_alpn_with_valid_cert() {
    let (cert_pem, key_pem) = localhost_cert();

    let cfg =
        oxirpc::tls::server_config(cert_pem.as_bytes(), key_pem.as_bytes()).expect("server_config");

    assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
}

#[test]
fn server_config_rejects_empty_cert() {
    let err = oxirpc::tls::server_config(b"", b"");
    assert!(err.is_err(), "expected error for empty cert/key, got Ok");
}

#[test]
fn server_config_rejects_missing_key() {
    let (cert_pem, _key_pem) = localhost_cert();
    // Pass cert PEM as both cert and key — the key field has no private key
    let err = oxirpc::tls::server_config(cert_pem.as_bytes(), cert_pem.as_bytes());
    assert!(
        err.is_err(),
        "expected error when no private key provided, got Ok"
    );
}

// ─── arc convenience wrappers ────────────────────────────────────────────────

#[test]
fn client_config_arc_returns_arc() {
    let roots = RootCertStore::empty();
    let arc_cfg = oxirpc::tls::client_config_arc(roots).expect("client_config_arc");
    assert_eq!(arc_cfg.alpn_protocols, vec![b"h2".to_vec()]);
}

#[test]
fn server_config_arc_returns_arc() {
    let (cert_pem, key_pem) = localhost_cert();
    let arc_cfg = oxirpc::tls::server_config_arc(cert_pem.as_bytes(), key_pem.as_bytes())
        .expect("server_config_arc");
    assert_eq!(arc_cfg.alpn_protocols, vec![b"h2".to_vec()]);
}

// ─── round-trip config construction ─────────────────────────────────────────

/// Validate that a server and client built from the same self-signed cert are
/// both constructed successfully, and both have h2 ALPN.
///
/// This is the closest M2 gets to an interop test; the actual TLS handshake
/// over a real socket is deferred to M3 (tonic TLS wiring).
#[test]
fn tls_roundtrip_config_construction_with_oxitls_provider() {
    let (cert_pem, key_pem) = localhost_cert();

    // Build server TLS config
    let server_cfg =
        oxirpc::tls::server_config(cert_pem.as_bytes(), key_pem.as_bytes()).expect("server_config");

    // Load the self-signed cert as the trusted CA for the client
    let mut roots = RootCertStore::empty();
    for der in certs(&mut BufReader::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .expect("cert DERs")
    {
        roots.add(der).expect("add root");
    }
    let client_cfg = oxirpc::tls::client_config(roots).expect("client_config");

    // Both configs must have h2 ALPN set
    assert_eq!(server_cfg.alpn_protocols, vec![b"h2".to_vec()]);
    assert_eq!(client_cfg.alpn_protocols, vec![b"h2".to_vec()]);
}
