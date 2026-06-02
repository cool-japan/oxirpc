//! TLS configuration helpers for OxiRPC using OxiTLS (Pure Rust, no ring, no FFI).
//!
//! All configs are constructed via `builder_with_provider(oxitls::pure_provider())`,
//! which injects the RustCrypto-backed provider per-config. This module never calls
//! `CryptoProvider::install_default()`.
//!
//! # Note on tonic integration
//!
//! Tonic 0.14's `ServerTlsConfig`/`ClientTlsConfig` types are only compiled when
//! the `tls-ring` or `tls-aws-lc` feature is enabled — both of which pull FFI crates
//! onto normal dependency edges, violating OxiRPC's Pure-Rust policy. Full tonic
//! round-trip TLS wiring is therefore deferred to M3 (see open question #5 in TODO.md).
//!
//! In M2, this module delivers:
//!
//! - `client_config` — pure-rustls `ClientConfig` with `h2` ALPN, ready for use
//!   with any `tokio-rustls`-based transport.
//! - `server_config` — pure-rustls `ServerConfig` with `h2` ALPN, accepting PEM
//!   cert + key bytes.
//!
//! # Example
//!
//! ```rust,no_run
//! use rustls::RootCertStore;
//! use oxirpc_core::tls::{client_config, server_config};
//!
//! # fn main() -> Result<(), oxirpc_core::OxiRpcError> {
//! // Client: trust an explicit CA cert
//! let roots = RootCertStore::empty();
//! let client_cfg = client_config(roots)?;
//! assert_eq!(client_cfg.alpn_protocols, vec![b"h2".to_vec()]);
//!
//! // Server: load PEM cert + key
//! // let server_cfg = server_config(cert_pem, key_pem)?;
//! // assert_eq!(server_cfg.alpn_protocols, vec![b"h2".to_vec()]);
//! # Ok(())
//! # }
//! ```

use std::io::BufReader;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::OxiRpcError;

/// ALPN protocol identifier for HTTP/2 (gRPC runs over h2).
const ALPN_H2: &[u8] = b"h2";

/// Build a pure-Rust TLS 1.2/1.3 client config using the OxiTLS RustCrypto provider.
///
/// The returned config has ALPN set to `["h2"]` for gRPC. The provider is injected
/// per-config via `builder_with_provider`; `CryptoProvider::install_default()` is
/// never called.
///
/// `roots` is the [`RootCertStore`] used to verify the server certificate. For
/// integration tests with self-signed certs, populate it with the test cert's DER.
/// For production use, populate it from `webpki-roots` or the platform CA store.
///
/// # Errors
///
/// Returns [`OxiRpcError::Tls`] if protocol version configuration fails.
pub fn client_config(roots: RootCertStore) -> Result<ClientConfig, OxiRpcError> {
    let provider = oxitls::pure_provider();

    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| OxiRpcError::Tls(format!("protocol versions: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    config.alpn_protocols = vec![ALPN_H2.to_vec()];
    Ok(config)
}

/// Build a pure-Rust TLS 1.2/1.3 server config from PEM-encoded cert and key.
///
/// The returned config has ALPN set to `["h2"]` for gRPC. The provider is injected
/// per-config via `builder_with_provider`; `CryptoProvider::install_default()` is
/// never called.
///
/// Both `cert_pem` and `key_pem` should be PEM-encoded byte slices. `cert_pem` may
/// contain a full chain (leaf first). `key_pem` must contain exactly one private key
/// in PKCS#8 or SEC1 (EC) format.
///
/// # Errors
///
/// Returns [`OxiRpcError::Tls`] if PEM parsing fails, no private key is found, or
/// `with_single_cert` rejects the cert/key pair.
pub fn server_config(cert_pem: &[u8], key_pem: &[u8]) -> Result<ServerConfig, OxiRpcError> {
    let cert_chain: Vec<CertificateDer<'static>> = certs(&mut BufReader::new(cert_pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| OxiRpcError::Tls(format!("cert PEM parse: {e}")))?;

    let key: PrivateKeyDer<'static> = private_key(&mut BufReader::new(key_pem))
        .map_err(|e| OxiRpcError::Tls(format!("key PEM parse: {e}")))?
        .ok_or_else(|| OxiRpcError::Tls("no private key found in PEM input".to_string()))?;

    let provider = oxitls::pure_provider();

    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| OxiRpcError::Tls(format!("protocol versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| OxiRpcError::Tls(format!("single cert: {e}")))?;

    config.alpn_protocols = vec![ALPN_H2.to_vec()];
    Ok(config)
}

/// Convenience: build a [`std::sync::Arc`]-wrapped client config.
///
/// Useful when passing the config to `tokio-rustls`'s `TlsConnector::from(Arc<ClientConfig>)`.
pub fn client_config_arc(roots: RootCertStore) -> Result<Arc<ClientConfig>, OxiRpcError> {
    client_config(roots).map(Arc::new)
}

/// Convenience: build a [`std::sync::Arc`]-wrapped server config.
///
/// Useful when passing the config to `tokio-rustls`'s `TlsAcceptor::from(Arc<ServerConfig>)`.
pub fn server_config_arc(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<Arc<ServerConfig>, OxiRpcError> {
    server_config(cert_pem, key_pem).map(Arc::new)
}
