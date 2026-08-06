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

/// ALPN protocol identifier for HTTP/3 (gRPC-over-QUIC runs over h3).
#[cfg(feature = "http3")]
const ALPN_H3: &[u8] = b"h3";

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

// ─────────────────────────────────────────────────────────────────────────────
// HTTP/3 (QUIC) TLS configs — feature `http3`
// ─────────────────────────────────────────────────────────────────────────────

/// Build a pure-Rust TLS 1.3 **QUIC** client config for HTTP/3 (ALPN `"h3"`).
///
/// This differs from [`client_config`] in three QUIC-mandated ways:
///
/// 1. **Provider** — the crypto provider is
///    [`oxiquic_crypto::quic_crypto_provider`], whose cipher suites carry the
///    `quic: Some(..)` key schedule needed to derive QUIC packet / header
///    protection keys. The generic [`oxitls::pure_provider`] used by
///    [`client_config`] has `quic: None` and would **fail** the QUIC handshake.
/// 2. **Protocol version** — pinned to TLS 1.3 only (`rustls::version::TLS13`).
///    QUIC is TLS-1.3-only (RFC 9001 §4.2); advertising TLS 1.2 is invalid.
/// 3. **ALPN** — set to `["h3"]` (RFC 9114 §3.3) rather than `["h2"]`.
///
/// `roots` is the [`RootCertStore`] used to verify the server certificate.
///
/// # Errors
///
/// Returns [`OxiRpcError::Tls`] if the TLS 1.3 protocol-version configuration is
/// rejected by rustls.
#[cfg(feature = "http3")]
pub fn client_config_h3(roots: RootCertStore) -> Result<ClientConfig, OxiRpcError> {
    let provider = Arc::new(oxiquic_crypto::quic_crypto_provider());

    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| OxiRpcError::Tls(format!("h3 protocol versions: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    config.alpn_protocols = vec![ALPN_H3.to_vec()];
    Ok(config)
}

/// Build a pure-Rust TLS 1.3 **QUIC** server config for HTTP/3 (ALPN `"h3"`).
///
/// Mirrors [`server_config`] but with the same three QUIC-mandated differences
/// documented on [`client_config_h3`]: the [`oxiquic_crypto::quic_crypto_provider`]
/// provider, a TLS-1.3-only version list, and `["h3"]` ALPN.
///
/// Both `cert_pem` and `key_pem` are PEM-encoded byte slices. `cert_pem` may
/// contain a full chain (leaf first). `key_pem` must contain exactly one private
/// key in PKCS#8 or SEC1 (EC) form.
///
/// # Errors
///
/// Returns [`OxiRpcError::Tls`] if PEM parsing fails, no private key is found,
/// the TLS 1.3 version list is rejected, or `with_single_cert` rejects the pair.
#[cfg(feature = "http3")]
pub fn server_config_h3(cert_pem: &[u8], key_pem: &[u8]) -> Result<ServerConfig, OxiRpcError> {
    let cert_chain: Vec<CertificateDer<'static>> = certs(&mut BufReader::new(cert_pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| OxiRpcError::Tls(format!("cert PEM parse: {e}")))?;

    let key: PrivateKeyDer<'static> = private_key(&mut BufReader::new(key_pem))
        .map_err(|e| OxiRpcError::Tls(format!("key PEM parse: {e}")))?
        .ok_or_else(|| OxiRpcError::Tls("no private key found in PEM input".to_string()))?;

    let provider = Arc::new(oxiquic_crypto::quic_crypto_provider());

    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| OxiRpcError::Tls(format!("h3 protocol versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| OxiRpcError::Tls(format!("single cert: {e}")))?;

    config.alpn_protocols = vec![ALPN_H3.to_vec()];
    Ok(config)
}

/// Convenience: build an [`Arc`]-wrapped HTTP/3 client config (see [`client_config_h3`]).
///
/// # Errors
///
/// Propagates any error from [`client_config_h3`].
#[cfg(feature = "http3")]
pub fn client_config_h3_arc(roots: RootCertStore) -> Result<Arc<ClientConfig>, OxiRpcError> {
    client_config_h3(roots).map(Arc::new)
}

/// Convenience: build an [`Arc`]-wrapped HTTP/3 server config (see [`server_config_h3`]).
///
/// # Errors
///
/// Propagates any error from [`server_config_h3`].
#[cfg(feature = "http3")]
pub fn server_config_h3_arc(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<Arc<ServerConfig>, OxiRpcError> {
    server_config_h3(cert_pem, key_pem).map(Arc::new)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "http3"))]
mod h3_tests {
    use super::*;

    /// The h3 client config must advertise exactly `["h3"]` as its ALPN.
    #[test]
    fn client_config_h3_sets_h3_alpn() {
        let cfg = client_config_h3(RootCertStore::empty()).expect("client_config_h3 builds");
        assert_eq!(cfg.alpn_protocols, vec![b"h3".to_vec()]);
    }

    /// A round-trip through `server_config_h3` with a real Ed25519 self-signed
    /// cert must succeed and advertise `["h3"]` ALPN.
    #[test]
    fn server_config_h3_round_trip_ed25519() {
        let ck = oxitls_rcgen::generate_self_signed_ed25519(&["localhost"])
            .expect("generate self-signed ed25519 cert");
        // oxitls-rcgen exposes the cert as PEM and the key as PKCS#8 DER; wrap
        // the DER key in a PEM envelope for the PEM-based helper.
        let key_pem = pkcs8_der_to_pem(&ck.pkcs8_der);
        let cfg = server_config_h3(ck.cert_pem.as_bytes(), key_pem.as_bytes())
            .expect("server_config_h3 builds");
        assert_eq!(cfg.alpn_protocols, vec![b"h3".to_vec()]);
    }

    /// Invalid PEM input must return a typed `OxiRpcError::Tls`, never panic.
    #[test]
    fn server_config_h3_rejects_invalid_pem() {
        let err = server_config_h3(b"not a pem", b"also not a pem")
            .expect_err("invalid PEM must be rejected");
        assert!(matches!(err, OxiRpcError::Tls(_)), "got {err:?}");
    }

    /// Minimal PKCS#8 DER → PEM wrapper (base64, 64-char lines) for tests only.
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

    /// Standard base64 (RFC 4648) encoder — test-only, avoids a dep.
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
}
