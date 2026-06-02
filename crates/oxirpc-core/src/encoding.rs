//! gRPC message compression encodings, backed by OxiARC (Pure Rust).
//!
//! gRPC negotiates per-message compression via the `grpc-encoding` and
//! `grpc-accept-encoding` headers. This module models the supported encodings
//! and dispatches compress/decompress to the appropriate OxiARC backend:
//!
//! - [`crate::encoding::CompressionEncoding::Identity`] — no compression.
//! - [`crate::encoding::CompressionEncoding::Gzip`] — RFC 1952 gzip via
//!   `oxiarc-deflate` (requires the `gzip` feature).
//! - [`crate::encoding::CompressionEncoding::Zstd`] — Zstandard via
//!   `oxiarc-zstd` (requires the `zstd` feature).
//!
//! No `flate2`, `zstd`, or other C/FFI compression crate is ever used.
//!
//! # Example
//!
//! ```rust
//! # #[cfg(feature = "gzip")]
//! # {
//! use oxirpc_core::encoding::{CompressionEncoding, compress, decompress};
//!
//! let original = b"hello gRPC compression";
//! let packed = compress(CompressionEncoding::Gzip, original).unwrap();
//! let unpacked = decompress(CompressionEncoding::Gzip, &packed).unwrap();
//! assert_eq!(unpacked, original);
//! # }
//! ```

/// A gRPC message-compression encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CompressionEncoding {
    /// No compression (the value `identity` on the wire).
    #[default]
    Identity,
    /// gzip (RFC 1952), backed by `oxiarc-deflate`.
    Gzip,
    /// Zstandard, backed by `oxiarc-zstd`.
    Zstd,
}

impl CompressionEncoding {
    /// The token used in `grpc-encoding` / `grpc-accept-encoding` headers.
    pub const fn as_str(self) -> &'static str {
        match self {
            CompressionEncoding::Identity => "identity",
            CompressionEncoding::Gzip => "gzip",
            CompressionEncoding::Zstd => "zstd",
        }
    }

    /// Parse a header token into a [`CompressionEncoding`].
    ///
    /// Matching is case-insensitive. Returns [`None`] for unknown tokens.
    pub fn from_str_opt(token: &str) -> Option<CompressionEncoding> {
        match token.trim().to_ascii_lowercase().as_str() {
            "identity" => Some(CompressionEncoding::Identity),
            "gzip" => Some(CompressionEncoding::Gzip),
            "zstd" => Some(CompressionEncoding::Zstd),
            _ => None,
        }
    }

    /// Whether this encoding actually transforms the payload (i.e. is not
    /// [`CompressionEncoding::Identity`]).
    pub const fn is_compressing(self) -> bool {
        !matches!(self, CompressionEncoding::Identity)
    }

    /// Negotiate the encoding to use given a peer's `grpc-accept-encoding`
    /// header and our own preference order.
    ///
    /// Returns the first of `preferences` that the peer also accepts, or
    /// [`CompressionEncoding::Identity`] if there is no overlap.
    pub fn negotiate(
        accept_header: &str,
        preferences: &[CompressionEncoding],
    ) -> CompressionEncoding {
        let accepted: Vec<CompressionEncoding> = accept_header
            .split(',')
            .filter_map(CompressionEncoding::from_str_opt)
            .collect();
        for pref in preferences {
            if pref.is_compressing() && accepted.contains(pref) {
                return *pref;
            }
        }
        CompressionEncoding::Identity
    }
}

impl core::fmt::Display for CompressionEncoding {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Errors produced by compress / decompress dispatch.
#[derive(Debug)]
pub enum EncodingError {
    /// The requested encoding's backend feature is not enabled at build time.
    Unsupported(CompressionEncoding),
    /// The underlying codec failed (corrupt data, internal error, …).
    Codec(String),
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingError::Unsupported(e) => {
                write!(f, "compression encoding {e} is not enabled in this build")
            }
            EncodingError::Codec(s) => write!(f, "compression codec error: {s}"),
        }
    }
}

impl std::error::Error for EncodingError {}

impl From<EncodingError> for crate::OxiRpcError {
    fn from(e: EncodingError) -> Self {
        crate::OxiRpcError::Compression(e.to_string())
    }
}

/// A pluggable message-encoding backend (compress + decompress).
///
/// Implementors transform raw message bytes. The built-in implementations are
/// `Identity`, `Gzip` (feature `gzip`), and `Zstd` (feature `zstd`);
/// downstream crates may supply their own.
pub trait Encoding: Send + Sync {
    /// The [`CompressionEncoding`] this backend implements.
    fn encoding(&self) -> CompressionEncoding;

    /// Compress `data`.
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError>;

    /// Decompress `data` previously produced by [`Encoding::encode`].
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError>;
}

/// The pass-through [`Encoding`]: returns its input unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl Encoding for Identity {
    fn encoding(&self) -> CompressionEncoding {
        CompressionEncoding::Identity
    }
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
        Ok(data.to_vec())
    }
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
        Ok(data.to_vec())
    }
}

/// gzip [`Encoding`] backed by `oxiarc-deflate`.
#[cfg(feature = "gzip")]
#[derive(Debug, Clone, Copy)]
pub struct Gzip {
    /// Compression level (0–9). Defaults to 6.
    pub level: u8,
}

#[cfg(feature = "gzip")]
impl Default for Gzip {
    fn default() -> Self {
        Self { level: 6 }
    }
}

#[cfg(feature = "gzip")]
impl Encoding for Gzip {
    fn encoding(&self) -> CompressionEncoding {
        CompressionEncoding::Gzip
    }
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
        oxiarc_deflate::gzip_compress(data, self.level.min(9))
            .map_err(|e| EncodingError::Codec(e.to_string()))
    }
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
        oxiarc_deflate::gzip_decompress(data).map_err(|e| EncodingError::Codec(e.to_string()))
    }
}

/// Zstandard [`Encoding`] backed by `oxiarc-zstd`.
#[cfg(feature = "zstd")]
#[derive(Debug, Clone, Copy, Default)]
pub struct Zstd;

#[cfg(feature = "zstd")]
impl Encoding for Zstd {
    fn encoding(&self) -> CompressionEncoding {
        CompressionEncoding::Zstd
    }
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
        oxiarc_zstd::compress(data).map_err(|e| EncodingError::Codec(e.to_string()))
    }
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
        oxiarc_zstd::decompress(data).map_err(|e| EncodingError::Codec(e.to_string()))
    }
}

/// Compress `data` with the given [`CompressionEncoding`].
///
/// # Errors
///
/// Returns [`EncodingError::Unsupported`] if the encoding's backend feature is
/// not enabled, or [`EncodingError::Codec`] on a codec failure.
pub fn compress(encoding: CompressionEncoding, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
    match encoding {
        CompressionEncoding::Identity => Ok(data.to_vec()),
        CompressionEncoding::Gzip => {
            #[cfg(feature = "gzip")]
            {
                Gzip::default().encode(data)
            }
            #[cfg(not(feature = "gzip"))]
            {
                Err(EncodingError::Unsupported(encoding))
            }
        }
        CompressionEncoding::Zstd => {
            #[cfg(feature = "zstd")]
            {
                Zstd.encode(data)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err(EncodingError::Unsupported(encoding))
            }
        }
    }
}

/// Server-side compression preferences injected by `ServerBuilder` into each request.
///
/// When present in `http::Request::extensions()`, overrides the service's default
/// feature-based encoding list. An empty `send` slice means "use compiled-in defaults".
#[derive(Clone, Debug, Default)]
pub struct ServerCompressionPrefs {
    /// Encodings the server is willing to use for responses (in preference order).
    /// If empty, the service falls back to its compiled-feature defaults.
    pub send: Vec<CompressionEncoding>,
    /// Encodings the server will accept in client request bodies (in any order).
    /// If empty, the server accepts any encoding it can decode (feature-gated defaults).
    /// When non-empty, requests with an encoding not in this list are rejected with
    /// gRPC status 12 (UNIMPLEMENTED) and a `grpc-accept-encoding` response header.
    pub accept: Vec<CompressionEncoding>,
}

/// Return the compression encodings that this oxirpc-core build can decode, in preference order.
///
/// This drives the default inbound-acceptance policy when no [`ServerCompressionPrefs::accept`]
/// list has been configured by the operator.
pub fn decodable_encodings() -> &'static [CompressionEncoding] {
    #[cfg(all(feature = "gzip", feature = "zstd"))]
    return &[CompressionEncoding::Gzip, CompressionEncoding::Zstd];
    #[cfg(all(feature = "gzip", not(feature = "zstd")))]
    return &[CompressionEncoding::Gzip];
    #[cfg(all(feature = "zstd", not(feature = "gzip")))]
    return &[CompressionEncoding::Zstd];
    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    return &[];
}

/// Return whether a request's `grpc-encoding` value is acceptable to this server.
///
/// - [`CompressionEncoding::Identity`] is always acceptable.
/// - If `accept` is empty, any encoding in [`decodable_encodings`] is acceptable.
/// - If `accept` is non-empty, only encodings in `accept` are acceptable.
/// - An unknown encoding (not represented as a [`CompressionEncoding`] variant) must be
///   passed as a `None` optional; it is always rejected.
pub fn is_request_encoding_acceptable(
    req_enc: Option<CompressionEncoding>,
    accept: &[CompressionEncoding],
) -> bool {
    match req_enc {
        None => false, // unknown token → reject
        Some(CompressionEncoding::Identity) => true,
        Some(enc) => {
            if accept.is_empty() {
                decodable_encodings().contains(&enc)
            } else {
                accept.contains(&enc)
            }
        }
    }
}

/// Build the `grpc-accept-encoding` response-header value for an explicit accept list.
///
/// Always includes `identity`. Does not deduplicate.
pub fn accept_encoding_header_value(accept: &[CompressionEncoding]) -> String {
    let mut parts: Vec<&str> = accept.iter().map(|e| e.as_str()).collect();
    if !parts.contains(&"identity") {
        parts.push("identity");
    }
    parts.join(",")
}

/// Decompress `data` with the given [`CompressionEncoding`].
///
/// # Errors
///
/// See [`compress`].
pub fn decompress(encoding: CompressionEncoding, data: &[u8]) -> Result<Vec<u8>, EncodingError> {
    match encoding {
        CompressionEncoding::Identity => Ok(data.to_vec()),
        CompressionEncoding::Gzip => {
            #[cfg(feature = "gzip")]
            {
                Gzip::default().decode(data)
            }
            #[cfg(not(feature = "gzip"))]
            {
                Err(EncodingError::Unsupported(encoding))
            }
        }
        CompressionEncoding::Zstd => {
            #[cfg(feature = "zstd")]
            {
                Zstd.decode(data)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err(EncodingError::Unsupported(encoding))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_request_encoding_acceptable ──────────────────────────────────────

    #[test]
    fn is_request_encoding_acceptable_identity_always_ok() {
        // Identity is always accepted, regardless of the accept list.
        assert!(is_request_encoding_acceptable(
            Some(CompressionEncoding::Identity),
            &[]
        ));
        assert!(is_request_encoding_acceptable(
            Some(CompressionEncoding::Identity),
            &[CompressionEncoding::Gzip]
        ));
        assert!(is_request_encoding_acceptable(
            Some(CompressionEncoding::Identity),
            &[CompressionEncoding::Identity]
        ));
    }

    #[test]
    fn is_request_encoding_acceptable_unknown_always_rejected() {
        // None represents an unknown token → always rejected.
        assert!(!is_request_encoding_acceptable(None, &[]));
        assert!(!is_request_encoding_acceptable(
            None,
            &[CompressionEncoding::Gzip]
        ));
    }

    #[test]
    fn is_request_encoding_acceptable_default_accepts_decodable() {
        // When accept is empty, only decodable encodings are accepted.
        for enc in decodable_encodings() {
            assert!(
                is_request_encoding_acceptable(Some(*enc), &[]),
                "decodable encoding {enc} should be accepted by default"
            );
        }
        // Identity is always accepted too.
        assert!(is_request_encoding_acceptable(
            Some(CompressionEncoding::Identity),
            &[]
        ));
    }

    #[test]
    fn is_request_encoding_acceptable_explicit_accept_narrows() {
        // When accept = [Identity], compressing encodings are rejected.
        assert!(!is_request_encoding_acceptable(
            Some(CompressionEncoding::Gzip),
            &[CompressionEncoding::Identity]
        ));
        assert!(!is_request_encoding_acceptable(
            Some(CompressionEncoding::Zstd),
            &[CompressionEncoding::Identity]
        ));
        // But Identity itself is always accepted (special-cased before checking the list).
        assert!(is_request_encoding_acceptable(
            Some(CompressionEncoding::Identity),
            &[CompressionEncoding::Identity]
        ));
    }

    // ── accept_encoding_header_value ────────────────────────────────────────

    #[test]
    fn accept_encoding_header_value_includes_identity() {
        let val = accept_encoding_header_value(&[CompressionEncoding::Gzip]);
        assert!(
            val.contains("gzip"),
            "header value should contain 'gzip', got: {val}"
        );
        assert!(
            val.contains("identity"),
            "header value should always contain 'identity', got: {val}"
        );
    }

    #[test]
    fn accept_encoding_header_value_identity_not_duplicated() {
        let val = accept_encoding_header_value(&[CompressionEncoding::Identity]);
        assert_eq!(
            val, "identity",
            "identity should not be duplicated, got: {val}"
        );
    }
}
