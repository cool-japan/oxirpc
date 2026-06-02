//! gRPC message compression via OxiArc DEFLATE (pure Rust gzip).
//!
//! This module provides [`crate::compression::OxiArcGzip`] — a thin static API
//! wrapping `oxiarc_deflate::gzip_compress` / `gzip_decompress` — and a
//! [`crate::compression::CompressionError`] type that integrates with
//! [`crate::OxiRpcError`].
//!
//! # Example
//!
//! ```rust
//! use oxirpc_core::compression::{OxiArcGzip, CompressionError};
//!
//! let original = b"hello gRPC world!";
//! let compressed = OxiArcGzip::compress(original).unwrap();
//! let decompressed = OxiArcGzip::decompress(&compressed).unwrap();
//! assert_eq!(&decompressed, original);
//! ```
//!
//! # Wire framing
//!
//! For full gRPC-over-HTTP/2 wire-level compression (toggling the
//! `grpc-encoding` / `grpc-accept-encoding` headers and the 5-byte message
//! frame flag), wrap this via a tower layer.  That wiring is Slice 7b scope.
//!
//! # Default compression level
//!
//! [`crate::compression::OxiArcGzip::compress`] uses level 6 (balanced
//! speed/ratio). Call [`crate::compression::OxiArcGzip::compress_with_level`]
//! for explicit control.

/// Default gzip compression level: 6 (balanced).
pub const DEFAULT_GZIP_LEVEL: u8 = 6;

/// Gzip compress/decompress backed by `oxiarc-deflate`.
///
/// All methods are pure functions — no internal state, no allocations
/// beyond the output buffer.
pub struct OxiArcGzip;

impl OxiArcGzip {
    /// Compress `data` with gzip at the default level (6).
    ///
    /// Returns a valid RFC 1952 gzip stream (starts with `1f 8b`).
    pub fn compress(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        oxiarc_deflate::gzip_compress(data, DEFAULT_GZIP_LEVEL)
            .map_err(|e| CompressionError::classify(e.to_string()))
    }

    /// Compress `data` with gzip at an explicit compression level (0–9).
    ///
    /// - Level 0: store only (no compression)
    /// - Levels 1–3: fast
    /// - Levels 4–6: balanced (6 is the standard default)
    /// - Levels 7–9: maximum compression (slower)
    ///
    /// Values above 9 are clamped to 9.
    pub fn compress_with_level(data: &[u8], level: u8) -> Result<Vec<u8>, CompressionError> {
        oxiarc_deflate::gzip_compress(data, level.min(9))
            .map_err(|e| CompressionError::classify(e.to_string()))
    }

    /// Decompress a gzip stream, returning the original bytes.
    ///
    /// Returns [`CompressionError::InvalidData`] for corrupted or
    /// non-gzip input, and [`CompressionError::Failed`] for unexpected
    /// internal errors.
    pub fn decompress(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        oxiarc_deflate::gzip_decompress(data).map_err(|e| CompressionError::classify(e.to_string()))
    }
}

/// Errors produced by [`OxiArcGzip`] compress / decompress operations.
#[derive(Debug)]
pub enum CompressionError {
    /// The input data is corrupted or not a valid gzip stream.
    InvalidData(String),
    /// Compression or decompression failed for an unexpected reason.
    Failed(String),
}

impl CompressionError {
    /// Classify an error message string into the most specific variant.
    fn classify(msg: String) -> Self {
        // Messages from OxiArcError that indicate structural invalidity
        // (magic, header, CRC, corruption) map to InvalidData.
        let lower = msg.to_ascii_lowercase();
        if lower.contains("invalid")
            || lower.contains("magic")
            || lower.contains("crc")
            || lower.contains("corrupt")
            || lower.contains("too short")
            || lower.contains("truncated")
            || lower.contains("unexpected end")
        {
            CompressionError::InvalidData(msg)
        } else {
            CompressionError::Failed(msg)
        }
    }
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionError::InvalidData(msg) => write!(f, "invalid compressed data: {msg}"),
            CompressionError::Failed(msg) => write!(f, "compression failed: {msg}"),
        }
    }
}

impl std::error::Error for CompressionError {}

impl From<CompressionError> for crate::OxiRpcError {
    fn from(e: CompressionError) -> Self {
        crate::OxiRpcError::Compression(e.to_string())
    }
}
