//! gRPC-Web content-type negotiation, header translation, and stream sequencing.
//!
//! This module provides:
//!
//! - [`negotiate`] — determine [`WebMode`] from a `Content-Type` header.
//! - [`response_content_type`] — the response `Content-Type` string for a mode.
//! - [`GrpcWebConfig`] — negotiated config bundle (mode + compression + size limit).
//! - [`metadata_to_wire_headers`] / [`wire_headers_to_metadata`] — translate between
//!   [`oxirpc_core::metadata::Metadata`] and `(name, value)` header pairs, base64-encoding
//!   binary (`-bin`) keys per the gRPC-Web spec.
//! - [`StreamSequencer`] — stateful incremental decoder that reassembles gRPC-Web frames
//!   from arbitrarily-split byte chunks (handles the 5-byte length-prefix split case).

use oxirpc_core::{
    encoding::CompressionEncoding,
    metadata::{base64_decode, base64_encode, Metadata, MetadataError},
};

use crate::codec::{Frame, FrameKind, FLAG_COMPRESSED, FLAG_TRAILER};

// ─── WebMode ────────────────────────────────────────────────────────────────

/// gRPC-Web transfer mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebMode {
    /// Binary mode: `application/grpc-web` or `application/grpc-web+proto`.
    Binary,
    /// Text mode: `application/grpc-web-text` or `application/grpc-web-text+proto`.
    Text,
}

// ─── Content-type negotiation ───────────────────────────────────────────────

/// Negotiate the [`WebMode`] from a `Content-Type` header value.
///
/// Returns [`None`] if the content-type is not a gRPC-Web type.
///
/// # Examples
///
/// ```rust
/// use oxirpc_web::negotiate::{negotiate, WebMode};
///
/// assert_eq!(negotiate("application/grpc-web"), Some(WebMode::Binary));
/// assert_eq!(negotiate("application/grpc-web+proto"), Some(WebMode::Binary));
/// assert_eq!(negotiate("application/grpc-web-text"), Some(WebMode::Text));
/// assert_eq!(negotiate("application/grpc-web-text+proto"), Some(WebMode::Text));
/// assert_eq!(negotiate("application/json"), None);
/// ```
pub fn negotiate(content_type: &str) -> Option<WebMode> {
    // Ignore parameters after `;`
    let ct = content_type.split(';').next().unwrap_or("").trim();
    match ct {
        "application/grpc-web" | "application/grpc-web+proto" => Some(WebMode::Binary),
        "application/grpc-web-text" | "application/grpc-web-text+proto" => Some(WebMode::Text),
        _ => None,
    }
}

/// Returns the response `Content-Type` string for a given [`WebMode`].
pub fn response_content_type(mode: WebMode) -> &'static str {
    match mode {
        WebMode::Binary => "application/grpc-web+proto",
        WebMode::Text => "application/grpc-web-text+proto",
    }
}

// ─── GrpcWebConfig ───────────────────────────────────────────────────────────

/// Combined gRPC-Web session configuration.
///
/// Carries the negotiated [`WebMode`], the message [`CompressionEncoding`], and
/// a maximum decoded message size (bytes).
#[derive(Clone, Debug)]
pub struct GrpcWebConfig {
    /// Binary or text (base64) transfer mode.
    pub mode: WebMode,
    /// The compression encoding to apply when encoding, or expected when decoding.
    pub compression: CompressionEncoding,
    /// Maximum message body size in bytes (decoded). Defaults to 4 MiB.
    pub max_message_bytes: usize,
}

impl GrpcWebConfig {
    /// Construct a config for the given mode with no compression and a 4 MiB limit.
    pub fn new(mode: WebMode) -> Self {
        Self {
            mode,
            compression: CompressionEncoding::Identity,
            max_message_bytes: 4 * 1024 * 1024,
        }
    }

    /// Override the compression encoding.
    pub fn with_compression(mut self, c: CompressionEncoding) -> Self {
        self.compression = c;
        self
    }

    /// Override the maximum message size in bytes.
    pub fn with_max_message_bytes(mut self, n: usize) -> Self {
        self.max_message_bytes = n;
        self
    }
}

// ─── Header translation ──────────────────────────────────────────────────────

/// Translate a [`Metadata`] map to HTTP `(name, value)` header pairs for
/// gRPC-Web response trailers or request headers.
///
/// Binary (`-bin`) keys are base64-encoded per the gRPC-Web spec. ASCII values
/// that cannot be represented as UTF-8 are silently skipped.
pub fn metadata_to_wire_headers(meta: &Metadata) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (key, value) in meta.iter() {
        if Metadata::is_binary_key(key) {
            out.push((key.to_string(), base64_encode(value)));
        } else if let Ok(s) = std::str::from_utf8(value) {
            out.push((key.to_string(), s.to_string()));
        }
    }
    out
}

/// Parse HTTP `(name, value)` header pairs into a [`Metadata`] map.
///
/// Binary (`-bin`) keys are base64-decoded; ASCII keys are stored verbatim.
///
/// # Errors
///
/// Returns [`MetadataError::InvalidBase64`] if a binary key carries an invalid
/// base64 value, or other [`MetadataError`] variants for illegal keys/values.
pub fn wire_headers_to_metadata(headers: &[(String, String)]) -> Result<Metadata, MetadataError> {
    let mut meta = Metadata::new();
    for (key, val) in headers {
        if Metadata::is_binary_key(key) {
            let bytes = base64_decode(val).ok_or(MetadataError::InvalidBase64)?;
            meta.insert_bin(key, &bytes)?;
        } else {
            meta.insert(key, val.as_str())?;
        }
    }
    Ok(meta)
}

// ─── StreamSequencer ─────────────────────────────────────────────────────────

/// Stateful incremental decoder for a gRPC-Web server-streaming body.
///
/// gRPC-Web frames follow the 5-byte prefix format:
///
/// ```text
/// [ flags:1 ][ length:4 (big-endian) ][ payload:length ]
/// ```
///
/// Byte chunks may arrive split anywhere — including inside the 5-byte header.
/// [`StreamSequencer::push`] buffers incomplete data and returns all complete
/// frames on each call.
///
/// Payloads are returned **as-is** (potentially still compressed if
/// `Frame::compressed` is set). The caller is responsible for decompression
/// using [`oxirpc_core::encoding::decompress`] when needed.
pub struct StreamSequencer {
    buf: Vec<u8>,
    max_message_bytes: usize,
}

/// Errors produced by [`StreamSequencer`].
#[derive(Debug, PartialEq, Eq)]
pub enum SequencerError {
    /// A frame payload exceeds the configured maximum size.
    MessageTooLarge {
        /// The declared frame payload length in bytes.
        length: usize,
        /// The configured maximum in bytes.
        max: usize,
    },
}

impl std::fmt::Display for SequencerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequencerError::MessageTooLarge { length, max } => {
                write!(
                    f,
                    "gRPC-Web frame payload {length} bytes exceeds max {max} bytes"
                )
            }
        }
    }
}

impl std::error::Error for SequencerError {}

impl StreamSequencer {
    /// Create a sequencer with no message-size limit (defaults to `usize::MAX`).
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            max_message_bytes: usize::MAX,
        }
    }

    /// Create a sequencer that rejects frames larger than `max_bytes`.
    pub fn with_max_message_bytes(max_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_message_bytes: max_bytes,
        }
    }

    /// Push a new byte chunk.
    ///
    /// Returns all complete frames that can be extracted, or an error if a frame
    /// payload exceeds the configured maximum size.
    ///
    /// On error the internal buffer is left in a defined (but potentially
    /// inconsistent) state; callers should discard the sequencer after an error.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Frame>, SequencerError> {
        self.buf.extend_from_slice(chunk);
        let mut frames = Vec::new();
        loop {
            if self.buf.len() < 5 {
                break;
            }
            let flags = self.buf[0];
            let len =
                u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
            if len > self.max_message_bytes {
                return Err(SequencerError::MessageTooLarge {
                    length: len,
                    max: self.max_message_bytes,
                });
            }
            if self.buf.len() < 5 + len {
                break;
            }
            let payload = self.buf[5..5 + len].to_vec();
            let kind = if flags & FLAG_TRAILER != 0 {
                FrameKind::Trailer
            } else {
                FrameKind::Data
            };
            let compressed = flags & FLAG_COMPRESSED != 0;
            frames.push(Frame {
                kind,
                compressed,
                payload,
            });
            self.buf.drain(..5 + len);
        }
        Ok(frames)
    }
}

impl Default for StreamSequencer {
    fn default() -> Self {
        Self::new()
    }
}
