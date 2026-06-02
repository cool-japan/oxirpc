//! Native gRPC-Web message framing (Pure Rust, no dependencies).
//!
//! gRPC-Web frames each message with a 5-byte prefix:
//!
//! ```text
//! +------------+------------------------+----------------------+
//! | 1 byte     | 4 bytes (big-endian)   | <length> bytes       |
//! | frame flag | message length         | payload              |
//! +------------+------------------------+----------------------+
//! ```
//!
//! The frame flag's most-significant bit (`0x80`) distinguishes a **trailer**
//! frame (HTTP-trailer key/value block) from a **data** frame. The low bit
//! (`0x01`) marks a compressed payload.
//!
//! In *text mode* (`application/grpc-web-text`) the entire framed body is then
//! base64-encoded.
//!
//! This module implements the framing in isolation so it can be unit-tested and
//! reused by a future native gRPC-Web bridge.

use oxirpc_core::{
    encoding::CompressionEncoding,
    metadata::{base64_decode, base64_encode},
};

/// Frame-flag bit indicating the frame carries trailers (not message data).
pub const FLAG_TRAILER: u8 = 0x80;
/// Frame-flag bit indicating the payload is compressed.
pub const FLAG_COMPRESSED: u8 = 0x01;

/// The kind of a gRPC-Web frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// A length-prefixed protobuf message.
    Data,
    /// A length-prefixed HTTP-trailer block (`key: value\r\n` lines).
    Trailer,
}

/// A decoded gRPC-Web frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Whether this is a data or trailer frame.
    pub kind: FrameKind,
    /// Whether the payload was flagged compressed.
    pub compressed: bool,
    /// The raw payload bytes (still compressed if `compressed` is set and
    /// decompression was not requested at decode time).
    pub payload: Vec<u8>,
}

impl Frame {
    /// Construct an uncompressed data frame.
    pub fn data(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: FrameKind::Data,
            compressed: false,
            payload: payload.into(),
        }
    }

    /// Construct a trailer frame from `(name, value)` pairs.
    ///
    /// Names are emitted lowercase, one `name: value\r\n` line per pair, which
    /// is the format gRPC-Web clients expect in the trailer block.
    pub fn trailers(pairs: &[(&str, &str)]) -> Self {
        let mut body = String::new();
        for (k, v) in pairs {
            body.push_str(&k.to_ascii_lowercase());
            body.push_str(": ");
            body.push_str(v);
            body.push_str("\r\n");
        }
        Self {
            kind: FrameKind::Trailer,
            compressed: false,
            payload: body.into_bytes(),
        }
    }

    /// The 1-byte frame flag for this frame.
    fn flag(&self) -> u8 {
        let mut f = 0u8;
        if self.kind == FrameKind::Trailer {
            f |= FLAG_TRAILER;
        }
        if self.compressed {
            f |= FLAG_COMPRESSED;
        }
        f
    }

    /// Parse a trailer-frame payload into `(name, value)` pairs.
    ///
    /// Returns an empty vector if this is not a trailer frame.
    pub fn parse_trailers(&self) -> Vec<(String, String)> {
        if self.kind != FrameKind::Trailer {
            return Vec::new();
        }
        let text = String::from_utf8_lossy(&self.payload);
        text.lines()
            .filter_map(|line| {
                let line = line.trim_end_matches(['\r', '\n']);
                let (k, v) = line.split_once(':')?;
                Some((k.trim().to_owned(), v.trim().to_owned()))
            })
            .collect()
    }
}

/// Errors produced while encoding or decoding a gRPC-Web body.
#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The body ended in the middle of a frame header or payload.
    Truncated,
    /// A frame declared a length that overflows the remaining body.
    LengthOverflow,
    /// Base64 decoding of a text-mode body failed.
    InvalidBase64,
    /// A compression or decompression codec error.
    Codec(String),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Truncated => write!(f, "truncated gRPC-Web frame"),
            FrameError::LengthOverflow => write!(f, "gRPC-Web frame length exceeds body"),
            FrameError::InvalidBase64 => write!(f, "invalid base64 in gRPC-Web text body"),
            FrameError::Codec(s) => write!(f, "gRPC-Web codec error: {s}"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Encode a single frame to its 5-byte-prefixed binary form, optionally
/// compressing the payload with `compression`.
///
/// When `compression` is [`CompressionEncoding::Identity`] no compression is
/// applied and the behaviour is identical to the pre-compression API.
///
/// # Errors
///
/// Returns [`FrameError::Codec`] if the compression backend fails.
pub fn encode_frame(
    frame: &Frame,
    compression: CompressionEncoding,
) -> Result<Vec<u8>, FrameError> {
    let (flag_compressed, body) =
        if compression != CompressionEncoding::Identity && frame.kind == FrameKind::Data {
            let compressed = oxirpc_core::encoding::compress(compression, &frame.payload)
                .map_err(|e| FrameError::Codec(e.to_string()))?;
            (FLAG_COMPRESSED, compressed)
        } else {
            (0u8, frame.payload.clone())
        };

    let flags = frame.flag() | flag_compressed;
    let len = body.len() as u32;
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(flags);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Encode a sequence of frames into one binary body.
///
/// All frames are encoded using the same `compression`.
///
/// # Errors
///
/// See [`encode_frame`].
pub fn encode_body(
    frames: &[Frame],
    compression: CompressionEncoding,
) -> Result<Vec<u8>, FrameError> {
    let mut out = Vec::new();
    for frame in frames {
        out.extend_from_slice(&encode_frame(frame, compression)?);
    }
    Ok(out)
}

/// Encode a sequence of frames into a base64 text-mode body
/// (`application/grpc-web-text`).
///
/// # Errors
///
/// See [`encode_frame`].
pub fn encode_text_body(
    frames: &[Frame],
    compression: CompressionEncoding,
) -> Result<String, FrameError> {
    let binary = encode_body(frames, compression)?;
    Ok(base64_encode(&binary))
}

/// Decode a binary gRPC-Web body into its constituent frames.
///
/// When `compression` is not [`CompressionEncoding::Identity`], any frame with
/// `FLAG_COMPRESSED` set will have its payload decompressed before being
/// returned. The returned [`Frame::compressed`] field will be `false` in that
/// case.
///
/// When `compression` is [`CompressionEncoding::Identity`] the function behaves
/// identically to the pre-compression API: frames are returned with their raw
/// (potentially compressed) payloads and `compressed` reflects the wire flag.
///
/// # Errors
///
/// Returns [`FrameError::Truncated`] or [`FrameError::LengthOverflow`] on a
/// malformed body, or [`FrameError::Codec`] on decompression failure.
pub fn decode_body(
    body: &[u8],
    compression: CompressionEncoding,
) -> Result<Vec<Frame>, FrameError> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < body.len() {
        if body.len() - pos < 5 {
            return Err(FrameError::Truncated);
        }
        let flag = body[pos];
        let len = u32::from_be_bytes([body[pos + 1], body[pos + 2], body[pos + 3], body[pos + 4]])
            as usize;
        pos += 5;
        if body.len() - pos < len {
            return Err(FrameError::LengthOverflow);
        }
        let raw_payload = body[pos..pos + len].to_vec();
        pos += len;

        let is_trailer = flag & FLAG_TRAILER != 0;
        let was_compressed = flag & FLAG_COMPRESSED != 0;

        let (payload, compressed) =
            if was_compressed && compression != CompressionEncoding::Identity {
                let decompressed = oxirpc_core::encoding::decompress(compression, &raw_payload)
                    .map_err(|e| FrameError::Codec(e.to_string()))?;
                (decompressed, false)
            } else {
                (raw_payload, was_compressed)
            };

        frames.push(Frame {
            kind: if is_trailer {
                FrameKind::Trailer
            } else {
                FrameKind::Data
            },
            compressed,
            payload,
        });
    }
    Ok(frames)
}

/// Decode a base64 text-mode body (`application/grpc-web-text`) into frames.
///
/// # Errors
///
/// Returns [`FrameError::InvalidBase64`] if the outer base64 is malformed, or a
/// framing error from [`decode_body`].
pub fn decode_text_body(
    text: &str,
    compression: CompressionEncoding,
) -> Result<Vec<Frame>, FrameError> {
    let bytes = base64_decode(text).ok_or(FrameError::InvalidBase64)?;
    decode_body(&bytes, compression)
}
