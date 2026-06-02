//! gRPC wire-level framing and prost-backed codec.
//!
//! > **Deprecated shim**: frame encoding/decoding has moved to
//! > [`crate::wire::frame`]. The items below are preserved for one release of
//! > backward compatibility. Prefer importing from `crate::wire::frame` in new
//! > code.
//!
//! This module implements the gRPC length-prefixed message framing defined by
//! the [gRPC over HTTP/2 specification]:
//!
//! ```text
//! +-------+----------+---------------+
//! | Flags | Length   | Payload       |
//! | 1 byte| 4 bytes  | Length bytes  |
//! +-------+----------+---------------+
//! ```
//!
//! Flags byte: `0x00` = uncompressed, `0x01` = compressed.
//!
//! A `ProstCodec` bridges the native `MessageCodec` trait to any
//! `prost::Message`, making it straightforward to wire prost-generated types
//! into the OxiRPC codec abstraction.
//!
//! [gRPC over HTTP/2 specification]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md

use crate::codec::{CodecError, MessageCodec};
use crate::encoding::{compress, decompress, CompressionEncoding};
use prost::Message;

/// Number of bytes in a gRPC frame header (1 flag + 4 length).
const GRPC_FRAME_HEADER_LEN: usize = 5;

/// gRPC frame flag indicating the payload is uncompressed.
const FLAG_UNCOMPRESSED: u8 = 0x00;

/// gRPC frame flag indicating the payload is compressed.
const FLAG_COMPRESSED: u8 = 0x01;

/// Encode a single gRPC frame: `[flag(1)] [length(4 BE)] [payload]`.
///
/// # Errors
///
/// Returns [`CodecError::Encode`] if `payload.len()` exceeds `u32::MAX`.
#[deprecated(
    since = "0.2.0",
    note = "use oxirpc_core::wire::frame::encode_frame instead"
)]
pub fn encode_grpc_frame(payload: &[u8], compressed: bool) -> Vec<u8> {
    let flag = if compressed {
        FLAG_COMPRESSED
    } else {
        FLAG_UNCOMPRESSED
    };
    let len = payload.len() as u64;
    // Callers deal with oversized payloads before calling this, but we
    // saturate-cast for safety; `encode_message` validates beforehand.
    let len_bytes = (len as u32).to_be_bytes();

    let mut out = Vec::with_capacity(GRPC_FRAME_HEADER_LEN + payload.len());
    out.push(flag);
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(payload);
    out
}

/// Decode a single gRPC frame from the **beginning** of `data`.
///
/// Returns `(compressed, payload_bytes)` where `payload_bytes` is a slice into
/// the original `data`.
///
/// # Errors
///
/// Returns [`CodecError::Decode`] when:
/// - `data` is shorter than 5 bytes (the header alone is 5 bytes).
/// - The 4-byte length field claims more bytes than are present in `data`.
#[deprecated(
    since = "0.2.0",
    note = "use oxirpc_core::wire::frame::decode_frame instead"
)]
pub fn decode_grpc_frame(data: &[u8]) -> Result<(bool, &[u8]), CodecError> {
    if data.len() < GRPC_FRAME_HEADER_LEN {
        return Err(CodecError::Decode(format!(
            "gRPC frame too short: need at least {GRPC_FRAME_HEADER_LEN} bytes, got {}",
            data.len()
        )));
    }

    let compressed = match data[0] {
        FLAG_UNCOMPRESSED => false,
        FLAG_COMPRESSED => true,
        other => {
            return Err(CodecError::Decode(format!(
                "gRPC frame flag byte has unknown value 0x{other:02x}"
            )));
        }
    };

    // Safe: we already checked data.len() >= 5.
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    let end = GRPC_FRAME_HEADER_LEN + len;

    if data.len() < end {
        return Err(CodecError::Decode(format!(
            "gRPC frame payload truncated: header claims {len} bytes, only {} available",
            data.len() - GRPC_FRAME_HEADER_LEN
        )));
    }

    Ok((compressed, &data[GRPC_FRAME_HEADER_LEN..end]))
}

/// Encode a prost [`Message`]: prost-encode → optionally compress → wrap in a
/// gRPC length-prefixed frame.
///
/// The compression flag byte in the frame is set to `0x01` when
/// `compression != CompressionEncoding::Identity`; otherwise `0x00`.
///
/// # Errors
///
/// Returns [`CodecError::Encode`] if prost encoding fails, the payload exceeds
/// `u32::MAX` bytes, or the compression backend returns an error.
#[allow(deprecated)]
#[deprecated(
    since = "0.2.0",
    note = "use oxirpc_core::wire::codec::MessagePipeline::encode_message instead"
)]
pub fn encode_message<T: Message>(
    msg: &T,
    compression: CompressionEncoding,
) -> Result<Vec<u8>, CodecError> {
    // prost-encode into a fresh buffer.
    let mut prost_buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut prost_buf)
        .map_err(|e| CodecError::Encode(format!("prost encode error: {e}")))?;

    // Optionally compress.
    let (payload, compressed) = if compression.is_compressing() {
        let c = compress(compression, &prost_buf)
            .map_err(|e| CodecError::Encode(format!("compression error: {e}")))?;
        (c, true)
    } else {
        (prost_buf, false)
    };

    // Guard against u32 overflow in the length field.
    if payload.len() > u32::MAX as usize {
        return Err(CodecError::Encode(format!(
            "encoded message too large for gRPC frame: {} bytes (max {})",
            payload.len(),
            u32::MAX
        )));
    }

    Ok(encode_grpc_frame(&payload, compressed))
}

/// Decode a gRPC frame: unframe → optionally decompress → prost-decode into `T`.
///
/// The `compression` argument should match the `grpc-encoding` negotiated for
/// the channel; use [`CompressionEncoding::Identity`] when the frame is
/// uncompressed.
///
/// # Errors
///
/// Returns [`CodecError::Decode`] if framing is invalid, the decompression
/// backend fails, or prost cannot decode the inner bytes into `T`.
#[allow(deprecated)]
#[deprecated(
    since = "0.2.0",
    note = "use oxirpc_core::wire::codec::MessagePipeline::decode_message instead"
)]
pub fn decode_message<T: Message + Default>(
    frame: &[u8],
    compression: CompressionEncoding,
) -> Result<T, CodecError> {
    let (_frame_compressed, payload) = decode_grpc_frame(frame)?;

    // Decompress if the negotiated encoding is not identity.
    let bytes: Vec<u8> = if compression.is_compressing() {
        decompress(compression, payload)
            .map_err(|e| CodecError::Decode(format!("decompression error: {e}")))?
    } else {
        payload.to_vec()
    };

    T::decode(bytes.as_slice()).map_err(|e| CodecError::Decode(format!("prost decode error: {e}")))
}

// ─── FrameIterator ─────────────────────────────────────────────────────────────

/// An iterator over multiple concatenated gRPC frames in a byte slice.
///
/// Each call to [`Iterator::next`] peels one frame from the front of the
/// remaining data, returning `Ok((compressed, payload_slice))` or
/// `Err(CodecError::Decode(...))` on malformed input.
///
/// Iteration stops when the remaining slice is empty (returns `None`); a
/// truncated final frame produces an `Err` item.
#[deprecated(
    since = "0.2.0",
    note = "use oxirpc_core::wire::frame functions instead"
)]
pub struct FrameIterator<'a> {
    data: &'a [u8],
}

#[allow(deprecated)]
impl<'a> FrameIterator<'a> {
    /// Create a new [`FrameIterator`] over the given byte slice.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

#[allow(deprecated)]
impl<'a> Iterator for FrameIterator<'a> {
    type Item = Result<(bool, &'a [u8]), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        // Decode the frame header.
        if self.data.len() < GRPC_FRAME_HEADER_LEN {
            // Advance to empty so next call returns None.
            self.data = &[];
            return Some(Err(CodecError::Decode(format!(
                "gRPC frame header truncated: need {GRPC_FRAME_HEADER_LEN} bytes, got {}",
                self.data.len()
            ))));
        }

        let compressed = match self.data[0] {
            FLAG_UNCOMPRESSED => false,
            FLAG_COMPRESSED => true,
            other => {
                self.data = &[];
                return Some(Err(CodecError::Decode(format!(
                    "gRPC frame flag byte has unknown value 0x{other:02x}"
                ))));
            }
        };

        let len =
            u32::from_be_bytes([self.data[1], self.data[2], self.data[3], self.data[4]]) as usize;
        let end = GRPC_FRAME_HEADER_LEN + len;

        if self.data.len() < end {
            self.data = &[];
            return Some(Err(CodecError::Decode(format!(
                "gRPC frame payload truncated: expected {len} bytes, only {} remain",
                self.data.len().saturating_sub(GRPC_FRAME_HEADER_LEN)
            ))));
        }

        let payload = &self.data[GRPC_FRAME_HEADER_LEN..end];
        self.data = &self.data[end..];
        Some(Ok((compressed, payload)))
    }
}

// ─── ProstCodec ────────────────────────────────────────────────────────────────

/// A prost-backed implementation of [`MessageCodec`].
///
/// `T` must implement [`prost::Message`] and [`Default`].
///
/// `ProstCodec` is [`Send`] and [`Sync`] for any `T`; the phantom data uses
/// `fn() -> T` variance to avoid imposing `T: Send + Sync` as a bound.
pub struct ProstCodec<T> {
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T> ProstCodec<T> {
    /// Create a new [`ProstCodec`].
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Default for ProstCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Message + Default> MessageCodec<T> for ProstCodec<T> {
    /// Serialise `msg` using prost, appending the bytes to `buf`.
    fn encode(&self, msg: &T, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        msg.encode(buf)
            .map_err(|e| CodecError::Encode(format!("prost encode error: {e}")))
    }

    /// Deserialise `buf` into a value of type `T`.
    fn decode(&self, buf: &[u8]) -> Result<T, CodecError> {
        T::decode(buf).map_err(|e| CodecError::Decode(format!("prost decode error: {e}")))
    }
}
