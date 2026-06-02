//! gRPC length-prefixed framing via `tokio_util::codec`.
//!
//! The gRPC over HTTP/2 spec defines a 5-byte framing header:
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
//! This module provides:
//! - [`FrameEncoder`] / [`FrameDecoder`] for use with `tokio_util::codec::Framed`.
//! - [`encode_frame`] / [`decode_frame`] free functions for non-streaming use.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::WireError;

/// Number of bytes in a gRPC frame header (1 flag + 4 big-endian length).
pub const GRPC_FRAME_HEADER_LEN: usize = 5;

/// gRPC frame flag: payload is not compressed.
pub const FLAG_UNCOMPRESSED: u8 = 0x00;

/// gRPC frame flag: payload is compressed with the negotiated encoding.
pub const FLAG_COMPRESSED: u8 = 0x01;

/// Default maximum frame size: 4 MiB. Guards against trivial DoS via crafted headers.
pub const MAX_FRAME_SIZE_DEFAULT: usize = 4 * 1024 * 1024;

// ─── Frame ────────────────────────────────────────────────────────────────────

/// A single decoded gRPC frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// `true` if the `FLAG_COMPRESSED` bit was set.
    pub compressed: bool,
    /// The raw payload bytes (may be compressed).
    pub payload: Bytes,
}

// ─── FrameOptions ─────────────────────────────────────────────────────────────

/// Tuning options for the frame encoder / decoder.
#[derive(Debug, Clone)]
pub struct FrameOptions {
    /// Maximum allowed payload length in a single frame.
    /// Frames claiming more bytes are rejected before any memory is reserved.
    pub max_frame_size: usize,
}

impl Default for FrameOptions {
    fn default() -> Self {
        Self {
            max_frame_size: MAX_FRAME_SIZE_DEFAULT,
        }
    }
}

// ─── FrameEncoder ─────────────────────────────────────────────────────────────

/// Encodes [`Frame`] values into the gRPC length-prefixed binary format.
#[derive(Default)]
pub struct FrameEncoder {
    /// Options governing maximum sizes (currently unused in encoding, reserved for future).
    pub options: FrameOptions,
}

impl Encoder<Frame> for FrameEncoder {
    type Error = WireError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let flag = if item.compressed {
            FLAG_COMPRESSED
        } else {
            FLAG_UNCOMPRESSED
        };
        let payload_len = item.payload.len();
        // Reserve: 1 flag + 4 length + payload
        dst.reserve(GRPC_FRAME_HEADER_LEN + payload_len);
        dst.put_u8(flag);
        dst.put_u32(payload_len as u32);
        dst.put_slice(&item.payload);
        Ok(())
    }
}

// ─── FrameDecoder ─────────────────────────────────────────────────────────────

/// Internal state kept between `decode` calls when header has been read but
/// payload has not yet arrived.
struct PendingFrame {
    compressed: bool,
    payload_len: usize,
}

/// Decodes the gRPC length-prefixed binary format into [`Frame`] values.
pub struct FrameDecoder {
    options: FrameOptions,
    pending: Option<PendingFrame>,
}

impl FrameDecoder {
    /// Create a new decoder with the given options.
    pub fn new(options: FrameOptions) -> Self {
        Self {
            options,
            pending: None,
        }
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(FrameOptions::default())
    }
}

impl Decoder for FrameDecoder {
    type Item = Frame;
    type Error = WireError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // ── Phase 1: Read header if no pending frame. ───────────────────────
        if self.pending.is_none() {
            if src.len() < GRPC_FRAME_HEADER_LEN {
                // Not enough data for a full header yet — wait.
                return Ok(None);
            }

            let flag = src[0];
            let compressed = match flag {
                FLAG_UNCOMPRESSED => false,
                FLAG_COMPRESSED => true,
                other => {
                    // Consume the bad byte so the decoder doesn't loop.
                    src.advance(1);
                    return Err(WireError::UnknownFlag(other));
                }
            };

            let payload_len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;

            // DoS guard: reject oversized frames BEFORE reserving memory.
            if payload_len > self.options.max_frame_size {
                return Err(WireError::FrameTooLarge {
                    max: self.options.max_frame_size,
                    got: payload_len,
                });
            }

            // Consume the 5-byte header now that we've validated it.
            src.advance(GRPC_FRAME_HEADER_LEN);
            self.pending = Some(PendingFrame {
                compressed,
                payload_len,
            });
        }

        // ── Phase 2: Read payload if header was already consumed. ────────────
        let pending = self
            .pending
            .as_ref()
            .expect("set just above or in prior call");
        let payload_len = pending.payload_len;

        if src.len() < payload_len {
            // Reserve the exact bytes still needed so the caller's accumulator
            // doesn't grow by amortized doubling as chunks arrive.
            src.reserve(payload_len - src.len());
            return Ok(None);
        }

        let compressed = pending.compressed;
        // Remove pending before returning so we're clean.
        self.pending = None;

        let payload = src.split_to(payload_len).freeze();
        Ok(Some(Frame {
            compressed,
            payload,
        }))
    }
}

// ─── Free functions ──────────────────────────────────────────────────────────

/// Encode a single gRPC frame into a [`Bytes`] buffer.
///
/// # Errors
///
/// Returns [`WireError::FrameTooLarge`] if `payload.len() > u32::MAX`.
pub fn encode_frame(payload: &[u8], compressed: bool) -> Result<Bytes, WireError> {
    if payload.len() > u32::MAX as usize {
        return Err(WireError::FrameTooLarge {
            max: u32::MAX as usize,
            got: payload.len(),
        });
    }
    let flag = if compressed {
        FLAG_COMPRESSED
    } else {
        FLAG_UNCOMPRESSED
    };
    let mut buf = BytesMut::with_capacity(GRPC_FRAME_HEADER_LEN + payload.len());
    buf.put_u8(flag);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

/// Decode a single gRPC frame from the beginning of `data`.
///
/// Returns `(frame, consumed_bytes)` where `consumed_bytes` is the total number
/// of bytes consumed from `data` (header + payload).
///
/// # Errors
///
/// - [`WireError::FrameTooShort`] — fewer bytes than the 5-byte header.
/// - [`WireError::UnknownFlag`] — flag byte is not 0x00 or 0x01.
/// - [`WireError::FrameTooShort`] — claimed payload length exceeds `data.len()`.
pub fn decode_frame(data: &[u8]) -> Result<(Frame, usize), WireError> {
    if data.len() < GRPC_FRAME_HEADER_LEN {
        return Err(WireError::FrameTooShort {
            need: GRPC_FRAME_HEADER_LEN,
            have: data.len(),
        });
    }

    let compressed = match data[0] {
        FLAG_UNCOMPRESSED => false,
        FLAG_COMPRESSED => true,
        other => return Err(WireError::UnknownFlag(other)),
    };

    let payload_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    let total = GRPC_FRAME_HEADER_LEN + payload_len;

    if data.len() < total {
        return Err(WireError::FrameTooShort {
            need: total,
            have: data.len(),
        });
    }

    let payload = Bytes::copy_from_slice(&data[GRPC_FRAME_HEADER_LEN..total]);
    Ok((
        Frame {
            compressed,
            payload,
        },
        total,
    ))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    fn make_decoder(max: usize) -> FrameDecoder {
        FrameDecoder::new(FrameOptions {
            max_frame_size: max,
        })
    }

    #[test]
    fn encode_round_trip() {
        let payload = b"hello world";
        let mut enc = FrameEncoder::default();
        let mut buf = BytesMut::new();
        enc.encode(
            Frame {
                compressed: false,
                payload: Bytes::from_static(payload),
            },
            &mut buf,
        )
        .unwrap();

        let mut dec = FrameDecoder::default();
        let frame = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.payload.as_ref(), payload);
        assert!(!frame.compressed);
    }

    #[test]
    fn decode_round_trip() {
        let raw = b"test payload";
        let encoded = encode_frame(raw, false).unwrap();
        let (frame, consumed) = decode_frame(&encoded).unwrap();
        assert_eq!(frame.payload.as_ref(), raw);
        assert!(!frame.compressed);
        assert_eq!(consumed, GRPC_FRAME_HEADER_LEN + raw.len());
    }

    #[test]
    fn decode_partial_header_returns_none() {
        // 4 bytes — not enough for a 5-byte header
        let data = [0x00, 0x00, 0x00, 0x04];
        let mut src = BytesMut::from(&data[..]);
        let result = FrameDecoder::default().decode(&mut src).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_header_present_no_payload_returns_none() {
        // Full 5-byte header claiming 4-byte payload but no payload bytes present
        let mut buf = BytesMut::new();
        buf.put_u8(FLAG_UNCOMPRESSED);
        buf.put_u32(4);
        // No payload bytes
        let mut dec = FrameDecoder::default();
        let result = dec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_oversize_rejected_before_allocation() {
        let max = 1024;
        let mut buf = BytesMut::new();
        buf.put_u8(FLAG_UNCOMPRESSED);
        buf.put_u32(2048); // > max
        let mut dec = make_decoder(max);
        let err = dec.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            WireError::FrameTooLarge {
                max: 1024,
                got: 2048
            }
        ));
    }

    #[test]
    fn decode_unknown_flag_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x02); // unknown flag
        buf.put_u32(0);
        let mut dec = FrameDecoder::default();
        let err = dec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, WireError::UnknownFlag(0x02)));
    }

    #[test]
    fn decode_multi_frame_in_one_buffer() {
        let p1 = b"frame one";
        let p2 = b"frame two";
        let mut buf = BytesMut::new();
        // Frame 1
        buf.put_u8(FLAG_UNCOMPRESSED);
        buf.put_u32(p1.len() as u32);
        buf.put_slice(p1);
        // Frame 2
        buf.put_u8(FLAG_UNCOMPRESSED);
        buf.put_u32(p2.len() as u32);
        buf.put_slice(p2);

        let mut dec = FrameDecoder::default();
        let f1 = dec.decode(&mut buf).unwrap().unwrap();
        let f2 = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1.payload.as_ref(), p1);
        assert_eq!(f2.payload.as_ref(), p2);
    }

    #[test]
    fn decode_empty_payload() {
        let encoded = encode_frame(&[], false).unwrap();
        let (frame, consumed) = decode_frame(&encoded).unwrap();
        assert!(frame.payload.is_empty());
        assert!(!frame.compressed);
        assert_eq!(consumed, GRPC_FRAME_HEADER_LEN);
    }

    #[test]
    fn decode_max_size_boundary_exact_ok() {
        let max = 16;
        let payload = vec![0u8; max];
        let encoded = encode_frame(&payload, false).unwrap();
        let mut buf = BytesMut::from(encoded.as_ref());
        let mut dec = make_decoder(max);
        let frame = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.payload.len(), max);
    }

    #[test]
    fn decode_max_size_boundary_plus_one_err() {
        let max = 16;
        let payload = vec![0u8; max + 1];
        let encoded = encode_frame(&payload, false).unwrap();
        let mut buf = BytesMut::from(encoded.as_ref());
        let mut dec = make_decoder(max);
        let err = dec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, WireError::FrameTooLarge { .. }));
    }

    #[test]
    fn compressed_flag_preserved() {
        let payload = b"compressed data";
        let encoded = encode_frame(payload, true).unwrap();
        let (frame, _) = decode_frame(&encoded).unwrap();
        assert!(frame.compressed);
        assert_eq!(frame.payload.as_ref(), payload);
    }

    #[test]
    fn consumed_bytes_exactly_one_frame() {
        let payload = b"exactly one frame worth of bytes";
        let extra = b"extra bytes not part of frame";
        let frame_bytes = encode_frame(payload, false).unwrap();
        let mut combined = Vec::new();
        combined.extend_from_slice(&frame_bytes);
        combined.extend_from_slice(extra);

        let (frame, consumed) = decode_frame(&combined).unwrap();
        assert_eq!(consumed, GRPC_FRAME_HEADER_LEN + payload.len());
        assert_eq!(frame.payload.as_ref(), payload);
        // Verify extra bytes are not consumed
        assert_eq!(&combined[consumed..], extra);
    }

    #[test]
    fn decoder_reserves_pending_payload_capacity() {
        const PAYLOAD_SIZE: usize = 1024;

        // Build a gRPC frame header for a 1024-byte payload.
        let mut src = BytesMut::new();
        src.put_u8(FLAG_UNCOMPRESSED);
        src.put_u32(PAYLOAD_SIZE as u32); // payload length (BE)

        // Push only 1 byte of payload (partial frame).
        src.put_u8(0xAA);

        let mut decoder = FrameDecoder::default();

        // Decode should return Ok(None) — incomplete payload.
        let result = decoder.decode(&mut src).expect("decode should not error");
        assert!(result.is_none(), "expected Ok(None) for partial payload");

        // After the decode call the 5-byte header has been consumed, so src holds
        // 1 byte of payload data. The decoder must have reserved capacity for the
        // remaining (PAYLOAD_SIZE - 1) bytes.
        assert!(
            src.capacity() >= PAYLOAD_SIZE - 1,
            "decoder should have reserved capacity for the pending payload (capacity={}, need>={})",
            src.capacity(),
            PAYLOAD_SIZE - 1,
        );

        // Push the remaining 1023 bytes.
        src.put_bytes(0xBB, PAYLOAD_SIZE - 1);

        // Decode should now succeed.
        let frame = decoder
            .decode(&mut src)
            .expect("decode should not error")
            .expect("frame should be fully decoded");

        assert_eq!(frame.payload.len(), PAYLOAD_SIZE);
        assert!(!frame.compressed);
    }
}
