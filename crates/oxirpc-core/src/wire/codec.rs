//! Message pipeline: compression + gRPC framing.
//!
//! [`MessagePipeline`] combines a [`CompressionEncoding`] with a
//! [`FrameOptions`] to provide encode/decode operations that handle both
//! compression and the 5-byte gRPC length-prefix framing.

use bytes::Bytes;

use crate::encoding::{compress, decompress, CompressionEncoding};
use crate::wire::{
    frame::{Frame, FrameDecoder, FrameEncoder, FrameOptions},
    WireError,
};

/// A message pipeline that glues gRPC framing together with optional compression.
pub struct MessagePipeline {
    /// The compression encoding negotiated for this channel.
    pub channel_encoding: CompressionEncoding,
    /// Frame encoder/decoder tuning options.
    pub frame_options: FrameOptions,
}

impl MessagePipeline {
    /// Create a new pipeline with the given encoding and frame options.
    pub fn new(encoding: CompressionEncoding, opts: FrameOptions) -> Self {
        Self {
            channel_encoding: encoding,
            frame_options: opts,
        }
    }

    /// Optionally compress `raw`, then wrap the result in a [`Frame`] with the
    /// appropriate compressed flag.
    ///
    /// When `channel_encoding` is [`CompressionEncoding::Identity`], the payload
    /// is returned as-is with `compressed = false`.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::Compression`] if the backend fails.
    pub fn encode_message(&self, raw: &[u8]) -> Result<Frame, WireError> {
        if self.channel_encoding.is_compressing() {
            let compressed_bytes = compress(self.channel_encoding, raw)
                .map_err(|e| WireError::Compression(e.to_string()))?;
            Ok(Frame {
                compressed: true,
                payload: Bytes::from(compressed_bytes),
            })
        } else {
            Ok(Frame {
                compressed: false,
                payload: Bytes::copy_from_slice(raw),
            })
        }
    }

    /// Decode a [`Frame`]: validate the compressed flag, decompress if needed,
    /// and return the raw payload bytes.
    ///
    /// - `compressed=true` + `Identity` → [`WireError::InvalidCompressedOnIdentityChannel`]
    /// - `compressed=false` + any encoding → returns payload as-is (valid: sender
    ///   chose not to compress this particular message).
    ///
    /// # Errors
    ///
    /// - [`WireError::InvalidCompressedOnIdentityChannel`]
    /// - [`WireError::Compression`] if decompression fails.
    pub fn decode_message(&self, frame: &Frame) -> Result<Bytes, WireError> {
        if frame.compressed {
            if !self.channel_encoding.is_compressing() {
                return Err(WireError::InvalidCompressedOnIdentityChannel);
            }
            let decompressed = decompress(self.channel_encoding, &frame.payload)
                .map_err(|e| WireError::Compression(e.to_string()))?;
            Ok(Bytes::from(decompressed))
        } else {
            // Uncompressed frame is always valid regardless of negotiated encoding.
            Ok(frame.payload.clone())
        }
    }

    /// Produce a [`FrameEncoder`] configured with this pipeline's options.
    pub fn encoder(&self) -> FrameEncoder {
        FrameEncoder {
            options: self.frame_options.clone(),
        }
    }

    /// Produce a [`FrameDecoder`] configured with this pipeline's options.
    pub fn decoder(&self) -> FrameDecoder {
        FrameDecoder::new(self.frame_options.clone())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::frame::FrameOptions;

    fn identity_pipeline() -> MessagePipeline {
        MessagePipeline::new(CompressionEncoding::Identity, FrameOptions::default())
    }

    #[test]
    fn identity_encoding_flag_uncompressed() {
        let pipeline = identity_pipeline();
        let frame = pipeline.encode_message(b"hello").unwrap();
        assert!(!frame.compressed);
        assert_eq!(frame.payload.as_ref(), b"hello");
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_encoding_compresses_and_decompresses() {
        let pipeline = MessagePipeline::new(CompressionEncoding::Gzip, FrameOptions::default());
        let original = b"hello gzip round-trip data that is definitely longer than a few bytes";
        let frame = pipeline.encode_message(original).unwrap();
        assert!(frame.compressed);
        let decoded = pipeline.decode_message(&frame).unwrap();
        assert_eq!(decoded.as_ref(), original);
    }

    #[test]
    fn compressed_flag_on_identity_channel_returns_error() {
        let pipeline = identity_pipeline();
        let frame = Frame {
            compressed: true,
            payload: bytes::Bytes::from_static(b"fake compressed"),
        };
        let err = pipeline.decode_message(&frame).unwrap_err();
        assert!(matches!(err, WireError::InvalidCompressedOnIdentityChannel));
    }

    #[test]
    fn uncompressed_flag_on_gzip_channel_returns_payload_as_is() {
        // Even when the channel has gzip negotiated, uncompressed frames are valid.
        let pipeline = MessagePipeline::new(CompressionEncoding::Gzip, FrameOptions::default());
        let raw = b"not actually compressed";
        let frame = Frame {
            compressed: false,
            payload: bytes::Bytes::from_static(raw),
        };
        let result = pipeline.decode_message(&frame).unwrap();
        assert_eq!(result.as_ref(), raw);
    }
}
