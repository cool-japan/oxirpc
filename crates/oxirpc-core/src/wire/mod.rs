//! Native gRPC-over-HTTP/2 wire format — byte-level encoding, framing, headers,
//! trailers, compression pipeline, and timeout codec.
//!
//! Every byte-level concern lives here. Downstream slices (Phase 3+) import from
//! this module rather than re-implementing wire details.

pub mod body;
pub mod codec;
pub mod frame;
pub mod header;
pub mod server;
pub mod timeout_codec;
pub mod trailer;

// Convenience re-exports
pub use body::{body_channel, NativeBody, NativeBodySender};
pub use codec::MessagePipeline;
pub use frame::{Frame, FrameDecoder, FrameEncoder, FrameOptions};
#[cfg(any(feature = "gzip", feature = "zstd"))]
pub use server::bidi_sequential_response_with_encoding;
pub use server::{
    bidi_sequential_response, decode_grpc_message, decode_grpc_message_with_encoding,
    encode_grpc_message, encode_grpc_message_with_encoding, error_grpc_trailers,
    error_response_body, grpc_response_headers, ok_grpc_trailers, read_unary_request,
    read_unary_request_with_encoding, streaming_response_body, unary_response_body,
    unary_response_body_compressed,
};
pub use timeout_codec::Deadline;
pub use trailer::GrpcResponseStatus;

/// Errors produced by byte-level wire operations.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// The frame buffer is too short to decode a complete header or payload.
    #[error("frame too short: need {need}, have {have}")]
    FrameTooShort {
        /// Number of bytes required.
        need: usize,
        /// Number of bytes actually available.
        have: usize,
    },

    /// The payload length in the frame header exceeds the configured maximum.
    #[error("frame too large: max {max}, got {got}")]
    FrameTooLarge {
        /// Configured maximum frame size.
        max: usize,
        /// Payload length claimed by the frame header.
        got: usize,
    },

    /// The flag byte is not 0x00 or 0x01.
    #[error("unknown grpc frame flag: 0x{0:02x}")]
    UnknownFlag(u8),

    /// A compressed frame arrived on a channel configured for identity-only.
    #[error("compressed frame received on identity-only channel")]
    InvalidCompressedOnIdentityChannel,

    /// A compression or decompression backend returned an error.
    #[error("compression error: {0}")]
    Compression(String),

    /// The `grpc-status` trailer was absent.
    #[error("missing grpc-status trailer")]
    MissingStatusTrailer,

    /// The `grpc-status` value could not be parsed as a u32.
    #[error("invalid grpc-status value: {0}")]
    BadStatusValue(String),

    /// The `grpc-status-details-bin` value is not valid base64.
    #[error("invalid grpc-status-details-bin (bad base64)")]
    BadStatusDetailsBin,

    /// A `grpc-timeout` header value could not be parsed.
    #[error("invalid grpc-timeout header: {0}")]
    BadTimeoutHeader(String),

    /// A header value contained characters that are not legal HTTP header value bytes.
    #[error("invalid header value: {0}")]
    BadHeaderValue(String),

    /// Percent-decoding a `grpc-message` field failed.
    #[error("percent-decode error in grpc-message: {0}")]
    PercentDecode(String),

    /// A prost protobuf decode error.
    #[error("proto decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    /// A prost protobuf encode error.
    #[error("proto encode error: {0}")]
    ProstEncode(#[from] prost::EncodeError),
}

/// `tokio_util::codec::{Encoder, Decoder}` require their error types to
/// implement `From<std::io::Error>`.
impl From<std::io::Error> for WireError {
    fn from(e: std::io::Error) -> Self {
        WireError::BadHeaderValue(e.to_string())
    }
}

impl From<WireError> for crate::OxiRpcError {
    fn from(e: WireError) -> crate::OxiRpcError {
        match e {
            WireError::Compression(msg) => crate::OxiRpcError::Compression(msg),
            WireError::ProstDecode(err) => crate::OxiRpcError::Proto(err.to_string()),
            WireError::ProstEncode(err) => crate::OxiRpcError::Proto(err.to_string()),
            other => crate::OxiRpcError::Transport(other.to_string()),
        }
    }
}
