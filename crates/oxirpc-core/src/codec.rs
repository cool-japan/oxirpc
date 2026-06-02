//! Native message codec trait for serialisation / deserialisation.
//!
//! Named [`MessageCodec`] (not `Codec` or `Encoding`) to avoid colliding with
//! the `encoding::Encoding` trait that already exists for compression backends.

/// Error produced during message encoding or decoding.
#[derive(Debug)]
pub enum CodecError {
    /// The message could not be serialised.
    Encode(String),
    /// The byte buffer could not be deserialised into the target type.
    Decode(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Encode(s) => write!(f, "codec encode error: {s}"),
            CodecError::Decode(s) => write!(f, "codec decode error: {s}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// A pluggable serialisation / deserialisation backend for message type `T`.
///
/// Implementations must be `Send + Sync` so they can be shared across threads.
pub trait MessageCodec<T>: Send + Sync {
    /// Serialise `msg`, appending the bytes to `buf`.
    fn encode(&self, msg: &T, buf: &mut Vec<u8>) -> Result<(), CodecError>;

    /// Deserialise `buf` into a value of type `T`.
    fn decode(&self, buf: &[u8]) -> Result<T, CodecError>;
}

/// A passthrough codec that treats raw byte vectors as messages.
///
/// `encode` appends the message bytes to the buffer without transformation;
/// `decode` returns a copy of the buffer.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityCodec;

impl MessageCodec<Vec<u8>> for IdentityCodec {
    fn encode(&self, msg: &Vec<u8>, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        buf.extend_from_slice(msg);
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(buf.to_vec())
    }
}
