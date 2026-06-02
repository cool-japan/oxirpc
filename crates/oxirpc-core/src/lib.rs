#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! `oxirpc-core` — core gRPC types and the OxiRPC error type.
//!
//! Re-exports tonic's request/response/status types for the current facade,
//! and provides native, dependency-light primitives that downstream code can
//! use without binding to tonic:
//!
//! - [`status::StatusCode`] — all 17 gRPC status codes.
//! - [`metadata::Metadata`] — ASCII + binary (`-bin`) typed headers.
//! - [`timeout`] — `grpc-timeout` header parsing/formatting.
//! - [`encoding::CompressionEncoding`] — Identity / Gzip / Zstd, backed by OxiARC.

pub use tonic::{Code, IntoRequest, Request, Response, Status};

pub mod cancel;
pub mod codec;
pub mod interceptor;
pub mod message;
pub mod metadata;
pub mod rpc;
pub mod status;
pub mod stream;
pub mod timeout;

pub use metadata::Metadata;
pub use status::StatusCode;

/// Convenience result alias for fallible OxiRPC operations.
pub type OxiRpcResult<T> = Result<T, OxiRpcError>;

/// Errors returned by oxirpc operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum OxiRpcError {
    /// A gRPC status error.
    Status(tonic::Status),
    /// A transport-layer error.
    Transport(String),
    /// A build-time error.
    Build(String),
    /// A TLS configuration or handshake error.
    Tls(String),
    /// A compression or decompression error.
    Compression(String),
    /// A protobuf encode/decode error.
    Proto(String),
    /// The deadline expired before the operation completed.
    Timeout,
    /// The operation was cancelled (e.g. by the client).
    Cancelled,
}

impl std::fmt::Display for OxiRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OxiRpcError::Status(s) => write!(f, "gRPC status: {s}"),
            OxiRpcError::Transport(s) => write!(f, "transport error: {s}"),
            OxiRpcError::Build(s) => write!(f, "build error: {s}"),
            OxiRpcError::Tls(s) => write!(f, "TLS error: {s}"),
            OxiRpcError::Compression(s) => write!(f, "compression error: {s}"),
            OxiRpcError::Proto(s) => write!(f, "proto error: {s}"),
            OxiRpcError::Timeout => write!(f, "deadline exceeded"),
            OxiRpcError::Cancelled => write!(f, "operation cancelled"),
        }
    }
}

impl std::error::Error for OxiRpcError {}

impl From<tonic::Status> for OxiRpcError {
    fn from(s: tonic::Status) -> Self {
        OxiRpcError::Status(s)
    }
}

impl From<OxiRpcError> for tonic::Status {
    #[allow(unreachable_patterns)]
    fn from(e: OxiRpcError) -> tonic::Status {
        match e {
            OxiRpcError::Status(s) => s,
            OxiRpcError::Transport(msg) => tonic::Status::unavailable(msg),
            OxiRpcError::Timeout => tonic::Status::deadline_exceeded("deadline exceeded"),
            OxiRpcError::Cancelled => tonic::Status::cancelled("operation cancelled"),
            OxiRpcError::Build(msg) => tonic::Status::internal(msg),
            OxiRpcError::Tls(msg) => tonic::Status::unavailable(msg),
            OxiRpcError::Compression(msg) => tonic::Status::internal(msg),
            OxiRpcError::Proto(msg) => tonic::Status::internal(msg),
            _ => tonic::Status::unknown("unknown error"),
        }
    }
}

impl OxiRpcError {
    /// Construct an `OxiRpcError::Status` from a native [`StatusCode`] and message,
    /// without requiring a `tonic::Status` directly.
    pub fn from_status_code(code: crate::status::StatusCode, msg: impl Into<String>) -> Self {
        OxiRpcError::Status(tonic::Status::new(tonic::Code::from(code), msg.into()))
    }
}

impl From<tonic::transport::Error> for OxiRpcError {
    fn from(e: tonic::transport::Error) -> Self {
        OxiRpcError::Transport(e.to_string())
    }
}

impl From<timeout::TimeoutError> for OxiRpcError {
    fn from(_: timeout::TimeoutError) -> Self {
        OxiRpcError::Timeout
    }
}

impl From<metadata::MetadataError> for OxiRpcError {
    fn from(e: metadata::MetadataError) -> Self {
        OxiRpcError::Transport(e.to_string())
    }
}

#[cfg(feature = "oxiproto")]
impl From<oxiproto::OxiProtoError> for OxiRpcError {
    fn from(e: oxiproto::OxiProtoError) -> Self {
        use oxiproto::OxiProtoError;
        match e {
            OxiProtoError::ParseError(s) => OxiRpcError::Proto(s),
            OxiProtoError::CodegenError(s) => OxiRpcError::Build(s),
            OxiProtoError::IoError(io_err) => OxiRpcError::Build(io_err.to_string()),
            OxiProtoError::WireFormatError(wire_err) => OxiRpcError::Proto(wire_err.to_string()),
            other => OxiRpcError::Proto(other.to_string()),
        }
    }
}

/// TLS configuration helpers (Pure Rust, no ring, no FFI).
///
/// Construct [`rustls::ClientConfig`] and [`rustls::ServerConfig`] backed by
/// `rustls-rustcrypto` via OxiTLS. Always injects the provider per-config;
/// never calls `CryptoProvider::install_default()`.
///
/// Enable via the `tls` feature.
#[cfg(feature = "tls")]
pub mod tls;

/// gRPC message compression via OxiArc DEFLATE (pure Rust gzip, no flate2).
///
/// Provides [`compression::OxiArcGzip`] for compress/decompress and
/// [`compression::CompressionError`] for structured error handling.
///
/// Enable via the `compression` feature.
#[cfg(feature = "compression")]
pub mod compression;

/// gRPC message-compression encodings (Identity / Gzip / Zstd), backed by
/// OxiARC.
///
/// [`encoding::CompressionEncoding`] models the negotiated encoding; the
/// [`encoding::Encoding`] trait and [`encoding::compress`] / [`encoding::decompress`]
/// free functions dispatch to the appropriate Pure-Rust backend. gzip requires
/// the `gzip` feature; zstd requires the `zstd` feature.
///
/// [`encoding::ServerCompressionPrefs`] carries server-operator preferences and
/// is injected into request extensions by the `CompressionPrefsLayer` in
/// `oxirpc-server`.
pub mod encoding;

pub use encoding::ServerCompressionPrefs;

/// gRPC wire-level framing and prost-backed codec.
///
/// Provides length-prefixed frame encode/decode (`encode_grpc_frame`,
/// `decode_grpc_frame`), full message pipelines (`encode_message`,
/// `decode_message`), a multi-frame [`grpc::FrameIterator`], and a
/// [`grpc::ProstCodec`] that adapts any `prost::Message + Default` to the
/// [`codec::MessageCodec`] trait.
pub mod grpc;

/// gRPC-over-HTTP/2 wire format constants and header helpers (Phase 2 foundation).
///
/// Provides type-safe, zero-dependency building blocks for the gRPC-over-HTTP/2
/// wire format: header name constants, [`h2::GrpcRequestHeaders`],
/// [`h2::GrpcResponseHeaders`], [`h2::GrpcStatusCode`], and content-type helpers.
///
/// This module does NOT implement H2 transport (that is Phase 3); it provides
/// the constants and types that a future native transport will use.
pub mod h2;

/// Native gRPC-over-HTTP/2 wire format — pure byte-level framing, headers,
/// trailers, compression pipeline, and timeout codec.
///
/// This is the authoritative implementation of every byte-level concern of
/// the gRPC-over-HTTP/2 wire protocol. Phase 3+ native transport slices build
/// on top of this module.
pub mod wire;

#[cfg(any(feature = "gzip", feature = "zstd"))]
pub use wire::bidi_sequential_response_with_encoding;
pub use wire::{
    bidi_sequential_response, body_channel, decode_grpc_message, decode_grpc_message_with_encoding,
    encode_grpc_message, encode_grpc_message_with_encoding, error_grpc_trailers,
    error_response_body, grpc_response_headers, ok_grpc_trailers, read_unary_request,
    read_unary_request_with_encoding, streaming_response_body, unary_response_body,
    unary_response_body_compressed, Deadline, Frame, FrameDecoder, FrameEncoder, FrameOptions,
    GrpcResponseStatus, MessagePipeline, NativeBody, NativeBodySender, WireError,
};
