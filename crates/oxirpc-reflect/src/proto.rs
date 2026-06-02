//! Native gRPC Server Reflection protocol types.
//!
//! Hand-authored prost-derived structs matching the gRPC Server Reflection v1 proto
//! (`grpc.reflection.v1`). Field tags match the official proto spec exactly.
//!
//! ## Wire format
//!
//! These types match the binary proto encoding of the official
//! `grpc.reflection.v1.ServerReflection` service proto.  They are suitable for
//! embedding in a custom reflection server that speaks the standard gRPC Server
//! Reflection protocol.
//!
//! ## Example
//!
//! ```rust
//! use oxirpc_reflect::proto::{ServerReflectionRequest, server_reflection_request};
//! use prost::Message;
//!
//! let req = ServerReflectionRequest {
//!     host: String::new(),
//!     message_request: Some(server_reflection_request::MessageRequest::ListServices(
//!         String::new(),
//!     )),
//! };
//! let bytes = req.encode_to_vec();
//! let decoded = ServerReflectionRequest::decode(bytes.as_slice()).expect("valid proto bytes");
//! assert!(decoded.message_request.is_some());
//! ```

// ─── Request ─────────────────────────────────────────────────────────────────

/// Request for the `ServerReflection.ServerReflectionInfo` bidi-streaming RPC.
///
/// Corresponds to `grpc.reflection.v1.ServerReflectionRequest`.
#[derive(Clone, prost::Message)]
pub struct ServerReflectionRequest {
    /// The host we are requesting reflection from (typically empty for local server).
    #[prost(string, tag = "1")]
    pub host: String,

    /// The request payload — one of the supported reflection query types.
    #[prost(
        oneof = "server_reflection_request::MessageRequest",
        tags = "3,4,5,6,7"
    )]
    pub message_request: Option<server_reflection_request::MessageRequest>,
}

/// Nested types for [`ServerReflectionRequest`].
pub mod server_reflection_request {
    /// The reflection query kind.
    ///
    /// Corresponds to the `message_request` oneof in
    /// `grpc.reflection.v1.ServerReflectionRequest`.
    #[derive(Clone, prost::Oneof)]
    pub enum MessageRequest {
        /// Find a proto file by filename (e.g. `"helloworld.proto"`).
        #[prost(string, tag = "3")]
        FileByFilename(String),

        /// Find the file containing a fully-qualified symbol (message, service, enum).
        #[prost(string, tag = "4")]
        FileContainingSymbol(String),

        /// Find the file containing a particular extension.
        #[prost(message, tag = "5")]
        FileContainingExtension(super::ExtensionRequest),

        /// List all extension field numbers of the given message type.
        #[prost(string, tag = "6")]
        AllExtensionNumbersOfType(String),

        /// List all available services.  The value is unused; pass `""`.
        #[prost(string, tag = "7")]
        ListServices(String),
    }
}

// ─── ExtensionRequest ────────────────────────────────────────────────────────

/// Identifies a particular extension field on a message type.
///
/// Corresponds to `grpc.reflection.v1.ExtensionRequest`.
#[derive(Clone, prost::Message)]
pub struct ExtensionRequest {
    /// Fully-qualified name of the message type being extended (no leading dot).
    #[prost(string, tag = "1")]
    pub containing_type: String,

    /// The extension field number.
    #[prost(int32, tag = "2")]
    pub extension_number: i32,
}

// ─── Response ────────────────────────────────────────────────────────────────

/// Response from the `ServerReflection.ServerReflectionInfo` bidi-streaming RPC.
///
/// Corresponds to `grpc.reflection.v1.ServerReflectionResponse`.
#[derive(Clone, prost::Message)]
pub struct ServerReflectionResponse {
    /// The host the response pertains to.
    #[prost(string, tag = "1")]
    pub valid_host: String,

    /// The original request this response corresponds to.
    #[prost(message, optional, tag = "2")]
    pub original_request: Option<ServerReflectionRequest>,

    /// The response payload.
    #[prost(
        oneof = "server_reflection_response::MessageResponse",
        tags = "4,5,6,7"
    )]
    pub message_response: Option<server_reflection_response::MessageResponse>,
}

/// Nested types for [`ServerReflectionResponse`].
pub mod server_reflection_response {
    /// The reflection response kind.
    ///
    /// Corresponds to the `message_response` oneof in
    /// `grpc.reflection.v1.ServerReflectionResponse`.
    #[derive(Clone, prost::Oneof)]
    pub enum MessageResponse {
        /// File descriptor bytes for the requested files.
        #[prost(message, tag = "4")]
        FileDescriptorResponse(super::FileDescriptorResponse),

        /// All extension numbers for the requested message type.
        #[prost(message, tag = "5")]
        AllExtensionNumbersResponse(super::ExtensionNumberResponse),

        /// List of registered services.
        #[prost(message, tag = "6")]
        ListServicesResponse(super::ListServiceResponse),

        /// An error response (e.g. NOT_FOUND).
        #[prost(message, tag = "7")]
        ErrorResponse(super::ErrorResponse),
    }
}

// ─── Response payloads ───────────────────────────────────────────────────────

/// Response containing serialized `FileDescriptorProto` bytes.
///
/// Corresponds to `grpc.reflection.v1.FileDescriptorResponse`.
#[derive(Clone, prost::Message)]
pub struct FileDescriptorResponse {
    /// Serialized `FileDescriptorProto` messages, one per file.
    ///
    /// Each element is the result of `prost::Message::encode_to_vec(&fdp)`.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub file_descriptor_proto: Vec<Vec<u8>>,
}

/// Response containing extension field numbers for a message type.
///
/// Corresponds to `grpc.reflection.v1.ExtensionNumberResponse`.
#[derive(Clone, prost::Message)]
pub struct ExtensionNumberResponse {
    /// The fully-qualified message type name (no leading dot).
    #[prost(string, tag = "1")]
    pub base_type_name: String,

    /// All extension field numbers defined for `base_type_name`.
    #[prost(int32, repeated, tag = "2")]
    pub extension_number: Vec<i32>,
}

/// Response containing the list of registered services.
///
/// Corresponds to `grpc.reflection.v1.ListServiceResponse`.
#[derive(Clone, prost::Message)]
pub struct ListServiceResponse {
    /// The registered services.
    #[prost(message, repeated, tag = "1")]
    pub service: Vec<ServiceResponse>,
}

/// A single service entry in a [`ListServiceResponse`].
///
/// Corresponds to `grpc.reflection.v1.ServiceResponse`.
#[derive(Clone, prost::Message)]
pub struct ServiceResponse {
    /// The fully-qualified service name (e.g. `"helloworld.Greeter"`).
    #[prost(string, tag = "1")]
    pub name: String,
}

/// An error response.
///
/// Corresponds to `grpc.reflection.v1.ErrorResponse`.
#[derive(Clone, prost::Message)]
pub struct ErrorResponse {
    /// A gRPC status code (e.g. `5` for `NOT_FOUND`).
    #[prost(int32, tag = "1")]
    pub error_code: i32,

    /// A human-readable error message.
    #[prost(string, tag = "2")]
    pub error_message: String,
}

impl ErrorResponse {
    /// Create a `NOT_FOUND` (gRPC status 5) error response for `name`.
    pub fn not_found(name: impl Into<String>) -> Self {
        Self {
            error_code: 5, // tonic::Code::NotFound
            error_message: format!("not found: {}", name.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn request_list_services_roundtrip() {
        let req = ServerReflectionRequest {
            host: String::new(),
            message_request: Some(server_reflection_request::MessageRequest::ListServices(
                String::new(),
            )),
        };
        let bytes = req.encode_to_vec();
        let decoded = ServerReflectionRequest::decode(bytes.as_slice()).expect("decode failed");
        assert!(decoded.message_request.is_some());
    }

    #[test]
    fn error_response_not_found_has_code_5() {
        let e = ErrorResponse::not_found("test");
        assert_eq!(e.error_code, 5);
        assert!(e.error_message.contains("test"));
    }
}
