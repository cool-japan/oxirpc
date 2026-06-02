//! Native gRPC Server Reflection service implementation.
//!
//! Provides [`NativeReflectionServiceV1`] and [`NativeReflectionServiceV1Alpha`]
//! — `tower::Service` implementations that handle the bidi-streaming
//! `ServerReflectionInfo` RPC of the `grpc.reflection.v1` and
//! `grpc.reflection.v1alpha` protocols respectively.
//!
//! Both are backed by a shared [`crate::DescriptorPool`] (and optionally an
//! `oxiproto_reflect::DescriptorPool` when the `oxiproto` feature is enabled)
//! and dispatch each incoming [`proto::ServerReflectionRequest`] to the
//! appropriate pool method.
//!
//! # Example — native pool
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use oxirpc_reflect::{DescriptorPoolBuilder, ReflectionBuilder};
//!
//! let pool = DescriptorPoolBuilder::new().build();
//! let (v1, _v1alpha) = ReflectionBuilder::new()
//!     .register_pool(pool)
//!     .build_native();
//! // Mount v1 via tonic::transport::Server::builder().add_service(v1).
//! ```
//!
//! # Example — oxiproto DescriptorPool (feature = "oxiproto")
//!
//! ```rust,no_run
//! # #[cfg(feature = "oxiproto")]
//! # {
//! use std::sync::Arc;
//! use oxirpc_reflect::{ReflectionBuilder};
//!
//! // Build an oxiproto_reflect pool from raw FDS bytes (e.g., include_bytes!).
//! let fds_bytes: &[u8] = &[]; // replace with real FDS bytes
//! let oxi_pool = oxiproto_reflect::pool_from_fds_bytes(fds_bytes)
//!     .expect("valid FDS");
//! let (v1, _v1alpha) = ReflectionBuilder::new()
//!     .register_oxiproto_pool(Arc::new(oxi_pool))
//!     .build_native();
//! // Mount v1 via tonic::transport::Server::builder().add_service(v1).
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_core::wire::NativeBody;
use oxirpc_core::ServerCompressionPrefs;
use prost::Message as _;

use crate::proto::{
    self, server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    ExtensionNumberResponse, FileDescriptorResponse, ListServiceResponse, ServerReflectionRequest,
    ServerReflectionResponse, ServiceResponse,
};
use crate::DescriptorPool;

// ─── Version tag ─────────────────────────────────────────────────────────────

/// Which reflection protocol version this service speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReflectVersion {
    /// The current stable gRPC Server Reflection v1 protocol.
    V1,
    /// The legacy v1alpha protocol (for older gRPC clients).
    V1Alpha,
}

// ─── PoolBackend ─────────────────────────────────────────────────────────────

/// The backing descriptor store for a [`NativeReflectionService`].
///
/// - `Native` — the built-in [`crate::DescriptorPool`] (always available).
/// - `Oxiproto` — the richer [`oxiproto_reflect::DescriptorPool`] from prost-reflect
///   (available when the `oxiproto` feature is enabled).
#[derive(Clone)]
pub(crate) enum PoolBackend {
    /// Built-in prost-types based descriptor pool.
    Native(Arc<DescriptorPool>),
    /// oxiproto-reflect (prost-reflect) descriptor pool.
    #[cfg(feature = "oxiproto")]
    Oxiproto(Arc<oxiproto_reflect::DescriptorPool>),
}

impl PoolBackend {
    /// Return a sorted list of fully-qualified service names.
    pub(crate) fn list_services(&self) -> Vec<String> {
        match self {
            PoolBackend::Native(pool) => pool.list_services(),
            #[cfg(feature = "oxiproto")]
            PoolBackend::Oxiproto(pool) => {
                pool.services().map(|s| s.full_name().to_owned()).collect()
            }
        }
    }

    /// Find the serialised `FileDescriptorProto` whose `name` field equals `name`.
    pub(crate) fn find_file_by_name(&self, name: &str) -> Option<Vec<u8>> {
        match self {
            PoolBackend::Native(pool) => {
                pool.find_file_by_name(name).map(|fdp| fdp.encode_to_vec())
            }
            #[cfg(feature = "oxiproto")]
            PoolBackend::Oxiproto(pool) => pool.get_file_by_name(name).map(|fd| fd.encode_to_vec()),
        }
    }

    /// Find the serialised `FileDescriptorProto` that defines `symbol`.
    ///
    /// `symbol` may optionally start with `.`.
    pub(crate) fn find_file_containing_symbol(&self, symbol: &str) -> Option<Vec<u8>> {
        match self {
            PoolBackend::Native(pool) => pool
                .find_file_containing_symbol(symbol)
                .map(|fdp| fdp.encode_to_vec()),
            #[cfg(feature = "oxiproto")]
            PoolBackend::Oxiproto(pool) => find_oxiproto_file_containing_symbol(pool, symbol),
        }
    }

    /// Find the serialised `FileDescriptorProto` containing an extension of
    /// `containing_type` with the given `extension_number`.
    pub(crate) fn find_file_containing_extension(
        &self,
        containing_type: &str,
        extension_number: i32,
    ) -> Option<Vec<u8>> {
        match self {
            PoolBackend::Native(pool) => pool
                .find_file_containing_extension(containing_type, extension_number)
                .map(|fdp| fdp.encode_to_vec()),
            #[cfg(feature = "oxiproto")]
            PoolBackend::Oxiproto(pool) => {
                find_oxiproto_file_containing_extension(pool, containing_type, extension_number)
            }
        }
    }

    /// Return all extension field numbers defined for `message_name`.
    pub(crate) fn extension_numbers(&self, message_name: &str) -> Vec<i32> {
        match self {
            PoolBackend::Native(pool) => pool.extension_numbers(message_name),
            #[cfg(feature = "oxiproto")]
            PoolBackend::Oxiproto(pool) => extension_numbers_oxiproto(pool, message_name),
        }
    }
}

// ─── oxiproto helpers ─────────────────────────────────────────────────────────

/// Find the serialised bytes of the file that defines `symbol` in an
/// `oxiproto_reflect::DescriptorPool`.
///
/// Searches services, messages, enums, and extensions (via their containing
/// message).  `symbol` may start with `.` (stripped before lookup).
#[cfg(feature = "oxiproto")]
fn find_oxiproto_file_containing_symbol(
    pool: &oxiproto_reflect::DescriptorPool,
    symbol: &str,
) -> Option<Vec<u8>> {
    let sym = symbol.trim_start_matches('.');

    // Service match
    if let Some(svc) = pool.get_service_by_name(sym) {
        return Some(svc.parent_file().encode_to_vec());
    }
    // Message match
    if let Some(msg) = pool.get_message_by_name(sym) {
        return Some(msg.parent_file().encode_to_vec());
    }
    // Enum match
    if let Some(en) = pool.get_enum_by_name(sym) {
        return Some(en.parent_file().encode_to_vec());
    }
    // Extension match — the symbol names an extension field itself
    if let Some(ext) = pool.get_extension_by_name(sym) {
        return Some(ext.parent_file().encode_to_vec());
    }

    // Method match — walk all services for a matching method FQN
    for svc in pool.services() {
        for method in svc.methods() {
            if method.full_name() == sym {
                return Some(svc.parent_file().encode_to_vec());
            }
        }
    }

    None
}

/// Return all extension field numbers for `message_name` in an
/// `oxiproto_reflect::DescriptorPool`.
#[cfg(feature = "oxiproto")]
fn extension_numbers_oxiproto(
    pool: &oxiproto_reflect::DescriptorPool,
    message_name: &str,
) -> Vec<i32> {
    let target = message_name.trim_start_matches('.');
    pool.all_extensions()
        .filter(|ext| ext.containing_message().full_name().trim_start_matches('.') == target)
        .map(|ext| ext.number() as i32)
        .collect()
}

/// Find the file containing an extension field on `containing_type` with the
/// given field number in an `oxiproto_reflect::DescriptorPool`.
#[cfg(feature = "oxiproto")]
fn find_oxiproto_file_containing_extension(
    pool: &oxiproto_reflect::DescriptorPool,
    containing_type: &str,
    extension_number: i32,
) -> Option<Vec<u8>> {
    let target = containing_type.trim_start_matches('.');
    pool.all_extensions()
        .find(|ext| {
            ext.containing_message().full_name().trim_start_matches('.') == target
                && ext.number() as i32 == extension_number
        })
        .map(|ext| ext.parent_file().encode_to_vec())
}

// ─── Core service ────────────────────────────────────────────────────────────

/// The inner bidi-streaming reflection service, shared between the v1 and
/// v1alpha wrappers.
#[derive(Clone)]
pub struct NativeReflectionService {
    backend: PoolBackend,
    version: ReflectVersion,
}

impl NativeReflectionService {
    /// Create a new service backed by the native `pool` speaking the given `version`.
    pub fn new(pool: Arc<DescriptorPool>, version: ReflectVersion) -> Self {
        Self {
            backend: PoolBackend::Native(pool),
            version,
        }
    }

    /// Create a new service backed by an [`oxiproto_reflect::DescriptorPool`]
    /// speaking the given `version`.
    ///
    /// This constructor is only available when the `oxiproto` feature is enabled.
    /// The `oxiproto_reflect::DescriptorPool` provides richer runtime reflection
    /// (e.g., dynamic message construction) compared to the built-in pool while
    /// still implementing all gRPC Server Reflection protocol operations.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "oxiproto")]
    /// # {
    /// use std::sync::Arc;
    /// use oxirpc_reflect::service::{NativeReflectionService, ReflectVersion};
    ///
    /// let fds_bytes: &[u8] = &[]; // replace with real FDS bytes
    /// let pool = oxiproto_reflect::pool_from_fds_bytes(fds_bytes)
    ///     .expect("valid FDS");
    /// let svc = NativeReflectionService::with_oxiproto_pool(
    ///     Arc::new(pool),
    ///     ReflectVersion::V1,
    /// );
    /// # }
    /// ```
    #[cfg(feature = "oxiproto")]
    pub fn with_oxiproto_pool(
        pool: Arc<oxiproto_reflect::DescriptorPool>,
        version: ReflectVersion,
    ) -> Self {
        Self {
            backend: PoolBackend::Oxiproto(pool),
            version,
        }
    }

    /// Returns the reflection protocol version this service speaks.
    pub fn version(&self) -> ReflectVersion {
        self.version
    }

    /// Handle a single [`ServerReflectionRequest`], returning the matching response.
    pub fn handle_request(&self, req: ServerReflectionRequest) -> ServerReflectionResponse {
        let original = req.clone();
        let message_response = match req.message_request {
            None => Some(MessageResponse::ErrorResponse(
                proto::ErrorResponse::not_found("empty request"),
            )),
            Some(MessageRequest::ListServices(_)) => {
                let services = self
                    .backend
                    .list_services()
                    .into_iter()
                    .map(|name| ServiceResponse { name })
                    .collect();
                Some(MessageResponse::ListServicesResponse(ListServiceResponse {
                    service: services,
                }))
            }
            Some(MessageRequest::FileByFilename(name)) => {
                match self.backend.find_file_by_name(&name) {
                    Some(bytes) => Some(MessageResponse::FileDescriptorResponse(
                        FileDescriptorResponse {
                            file_descriptor_proto: vec![bytes],
                        },
                    )),
                    None => Some(MessageResponse::ErrorResponse(
                        proto::ErrorResponse::not_found(name),
                    )),
                }
            }
            Some(MessageRequest::FileContainingSymbol(symbol)) => {
                match self.backend.find_file_containing_symbol(&symbol) {
                    Some(bytes) => Some(MessageResponse::FileDescriptorResponse(
                        FileDescriptorResponse {
                            file_descriptor_proto: vec![bytes],
                        },
                    )),
                    None => Some(MessageResponse::ErrorResponse(
                        proto::ErrorResponse::not_found(symbol),
                    )),
                }
            }
            Some(MessageRequest::FileContainingExtension(ext_req)) => {
                match self.backend.find_file_containing_extension(
                    &ext_req.containing_type,
                    ext_req.extension_number,
                ) {
                    Some(bytes) => Some(MessageResponse::FileDescriptorResponse(
                        FileDescriptorResponse {
                            file_descriptor_proto: vec![bytes],
                        },
                    )),
                    None => Some(MessageResponse::ErrorResponse(
                        proto::ErrorResponse::not_found(format!(
                            "{}.{}",
                            ext_req.containing_type, ext_req.extension_number
                        )),
                    )),
                }
            }
            Some(MessageRequest::AllExtensionNumbersOfType(type_name)) => {
                let numbers = self.backend.extension_numbers(&type_name);
                let base_type_name = type_name.trim_start_matches('.').to_owned();
                Some(MessageResponse::AllExtensionNumbersResponse(
                    ExtensionNumberResponse {
                        base_type_name,
                        extension_number: numbers,
                    },
                ))
            }
        };

        ServerReflectionResponse {
            valid_host: original.host.clone(),
            original_request: Some(original),
            message_response,
        }
    }
}

// ─── tower::Service impl for NativeReflectionService ─────────────────────────

impl<B> tower::Service<http::Request<B>> for NativeReflectionService
where
    B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
    B::Error: Into<oxirpc_core::OxiRpcError> + Send,
{
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move {
            let resp = handle_reflection(svc, req).await;
            Ok(resp)
        })
    }
}

// ─── Compression helpers ──────────────────────────────────────────────────────

/// Return the slice of [`oxirpc_core::encoding::CompressionEncoding`] values this
/// build supports, in preference order (gzip before zstd).
fn supported_encodings() -> &'static [CompressionEncoding] {
    #[cfg(all(feature = "gzip", feature = "zstd"))]
    {
        &[CompressionEncoding::Gzip, CompressionEncoding::Zstd]
    }
    #[cfg(all(feature = "gzip", not(feature = "zstd")))]
    {
        &[CompressionEncoding::Gzip]
    }
    #[cfg(all(feature = "zstd", not(feature = "gzip")))]
    {
        &[CompressionEncoding::Zstd]
    }
    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    {
        &[]
    }
}

/// Negotiate the response encoding from the client's `grpc-accept-encoding` header.
///
/// When `server_prefs` is `Some` and its `send` slice is non-empty, that slice
/// is used in preference to the compiled-feature defaults.
fn negotiate_response_encoding(
    headers: &http::HeaderMap,
    server_prefs: Option<&ServerCompressionPrefs>,
) -> CompressionEncoding {
    let accept = headers
        .get("grpc-accept-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let effective: &[CompressionEncoding] = match server_prefs {
        Some(p) if !p.send.is_empty() => &p.send,
        _ => supported_encodings(),
    };
    if effective.is_empty() {
        return CompressionEncoding::Identity;
    }
    CompressionEncoding::negotiate(accept, effective)
}

/// Build the `grpc-accept-encoding` advertisement string for this build
/// (e.g. `"gzip,identity"` or `"identity"` when no compression is compiled in).
fn server_accept_encoding_value() -> &'static str {
    #[cfg(all(feature = "gzip", feature = "zstd"))]
    {
        "gzip,zstd,identity"
    }
    #[cfg(all(feature = "gzip", not(feature = "zstd")))]
    {
        "gzip,identity"
    }
    #[cfg(all(feature = "zstd", not(feature = "gzip")))]
    {
        "zstd,identity"
    }
    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    {
        "identity"
    }
}

/// Construct the response body, dispatching to the encoding-aware bidi helper
/// when a compressing encoding was negotiated (and the feature is compiled in),
/// otherwise falling through to the identity path.
fn make_bidi_body<B>(
    req: http::Request<B>,
    encoding: CompressionEncoding,
    svc: NativeReflectionService,
) -> NativeBody
where
    B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
    B::Error: Into<oxirpc_core::OxiRpcError> + Send,
{
    use oxirpc_core::wire::server::bidi_sequential_response;

    #[cfg(any(feature = "gzip", feature = "zstd"))]
    if encoding.is_compressing() {
        use oxirpc_core::wire::server::bidi_sequential_response_with_encoding;
        return bidi_sequential_response_with_encoding(
            req.into_body(),
            encoding,
            move |r: ServerReflectionRequest| Ok(svc.handle_request(r)),
        );
    }

    // Silence the unused-variable lint in no-feature builds (encoding == Identity).
    let _ = encoding;
    bidi_sequential_response(req.into_body(), move |r: ServerReflectionRequest| {
        Ok(svc.handle_request(r))
    })
}

// ─────────────────────────────────────────────────────────────────────────────

/// Drive the bidi-sequential reflection RPC using the native server wire helpers.
async fn handle_reflection<B>(
    svc: NativeReflectionService,
    req: http::Request<B>,
) -> http::Response<NativeBody>
where
    B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
    B::Error: Into<oxirpc_core::OxiRpcError> + Send,
{
    use oxirpc_core::wire::server::grpc_response_headers;

    // Extract server prefs from extensions *before* `req` is consumed by make_bidi_body.
    let server_prefs = req
        .extensions()
        .get::<std::sync::Arc<ServerCompressionPrefs>>()
        .map(std::sync::Arc::as_ref);
    let response_enc = negotiate_response_encoding(req.headers(), server_prefs);
    let native_body = make_bidi_body(req, response_enc, svc);

    let mut response = http::Response::new(native_body);
    *response.status_mut() = http::StatusCode::OK;
    let headers = grpc_response_headers();
    for (k, v) in &headers {
        response.headers_mut().insert(k.clone(), v.clone());
    }
    // Advertise supported encodings to the client.
    let accept_val = server_accept_encoding_value();
    if accept_val != "identity" {
        if let Ok(v) = http::HeaderValue::from_str(accept_val) {
            response.headers_mut().insert("grpc-accept-encoding", v);
        }
    }
    // Set response encoding header when compressing.
    if response_enc.is_compressing() {
        if let Ok(v) = http::HeaderValue::from_str(response_enc.as_str()) {
            response.headers_mut().insert("grpc-encoding", v);
        }
    }
    response
}

// ─── V1 wrapper ──────────────────────────────────────────────────────────────

/// A `tower::Service` implementing the stable `grpc.reflection.v1.ServerReflection`
/// protocol (bidi-streaming `ServerReflectionInfo` RPC).
#[derive(Clone)]
pub struct NativeReflectionServiceV1(pub(crate) NativeReflectionService);

impl tonic::server::NamedService for NativeReflectionServiceV1 {
    const NAME: &'static str = "grpc.reflection.v1.ServerReflection";
}

impl<B> tower::Service<http::Request<B>> for NativeReflectionServiceV1
where
    B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
    B::Error: Into<oxirpc_core::OxiRpcError> + Send,
{
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <NativeReflectionService as tower::Service<http::Request<B>>>::poll_ready(&mut self.0, cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        <NativeReflectionService as tower::Service<http::Request<B>>>::call(&mut self.0, req)
    }
}

// ─── V1Alpha wrapper ─────────────────────────────────────────────────────────

/// A `tower::Service` implementing the legacy `grpc.reflection.v1alpha.ServerReflection`
/// protocol (bidi-streaming `ServerReflectionInfo` RPC).
#[derive(Clone)]
pub struct NativeReflectionServiceV1Alpha(pub(crate) NativeReflectionService);

impl tonic::server::NamedService for NativeReflectionServiceV1Alpha {
    const NAME: &'static str = "grpc.reflection.v1alpha.ServerReflection";
}

impl<B> tower::Service<http::Request<B>> for NativeReflectionServiceV1Alpha
where
    B: http_body::Body<Data = bytes::Bytes> + Send + Unpin + 'static,
    B::Error: Into<oxirpc_core::OxiRpcError> + Send,
{
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <NativeReflectionService as tower::Service<http::Request<B>>>::poll_ready(&mut self.0, cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        <NativeReflectionService as tower::Service<http::Request<B>>>::call(&mut self.0, req)
    }
}
