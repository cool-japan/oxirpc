#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! `oxirpc-web` — gRPC-Web bridge for OxiRPC.
//!
//! Wraps [`tonic_web`]'s [`GrpcWebLayer`] as an oxirpc-flavored tower layer.
//! Adding this layer to a tonic server enables browser clients using the
//! gRPC-Web protocol (base64 framing + CORS handling).
//!
//! In addition to the tonic-web layer, this crate provides native, Pure-Rust,
//! dependency-light primitives for building a gRPC-Web bridge directly:
//!
//! - [`codec`] — gRPC-Web message framing (5-byte prefix, trailer frames,
//!   base64 text mode, compression via OxiARC).
//! - [`cors`] — a [`cors::CorsPolicy`] builder that produces preflight and
//!   response header sets.
//! - [`mod@negotiate`] — content-type negotiation, header translation,
//!   [`negotiate::GrpcWebConfig`], and [`negotiate::StreamSequencer`].
//! - [`prefix`] — [`GrpcWebPrefixLayer`] strips a URI path prefix from
//!   incoming requests, enabling sub-path mounting.
//! - [`translate`] — low-level gRPC-Web ↔ gRPC request/response translation
//!   functions and [`translate::GrpcWebContentType`] detection.
//! - [`native`] — [`native::NativeGrpcWebLayer`] / [`native::NativeGrpcWebService`]:
//!   a fully native Pure-Rust gRPC-Web Tower layer that does not depend on
//!   tonic-web at runtime.  Use this as an alternative to the tonic-web path
//!   when you need full control over frame encoding and trailer handling.
//!
//! ## Two gRPC-Web paths
//!
//! | Path | Entry point | Notes |
//! |------|-------------|-------|
//! | **tonic-web** | [`grpc_web_layer()`] | Wraps `tonic_web::GrpcWebLayer`. Mature, well-tested upstream. |
//! | **native** | [`native_grpc_web_layer()`] | Pure-Rust, no tonic-web runtime dep. Full trailer embedding. |
//!
//! # HTTP/1.1 requirement
//!
//! All gRPC-Web layers require the server to have `accept_http1(true)` set —
//! gRPC-Web requests from browsers arrive over HTTP/1.1 by default.
//!
//! ## Integrating with oxirpc-server
//!
//! When using the OxiRPC server builder, enable HTTP/1.1 so that browser
//! clients (which speak gRPC-Web over HTTP/1.1) can connect.  The pattern is:
//!
//! ```text
//! use oxirpc_server::ServerBuilder;
//! use oxirpc_web::grpc_web_layer;
//!
//! // ServerBuilder::new()
//! //     .accept_http1(true)
//! //     .layer(grpc_web_layer())
//! //     .add_service(my_service)
//! //     .serve(addr)
//! //     .await?;
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use oxirpc_web::grpc_web_layer;
//!
//! // tonic::transport::Server::builder()
//! //     .accept_http1(true)
//! //     .layer(grpc_web_layer())
//! //     .add_service(my_service)
//! //     .serve(addr)
//! //     .await?;
//! ```

pub use tonic_web::GrpcWebLayer;
pub use tonic_web::GrpcWebService;

pub mod codec;
pub mod cors;
pub mod native;
pub mod negotiate;
pub mod prefix;
pub mod translate;
pub mod transport;

pub use prefix::{GrpcWebPrefixLayer, GrpcWebPrefixService};

pub use negotiate::{
    metadata_to_wire_headers, negotiate, response_content_type, wire_headers_to_metadata,
    GrpcWebConfig, StreamSequencer, WebMode,
};

pub use native::{
    native_grpc_web, native_grpc_web_layer, NativeGrpcWebLayer, NativeGrpcWebService,
};
pub use translate::GrpcWebContentType;
pub use transport::GrpcWebServer;

/// Create a new gRPC-Web tower layer.
///
/// Apply this layer to a tonic server or router to enable the gRPC-Web protocol
/// translation (base64 message framing and CORS header handling).
///
/// To also handle HTTP/1.1 connections you must call
/// `tonic::transport::Server::builder().accept_http1(true)` — gRPC-Web requests
/// from browsers arrive over HTTP/1.1 by default.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_web::grpc_web_layer;
///
/// let layer = grpc_web_layer();
/// // tonic::transport::Server::builder()
/// //     .accept_http1(true)
/// //     .layer(layer)
/// //     .add_service(my_service)
/// //     .serve(addr)
/// //     .await?;
/// ```
pub fn grpc_web_layer() -> GrpcWebLayer {
    GrpcWebLayer::new()
}

/// gRPC-Web layer composed with CORS support.
///
/// Returns a [`tower_layer::Stack`] that first handles CORS (outermost —
/// intercepts `OPTIONS` preflights before any gRPC-Web translation) and then
/// applies the standard [`tonic_web::GrpcWebLayer`] translation.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_web::{grpc_web_layer_with_cors, cors::CorsPolicy};
///
/// let layer = grpc_web_layer_with_cors(CorsPolicy::new().allow_any_origin());
/// // tonic::transport::Server::builder()
/// //     .accept_http1(true)
/// //     .layer(layer)
/// //     .add_service(my_service)
/// //     .serve(addr)
/// //     .await?;
/// ```
pub fn grpc_web_layer_with_cors(
    cors: cors::CorsPolicy,
) -> tower_layer::Stack<cors::CorsLayer, GrpcWebLayer> {
    tower_layer::Stack::new(cors::CorsLayer::new(cors), GrpcWebLayer::new())
}

/// gRPC-Web layer with optional CORS, driven by a [`negotiate::GrpcWebConfig`].
///
/// When `cors` is `Some`, CORS headers are injected outermost (before gRPC-Web
/// translation).  When `cors` is `None`, an allow-any-origin policy is applied.
///
/// The `_config` parameter is accepted for API stability and future integration
/// when the native gRPC-Web bridge gains configuration-aware behaviour.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_web::{grpc_web_layer_with_config, cors::CorsPolicy, negotiate::{GrpcWebConfig, WebMode}};
///
/// let layer = grpc_web_layer_with_config(
///     GrpcWebConfig::new(WebMode::Binary),
///     Some(CorsPolicy::new().allow_origin("https://app.example.com")),
/// );
/// ```
pub fn grpc_web_layer_with_config(
    _config: negotiate::GrpcWebConfig,
    cors: Option<cors::CorsPolicy>,
) -> tower_layer::Stack<cors::CorsLayer, GrpcWebLayer> {
    let cors_policy = cors.unwrap_or_else(|| cors::CorsPolicy::new().allow_any_origin());
    grpc_web_layer_with_cors(cors_policy)
}

/// Create a composed Tower layer: [`GrpcWebPrefixLayer`] + [`GrpcWebLayer`].
///
/// The outer layer strips `prefix` from the request URI; the inner layer
/// translates gRPC-Web framing to native gRPC.  Use this to expose gRPC-Web
/// on a sub-path (e.g., `/api`).
///
/// # Note
///
/// The server **must** have `accept_http1(true)` set for gRPC-Web to work
/// with HTTP/1.1 clients.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_web::grpc_web_layer_with_prefix;
///
/// let layer = grpc_web_layer_with_prefix("/api");
/// // tonic::transport::Server::builder()
/// //     .accept_http1(true)
/// //     .layer(layer)
/// //     .add_service(my_service)
/// //     .serve(addr)
/// //     .await?;
/// ```
pub fn grpc_web_layer_with_prefix(
    prefix: impl Into<String>,
) -> tower_layer::Stack<GrpcWebLayer, GrpcWebPrefixLayer> {
    tower_layer::Stack::new(GrpcWebLayer::new(), GrpcWebPrefixLayer::new(prefix))
}
