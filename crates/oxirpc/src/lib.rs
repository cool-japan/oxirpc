#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # oxirpc — Pure-Rust gRPC stack
//!
//! `oxirpc` is a facade over [tonic](https://docs.rs/tonic/0.14) that provides
//! ergonomic re-exports, native gRPC primitives, and COOLJAPAN Pure-Rust defaults
//! (no ring, no openssl, no native-tls on default features).
//!
//! ## Quick start
//!
//! Add to your `Cargo.toml`:
//! ```toml
//! [dependencies]
//! oxirpc = { version = "0.1", features = ["client", "server", "tls"] }
//! ```
//!
//! Add to your `build.rs`:
//! ```rust,ignore
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     oxirpc_build::compile_protos(&["proto/service.proto"], &["proto/"])?;
//!     Ok(())
//! }
//! ```
//!
//! ```rust,no_run
//! # #[cfg(feature = "client")]
//! # {
//! // Client side
//! async fn run_client() -> Result<(), oxirpc::OxiRpcError> {
//!     let _channel = oxirpc::ClientBuilder::new("http://127.0.0.1:50051")
//!         .connect()
//!         .await?;
//!     // let client = MyServiceClient::new(_channel);  // generated client
//!     Ok(())
//! }
//! # }
//! ```
//!
//! ## Feature flags
//!
//! | Feature | What it enables |
//! |---------|----------------|
//! | `client` | `ClientBuilder`, channel pool, load balancing, resilience |
//! | `server` | `ServerBuilder`, middleware layers |
//! | `tls` | TLS via OxiTLS (Pure-Rust, no ring) |
//! | `compression` | OxiARC-backed compress/decompress API (`OxiArcGzip`) |
//! | `gzip` | gzip message compression via OxiARC (oxiarc-deflate) |
//! | `zstd` | Zstandard compression via OxiARC (oxiarc-zstd) |
//! | `reflect` | gRPC server reflection (v1 + v1alpha) |
//! | `health` | gRPC health checking protocol |
//! | `web` | gRPC-Web bridge (HTTP/1.1 → gRPC) + CORS layer |
//! | `oxiproto` | OxiProto type-system bridge — `proto::OxiMessage`, `proto::OxiProtoError`, prost types |
//! | `full` | All Pure-Rust sub-features (no `oxiproto`) |
//!
//! ## Migrating from tonic
//!
//! `oxirpc` is a transparent wrapper over tonic 0.14. If you already use tonic,
//! migrating to oxirpc requires only dependency and import changes:
//!
//! ```text
//! Before (tonic):
//!   use tonic::transport::Server;
//!   use tonic::{Request, Response, Status};
//!
//! After (oxirpc):
//!   use oxirpc::ServerBuilder;  // wrapper with ergonomic extras
//!   use oxirpc::{Request, Response, Status};  // same types, re-exported
//! ```
//!
//! All tonic-generated stubs work with oxirpc without modification.
//!
//! ## Architecture
//!
//! ```text
//! oxirpc (facade)
//! ├── oxirpc-core   — native types: Status, Metadata, Codec, Timeout, Encoding
//! ├── oxirpc-client — ClientBuilder, load balancing, resilience (retry/circuit-breaker)
//! ├── oxirpc-server — ServerBuilder, middleware layers
//! ├── oxirpc-reflect — gRPC reflection services (v1/v1alpha)
//! ├── oxirpc-health — health checking + K8s probes
//! ├── oxirpc-web    — gRPC-Web codec + CORS tower layer
//! └── oxirpc-build  — build-time proto compilation (no protoc required)
//! ```

pub use oxirpc_core::{
    Code, Metadata, OxiRpcError, OxiRpcResult, Request, Response, Status, StatusCode,
};

/// Native gRPC primitives re-exported from `oxirpc-core`.
///
/// - [`oxirpc_core::status::StatusCode`] — all 17 gRPC status codes.
/// - [`oxirpc_core::metadata::Metadata`] — ASCII + binary metadata.
/// - [`oxirpc_core::timeout`] — `grpc-timeout` parse/format.
/// - [`oxirpc_core::encoding::CompressionEncoding`] — Identity / Gzip / Zstd.
pub mod core {
    pub use oxirpc_core::{encoding, metadata, status, timeout};
}

#[cfg(feature = "client")]
pub use oxirpc_client::ClientBuilder;

#[cfg(feature = "server")]
pub use oxirpc_server::ServerBuilder;

/// OxiRPC client types: builder, channel pool, typed channels, and metrics.
///
/// Enable with the `client` feature.  All items are also available at the
/// top-level facade (e.g. `oxirpc::ClientBuilder`).
#[cfg(feature = "client")]
pub mod client {
    pub use oxirpc_client::balance;
    pub use oxirpc_client::resilience;
    pub use oxirpc_client::{ChannelPool, ClientBuilder, RpcMetrics, TypedChannel};
}

/// OxiRPC server types: builder, middleware layers.
///
/// Enable with the `server` feature.  `ServerBuilder` is also available at
/// the top-level facade as `oxirpc::ServerBuilder`.
#[cfg(feature = "server")]
pub mod server {
    pub use oxirpc_server::{
        IpRateLimiterLayer, IpRateLimiterService, MethodInterceptorBuilder, MethodInterceptorLayer,
        MethodInterceptorService, ServerBuilder,
    };
}

/// The version of the `oxirpc` crate, as reported by Cargo at build time.
///
/// # Example
///
/// ```rust
/// assert!(!oxirpc::version().is_empty());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(feature = "server")]
/// Start a gRPC server on `addr` serving a single `service`.
///
/// A one-liner convenience that creates a [`ServerBuilder`], mounts the service, and serves.
/// For advanced configuration (TLS, keepalive, multi-service), use [`ServerBuilder`] directly.
pub async fn serve<S>(addr: std::net::SocketAddr, service: S) -> Result<(), OxiRpcError>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    <S as tower::Service<http::Request<tonic::body::Body>>>::Future: Send + 'static,
    <S as tower::Service<http::Request<tonic::body::Body>>>::Response: axum::response::IntoResponse,
{
    ServerBuilder::new().add_service(service).serve(addr).await
}

#[cfg(feature = "server")]
/// Start a gRPC server on `addr` and shut down when `signal` resolves.
pub async fn serve_with_shutdown<S, F>(
    addr: std::net::SocketAddr,
    service: S,
    signal: F,
) -> Result<(), OxiRpcError>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    <S as tower::Service<http::Request<tonic::body::Body>>>::Future: Send + 'static,
    <S as tower::Service<http::Request<tonic::body::Body>>>::Response: axum::response::IntoResponse,
    F: std::future::Future<Output = ()>,
{
    ServerBuilder::new()
        .add_service(service)
        .serve_with_shutdown(addr, signal)
        .await
}

#[cfg(feature = "client")]
/// Connect to a gRPC server at `endpoint` (e.g., `"http://localhost:50051"`).
///
/// Performs an immediate TCP/H2 handshake. Use [`connect_lazy`] to defer the handshake.
pub async fn connect(endpoint: &str) -> Result<tonic::transport::Channel, OxiRpcError> {
    ClientBuilder::new(endpoint).connect().await
}

#[cfg(feature = "client")]
/// Connect lazily to a gRPC server — no TCP handshake until the first RPC.
pub fn connect_lazy(endpoint: &str) -> Result<tonic::transport::Channel, OxiRpcError> {
    ClientBuilder::new(endpoint).connect_lazy()
}

/// Glob-importable common types.
///
/// ```rust
/// use oxirpc::prelude::*;
/// let _ok = StatusCode::Ok;
/// ```
pub mod prelude {
    pub use crate::core::encoding::CompressionEncoding;
    pub use crate::version;
    pub use oxirpc_core::{
        Code, Metadata, OxiRpcError, OxiRpcResult, Request, Response, Status, StatusCode,
    };

    #[cfg(feature = "client")]
    pub use crate::ClientBuilder;
    #[cfg(feature = "server")]
    pub use crate::ServerBuilder;
}

/// TLS configuration helpers (Pure Rust, no ring, no FFI).
///
/// Construct [`rustls::ClientConfig`] and [`rustls::ServerConfig`] via the
/// OxiTLS RustCrypto provider. Both configs get `h2` ALPN set automatically.
/// Enable with the `tls` feature.
///
/// # Note on tonic wiring
///
/// Tonic 0.14's `ServerTlsConfig`/`ClientTlsConfig` require the `tls-ring` or
/// `tls-aws-lc` feature, which pull FFI crates onto normal edges. Full tonic TLS
/// round-trip wiring is therefore deferred to M3. See open question #5 in TODO.md.
#[cfg(feature = "tls")]
pub use oxirpc_core::tls;

/// gRPC server reflection helpers (v1 + v1alpha).
///
/// Enable with the `reflect` feature. Wraps [`oxirpc_reflect`]'s
/// `reflection_service_owned` / `reflection_service_from_static` builders.
/// The returned service can be added to a tonic server via `add_service`.
#[cfg(feature = "reflect")]
pub mod reflect {
    pub use oxirpc_reflect::*;
}

/// gRPC-Web bridge tower layer.
///
/// Enable with the `web` feature. Wraps [`oxirpc_web`]'s [`GrpcWebLayer`]
/// and exposes [`grpc_web_layer`] for convenience.
///
/// [`GrpcWebLayer`]: oxirpc_web::GrpcWebLayer
/// [`grpc_web_layer`]: oxirpc_web::grpc_web_layer
#[cfg(feature = "web")]
pub mod web {
    pub use oxirpc_web::*;
}

/// gRPC health checking service (v1 — Check + Watch).
///
/// Enable with the `health` feature. Use [`HealthBuilder`] to construct a health
/// service and handle pair.  Call [`HealthBuilder::build_native`] for a
/// `NativeHealthService` backed by [`HealthState`], or [`HealthBuilder::build`] for
/// a standard `HealthServer<impl Health>` mountable on a tonic server.
///
/// [`HealthBuilder`]: oxirpc_health::HealthBuilder
/// [`HealthBuilder::build_native`]: oxirpc_health::HealthBuilder::build_native
/// [`HealthBuilder::build`]: oxirpc_health::HealthBuilder::build
/// [`HealthState`]: oxirpc_health::HealthState
#[cfg(feature = "health")]
pub mod health {
    pub use oxirpc_health::{HealthBuilder, HealthHandle, ServingStatus};
}

/// Ready-to-use gRPC interceptor patterns.
///
/// Provides the following [`tonic::service::Interceptor`] implementations:
///
/// - [`crate::interceptors::BearerAuthInterceptor`] — `Authorization: Bearer` validation.
/// - [`crate::interceptors::TracingInterceptor`] — monotonic `x-request-id` injection.
/// - [`crate::interceptors::DeadlineInterceptor`] — millisecond-precision `grpc-timeout` (back-compat).
/// - [`crate::interceptors::RateLimitInterceptor`] — token-bucket rate limiting.
/// - [`crate::interceptors::LoggingInterceptor`] — callback-based request logging.
/// - [`crate::interceptors::MetricsInterceptor`] — atomic request counters with `snapshot()`.
/// - [`crate::interceptors::TimeoutInterceptor`] — precise `grpc-timeout` via `format_grpc_timeout`.
/// - [`crate::interceptors::InterceptorChain`] — composable ordered chain with short-circuit on error.
pub mod interceptors;

/// Build utilities — see the `oxirpc-build` crate for direct use in `build.rs`.
///
/// `oxirpc-build` is a *build-dependency*; it cannot be re-exported as a normal
/// runtime dependency here. In a `build.rs`, add `oxirpc-build` to
/// `[build-dependencies]` and call it directly:
///
/// ```rust,ignore
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     oxirpc_build::compile_protos(&["proto/service.proto"], &["proto/"])?;
///     Ok(())
/// }
/// ```
///
/// This module documents the integration contract and provides links; it holds
/// no runtime items.
pub mod build {}

/// gRPC message compression via OxiArc DEFLATE (pure Rust gzip, no flate2).
///
/// Exposes [`oxirpc_core::compression::OxiArcGzip`] for manual
/// compress/decompress of gRPC message payloads, and
/// [`oxirpc_core::compression::CompressionError`] for error handling. Full
/// wire-level tower middleware (Slice 7b) will hook into tonic's 5-byte frame
/// format.
///
/// Enable with the `compression` feature.
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "compression")]
/// # {
/// use oxirpc::compression::OxiArcGzip;
///
/// let data = b"hello gRPC!";
/// let compressed = OxiArcGzip::compress(data).unwrap();
/// let decompressed = OxiArcGzip::decompress(&compressed).unwrap();
/// assert_eq!(&decompressed, data);
/// # }
/// ```
#[cfg(feature = "compression")]
pub mod compression {
    pub use oxirpc_core::compression::*;
}

/// Proto type system integration via [OxiProto](https://github.com/cool-japan/oxiproto).
///
/// Enable with the `oxiproto` feature. Exposes the core OxiProto traits and
/// error types through the `oxirpc` facade, so downstream users can write
/// `use oxirpc::proto::OxiProtoError` without a direct `oxiproto` dependency.
///
/// This module provides access to OxiProto's wire-format traits, error type,
/// and prost compatibility types. For reflection types (`DescriptorPool`,
/// `DynamicMessage`), enable oxiproto's own `reflect` feature directly.
///
/// # Example
///
/// ```rust,no_run
/// #[cfg(feature = "oxiproto")]
/// {
///     use oxirpc::proto::{OxiMessage, OxiName, OxiProtoError, OxiProtoResult};
///     // prost_types gives access to well-known proto descriptors:
///     use oxirpc::proto::prost_types::FileDescriptorSet;
///
///     // Construct a canonical proto error type:
///     let _err: OxiProtoError = OxiProtoError::ParseError("invalid field".into());
///     // Access FileDescriptorSet through the prost_types re-export:
///     let _fds: FileDescriptorSet = FileDescriptorSet::default();
/// }
/// ```
#[cfg(feature = "oxiproto")]
pub mod proto {
    pub use oxiproto::prost_types;
    pub use oxiproto::wire;
    pub use oxiproto::{
        Extensions, Message, Name, OxiMessage, OxiName, OxiOneof, OxiProtoError, OxiProtoResult,
    };
}
