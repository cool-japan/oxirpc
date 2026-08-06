#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! OxiRPC server: plaintext and TLS server builder with graceful shutdown.
//!
//! This is a thin ergonomic wrapper over [`tonic::transport::Server`].
//! For advanced configuration (middleware layers, etc.) access the
//! inner server via [`ServerBuilder::inner`] or [`ServerBuilder::into_inner`].
//!
//! # TLS
//!
//! Enable the `tls` cargo feature, then call `ServerBuilder::tls` with a
//! `rustls::ServerConfig` built via `oxirpc_core::tls::server_config`.  The
//! server will bind a `TcpListener` and feed TLS-wrapped streams into
//! `tonic`'s `serve_with_incoming` path.
//!
//! ```rust,no_run
//! # #[cfg(feature = "tls")]
//! # async fn run() -> Result<(), oxirpc_server::OxiRpcError> {
//! // let cfg = oxirpc_core::tls::server_config(cert_pem, key_pem)?;
//! // oxirpc_server::ServerBuilder::new()
//! //     .tls(cfg)
//! //     .add_service(my_svc)
//! //     .serve("0.0.0.0:50051".parse().unwrap())
//! //     .await?;
//! # Ok(()) }
//! ```
//!
//! # Per-service limits
//!
//! **Per-message size limits and compression settings are per-service only in
//! tonic 0.14.**  They must be configured on the generated `*Server<T>` types
//! (e.g. `HealthServer::max_decoding_message_size`), *not* on
//! `ServerBuilder`. There is no transport-level API for those knobs in
//! tonic 0.14 — do not add them to `ServerBuilder`.
//!
//! # Tower layer middleware
//!
//! **`ServerBuilder::layer` is intentionally absent.**  `tonic::transport::Server::layer`
//! returns `Server<Stack<NewLayer, L>>`, which changes the type parameter.
//! Wrapping that in `ServerBuilder` would require making `ServerBuilder` generic,
//! breaking the existing non-generic API.  `tonic::transport::Router` (used
//! inside `ServeReady`) does not expose a `layer` method in tonic 0.14 either.
//! Access the inner server via [`ServerBuilder::into_inner`] to apply layers
//! before calling `add_service`, then build your own `Router`.
//!
//! # `trace_fn`
//!
//! **`ServerBuilder::trace_fn` is intentionally absent.**  The method requires
//! `tracing::Span` in its bound.  The `tracing` crate is not a dependency of
//! this crate, and adding it would pull in additional transitive deps.
//! Call [`ServerBuilder::into_inner`] and use `tonic::transport::Server::trace_fn`
//! directly if you need tracing integration.

pub mod middleware;
/// OxiRPC-native service name trait that decouples the registry from tonic's
/// [`tonic::server::NamedService`].  See [`OxiNamedService`] for the full API.
pub mod named_service;
pub mod routing;

#[cfg(feature = "native")]
mod compression_layer;
#[cfg(feature = "native")]
use compression_layer::CompressionPrefsLayer;

pub use middleware::{
    IpRateLimiterLayer, IpRateLimiterService, MethodInterceptorBuilder, MethodInterceptorLayer,
    MethodInterceptorService,
};
pub use named_service::OxiNamedService;
pub use routing::MethodRouter;

/// TLS acceptor helpers (requires `tls` feature).
#[cfg(feature = "tls")]
pub mod tls;

/// Native HTTP/2 transport via hyper's `http2::Builder` (requires `native` feature).
#[cfg(feature = "native")]
pub mod native_transport;

/// Native service registry for the hyper H2 transport (requires `native` feature).
///
/// [`NativeServiceRegistry`] collects named services and type-erases them
/// into a [`RegistryService`] that can be passed to the native transport.
#[cfg(feature = "native")]
pub mod native_registry;

#[cfg(feature = "native")]
pub use native_registry::{NativeServiceRegistry, RegistryService};

/// Native HTTP/3 (gRPC-over-QUIC) transport via OxiQUIC + `h3` (requires `http3`).
#[cfg(feature = "http3")]
pub mod native_transport_h3;

#[cfg(feature = "http3")]
pub use native_transport_h3::{bind_h3_endpoint, serve_native_h3_with_service};

/// Re-export of the QUIC transport types needed to drive the HTTP/3 server
/// entry points (requires `http3`).
#[cfg(feature = "http3")]
pub use oxiquic_transport::{ServerEndpoint, TransportConfig};

use std::net::SocketAddr;
#[cfg(feature = "tls")]
use std::sync::Arc;
use std::time::Duration;

pub use oxirpc_core::OxiRpcError;
pub use tonic::transport::Server;
use tonic::{server::NamedService, transport::server::Router};
#[cfg(feature = "native")]
use tower::Layer;
use tower::Service;

// ─── Body-adapter helper ──────────────────────────────────────────────────────

/// Newtype wrapper that adapts a service returning `Response<NativeBody>` to
/// one returning `Response<tonic::body::Body>`.
///
/// This satisfies the `Response<tonic::body::Body>` bound required by the
/// tonic-Routes path (`ServerBuilder::add_service`).  Used by:
/// - The `health_service()` and `reflection_service()` convenience methods.
/// - The public [`ServerBuilder::add_native_service`] / [`ServeReady::add_native_service`]
///   methods, which allow callers to mount any native service (returning
///   `Response<NativeBody>`) onto the standard tonic-Routes path.
///
/// The native registry path (`NativeServiceRegistry`) does not need this wrapper
/// — it accepts `Response<NativeBody>` directly (Phase 4.1).
#[derive(Clone)]
struct NativeBodyAdapter<S>(S);

impl<S> tonic::server::NamedService for NativeBodyAdapter<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<tonic::body::Body>> for NativeBodyAdapter<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<oxirpc_core::wire::NativeBody>,
            Error = std::convert::Infallible,
        > + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let fut = self.0.call(req);
        Box::pin(async move {
            let resp = fut.await?;
            let (parts, native_body) = resp.into_parts();
            let tonic_body = tonic::body::Body::new(native_body);
            Ok(http::Response::from_parts(parts, tonic_body))
        })
    }
}

/// Wrap a native service (returning `Response<NativeBody>`) for use on the
/// tonic-Routes path (which requires `Response<tonic::body::Body>`).
fn wrap_native_body_svc<S>(svc: S) -> NativeBodyAdapter<S>
where
    S: tonic::server::NamedService,
{
    NativeBodyAdapter(svc)
}

/// Builder for a plaintext or TLS gRPC server.
///
/// # Example
///
/// ```rust,no_run
/// # use oxirpc_server::ServerBuilder;
/// # async fn run() {
/// // ServerBuilder::new()
/// //     .add_service(my_svc)
/// //     .serve("0.0.0.0:50051".parse().unwrap())
/// //     .await
/// //     .unwrap();
/// # }
/// ```
pub struct ServerBuilder {
    inner: tonic::transport::Server,
    /// Tracked value of the last `accept_http1` call (default `false`).
    display_accept_http1: bool,
    /// Tracked timeout set via [`ServerBuilder::timeout`].
    display_timeout: Option<Duration>,
    /// Tracked value set via [`ServerBuilder::max_concurrent_streams`].
    display_max_concurrent_streams: Option<u32>,
    /// Tracked value of the last `tcp_nodelay` call (default `true` per tonic).
    display_tcp_nodelay: bool,
    /// Accepted compression encodings (advertised in grpc-accept-encoding responses).
    pub(crate) accept_encoding: Vec<oxirpc_core::encoding::CompressionEncoding>,
    /// Default send encoding for response bodies.
    pub(crate) send_encoding: Option<oxirpc_core::encoding::CompressionEncoding>,
    /// TLS server configuration (requires `tls` feature).
    #[cfg(feature = "tls")]
    tls_config: Option<Arc<rustls::ServerConfig>>,
}

impl ServerBuilder {
    /// Create a new server builder with tonic defaults.
    pub fn new() -> Self {
        Self {
            inner: Server::builder(),
            display_accept_http1: false,
            display_timeout: None,
            display_max_concurrent_streams: None,
            // tonic's default for tcp_nodelay is `true`.
            display_tcp_nodelay: true,
            accept_encoding: Vec::new(),
            send_encoding: None,
            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }

    /// Advertise that the server accepts the given compression encoding.
    ///
    /// Multiple encodings can be registered by calling this method repeatedly.
    /// The encodings are surfaced in the `grpc-accept-encoding` response header
    /// when negotiating per-RPC compression with the client.
    ///
    /// Duplicates are silently deduplicated.
    pub fn accept_compressed(
        mut self,
        encoding: oxirpc_core::encoding::CompressionEncoding,
    ) -> Self {
        if !self.accept_encoding.contains(&encoding) {
            self.accept_encoding.push(encoding);
        }
        self
    }

    /// Set the default compression encoding for server-sent messages.
    ///
    /// Applied when the client indicates acceptance of this encoding via the
    /// `grpc-accept-encoding` request header.  Falls back to
    /// [`CompressionEncoding::Identity`] if the client does not accept the
    /// configured encoding.
    ///
    /// [`CompressionEncoding::Identity`]: oxirpc_core::encoding::CompressionEncoding::Identity
    pub fn send_compressed(mut self, encoding: oxirpc_core::encoding::CompressionEncoding) -> Self {
        self.send_encoding = Some(encoding);
        self
    }

    /// Returns the slice of advertised accepted encodings.
    ///
    /// Use this to inspect which encodings the builder currently advertises,
    /// e.g. for testing or logging.
    pub fn accepted_encodings(&self) -> &[oxirpc_core::encoding::CompressionEncoding] {
        &self.accept_encoding
    }

    /// Returns the configured default send encoding, if any.
    ///
    /// Returns `None` if [`ServerBuilder::send_compressed`] has not been called.
    pub fn send_encoding(&self) -> Option<oxirpc_core::encoding::CompressionEncoding> {
        self.send_encoding
    }

    /// Borrow the underlying [`tonic::transport::Server`] for advanced configuration.
    pub fn inner(&self) -> &tonic::transport::Server {
        &self.inner
    }

    /// Consume the builder, returning the underlying [`tonic::transport::Server`].
    pub fn into_inner(self) -> tonic::transport::Server {
        self.inner
    }

    /// Allow this server to accept HTTP/1.1 requests.
    ///
    /// Useful when deploying `grpc-web` enabled services behind a proxy that
    /// cannot upgrade connections to HTTP/2.  Default is `false`.
    pub fn accept_http1(mut self, accept: bool) -> Self {
        self.inner = self.inner.accept_http1(accept);
        self.display_accept_http1 = accept;
        self
    }

    /// Limit the number of concurrent in-flight requests per connection.
    pub fn concurrency_limit_per_connection(mut self, limit: usize) -> Self {
        self.inner = self.inner.concurrency_limit_per_connection(limit);
        self
    }

    /// Set the maximum number of concurrent HTTP/2 streams per connection.
    pub fn max_concurrent_streams(mut self, max: u32) -> Self {
        self.inner = self.inner.max_concurrent_streams(Some(max));
        self.display_max_concurrent_streams = Some(max);
        self
    }

    /// Apply a per-request timeout to every RPC handled by this server.
    pub fn timeout(mut self, dur: Duration) -> Self {
        self.inner = self.inner.timeout(dur);
        self.display_timeout = Some(dur);
        self
    }

    /// Enable or disable `TCP_NODELAY` on accepted connections.
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.inner = self.inner.tcp_nodelay(enabled);
        self.display_tcp_nodelay = enabled;
        self
    }

    /// Set the TCP keepalive idle duration for accepted connections.
    ///
    /// Equivalent to calling `tonic::transport::Server::tcp_keepalive(Some(dur))`.
    pub fn tcp_keepalive(mut self, dur: Duration) -> Self {
        self.inner = self.inner.tcp_keepalive(Some(dur));
        self
    }

    /// Set the HTTP/2 keepalive ping interval.
    ///
    /// Equivalent to calling `tonic::transport::Server::http2_keepalive_interval(Some(dur))`.
    pub fn http2_keepalive_interval(mut self, dur: Duration) -> Self {
        self.inner = self.inner.http2_keepalive_interval(Some(dur));
        self
    }

    /// Set the timeout for HTTP/2 keepalive ping acknowledgement.
    ///
    /// If the ping is not acknowledged within this duration the connection is
    /// closed.  Has no effect unless [`http2_keepalive_interval`] is also set.
    ///
    /// [`http2_keepalive_interval`]: Self::http2_keepalive_interval
    pub fn http2_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.http2_keepalive_timeout(Some(timeout));
        self
    }

    /// Sets the [`SETTINGS_INITIAL_WINDOW_SIZE`] for HTTP/2 stream-level flow
    /// control.  Default is 65,535.
    ///
    /// [`SETTINGS_INITIAL_WINDOW_SIZE`]: https://httpwg.org/specs/rfc9113.html#InitialWindowSize
    pub fn initial_stream_window_size(mut self, sz: u32) -> Self {
        self.inner = self.inner.initial_stream_window_size(Some(sz));
        self
    }

    /// Sets the max connection-level flow control for HTTP/2.  Default is
    /// 65,535.
    pub fn initial_connection_window_size(mut self, sz: u32) -> Self {
        self.inner = self.inner.initial_connection_window_size(Some(sz));
        self
    }

    /// Sets the maximum HTTP/2 frame size.
    ///
    /// Passing this value overrides the default from the underlying transport
    /// (hyper).  Values outside `[16_384, 16_777_215]` will be clamped by the
    /// HTTP/2 layer.
    pub fn max_frame_size(mut self, sz: u32) -> Self {
        self.inner = self.inner.max_frame_size(Some(sz));
        self
    }

    /// Sets the maximum size of received HTTP/2 header frames.
    ///
    /// Defaults to whatever hyper uses (16 KiB as of v1.4.1).
    pub fn http2_max_header_list_size(mut self, max: u32) -> Self {
        self.inner = self.inner.http2_max_header_list_size(Some(max));
        self
    }

    /// Enable or disable load shedding.
    ///
    /// When enabled, if the underlying service is not ready, the request is
    /// rejected immediately with `resource_exhausted` instead of being
    /// buffered.  Most useful in combination with
    /// [`concurrency_limit_per_connection`].
    ///
    /// [`concurrency_limit_per_connection`]: Self::concurrency_limit_per_connection
    pub fn load_shed(mut self, enabled: bool) -> Self {
        self.inner = self.inner.load_shed(enabled);
        self
    }

    /// Sets the maximum connection age.
    ///
    /// After this duration a connection will begin graceful shutdown.
    /// See also [`max_connection_age_grace`].
    ///
    /// [`max_connection_age_grace`]: Self::max_connection_age_grace
    pub fn max_connection_age(mut self, max_age: Duration) -> Self {
        self.inner = self.inner.max_connection_age(max_age);
        self
    }

    /// Sets the grace period after [`max_connection_age`] during which in-flight
    /// RPCs may complete before the connection is forcefully closed.
    ///
    /// [`max_connection_age`]: Self::max_connection_age
    pub fn max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.inner = self.inner.max_connection_age_grace(grace);
        self
    }

    /// Enables or disables HTTP/2 adaptive flow control.
    pub fn http2_adaptive_window(mut self, enabled: Option<bool>) -> Self {
        self.inner = self.inner.http2_adaptive_window(enabled);
        self
    }

    /// Sets max HTTP/2 pending accept reset streams.
    pub fn http2_max_pending_accept_reset_streams(mut self, max: Option<usize>) -> Self {
        self.inner = self.inner.http2_max_pending_accept_reset_streams(max);
        self
    }

    /// Sets max HTTP/2 local error reset streams.
    pub fn http2_max_local_error_reset_streams(mut self, max: Option<usize>) -> Self {
        self.inner = self.inner.http2_max_local_error_reset_streams(max);
        self
    }

    /// Sets the interval for TCP keepalive probes.
    ///
    /// Note: ignored when using `serve_with_incoming` / `serve_unix` — configure keepalive
    /// on the `TcpListener` directly in that case.
    pub fn tcp_keepalive_interval(mut self, dur: Option<Duration>) -> Self {
        self.inner = self.inner.tcp_keepalive_interval(dur);
        self
    }

    /// Sets the number of TCP keepalive retries before dropping the connection.
    pub fn tcp_keepalive_retries(mut self, retries: Option<u32>) -> Self {
        self.inner = self.inner.tcp_keepalive_retries(retries);
        self
    }

    /// Configure TLS by wrapping the provided `rustls::ServerConfig`.
    ///
    /// Once set, [`ServeReady::serve`] and [`ServeReady::serve_with_shutdown`]
    /// will bind a `TcpListener` and perform TLS handshakes via `tokio-rustls`.
    /// The pure-Rust OxiTLS RustCrypto provider is used; no `ring` or `aws-lc`
    /// is pulled onto normal dependency edges.
    ///
    /// The `ServerConfig` is wrapped in an [`Arc`] internally.  Use
    /// [`ServerBuilder::tls_arc`] if you already hold an `Arc<ServerConfig>`.
    ///
    /// Requires the `tls` cargo feature.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, config: rustls::ServerConfig) -> Self {
        self.tls_config = Some(Arc::new(config));
        self
    }

    /// Configure TLS from an already-`Arc`-wrapped `rustls::ServerConfig`.
    ///
    /// Useful when the config is shared between multiple builders.
    ///
    /// Requires the `tls` cargo feature.
    #[cfg(feature = "tls")]
    pub fn tls_arc(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Returns `true` if a TLS config has been set via [`ServerBuilder::tls`]
    /// or [`ServerBuilder::tls_arc`].
    ///
    /// Requires the `tls` cargo feature.
    #[cfg(feature = "tls")]
    pub fn is_tls(&self) -> bool {
        self.tls_config.is_some()
    }

    /// Register a gRPC server reflection service (v1) from raw
    /// `FileDescriptorSet` bytes.
    ///
    /// This is a convenience wrapper that decodes `fds` into a
    /// [`oxirpc_reflect::DescriptorPool`] and adds a
    /// [`oxirpc_reflect::NativeReflectionServiceV1`] to this server via
    /// [`ServerBuilder::add_service`].
    ///
    /// `fds` must contain a proto-encoded `FileDescriptorSet` (e.g., the output
    /// of `prost::Message::encode_to_vec` on a `prost_types::FileDescriptorSet`
    /// or the bytes produced by `protox::compile(...).encode_to_vec()`).
    ///
    /// # Errors
    ///
    /// Returns [`oxirpc_reflect::ReflectError::Decode`] if the bytes cannot be
    /// decoded as a valid `FileDescriptorSet`.
    ///
    /// Requires the `reflect` cargo feature.
    #[cfg(feature = "reflect")]
    pub fn reflection_service(
        self,
        fds: bytes::Bytes,
    ) -> Result<ServeReady, oxirpc_reflect::ReflectError> {
        // Validate bytes by decoding via DescriptorPoolBuilder::register_bytes.
        let pool = oxirpc_reflect::DescriptorPoolBuilder::new()
            .register_bytes(&fds)?
            .build();
        let svc = oxirpc_reflect::ReflectionBuilder::new()
            .register_pool(pool)
            .build_native_v1();
        // NativeReflectionServiceV1 now returns Response<NativeBody> (Phase 4.1).
        // Wrap for the tonic-Routes (add_service) path.
        Ok(self.add_service(wrap_native_body_svc(svc)))
    }

    /// Convenience: attach the native health-check service to this server.
    ///
    /// Equivalent to calling
    /// `.add_service(oxirpc_health::NativeHealthService::default())`.
    /// The health service is backed by a fresh, empty [`oxirpc_health::HealthState`];
    /// all services report `SERVICE_UNKNOWN` until updated.
    ///
    /// Gate: requires the `health` cargo feature on `oxirpc-server`.
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "health")]
    /// # async fn run() -> Result<(), oxirpc_server::OxiRpcError> {
    /// oxirpc_server::ServerBuilder::new()
    ///     .health_service()
    ///     .serve("0.0.0.0:50051".parse().unwrap())
    ///     .await?;
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "health")]
    pub fn health_service(self) -> ServeReady {
        // NativeHealthService now returns Response<NativeBody> (Phase 4.1).
        // The tonic-Routes path (add_service) requires Response<tonic::body::Body>,
        // so we wrap the response body here at the ServerBuilder boundary.
        self.add_service(wrap_native_body_svc(
            oxirpc_health::NativeHealthService::default(),
        ))
    }

    /// Add a service and return a [`ServeReady`] that can be bound to an address.
    /// Add a service and return a [`ServeReady`] that can be bound to an address.
    pub fn add_service<S>(mut self, svc: S) -> ServeReady
    where
        S: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Response: axum::response::IntoResponse,
    {
        #[cfg(feature = "native")]
        let native_routes = tonic::service::Routes::new(svc.clone());
        let router = self.inner.add_service(svc);
        ServeReady {
            router,
            #[cfg(feature = "tls")]
            tls_config: self.tls_config,
            #[cfg(feature = "native")]
            native_routes,
            send_encoding: self.send_encoding,
            accept_encoding: self.accept_encoding,
        }
    }

    /// Add a native gRPC service (returning `Response<NativeBody>`) to the router.
    ///
    /// Use this method when mounting services that return
    /// `Response<oxirpc_core::wire::NativeBody>` — such as a
    /// `oxirpc_health::NativeHealthService` or
    /// `oxirpc_reflect::NativeReflectionServiceV1` built manually.  The native
    /// response body is bridged to the tonic-Routes path via the internal
    /// `NativeBodyAdapter`.
    ///
    /// For convenience methods that build health or reflection services
    /// internally, see `health_service` and `reflection_service`.
    pub fn add_native_service<S>(self, svc: S) -> ServeReady
    where
        S: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<oxirpc_core::wire::NativeBody>,
                Error = std::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.add_service(wrap_native_body_svc(svc))
    }

    // ─── Direct registry entry points (no tonic Routes needed) ───────────────

    /// Bind `addr` and serve using a [`NativeServiceRegistry`] directly, without
    /// requiring any services to be added to the tonic router first.
    ///
    /// This is the preferred entry point when all services are native (i.e., return
    /// `Response<NativeBody>`).  No call to [`add_service`](Self::add_service) is needed.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry(
        self,
        addr: std::net::SocketAddr,
        registry: crate::native_registry::NativeServiceRegistry,
    ) -> Result<(), OxiRpcError> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        self.serve_native_registry_with_listener(listener, registry)
            .await
    }

    /// Serve from a pre-bound listener using a [`NativeServiceRegistry`] directly,
    /// without requiring any services to be added to the tonic router first.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry_with_listener(
        self,
        listener: tokio::net::TcpListener,
        registry: crate::native_registry::NativeServiceRegistry,
    ) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        let tls_config = self.tls_config;

        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let svc = CompressionPrefsLayer::new(prefs).layer(registry.into_service());

        native_transport::serve_native_with_service(
            listener,
            svc,
            #[cfg(feature = "tls")]
            tls_config,
            #[cfg(not(feature = "tls"))]
            None::<std::convert::Infallible>,
            None,
        )
        .await
    }

    /// Serve from a pre-bound listener using a [`NativeServiceRegistry`], shutting
    /// down gracefully when `signal` resolves.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry_with_listener_shutdown(
        self,
        listener: tokio::net::TcpListener,
        registry: crate::native_registry::NativeServiceRegistry,
        signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), OxiRpcError> {
        let (tx, rx) = tokio::sync::watch::channel(());
        tokio::spawn(async move {
            signal.await;
            let _ = tx.send(());
        });

        #[cfg(feature = "tls")]
        let tls_config = self.tls_config;

        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let svc = CompressionPrefsLayer::new(prefs).layer(registry.into_service());

        native_transport::serve_native_with_service(
            listener,
            svc,
            #[cfg(feature = "tls")]
            tls_config,
            #[cfg(not(feature = "tls"))]
            None::<std::convert::Infallible>,
            Some(rx),
        )
        .await
    }

    /// Bind `bind_addr` as a QUIC endpoint and serve a [`NativeServiceRegistry`]
    /// over HTTP/3 (gRPC-over-QUIC).
    ///
    /// `server_cfg` must be a QUIC-capable rustls [`ServerConfig`](rustls::ServerConfig)
    /// built from [`oxirpc_core::tls::server_config_h3`]. Bind to port `0` for an
    /// OS-assigned port and use
    /// [`serve_native_registry_h3_with_endpoint`](Self::serve_native_registry_h3_with_endpoint)
    /// when the assigned port must be observed.
    ///
    /// Requires the `http3` cargo feature.
    #[cfg(feature = "http3")]
    pub async fn serve_native_registry_h3(
        self,
        bind_addr: std::net::SocketAddr,
        server_cfg: Arc<rustls::ServerConfig>,
        registry: crate::native_registry::NativeServiceRegistry,
    ) -> Result<(), OxiRpcError> {
        let endpoint = native_transport_h3::bind_h3_endpoint(
            bind_addr,
            server_cfg,
            oxiquic_transport::TransportConfig::default(),
        )
        .await?;
        self.serve_native_registry_h3_with_endpoint(endpoint, registry, None)
            .await
    }

    /// Serve a [`NativeServiceRegistry`] over HTTP/3 from a pre-bound QUIC
    /// `ServerEndpoint`.
    ///
    /// Binding the endpoint separately lets the caller read the OS-assigned port
    /// via `endpoint.local_addr()` before serving. `shutdown` optionally stops
    /// the accept loop when it fires.
    ///
    /// Requires the `http3` cargo feature.
    #[cfg(feature = "http3")]
    pub async fn serve_native_registry_h3_with_endpoint(
        self,
        endpoint: oxiquic_transport::ServerEndpoint,
        registry: crate::native_registry::NativeServiceRegistry,
        shutdown: Option<tokio::sync::watch::Receiver<()>>,
    ) -> Result<(), OxiRpcError> {
        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let svc = CompressionPrefsLayer::new(prefs).layer(registry.into_service());
        native_transport_h3::serve_native_h3_with_service(endpoint, svc, shutdown).await
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timeout = match self.display_timeout {
            Some(d) => format!("{}ms", d.as_millis()),
            None => "none".to_owned(),
        };
        let max_streams = match self.display_max_concurrent_streams {
            Some(n) => n.to_string(),
            None => "none".to_owned(),
        };
        // Format accepted encodings as a bracket-enclosed list, e.g. "[gzip, zstd]".
        let accept_enc = if self.accept_encoding.is_empty() {
            "[]".to_owned()
        } else {
            let parts: Vec<&str> = self.accept_encoding.iter().map(|e| e.as_str()).collect();
            format!("[{}]", parts.join(", "))
        };

        #[cfg(feature = "tls")]
        let tls_flag = self.tls_config.is_some();
        #[cfg(not(feature = "tls"))]
        let tls_flag = false;

        write!(
            f,
            "ServerBuilder(accept_http1={}, timeout={}, max_concurrent_streams={}, tcp_nodelay={}, accept_encoding={}, tls={})",
            self.display_accept_http1, timeout, max_streams, self.display_tcp_nodelay, accept_enc, tls_flag,
        )
    }
}

/// A server ready to be bound and served.
///
/// Obtained via [`ServerBuilder::add_service`].  Use [`ServeReady::add_service`]
/// to register additional services before binding.
pub struct ServeReady {
    router: Router,
    /// TLS server configuration (requires `tls` feature).
    #[cfg(feature = "tls")]
    tls_config: Option<Arc<rustls::ServerConfig>>,
    /// Mirrored service routes for the native hyper H2 transport (requires `native` feature).
    /// Maintained in parallel with `router` so `serve_native_*` can call it directly.
    #[cfg(feature = "native")]
    native_routes: tonic::service::Routes,
    /// Server-side send encoding configured via [`ServerBuilder::send_compressed`].
    /// Propagated into native serve paths via [`CompressionPrefsLayer`].
    send_encoding: Option<oxirpc_core::encoding::CompressionEncoding>,
    /// Accepted inbound compression encodings configured via [`ServerBuilder::accept_compressed`].
    /// Propagated into native serve paths via [`CompressionPrefsLayer`] for validation.
    accept_encoding: Vec<oxirpc_core::encoding::CompressionEncoding>,
}

impl ServeReady {
    /// Add another service to the already-configured router.
    ///
    /// This allows multiple gRPC services to be hosted on the same port:
    ///
    /// ```rust,no_run
    /// # use oxirpc_server::ServerBuilder;
    /// # use tower::Service;
    /// # async fn run<A, B>(svc_a: A, svc_b: B)
    /// # where
    /// #     A: tonic::server::NamedService
    /// #         + Service<http::Request<tonic::body::Body>,
    /// #             Response = http::Response<tonic::body::Body>,
    /// #             Error = std::convert::Infallible>
    /// #         + Clone + Send + Sync + 'static,
    /// #     A::Future: Send + 'static,
    /// #     A::Response: axum::response::IntoResponse,
    /// #     B: tonic::server::NamedService
    /// #         + Service<http::Request<tonic::body::Body>,
    /// #             Response = http::Response<tonic::body::Body>,
    /// #             Error = std::convert::Infallible>
    /// #         + Clone + Send + Sync + 'static,
    /// #     B::Future: Send + 'static,
    /// #     B::Response: axum::response::IntoResponse,
    /// # {
    /// ServerBuilder::new()
    ///     .add_service(svc_a)
    ///     .add_service(svc_b)  // <-- multi-service chaining
    ///     .serve("0.0.0.0:50051".parse().unwrap())
    ///     .await
    ///     .ok();
    /// # }
    /// ```
    ///
    /// # Graceful drain
    ///
    /// Graceful drain is handled by [`ServeReady::serve_with_shutdown`]: pass
    /// a future that resolves when the shutdown signal is received (e.g. from
    /// `tokio::signal::ctrl_c()`).  In-flight RPCs will be allowed to complete
    /// before the server exits.
    pub fn add_service<S>(self, svc: S) -> ServeReady
    where
        S: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Response: axum::response::IntoResponse,
    {
        #[cfg(feature = "native")]
        let native_routes = self.native_routes.add_service(svc.clone());
        ServeReady {
            router: self.router.add_service(svc),
            #[cfg(feature = "tls")]
            tls_config: self.tls_config,
            #[cfg(feature = "native")]
            native_routes,
            send_encoding: self.send_encoding,
            accept_encoding: self.accept_encoding,
        }
    }

    /// Add an additional native service to an already-configured router.
    ///
    /// See [`ServerBuilder::add_native_service`] for details.
    pub fn add_native_service<S>(self, svc: S) -> ServeReady
    where
        S: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<oxirpc_core::wire::NativeBody>,
                Error = std::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.add_service(wrap_native_body_svc(svc))
    }

    /// Serve on the given address until the process exits.
    ///
    /// When a TLS config is present (set via `ServerBuilder::tls`), binds a
    /// `TcpListener` at `addr` and serves over TLS.  Otherwise falls through to
    /// the plain-HTTP/2 path.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        if let Some(tls_cfg) = self.tls_config {
            use tokio::net::TcpListener;
            use tokio_rustls::TlsAcceptor;
            let acceptor = TlsAcceptor::from(tls_cfg);
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
            let incoming = crate::tls::incoming_tls(listener, acceptor);
            return self
                .router
                .serve_with_incoming(incoming)
                .await
                .map_err(OxiRpcError::from);
        }
        self.router.serve(addr).await.map_err(OxiRpcError::from)
    }

    /// Serve with a graceful-shutdown signal.
    ///
    /// The server will stop accepting new connections once the `signal` future
    /// resolves.  In-flight RPCs are allowed to complete before the server exits.
    ///
    /// When a TLS config is present, the TLS path is used (same as [`serve`]).
    ///
    /// [`serve`]: Self::serve
    pub async fn serve_with_shutdown(
        self,
        addr: SocketAddr,
        signal: impl std::future::Future<Output = ()>,
    ) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        if let Some(tls_cfg) = self.tls_config {
            use tokio::net::TcpListener;
            use tokio_rustls::TlsAcceptor;
            let acceptor = TlsAcceptor::from(tls_cfg);
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
            let incoming = crate::tls::incoming_tls(listener, acceptor);
            return self
                .router
                .serve_with_incoming_shutdown(incoming, signal)
                .await
                .map_err(OxiRpcError::from);
        }
        self.router
            .serve_with_shutdown(addr, signal)
            .await
            .map_err(OxiRpcError::from)
    }

    /// Serve over a pre-bound [`tokio::net::TcpListener`].
    ///
    /// This lets the caller learn the bound address *before* serving — useful
    /// when binding to port `0` for an OS-assigned port:
    ///
    /// ```rust,no_run
    /// # use oxirpc_server::ServerBuilder;
    /// # async fn run(svc_ready: oxirpc_server::ServeReady) -> Result<(), oxirpc_server::OxiRpcError> {
    /// let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
    ///     .map_err(|e| oxirpc_server::OxiRpcError::Transport(e.to_string()))?;
    /// let addr = listener.local_addr()
    ///     .map_err(|e| oxirpc_server::OxiRpcError::Transport(e.to_string()))?;
    /// println!("listening on {addr}");
    /// svc_ready.serve_with_listener(listener).await
    /// # }
    /// ```
    ///
    /// When a TLS config is present (set via `ServerBuilder::tls`), the
    /// pre-bound listener is wrapped with a TLS layer before serving.
    pub async fn serve_with_listener(
        self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        if let Some(tls_cfg) = self.tls_config {
            use tokio_rustls::TlsAcceptor;
            let acceptor = TlsAcceptor::from(tls_cfg);
            let incoming = crate::tls::incoming_tls(listener, acceptor);
            return self
                .router
                .serve_with_incoming(incoming)
                .await
                .map_err(OxiRpcError::from);
        }
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        self.router
            .serve_with_incoming(incoming)
            .await
            .map_err(OxiRpcError::from)
    }

    /// Serve over a pre-bound listener with a graceful-shutdown signal.
    ///
    /// See [`ServeReady::serve_with_listener`] for the address-discovery pattern.
    ///
    /// When a TLS config is present (set via `ServerBuilder::tls`), the
    /// pre-bound listener is wrapped with a TLS layer before serving.
    pub async fn serve_with_listener_shutdown(
        self,
        listener: tokio::net::TcpListener,
        signal: impl std::future::Future<Output = ()>,
    ) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        if let Some(tls_cfg) = self.tls_config {
            use tokio_rustls::TlsAcceptor;
            let acceptor = TlsAcceptor::from(tls_cfg);
            let incoming = crate::tls::incoming_tls(listener, acceptor);
            return self
                .router
                .serve_with_incoming_shutdown(incoming, signal)
                .await
                .map_err(OxiRpcError::from);
        }
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        self.router
            .serve_with_incoming_shutdown(incoming, signal)
            .await
            .map_err(OxiRpcError::from)
    }

    /// Serves on a Unix domain socket at the given path.
    ///
    /// Binds `path`, removes any existing socket file first, and serves until completion.
    /// TCP keepalive settings from `ServerBuilder` are ignored for Unix sockets.
    ///
    /// Only available on Unix platforms.
    #[cfg(unix)]
    pub async fn serve_unix(self, path: impl AsRef<std::path::Path>) -> Result<(), OxiRpcError> {
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::UnixListenerStream;
        let listener =
            UnixListener::bind(path).map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        let stream = UnixListenerStream::new(listener);
        self.router
            .serve_with_incoming(stream)
            .await
            .map_err(OxiRpcError::from)
    }

    /// Serves on a Unix domain socket, shutting down when `signal` resolves.
    ///
    /// Only available on Unix platforms.
    #[cfg(unix)]
    pub async fn serve_unix_with_shutdown<F>(
        self,
        path: impl AsRef<std::path::Path>,
        signal: F,
    ) -> Result<(), OxiRpcError>
    where
        F: std::future::Future<Output = ()>,
    {
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::UnixListenerStream;
        let listener =
            UnixListener::bind(path).map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        let stream = UnixListenerStream::new(listener);
        self.router
            .serve_with_incoming_shutdown(stream, signal)
            .await
            .map_err(OxiRpcError::from)
    }

    // ─── Native HTTP/2 transport (requires `native` feature) ─────────────────

    /// Serve gRPC over HTTP/2 on `addr` using the native hyper H2 transport.
    ///
    /// Unlike [`serve`](Self::serve), this binds a [`tokio::net::TcpListener`]
    /// and drives the connection directly with `hyper::server::conn::http2::Builder`
    /// — tonic's transport layer is not in the hot path.
    ///
    /// When the `tls` feature is also enabled and a TLS config has been set
    /// via [`ServerBuilder::tls`], connections are wrapped with TLS before the
    /// H2 handshake.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native(self, addr: std::net::SocketAddr) -> Result<(), OxiRpcError> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        self.serve_native_with_listener(listener).await
    }

    /// Serve gRPC over HTTP/2 on `addr`, shutting down gracefully when `signal`
    /// resolves.
    ///
    /// See [`serve_native`](Self::serve_native) for details.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_with_shutdown(
        self,
        addr: std::net::SocketAddr,
        signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), OxiRpcError> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        self.serve_native_with_listener_shutdown(listener, signal)
            .await
    }

    /// Serve gRPC over HTTP/2 on a pre-bound [`tokio::net::TcpListener`] using
    /// the native hyper H2 transport.
    ///
    /// Use this variant when you need to know the bound address before serving
    /// (e.g., when binding to port `0` for an OS-assigned port):
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "native")]
    /// # async fn run(ready: oxirpc_server::ServeReady) -> Result<(), oxirpc_server::OxiRpcError> {
    /// let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
    ///     .map_err(|e| oxirpc_server::OxiRpcError::Transport(e.to_string()))?;
    /// let addr = listener.local_addr()
    ///     .map_err(|e| oxirpc_server::OxiRpcError::Transport(e.to_string()))?;
    /// println!("listening on {addr}");
    /// ready.serve_native_with_listener(listener).await
    /// # }
    /// ```
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_with_listener(
        self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        let tls_config = self.tls_config;

        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let routes = self.native_routes.prepare();
        let svc = CompressionPrefsLayer::for_routes(prefs).layer(routes);

        native_transport::serve_native_with_service(
            listener,
            svc,
            #[cfg(feature = "tls")]
            tls_config,
            #[cfg(not(feature = "tls"))]
            None::<std::convert::Infallible>,
            None,
        )
        .await
    }

    /// Serve gRPC over HTTP/2 on a pre-bound listener, shutting down gracefully
    /// when `signal` resolves.
    ///
    /// See [`serve_native_with_listener`](Self::serve_native_with_listener) for
    /// the address-discovery pattern.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_with_listener_shutdown(
        self,
        listener: tokio::net::TcpListener,
        signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), OxiRpcError> {
        let (tx, rx) = tokio::sync::watch::channel(());
        tokio::spawn(async move {
            signal.await;
            let _ = tx.send(());
        });

        #[cfg(feature = "tls")]
        let tls_config = self.tls_config;

        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let routes = self.native_routes.prepare();
        let svc = CompressionPrefsLayer::for_routes(prefs).layer(routes);

        native_transport::serve_native_with_service(
            listener,
            svc,
            #[cfg(feature = "tls")]
            tls_config,
            #[cfg(not(feature = "tls"))]
            None::<std::convert::Infallible>,
            Some(rx),
        )
        .await
    }

    // ─── Native registry transport ────────────────────────────────────────────

    /// Serve gRPC over HTTP/2 on `addr` using a [`NativeServiceRegistry`].
    ///
    /// Unlike [`serve_native`](Self::serve_native), the service set is defined
    /// by the provided registry rather than the services added via
    /// [`ServerBuilder::add_service`].  This allows full control over which
    /// services are exposed on the native H2 path without also routing them
    /// through tonic's transport layer.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry(
        self,
        addr: std::net::SocketAddr,
        registry: crate::native_registry::NativeServiceRegistry,
    ) -> Result<(), OxiRpcError> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        self.serve_native_registry_with_listener(listener, registry)
            .await
    }

    /// Serve gRPC over HTTP/2 on a pre-bound listener using a
    /// [`NativeServiceRegistry`].
    ///
    /// Use this variant when you need to know the bound address before serving
    /// (e.g., binding to port `0` for an OS-assigned port).
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry_with_listener(
        self,
        listener: tokio::net::TcpListener,
        registry: crate::native_registry::NativeServiceRegistry,
    ) -> Result<(), OxiRpcError> {
        #[cfg(feature = "tls")]
        let tls_config = self.tls_config;

        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let svc = CompressionPrefsLayer::new(prefs).layer(registry.into_service());

        native_transport::serve_native_with_service(
            listener,
            svc,
            #[cfg(feature = "tls")]
            tls_config,
            #[cfg(not(feature = "tls"))]
            None::<std::convert::Infallible>,
            None,
        )
        .await
    }

    /// Serve gRPC over HTTP/2 on `addr` using a [`NativeServiceRegistry`],
    /// shutting down gracefully when `signal` resolves.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry_with_shutdown(
        self,
        addr: std::net::SocketAddr,
        registry: crate::native_registry::NativeServiceRegistry,
        signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), OxiRpcError> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        self.serve_native_registry_with_listener_shutdown(listener, registry, signal)
            .await
    }

    /// Serve gRPC over HTTP/2 on a pre-bound listener using a
    /// [`NativeServiceRegistry`], shutting down gracefully when `signal`
    /// resolves.
    ///
    /// Requires the `native` cargo feature.
    #[cfg(feature = "native")]
    pub async fn serve_native_registry_with_listener_shutdown(
        self,
        listener: tokio::net::TcpListener,
        registry: crate::native_registry::NativeServiceRegistry,
        signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), OxiRpcError> {
        let (tx, rx) = tokio::sync::watch::channel(());
        tokio::spawn(async move {
            signal.await;
            let _ = tx.send(());
        });

        #[cfg(feature = "tls")]
        let tls_config = self.tls_config;

        let prefs = oxirpc_core::ServerCompressionPrefs {
            send: self.send_encoding.into_iter().collect(),
            accept: self.accept_encoding,
        };
        let svc = CompressionPrefsLayer::new(prefs).layer(registry.into_service());

        native_transport::serve_native_with_service(
            listener,
            svc,
            #[cfg(feature = "tls")]
            tls_config,
            #[cfg(not(feature = "tls"))]
            None::<std::convert::Infallible>,
            Some(rx),
        )
        .await
    }
}
