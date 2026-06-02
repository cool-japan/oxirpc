#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! OxiRPC client: plaintext channel builder, load-balancing, resilience, and metrics.
//!
//! [`ClientBuilder`] wraps [`tonic::transport::Endpoint`] with an ergonomic,
//! cloneable configuration surface (timeouts, user-agent, HTTP/2 window sizes,
//! TCP tuning) and produces a connected or lazy [`Channel`].
//!
//! See [`balance`] for endpoint resolution and load-balancing policies, and
//! [`resilience`] for retry, circuit-breaking, and hedging policies.
//!
//! See [`metrics`] for lightweight, `Arc`-shared RPC counters.
//!
//! See [`monitor`] for cooperative connection-state tracking via
//! [`ChannelMonitor`] and [`ConnectionState`].
//!
//! The `tls` feature enables `tls_connector::PureRustTlsConnector` and
//! the `ClientBuilder::tls` method for Pure-Rust encrypted connections.

// Compression policy: only oxiarc-deflate and oxiarc-zstd (via oxirpc-core::encoding).
// Never flate2 or the C-backed zstd crate.
pub mod balance;
pub mod load_reporting;
pub mod metrics;
pub mod monitor;
pub mod native_channel;
pub mod pool;
pub mod resilience;
pub mod xds;

#[cfg(feature = "tls")]
pub mod tls_connector;

pub use load_reporting::CallTelemetry;
pub use metrics::RpcMetrics;
pub use monitor::{ChannelMonitor, ConnectionState};
pub use native_channel::{NativeBody, NativeChannel, NativeChannelBuilder};
pub use pool::{ChannelPool, TypedChannel};
pub use xds::{XdsResolver, XdsWatcher};

/// Pure-Rust TLS connector for use with `tonic::transport::Endpoint::connect_with_connector`.
///
/// Wraps `oxirpc_core::tls::client_config` and `tokio_rustls` to provide a
/// `tower::Service<Uri>` that performs TLS negotiation with the ALPN protocol `h2`
/// without relying on tonic's own FFI-gated TLS types (`tls-ring` / `tls-aws-lc`).
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "tls")]
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use oxirpc_client::PureRustTlsConnector;
/// use oxirpc_core::tls::client_config;
/// use rustls::RootCertStore;
/// use rustls_pki_types::ServerName;
/// use tonic::transport::Endpoint;
///
/// let roots = RootCertStore::empty();
/// let cfg = client_config(roots)?; // trust only the provided root store
/// let server_name = ServerName::try_from("example.com").map_err(|e| e.to_string())?;
/// let connector = PureRustTlsConnector::new(cfg, server_name);
/// let channel = Endpoint::from_static("https://example.com:443")
///     .connect_with_connector(connector)
///     .await?;
/// # let _ = channel;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tls")]
pub use crate::tls_connector::PureRustTlsConnector;

use std::time::Duration;

pub use oxirpc_core::OxiRpcError;
pub use tonic::transport::Channel;

#[cfg(feature = "tls")]
use rustls::ClientConfig as RustlsClientConfig;
#[cfg(feature = "tls")]
use rustls_pki_types::ServerName;

/// Builder for a plaintext (or TLS) gRPC channel.
///
/// Configuration is accumulated and applied when [`ClientBuilder::connect`] or
/// [`ClientBuilder::connect_lazy`] is called. The builder is [`Clone`], so a
/// configured template can be reused to open several channels.
///
/// Note: message compression (gzip/zstd) is configured per-service on the generated
/// `*Client<T>` stub (e.g., `GreeterClient::new(channel).send_compressed(...)`),
/// not on the channel itself.
///
/// ## TLS
///
/// Enable the `tls` feature and call `ClientBuilder::tls` to use Pure-Rust TLS
/// (via `tokio-rustls` + `rustls-rustcrypto`). Build the `rustls::ClientConfig`
/// with `oxirpc_core::tls::client_config`.
///
/// ## Metrics
///
/// Attach an [`RpcMetrics`] instance with [`ClientBuilder::with_metrics`]. All
/// clones of the metrics share the same underlying `Arc` counters.
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    endpoint: String,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    user_agent: Option<String>,
    origin: Option<String>,
    concurrency_limit: Option<usize>,
    init_conn_window: Option<u32>,
    init_stream_window: Option<u32>,
    tcp_nodelay: Option<bool>,
    tcp_keepalive: Option<Duration>,
    http2_keep_alive_interval: Option<Duration>,
    keep_alive_timeout: Option<Duration>,
    keep_alive_while_idle: Option<bool>,
    buffer_size: Option<usize>,
    rate_limit: Option<(u64, Duration)>,
    http2_adaptive_window: Option<bool>,
    max_frame_size: Option<u32>,
    /// Optional metrics hook shared across clones via Arc.
    metrics: Option<RpcMetrics>,
    /// Optional Pure-Rust TLS config (enabled by the `tls` feature).
    #[cfg(feature = "tls")]
    tls_config: Option<(RustlsClientConfig, ServerName<'static>)>,
}

impl ClientBuilder {
    /// Create a builder pointing at the given URI (e.g. `"http://127.0.0.1:50051"`).
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout: None,
            connect_timeout: None,
            user_agent: None,
            origin: None,
            concurrency_limit: None,
            init_conn_window: None,
            init_stream_window: None,
            tcp_nodelay: None,
            tcp_keepalive: None,
            http2_keep_alive_interval: None,
            keep_alive_timeout: None,
            keep_alive_while_idle: None,
            buffer_size: None,
            rate_limit: None,
            http2_adaptive_window: None,
            max_frame_size: None,
            metrics: None,
            #[cfg(feature = "tls")]
            tls_config: None,
        }
    }

    /// Set a per-RPC timeout applied to every request on the channel.
    pub fn timeout(mut self, dur: Duration) -> Self {
        self.timeout = Some(dur);
        self
    }

    /// Set the timeout for establishing the TCP connection.
    pub fn connect_timeout(mut self, dur: Duration) -> Self {
        self.connect_timeout = Some(dur);
        self
    }

    /// Set the `user-agent` header sent with each request.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    /// Override the HTTP/2 `:authority` (origin) header.
    pub fn origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = Some(origin.into());
        self
    }

    /// Limit the number of concurrent in-flight requests on the channel.
    pub fn concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = Some(limit);
        self
    }

    /// Set the HTTP/2 initial connection-level flow-control window (bytes).
    pub fn initial_connection_window_size(mut self, bytes: u32) -> Self {
        self.init_conn_window = Some(bytes);
        self
    }

    /// Set the HTTP/2 initial stream-level flow-control window (bytes).
    pub fn initial_stream_window_size(mut self, bytes: u32) -> Self {
        self.init_stream_window = Some(bytes);
        self
    }

    /// Enable or disable `TCP_NODELAY` (Nagle's algorithm) on the connection.
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = Some(enabled);
        self
    }

    /// Set the TCP keepalive duration.
    pub fn tcp_keepalive(mut self, dur: Duration) -> Self {
        self.tcp_keepalive = Some(dur);
        self
    }

    /// Sets the HTTP/2 keep-alive interval.
    ///
    /// Determines how frequently the client sends keep-alive pings to the server.
    pub fn http2_keep_alive_interval(mut self, dur: Duration) -> Self {
        self.http2_keep_alive_interval = Some(dur);
        self
    }

    /// Sets the HTTP/2 keep-alive timeout.
    ///
    /// If no response is received within this duration after sending a keep-alive
    /// ping, the connection is considered dead and closed.
    pub fn keep_alive_timeout(mut self, dur: Duration) -> Self {
        self.keep_alive_timeout = Some(dur);
        self
    }

    /// Whether to send HTTP/2 keep-alive pings while the connection is idle.
    pub fn keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.keep_alive_while_idle = Some(enabled);
        self
    }

    /// Sets the internal buffer size for outbound requests.
    ///
    /// Pass `None` to use the default.
    pub fn buffer_size(mut self, sz: Option<usize>) -> Self {
        self.buffer_size = sz;
        self
    }

    /// Limits the rate of outbound requests to `limit` per `per` duration.
    pub fn rate_limit(mut self, limit: u64, per: Duration) -> Self {
        self.rate_limit = Some((limit, per));
        self
    }

    /// Enables or disables HTTP/2 adaptive flow control.
    ///
    /// When enabled, the connection-level flow control window is automatically
    /// adjusted based on observed throughput.
    pub fn http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.http2_adaptive_window = Some(enabled);
        self
    }

    /// Sets the HTTP/2 maximum frame size.
    ///
    /// Pass `None` to use the default.
    pub fn max_frame_size(mut self, sz: Option<u32>) -> Self {
        self.max_frame_size = sz;
        self
    }

    /// Attach an [`RpcMetrics`] instance to this builder.
    ///
    /// The metrics counters are shared via `Arc`; cloning the metrics (or the
    /// builder) gives a view into the same underlying counters.
    pub fn with_metrics(mut self, metrics: RpcMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Use Pure-Rust TLS for this connection.
    ///
    /// `config` is a [`rustls::ClientConfig`] (build with
    /// [`oxirpc_core::tls::client_config`] for Pure-Rust, no ring, no aws-lc-rs).
    ///
    /// `server_name` is used for SNI during the TLS handshake. Build it with
    /// `rustls_pki_types::ServerName::try_from("hostname")`.
    ///
    /// ## Feature gate
    ///
    /// Requires the `tls` feature on this crate.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, config: RustlsClientConfig, server_name: ServerName<'static>) -> Self {
        self.tls_config = Some((config, server_name));
        self
    }

    /// Build a configured [`tonic::transport::Endpoint`] from this builder.
    fn build_endpoint(&self) -> Result<tonic::transport::Endpoint, OxiRpcError> {
        let mut ep = tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

        if let Some(t) = self.timeout {
            ep = ep.timeout(t);
        }
        if let Some(t) = self.connect_timeout {
            ep = ep.connect_timeout(t);
        }
        if let Some(ref ua) = self.user_agent {
            ep = ep
                .user_agent(ua.clone())
                .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
        }
        if let Some(ref origin) = self.origin {
            let uri = origin
                .parse()
                .map_err(|e: http::uri::InvalidUri| OxiRpcError::Transport(e.to_string()))?;
            ep = ep.origin(uri);
        }
        if let Some(limit) = self.concurrency_limit {
            ep = ep.concurrency_limit(limit);
        }
        if let Some(w) = self.init_conn_window {
            ep = ep.initial_connection_window_size(w);
        }
        if let Some(w) = self.init_stream_window {
            ep = ep.initial_stream_window_size(w);
        }
        if let Some(nd) = self.tcp_nodelay {
            ep = ep.tcp_nodelay(nd);
        }
        if let Some(k) = self.tcp_keepalive {
            ep = ep.tcp_keepalive(Some(k));
        }
        if let Some(v) = self.http2_keep_alive_interval {
            ep = ep.http2_keep_alive_interval(v);
        }
        if let Some(v) = self.keep_alive_timeout {
            ep = ep.keep_alive_timeout(v);
        }
        if let Some(v) = self.keep_alive_while_idle {
            ep = ep.keep_alive_while_idle(v);
        }
        if let Some(v) = self.buffer_size {
            ep = ep.buffer_size(v);
        }
        if let Some((limit, per)) = self.rate_limit {
            ep = ep.rate_limit(limit, per);
        }
        if let Some(v) = self.http2_adaptive_window {
            ep = ep.http2_adaptive_window(v);
        }
        if let Some(v) = self.max_frame_size {
            ep = ep.max_frame_size(v);
        }
        Ok(ep)
    }

    /// Connect and return a [`Channel`].
    ///
    /// Performs the TCP/H2 handshake immediately.
    ///
    /// When the `tls` feature is enabled and `ClientBuilder::tls` has been
    /// called, the connection is established over Pure-Rust TLS using the
    /// configured `rustls::ClientConfig`.
    pub async fn connect(self) -> Result<Channel, OxiRpcError> {
        #[cfg(feature = "tls")]
        if let Some((config, server_name)) = self.tls_config.clone() {
            let connector = tls_connector::PureRustTlsConnector::new(config, server_name);
            let ep = self.build_endpoint()?;
            return ep
                .connect_with_connector(connector)
                .await
                .map_err(|e| OxiRpcError::Transport(e.to_string()));
        }

        self.build_endpoint()?
            .connect()
            .await
            .map_err(OxiRpcError::from)
    }

    /// Connect lazily — no handshake until the first RPC.
    pub fn connect_lazy(self) -> Result<Channel, OxiRpcError> {
        Ok(self.build_endpoint()?.connect_lazy())
    }

    /// Connects and wraps the channel with a tonic interceptor.
    ///
    /// The returned [`tonic::service::interceptor::InterceptedService`] implements the tower
    /// `Service` trait and can be passed directly to any generated tonic client stub.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URI is invalid or the transport handshake
    /// fails (same as [`ClientBuilder::connect`]).
    pub async fn connect_with_interceptor<F>(
        self,
        f: F,
    ) -> Result<tonic::service::interceptor::InterceptedService<Channel, F>, OxiRpcError>
    where
        F: tonic::service::Interceptor,
    {
        let channel = self.connect().await?;
        Ok(tonic::service::interceptor::InterceptedService::new(
            channel, f,
        ))
    }

    /// Connects lazily and wraps the channel with a tonic interceptor.
    ///
    /// No TCP handshake is performed until the first RPC is sent.  The
    /// [`tonic::service::interceptor::InterceptedService`] can be passed directly to any
    /// generated tonic client stub.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URI is invalid (same as
    /// [`ClientBuilder::connect_lazy`]).
    pub fn connect_lazy_with_interceptor<F>(
        self,
        f: F,
    ) -> Result<tonic::service::interceptor::InterceptedService<Channel, F>, OxiRpcError>
    where
        F: tonic::service::Interceptor,
    {
        let channel = self.connect_lazy()?;
        Ok(tonic::service::interceptor::InterceptedService::new(
            channel, f,
        ))
    }
}
