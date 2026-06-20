//! Builder for [`NativeChannel`].
//!
//! Use [`NativeChannelBuilder::new`] to configure a channel and call
//! [`NativeChannelBuilder::build`] to construct it.

use std::sync::Arc;
use std::time::Duration;

use oxirpc_core::OxiRpcError;

use crate::balance::{DynResolver, Resolver};

use super::channel::{ChannelConfig, NativeChannel};
#[cfg(feature = "tls")]
use super::connection::TlsConfig;

// ── NativeChannelBuilder ──────────────────────────────────────────────────────

/// Builder for a [`NativeChannel`].
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_client::native_channel::NativeChannelBuilder;
/// use oxirpc_client::balance::StaticResolver;
///
/// # async fn example() -> Result<(), oxirpc_core::OxiRpcError> {
/// let ch = NativeChannelBuilder::new()
///     .resolver(StaticResolver::new(vec![
///         oxirpc_client::balance::Endpoint::new(
///             "http://127.0.0.1:50051".parse().unwrap()
///         ),
///     ]))
///     .connect_timeout(std::time::Duration::from_secs(5))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct NativeChannelBuilder {
    resolver: Option<Box<dyn DynResolver>>,
    max_concurrent_streams_per_conn: usize,
    max_connections_per_endpoint: usize,
    connect_timeout: Duration,
    keep_alive_while_idle: bool,
    initial_conn_window: u32,
    initial_stream_window: u32,
    resolver_refresh_interval: Duration,
    user_agent: Option<String>,
    async_interceptor: Option<std::sync::Arc<dyn oxirpc_core::interceptor::AsyncInterceptor>>,
    /// Optional TLS config (enabled by the `tls` feature).
    #[cfg(feature = "tls")]
    tls: Option<TlsConfig>,
}

impl NativeChannelBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            resolver: None,
            max_concurrent_streams_per_conn: 100,
            max_connections_per_endpoint: 4,
            connect_timeout: Duration::from_secs(10),
            keep_alive_while_idle: false,
            initial_conn_window: 65535,
            initial_stream_window: 65535,
            resolver_refresh_interval: Duration::from_secs(30),
            user_agent: None,
            async_interceptor: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Set the endpoint resolver.
    ///
    /// Any type implementing [`Resolver`] is accepted; it is heap-boxed
    /// and erased to [`DynResolver`] internally.
    pub fn resolver<R: Resolver + 'static>(mut self, r: R) -> Self {
        self.resolver = Some(Box::new(r));
        self
    }

    /// Set the maximum number of concurrent H2 streams per connection.
    ///
    /// Default: 100.
    pub fn max_concurrent_streams_per_conn(mut self, n: usize) -> Self {
        self.max_concurrent_streams_per_conn = n;
        self
    }

    /// Set the maximum number of connections opened to a single endpoint.
    ///
    /// Default: 4.
    pub fn max_connections_per_endpoint(mut self, n: usize) -> Self {
        self.max_connections_per_endpoint = n;
        self
    }

    /// Set the TCP connect timeout.
    ///
    /// Default: 10 seconds.
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = d;
        self
    }

    /// Enable or disable HTTP/2 keep-alive pings while idle.
    ///
    /// Default: `false`.
    pub fn keep_alive_while_idle(mut self, on: bool) -> Self {
        self.keep_alive_while_idle = on;
        self
    }

    /// Set the initial HTTP/2 connection-level flow-control window size.
    ///
    /// Default: 65535 bytes (h2 spec minimum).
    pub fn initial_conn_window(mut self, bytes: u32) -> Self {
        self.initial_conn_window = bytes;
        self
    }

    /// Set the initial HTTP/2 stream-level flow-control window size.
    ///
    /// Default: 65535 bytes.
    pub fn initial_stream_window(mut self, bytes: u32) -> Self {
        self.initial_stream_window = bytes;
        self
    }

    /// Set how often the background task re-resolves endpoints.
    ///
    /// Default: 30 seconds.
    pub fn resolver_refresh_interval(mut self, d: Duration) -> Self {
        self.resolver_refresh_interval = d;
        self
    }

    /// Set the `user-agent` header value sent with each request.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    /// Set an opt-in asynchronous request interceptor.
    ///
    /// The interceptor runs on every outgoing request, just before the H2
    /// stream is opened. It receives a metadata-only
    /// [`oxirpc_core::message::Request`] populated from the request headers and
    /// may mutate that metadata (the changes are merged back into the outgoing
    /// headers) or return a [`oxirpc_core::rpc::Status`] to abort the call.
    ///
    /// When no interceptor is set, the request path is unchanged.
    pub fn with_async_interceptor(
        mut self,
        interceptor: std::sync::Arc<dyn oxirpc_core::interceptor::AsyncInterceptor>,
    ) -> Self {
        self.async_interceptor = Some(interceptor);
        self
    }

    /// Use Pure-Rust TLS for all connections in this channel.
    ///
    /// Pass a [`TlsConfig`] built from a `rustls::ClientConfig` (use
    /// [`oxirpc_core::tls::client_config`] for a Pure-Rust config with h2 ALPN
    /// already set) and a `rustls_pki_types::ServerName` for SNI.
    ///
    /// # Feature gate
    ///
    /// Requires the `tls` feature on this crate.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, cfg: TlsConfig) -> Self {
        self.tls = Some(cfg);
        self
    }

    /// Construct the [`NativeChannel`].
    ///
    /// Performs the initial endpoint resolution. Returns an error if no
    /// resolver has been set or if the initial resolution fails.
    pub async fn build(self) -> Result<NativeChannel, OxiRpcError> {
        let resolver: Arc<dyn DynResolver> = match self.resolver {
            Some(r) => Arc::from(r),
            None => {
                return Err(OxiRpcError::Build(
                    "NativeChannelBuilder: no resolver set".to_owned(),
                ))
            }
        };

        let cfg = ChannelConfig {
            max_concurrent_streams_per_conn: self.max_concurrent_streams_per_conn,
            max_connections_per_endpoint: self.max_connections_per_endpoint,
            connect_timeout: self.connect_timeout,
            keep_alive_while_idle: self.keep_alive_while_idle,
            initial_conn_window: self.initial_conn_window,
            initial_stream_window: self.initial_stream_window,
            resolver_refresh_interval: self.resolver_refresh_interval,
            user_agent: self.user_agent,
            async_interceptor: self.async_interceptor,
            #[cfg(feature = "tls")]
            tls: self.tls,
        };

        NativeChannel::new(resolver, cfg).await
    }
}

impl Default for NativeChannelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeChannel {
    /// Create a [`NativeChannelBuilder`] — convenience alias for
    /// [`NativeChannelBuilder::new`].
    pub fn builder() -> NativeChannelBuilder {
        NativeChannelBuilder::new()
    }
}
