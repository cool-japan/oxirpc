//! Multi-connection, multi-endpoint NativeChannel.
//!
//! [`NativeChannel`] pools `h2` connections per endpoint, uses a background
//! refresh loop to re-resolve endpoints, and implements [`tower::Service`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{watch, RwLock};

use oxirpc_core::OxiRpcError;

use crate::balance::DynResolver;

use super::body::NativeBody;
use super::call::execute_unary;
use super::connection::{Connection, ConnectionConfig, STATE_DEAD};

// ── ChannelConfig ─────────────────────────────────────────────────────────────

/// Tuning parameters for a [`NativeChannel`].
#[derive(Clone)]
pub struct ChannelConfig {
    /// Maximum number of concurrent streams per connection.
    pub max_concurrent_streams_per_conn: usize,
    /// Maximum number of connections to open to one endpoint.
    pub max_connections_per_endpoint: usize,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Send PING frames while idle.
    pub keep_alive_while_idle: bool,
    /// Initial HTTP/2 connection-level window.
    pub initial_conn_window: u32,
    /// Initial HTTP/2 stream-level window.
    pub initial_stream_window: u32,
    /// How often to re-resolve endpoints in the background.
    pub resolver_refresh_interval: Duration,
    /// Optional `user-agent` header value.
    pub user_agent: Option<String>,
    /// Optional opt-in asynchronous request interceptor (runs before each H2 stream opens).
    pub async_interceptor: Option<std::sync::Arc<dyn oxirpc_core::interceptor::AsyncInterceptor>>,
    /// Optional TLS configuration. `None` → raw TCP.
    ///
    /// When `Some`, all connections in the pool are established over TLS using
    /// the supplied [`crate::native_channel::connection::TlsConfig`].
    #[cfg(feature = "tls")]
    pub tls: Option<super::connection::TlsConfig>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
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
}

impl std::fmt::Debug for ChannelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelConfig")
            .field(
                "max_concurrent_streams_per_conn",
                &self.max_concurrent_streams_per_conn,
            )
            .field(
                "max_connections_per_endpoint",
                &self.max_connections_per_endpoint,
            )
            .field("connect_timeout", &self.connect_timeout)
            .field("keep_alive_while_idle", &self.keep_alive_while_idle)
            .field("initial_conn_window", &self.initial_conn_window)
            .field("initial_stream_window", &self.initial_stream_window)
            .field("resolver_refresh_interval", &self.resolver_refresh_interval)
            .field("user_agent", &self.user_agent)
            .field("async_interceptor_set", &self.async_interceptor.is_some())
            .finish_non_exhaustive()
    }
}

// ── ChannelInner ──────────────────────────────────────────────────────────────

/// Shared interior of a [`NativeChannel`].
pub(crate) struct ChannelInner {
    pub(crate) resolver: Arc<dyn DynResolver>,
    /// Current endpoint list (refreshed periodically).
    pub(crate) endpoints: RwLock<Vec<crate::balance::Endpoint>>,
    /// Maps endpoint authority string → open connections.
    pub(crate) pools: RwLock<HashMap<String, Vec<Arc<Connection>>>>,
    pub(crate) cfg: ChannelConfig,
    /// Becomes `true` once at least one usable connection exists.
    pub(crate) ready_tx: watch::Sender<bool>,
    /// Background refresh task handle. Set once after Arc construction.
    /// Using a `tokio::sync::Mutex<Option<_>>` avoids the Arc::try_unwrap
    /// pattern that would require sole ownership after cloning.
    refresh_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

// ── NativeChannel ─────────────────────────────────────────────────────────────

/// A pure-native HTTP/2 gRPC channel backed by the `h2` crate.
///
/// Manages connection pools per endpoint and implements [`tower::Service`].
#[derive(Clone)]
pub struct NativeChannel {
    pub(crate) inner: Arc<ChannelInner>,
}

impl std::fmt::Debug for NativeChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeChannel")
            .field("cfg", &self.inner.cfg)
            .finish_non_exhaustive()
    }
}

impl NativeChannel {
    /// Construct a new channel. Called by [`NativeChannelBuilder::build`].
    pub(crate) async fn new(
        resolver: Arc<dyn DynResolver>,
        cfg: ChannelConfig,
    ) -> Result<Self, OxiRpcError> {
        // Perform the initial resolution.
        let endpoints = resolver
            .resolve_boxed()
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

        let (ready_tx, _ready_rx) = watch::channel(false);

        // Construct the shared interior with no refresh handle yet.
        let inner = Arc::new(ChannelInner {
            resolver: Arc::clone(&resolver),
            endpoints: RwLock::new(endpoints),
            pools: RwLock::new(HashMap::new()),
            cfg,
            ready_tx,
            refresh_handle: tokio::sync::Mutex::new(None),
        });

        // Spawn the refresh loop — it takes a clone of the Arc, not a borrow.
        // We store the handle inside the ChannelInner so it's cancelled on drop.
        let inner2 = Arc::clone(&inner);
        let handle = tokio::spawn(Self::refresh_loop(inner2));
        *inner.refresh_handle.lock().await = Some(handle);

        Ok(Self { inner })
    }

    /// Wait until at least one healthy connection is available.
    pub async fn ready(&self) {
        let mut rx = self.inner.ready_tx.subscribe();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            // Try to establish a connection immediately.
            if let Ok(conn) = self.pick_connection().await {
                if conn.is_usable() {
                    let _ = self.inner.ready_tx.send(true);
                    return;
                }
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Execute a single gRPC call on this channel.
    pub async fn call(
        &self,
        req: http::Request<NativeBody>,
    ) -> Result<http::Response<NativeBody>, OxiRpcError> {
        let conn = self.pick_connection().await?;
        let slot = conn.try_acquire()?;
        execute_unary(
            conn,
            req,
            None,
            self.inner.cfg.async_interceptor.clone(),
            slot,
        )
        .await
    }

    /// Execute a gRPC call with an explicit deadline.
    pub async fn call_with_deadline(
        &self,
        req: http::Request<NativeBody>,
        deadline: std::time::Instant,
    ) -> Result<http::Response<NativeBody>, OxiRpcError> {
        let conn = self.pick_connection().await?;
        let slot = conn.try_acquire()?;
        execute_unary(
            conn,
            req,
            Some(deadline),
            self.inner.cfg.async_interceptor.clone(),
            slot,
        )
        .await
    }

    /// Pick (or create) a connection from the pool.
    async fn pick_connection(&self) -> Result<Arc<Connection>, OxiRpcError> {
        let endpoints = self.inner.endpoints.read().await;
        if endpoints.is_empty() {
            return Err(OxiRpcError::Transport("no endpoints available".to_owned()));
        }

        // Simple pick: try each endpoint in order, find the first usable conn.
        for ep in endpoints.iter() {
            let key = endpoint_key(ep);
            let pools_read = self.inner.pools.read().await;
            if let Some(conns) = pools_read.get(&key) {
                if let Some(conn) = conns.iter().find(|c| c.is_usable()) {
                    let conn = Arc::clone(conn);
                    drop(pools_read);
                    return Ok(conn);
                }
            }
            drop(pools_read);

            // No usable connection — try to create one.
            let ep = ep.clone();
            if let Ok(conn) = self.try_create_connection(&ep).await {
                let _ = self.inner.ready_tx.send(true);
                return Ok(conn);
            }
        }

        Err(OxiRpcError::Transport(
            "all endpoints are unavailable".to_owned(),
        ))
    }

    /// Create a new connection to `ep` if the pool has room.
    async fn try_create_connection(
        &self,
        ep: &crate::balance::Endpoint,
    ) -> Result<Arc<Connection>, OxiRpcError> {
        let key = endpoint_key(ep);
        let mut pools = self.inner.pools.write().await;
        let conns = pools.entry(key.clone()).or_insert_with(Vec::new);

        // Evict dead connections.
        conns.retain(|c| {
            use std::sync::atomic::Ordering;
            c.state.load(Ordering::Acquire) != STATE_DEAD
        });

        if conns.len() >= self.inner.cfg.max_connections_per_endpoint {
            return Err(OxiRpcError::Transport("connection pool full".to_owned()));
        }

        let conn_cfg = ConnectionConfig {
            connect_timeout: self.inner.cfg.connect_timeout,
            max_concurrent_streams: self.inner.cfg.max_concurrent_streams_per_conn,
            initial_conn_window: self.inner.cfg.initial_conn_window,
            initial_stream_window: self.inner.cfg.initial_stream_window,
            keep_alive_while_idle: self.inner.cfg.keep_alive_while_idle,
            #[cfg(feature = "tls")]
            tls: self.inner.cfg.tls.clone(),
        };

        let conn = Connection::dial(ep, &conn_cfg).await?;
        conns.push(Arc::clone(&conn));
        Ok(conn)
    }

    /// Background loop: re-resolve endpoints at the configured interval.
    async fn refresh_loop(inner: Arc<ChannelInner>) {
        loop {
            tokio::time::sleep(inner.cfg.resolver_refresh_interval).await;
            match inner.resolver.resolve_boxed().await {
                Ok(new_eps) => {
                    let mut eps = inner.endpoints.write().await;
                    *eps = new_eps;
                }
                Err(_) => {
                    // Keep existing endpoints on resolution failure.
                }
            }
        }
    }
}

impl Drop for ChannelInner {
    fn drop(&mut self) {
        // Abort the background refresh task when the last Arc<ChannelInner> drops.
        // The Mutex is synchronous-safe here since we're the last holder.
        if let Ok(mut guard) = self.refresh_handle.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

/// Compute a stable string key for an endpoint (used as pool map key).
fn endpoint_key(ep: &crate::balance::Endpoint) -> String {
    ep.uri
        .authority()
        .map(|a| a.to_string())
        .unwrap_or_else(|| ep.uri.to_string())
}

// ── tower::Service ─────────────────────────────────────────────────────────────

impl tower::Service<http::Request<NativeBody>> for NativeChannel {
    type Response = http::Response<NativeBody>;
    type Error = OxiRpcError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness is checked lazily inside `call`. Always report ready here
        // to satisfy tower's `poll_ready` → `call` invariant.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<NativeBody>) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let ch = NativeChannel { inner };
            ch.call(req).await
        })
    }
}
