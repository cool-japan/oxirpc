//! Single-endpoint HTTP/3 (gRPC-over-QUIC) channel.
//!
//! [`H3Channel`] dials one endpoint over QUIC and multiplexes every gRPC call
//! onto the resulting HTTP/3 connection (QUIC natively supports many concurrent
//! streams, so a single connection suffices). The connection is established
//! lazily on the first call and transparently re-dialled if it dies.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use oxiquic_transport::TransportConfig;
use oxirpc_core::OxiRpcError;

use super::super::body::NativeBody;
use super::call::execute_h3;
use super::connection::{H3Connection, DEFAULT_MAX_FIELD_SECTION_SIZE};

// ── H3Channel ─────────────────────────────────────────────────────────────────

/// A pure-native HTTP/3 gRPC channel to a single endpoint.
///
/// Cheaply cloneable: clones share the same lazily-established connection.
#[derive(Clone)]
pub struct H3Channel {
    inner: Arc<Inner>,
}

struct Inner {
    addr: SocketAddr,
    server_name: String,
    tls: Arc<rustls::ClientConfig>,
    transport: TransportConfig,
    max_field_section_size: u64,
    /// The current live connection, established lazily and re-dialled on death.
    conn: Mutex<Option<Arc<H3Connection>>>,
}

impl std::fmt::Debug for H3Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Channel")
            .field("addr", &self.inner.addr)
            .field("server_name", &self.inner.server_name)
            .finish_non_exhaustive()
    }
}

impl H3Channel {
    /// Eagerly establish the underlying HTTP/3 connection.
    ///
    /// Not required — [`call`](Self::call) connects lazily — but useful to fail
    /// fast or to warm the connection before the first request.
    ///
    /// # Errors
    ///
    /// Returns [`OxiRpcError::Transport`] if the QUIC/HTTP-3 handshake fails.
    pub async fn ready(&self) -> Result<(), OxiRpcError> {
        self.get_or_connect().await.map(|_| ())
    }

    /// Execute a single gRPC call on this channel.
    ///
    /// # Errors
    ///
    /// Returns a typed [`OxiRpcError`] on transport failure or an error status.
    pub async fn call(
        &self,
        req: http::Request<NativeBody>,
    ) -> Result<http::Response<NativeBody>, OxiRpcError> {
        self.dispatch(req, None).await
    }

    /// Execute a gRPC call with an explicit deadline.
    ///
    /// # Errors
    ///
    /// Returns [`OxiRpcError::Timeout`] if the deadline elapses, or any other
    /// typed error from the call.
    pub async fn call_with_deadline(
        &self,
        req: http::Request<NativeBody>,
        deadline: Instant,
    ) -> Result<http::Response<NativeBody>, OxiRpcError> {
        self.dispatch(req, Some(deadline)).await
    }

    async fn dispatch(
        &self,
        req: http::Request<NativeBody>,
        deadline: Option<Instant>,
    ) -> Result<http::Response<NativeBody>, OxiRpcError> {
        let conn = self.get_or_connect().await?;
        let result = execute_h3(Arc::clone(&conn), req, deadline).await;
        // On a transport-level failure the connection may be dead; drop the cached
        // handle so the next call transparently re-dials.
        if let Err(OxiRpcError::Transport(_)) = &result {
            let mut guard = self.inner.conn.lock().await;
            if let Some(cur) = guard.as_ref() {
                if Arc::ptr_eq(cur, &conn) {
                    *guard = None;
                }
            }
        }
        result
    }

    /// Return the cached connection or dial a fresh one.
    async fn get_or_connect(&self) -> Result<Arc<H3Connection>, OxiRpcError> {
        let mut guard = self.inner.conn.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(Arc::clone(conn));
        }
        let conn = H3Connection::connect(
            self.inner.addr,
            &self.inner.server_name,
            Arc::clone(&self.inner.tls),
            self.inner.transport.clone(),
            self.inner.max_field_section_size,
        )
        .await?;
        *guard = Some(Arc::clone(&conn));
        Ok(conn)
    }
}

// ── H3ChannelBuilder ──────────────────────────────────────────────────────────

/// Builder for [`H3Channel`].
///
/// The socket address, SNI server name, and a QUIC-capable
/// [`rustls::ClientConfig`] (from [`oxirpc_core::tls::client_config_h3`]) are
/// required; the transport config and `SETTINGS_MAX_FIELD_SECTION_SIZE` are
/// optional.
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "http3")]
/// # async fn ex() -> Result<(), oxirpc_core::OxiRpcError> {
/// use std::sync::Arc;
/// use rustls::RootCertStore;
/// use oxirpc_client::native_channel::h3::H3ChannelBuilder;
///
/// let roots = RootCertStore::empty();
/// let tls = oxirpc_core::tls::client_config_h3_arc(roots)?;
/// let channel = H3ChannelBuilder::new()
///     .addr("127.0.0.1:4433".parse().unwrap())
///     .server_name("localhost")
///     .tls(tls)
///     .build()?;
/// # let _ = channel;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct H3ChannelBuilder {
    addr: Option<SocketAddr>,
    server_name: Option<String>,
    tls: Option<Arc<rustls::ClientConfig>>,
    transport: Option<TransportConfig>,
    max_field_section_size: Option<u64>,
}

impl H3ChannelBuilder {
    /// Create a new, empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the server socket address to dial (required).
    #[must_use]
    pub fn addr(mut self, addr: SocketAddr) -> Self {
        self.addr = Some(addr);
        self
    }

    /// Set the SNI / certificate server name (required).
    #[must_use]
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Set the QUIC-capable rustls client config (required).
    ///
    /// Must be built from [`oxirpc_core::tls::client_config_h3`] /
    /// [`oxirpc_core::tls::client_config_h3_arc`].
    #[must_use]
    pub fn tls(mut self, tls: Arc<rustls::ClientConfig>) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Override the QUIC transport configuration (default: [`TransportConfig::default`]).
    #[must_use]
    pub fn transport(mut self, transport: TransportConfig) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Override `SETTINGS_MAX_FIELD_SECTION_SIZE` (default 16 384).
    #[must_use]
    pub fn max_field_section_size(mut self, size: u64) -> Self {
        self.max_field_section_size = Some(size);
        self
    }

    /// Build the [`H3Channel`] (does not connect yet).
    ///
    /// # Errors
    ///
    /// Returns [`OxiRpcError::Build`] if `addr`, `server_name`, or `tls` is missing.
    pub fn build(self) -> Result<H3Channel, OxiRpcError> {
        let addr = self
            .addr
            .ok_or_else(|| OxiRpcError::Build("H3ChannelBuilder: addr is required".to_owned()))?;
        let server_name = self.server_name.ok_or_else(|| {
            OxiRpcError::Build("H3ChannelBuilder: server_name is required".to_owned())
        })?;
        let tls = self
            .tls
            .ok_or_else(|| OxiRpcError::Build("H3ChannelBuilder: tls is required".to_owned()))?;

        Ok(H3Channel {
            inner: Arc::new(Inner {
                addr,
                server_name,
                tls,
                transport: self.transport.unwrap_or_default(),
                max_field_section_size: self
                    .max_field_section_size
                    .unwrap_or(DEFAULT_MAX_FIELD_SECTION_SIZE),
                conn: Mutex::new(None),
            }),
        })
    }
}
