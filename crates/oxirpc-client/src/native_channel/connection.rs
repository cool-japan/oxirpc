//! Single HTTP/2 connection wrapper for NativeChannel.
//!
//! [`Connection`] owns one `h2::client::SendRequest` and the background driver
//! task. It tracks in-flight stream count, draining/dead state, and translates
//! h2 errors to [`OxiRpcError`].

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h2::client::{Builder as H2Builder, ResponseFuture, SendRequest};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

use oxirpc_core::OxiRpcError;

use crate::balance::Endpoint;

// ── AnyIo (TLS feature) ───────────────────────────────────────────────────────

/// A pinned enum over the two possible transport streams.
///
/// When the `tls` feature is disabled this type is not present; `connection.rs`
/// uses `tokio::net::TcpStream` directly.  When the `tls` feature is enabled the
/// h2 handshake receives an `AnyIo` value so that both plain-TCP and TLS-over-TCP
/// connections share a single code path.
#[cfg(feature = "tls")]
mod any_io {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use pin_project_lite::pin_project;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::TcpStream;
    use tokio_rustls::client::TlsStream;

    pin_project! {
        /// Either a raw TCP stream or a TLS-over-TCP stream.
        #[project = AnyIoProj]
        pub(super) enum AnyIo {
            /// Unencrypted TCP connection.
            Plain { #[pin] inner: TcpStream },
            /// TLS-encrypted TCP connection via `tokio-rustls`.
            Tls   { #[pin] inner: TlsStream<TcpStream> },
        }
    }

    impl AsyncRead for AnyIo {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.project() {
                AnyIoProj::Plain { inner } => inner.poll_read(cx, buf),
                AnyIoProj::Tls { inner } => inner.poll_read(cx, buf),
            }
        }
    }

    impl AsyncWrite for AnyIo {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.project() {
                AnyIoProj::Plain { inner } => inner.poll_write(cx, buf),
                AnyIoProj::Tls { inner } => inner.poll_write(cx, buf),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.project() {
                AnyIoProj::Plain { inner } => inner.poll_flush(cx),
                AnyIoProj::Tls { inner } => inner.poll_flush(cx),
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.project() {
                AnyIoProj::Plain { inner } => inner.poll_shutdown(cx),
                AnyIoProj::Tls { inner } => inner.poll_shutdown(cx),
            }
        }
    }
}

// ── State constants ───────────────────────────────────────────────────────────

/// Connection state: active and accepting new streams.
pub const STATE_ACTIVE: u8 = 0;
/// Connection state: draining — no new streams, existing ones may finish.
pub const STATE_DRAINING: u8 = 1;
/// Connection state: dead — cannot be used.
pub const STATE_DEAD: u8 = 2;

// ── ConnectionConfig ──────────────────────────────────────────────────────────

/// Tuning parameters for a single [`Connection`].
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Timeout for the TCP connection attempt (not the TLS handshake).
    pub connect_timeout: Duration,
    /// Initial cap on the number of concurrent streams per connection.
    pub max_concurrent_streams: usize,
    /// Initial HTTP/2 connection-level flow-control window in bytes.
    pub initial_conn_window: u32,
    /// Initial HTTP/2 stream-level flow-control window in bytes.
    pub initial_stream_window: u32,
    /// Whether to send HTTP/2 PING frames while the connection is idle.
    pub keep_alive_while_idle: bool,
    /// Optional TLS config. `None` → raw TCP.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
}

/// TLS configuration bundle passed to the dialler.
#[cfg(feature = "tls")]
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// rustls client configuration.
    pub client_config: Arc<rustls::ClientConfig>,
    /// SNI server name.
    pub server_name: rustls_pki_types::ServerName<'static>,
}

#[cfg(feature = "tls")]
impl TlsConfig {
    /// Create a new [`TlsConfig`] from an Arc-wrapped rustls [`ClientConfig`] and
    /// an SNI [`ServerName`].
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use rustls::RootCertStore;
    /// use rustls_pki_types::ServerName;
    /// use oxirpc_client::native_channel::TlsConfig;
    ///
    /// let roots = RootCertStore::empty();
    /// let cfg = oxirpc_core::tls::client_config(roots).unwrap();
    /// let name = ServerName::try_from("example.com").unwrap();
    /// let tls = TlsConfig::new(Arc::new(cfg), name);
    /// ```
    pub fn new(
        client_config: Arc<rustls::ClientConfig>,
        server_name: rustls_pki_types::ServerName<'static>,
    ) -> Self {
        Self {
            client_config,
            server_name,
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            max_concurrent_streams: 100,
            initial_conn_window: 65535,
            initial_stream_window: 65535,
            keep_alive_while_idle: false,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

// ── Connection ────────────────────────────────────────────────────────────────

/// A single live HTTP/2 connection to one backend endpoint.
///
/// Thread-safe: all fields are either atomic or behind a mutex. Clone the
/// `Arc<Connection>` to share across tasks.
pub struct Connection {
    /// The h2 send-half, guarded so only one caller issues the h2 `send_request`
    /// at a time (h2 0.4 requires poll_ready before each call).
    pub(crate) sender: Mutex<SendRequest<Bytes>>,
    /// How many streams are currently in flight.
    streams_in_flight: AtomicU64,
    /// The soft cap on concurrent streams (may be lowered by SETTINGS frames).
    max_concurrent_streams: AtomicU64,
    /// Current lifecycle state (STATE_ACTIVE / STATE_DRAINING / STATE_DEAD).
    pub(crate) state: AtomicU8,
    /// Last stream-id seen in a GOAWAY frame (0 if no GOAWAY received).
    pub(crate) last_stream_id: AtomicU32,
    /// The URI authority of this endpoint, for debugging.
    pub(crate) endpoint_uri: http::Uri,
    /// Notifies watchers when the connection dies.
    death_tx: watch::Sender<Option<OxiRpcError>>,
    /// Background H2 driver. We hold it so that dropping Connection calls abort().
    _driver: JoinHandle<()>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("uri", &self.endpoint_uri)
            .field("state", &self.state.load(Ordering::Relaxed))
            .field("streams", &self.streams_in_flight.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Abort the driver task; otherwise it leaks if the last Arc is dropped.
        self._driver.abort();
    }
}

impl Connection {
    /// Dial a new HTTP/2 connection to the given endpoint.
    ///
    /// Performs TCP connect (with timeout), h2 handshake, and spawns the driver
    /// task. Returns `Arc<Connection>` ready to issue requests.
    pub async fn dial(
        endpoint: &Endpoint,
        cfg: &ConnectionConfig,
    ) -> Result<Arc<Self>, OxiRpcError> {
        let host = endpoint
            .uri
            .host()
            .ok_or_else(|| OxiRpcError::Transport("endpoint URI missing host".to_owned()))?
            .to_owned();
        let port = endpoint.uri.port_u16().unwrap_or(80);
        let addr = format!("{host}:{port}");

        // ── TCP connect with timeout ──────────────────────────────────────────
        let tcp = tokio::time::timeout(cfg.connect_timeout, tokio::net::TcpStream::connect(&addr))
            .await
            .map_err(|_| OxiRpcError::Timeout)?
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

        tcp.set_nodelay(true)
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

        // ── Optional TLS upgrade ──────────────────────────────────────────────
        #[cfg(feature = "tls")]
        let io = {
            use any_io::AnyIo;
            match cfg.tls.as_ref() {
                None => AnyIo::Plain { inner: tcp },
                Some(tls_cfg) => {
                    let connector = tokio_rustls::TlsConnector::from(tls_cfg.client_config.clone());
                    let tls_stream = connector
                        .connect(tls_cfg.server_name.clone(), tcp)
                        .await
                        .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
                    AnyIo::Tls { inner: tls_stream }
                }
            }
        };
        #[cfg(not(feature = "tls"))]
        let io = tcp;

        // ── H2 handshake ─────────────────────────────────────────────────────
        let (send_request, h2_conn) = H2Builder::new()
            .initial_connection_window_size(cfg.initial_conn_window)
            .initial_window_size(cfg.initial_stream_window)
            .handshake(io)
            .await
            .map_err(h2_error_to_oxirpc)?;

        let (death_tx, _death_rx) = watch::channel(None::<OxiRpcError>);
        let death_tx_clone = death_tx.clone();

        let driver = tokio::spawn(async move {
            if let Err(e) = h2_conn.await {
                let oxe = h2_error_to_oxirpc(e);
                // Ignore send error — receiver may already be dropped.
                let _ = death_tx_clone.send(Some(oxe));
            }
        });

        Ok(Arc::new(Self {
            sender: Mutex::new(send_request),
            streams_in_flight: AtomicU64::new(0),
            max_concurrent_streams: AtomicU64::new(cfg.max_concurrent_streams as u64),
            state: AtomicU8::new(STATE_ACTIVE),
            last_stream_id: AtomicU32::new(0),
            endpoint_uri: endpoint.uri.clone(),
            death_tx,
            _driver: driver,
        }))
    }

    /// Returns `true` if this connection can accept a new stream right now.
    pub fn is_usable(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_ACTIVE
            && self.streams_in_flight.load(Ordering::Acquire)
                < self.max_concurrent_streams.load(Ordering::Acquire)
    }

    /// Try to reserve a stream slot on this connection.
    ///
    /// Returns a [`StreamSlot`] RAII guard that decrements the counter on drop.
    /// Returns `Err` if the connection is draining/dead or at capacity.
    pub fn try_acquire(self: &Arc<Self>) -> Result<StreamSlot, OxiRpcError> {
        if self.state.load(Ordering::Acquire) != STATE_ACTIVE {
            return Err(OxiRpcError::Transport(
                "connection is not active".to_owned(),
            ));
        }
        let max = self.max_concurrent_streams.load(Ordering::Acquire);
        // Optimistically increment; roll back if over limit.
        let prev = self.streams_in_flight.fetch_add(1, Ordering::AcqRel);
        if prev >= max {
            self.streams_in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(OxiRpcError::Transport("connection at capacity".to_owned()));
        }
        Ok(StreamSlot {
            conn: Arc::clone(self),
        })
    }

    /// Open an HTTP/2 stream. Returns `(ResponseFuture, SendStream<Bytes>)`.
    ///
    /// The caller sends the request body via the `SendStream` and awaits the
    /// response via the `ResponseFuture`.
    pub async fn open_stream(
        &self,
        req: http::Request<()>,
    ) -> Result<(ResponseFuture, h2::SendStream<Bytes>), OxiRpcError> {
        let mut guard = self.sender.lock().await;
        // poll_ready must be called before send_request.
        futures_util::future::poll_fn(|cx| guard.poll_ready(cx))
            .await
            .map_err(h2_error_to_oxirpc)?;
        guard.send_request(req, false).map_err(h2_error_to_oxirpc)
    }

    /// Subscribe to connection death notifications.
    ///
    /// The watch receiver fires when the H2 driver task exits with an error.
    pub fn watch_death(&self) -> watch::Receiver<Option<OxiRpcError>> {
        self.death_tx.subscribe()
    }

    /// Mark the connection as dead with a reason.
    pub fn mark_dead(&self, reason: OxiRpcError) {
        self.state.store(STATE_DEAD, Ordering::Release);
        let _ = self.death_tx.send(Some(reason));
    }

    /// Begin draining after receiving a GOAWAY frame.
    ///
    /// Records the last processed stream-id from the GOAWAY frame and stops
    /// accepting new streams. Existing streams whose ID is ≤ `last_stream` may
    /// continue; the caller is responsible for managing them.
    pub fn mark_draining_on_goaway(&self, last_stream: u32) {
        self.last_stream_id.store(last_stream, Ordering::Release);
        // Only transition from ACTIVE → DRAINING.
        let _ = self.state.compare_exchange(
            STATE_ACTIVE,
            STATE_DRAINING,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Begin draining: stop accepting new streams, let existing ones finish.
    pub fn mark_draining(&self) {
        self.mark_draining_on_goaway(0);
    }

    /// The last stream-id received in a GOAWAY frame (0 if no GOAWAY received).
    pub fn goaway_last_stream_id(&self) -> u32 {
        self.last_stream_id.load(Ordering::Acquire)
    }

    /// The current stream count for observability.
    pub fn streams_in_flight(&self) -> u64 {
        self.streams_in_flight.load(Ordering::Relaxed)
    }

    /// Update the max concurrent streams from a remote SETTINGS frame.
    ///
    /// gRPC servers may advertise a lower limit via HTTP/2 SETTINGS. Call this
    /// when a SETTINGS_MAX_CONCURRENT_STREAMS parameter is observed.
    pub fn set_max_concurrent_streams(&self, n: u64) {
        self.max_concurrent_streams.store(n, Ordering::Release);
    }
}

// ── StreamSlot ────────────────────────────────────────────────────────────────

/// RAII guard that decrements the in-flight stream counter on drop.
pub struct StreamSlot {
    conn: Arc<Connection>,
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        self.conn.streams_in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

// ── h2_error_to_oxirpc ────────────────────────────────────────────────────────

/// Map an `h2::Error` to an [`OxiRpcError`].
///
/// Mapping rules:
/// - I/O error → `Transport`
/// - REFUSED_STREAM → `Status(UNAVAILABLE)` (retryable)
/// - CANCEL → `Cancelled`
/// - ENHANCE_YOUR_CALM → `Status(RESOURCE_EXHAUSTED)`
/// - GOAWAY → `Status(UNAVAILABLE)` (retryable)
/// - Other RST_STREAM → `Status(INTERNAL, "rst {code}")`
pub fn h2_error_to_oxirpc(e: h2::Error) -> OxiRpcError {
    use h2::Reason;
    use oxirpc_core::status::StatusCode;

    if e.is_io() {
        return OxiRpcError::Transport(e.to_string());
    }

    if e.is_go_away() {
        return OxiRpcError::from_status_code(StatusCode::Unavailable, "GOAWAY received");
    }

    if let Some(reason) = e.reason() {
        return match reason {
            Reason::REFUSED_STREAM => {
                OxiRpcError::from_status_code(StatusCode::Unavailable, "REFUSED_STREAM")
            }
            Reason::CANCEL => OxiRpcError::Cancelled,
            Reason::ENHANCE_YOUR_CALM => {
                OxiRpcError::from_status_code(StatusCode::ResourceExhausted, "ENHANCE_YOUR_CALM")
            }
            other => {
                let raw: u32 = other.into();
                OxiRpcError::from_status_code(StatusCode::Internal, format!("rst {raw}"))
            }
        };
    }

    OxiRpcError::Transport(e.to_string())
}
