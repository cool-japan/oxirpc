//! Single HTTP/3 connection wrapper for the native h3 channel.
//!
//! [`H3Connection`] owns one `h3::client::SendRequest` and the background
//! connection-driver task that must continuously poll
//! `h3::client::Connection::poll_close` so control-stream frames (SETTINGS,
//! GOAWAY) and flow-control are serviced. Dropping the connection aborts the
//! driver task, mirroring the h2 [`super::super::connection::Connection`] Drop.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use oxiquic_h3::OxiQuicOpenStreams;
use oxiquic_transport::{ClientEndpoint, TransportConfig};
use oxirpc_core::status::StatusCode;
use oxirpc_core::OxiRpcError;

/// RFC 9114 §4.2.2 default for `SETTINGS_MAX_FIELD_SECTION_SIZE`.
///
/// Matches the value used by OxiQUIC's own `H3Client` so both ends agree on the
/// maximum encoded header-block size a peer may send.
pub const DEFAULT_MAX_FIELD_SECTION_SIZE: u64 = 16_384;

/// A single live HTTP/3 connection to one backend endpoint.
///
/// Thread-safe: the `h3` send-half is behind an async mutex (only one caller may
/// issue `send_request` at a time). Clone the `Arc<H3Connection>` to share it.
pub struct H3Connection {
    /// The h3 send-half. Guarded so only one caller opens a request stream at a
    /// time; the guard is released as soon as the stream is opened.
    send: Mutex<h3::client::SendRequest<OxiQuicOpenStreams, Bytes>>,
    /// The authority (`host:port`) this connection targets, for request rewriting.
    authority: String,
    /// Background driver polling `Connection::poll_close`. Aborted on Drop.
    _driver: JoinHandle<()>,
}

impl std::fmt::Debug for H3Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Connection")
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

impl Drop for H3Connection {
    fn drop(&mut self) {
        // Abort the driver task; otherwise it leaks when the last Arc is dropped.
        self._driver.abort();
    }
}

impl H3Connection {
    /// Dial a new HTTP/3 connection.
    ///
    /// Steps (mirroring OxiQUIC's `H3ClientBuilder::connect`):
    /// 1. Bind an ephemeral UDP client endpoint with the supplied QUIC TLS config.
    /// 2. Perform the QUIC handshake to `server_addr` verifying `server_name`.
    /// 3. Enforce that the negotiated ALPN is exactly `b"h3"` (RFC 9114 §3.3).
    /// 4. Convert to a `DrivenConnection` and perform the HTTP/3 handshake.
    /// 5. Spawn the connection driver and return the send-half.
    ///
    /// `tls` **must** be built from [`oxirpc_core::tls::client_config_h3`] (QUIC
    /// crypto provider + TLS 1.3 + `"h3"` ALPN); see the module docs.
    ///
    /// # Errors
    ///
    /// Returns [`OxiRpcError::Transport`] on bind/handshake failure or ALPN
    /// mismatch, and never panics on peer-supplied values.
    pub async fn connect(
        server_addr: SocketAddr,
        server_name: &str,
        tls: Arc<rustls::ClientConfig>,
        transport: TransportConfig,
        max_field_section_size: u64,
    ) -> Result<Arc<Self>, OxiRpcError> {
        let bind_addr: SocketAddr = ([0u8, 0, 0, 0], 0u16).into();
        let endpoint = ClientEndpoint::bind(bind_addr, tls, transport)
            .await
            .map_err(|e| OxiRpcError::Transport(format!("h3 client bind: {e}")))?;

        let quic = endpoint
            .connect(server_addr, server_name)
            .await
            .map_err(|e| OxiRpcError::Transport(format!("h3 quic connect: {e}")))?;

        // Enforce ALPN == "h3" (RFC 9114 §3.3). A mismatch means the server did
        // not agree to speak HTTP/3 and we must not proceed.
        match quic.negotiated_alpn() {
            Some(alpn) if alpn == b"h3" => {}
            Some(alpn) => {
                return Err(OxiRpcError::Transport(format!(
                    "h3 ALPN mismatch: expected \"h3\", got {:?}",
                    String::from_utf8_lossy(&alpn)
                )));
            }
            None => {
                return Err(OxiRpcError::Transport(
                    "h3 ALPN mismatch: server negotiated no ALPN protocol".to_owned(),
                ));
            }
        }

        let driven = quic.into_driven();
        let (mut h3_conn, send) = oxiquic_h3::connect_h3_with(driven, max_field_section_size)
            .await
            .map_err(|e| OxiRpcError::Transport(format!("h3 handshake: {e}")))?;

        // The h3 connection must be driven continuously so control-stream frames
        // (SETTINGS, GOAWAY) and QPACK streams are processed. Own it in a task.
        let driver = tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| h3_conn.poll_close(cx)).await;
        });

        Ok(Arc::new(Self {
            send: Mutex::new(send),
            authority: format!("{server_addr}"),
            _driver: driver,
        }))
    }

    /// The authority (`host:port`) this connection targets.
    pub(crate) fn authority(&self) -> &str {
        &self.authority
    }

    /// Access the guarded send-half.
    pub(crate) fn send(&self) -> &Mutex<h3::client::SendRequest<OxiQuicOpenStreams, Bytes>> {
        &self.send
    }
}

// ── error mapping ───────────────────────────────────────────────────────────

/// Map an [`h3::error::StreamError`] to an [`OxiRpcError`].
///
/// Mapping rules mirror the h2 `h2_error_to_oxirpc`:
/// - `H3_REQUEST_CANCELLED` / `H3_REQUEST_REJECTED` → `Cancelled`
/// - a stream error whose code maps to a gRPC-retryable condition → `Status(Unavailable)`
/// - connection-level errors → delegate to [`h3_conn_error_to_oxirpc`]
/// - anything else → `Transport`
pub fn h3_stream_error_to_oxirpc(e: h3::error::StreamError) -> OxiRpcError {
    use h3::error::StreamError;

    match e {
        StreamError::StreamError { code, reason } => map_h3_code(code, &reason),
        StreamError::RemoteTerminate { code } => map_h3_code(code, "remote terminated stream"),
        StreamError::RemoteClosing => {
            OxiRpcError::from_status_code(StatusCode::Unavailable, "h3 remote closing")
        }
        StreamError::ConnectionError(conn) => h3_conn_error_to_oxirpc(conn),
        StreamError::HeaderTooBig {
            actual_size,
            max_size,
        } => OxiRpcError::from_status_code(
            StatusCode::ResourceExhausted,
            format!("h3 header block too big: {actual_size} > {max_size}"),
        ),
        StreamError::Undefined(err) => OxiRpcError::Transport(format!("h3 stream: {err}")),
        other => OxiRpcError::Transport(format!("h3 stream: {other}")),
    }
}

/// Map an [`h3::error::ConnectionError`] to an [`OxiRpcError`].
pub fn h3_conn_error_to_oxirpc(e: h3::error::ConnectionError) -> OxiRpcError {
    use h3::error::ConnectionError;

    if e.is_h3_no_error() {
        // A graceful close with H3_NO_ERROR is not an RPC failure by itself; the
        // caller surfaces the missing grpc-status trailer separately.
        return OxiRpcError::from_status_code(StatusCode::Unavailable, "h3 connection closed");
    }
    match e {
        ConnectionError::Timeout => OxiRpcError::Timeout,
        other => OxiRpcError::Transport(format!("h3 connection: {other}")),
    }
}

/// Translate an HTTP/3 error [`Code`](h3::error::Code) + reason into an
/// [`OxiRpcError`], honouring the gRPC-retryable request codes.
fn map_h3_code(code: h3::error::Code, reason: &str) -> OxiRpcError {
    use h3::error::Code;

    if code == Code::H3_REQUEST_CANCELLED || code == Code::H3_REQUEST_REJECTED {
        return OxiRpcError::Cancelled;
    }
    OxiRpcError::from_status_code(
        StatusCode::Unavailable,
        format!("h3 stream reset (code {}): {reason}", code.value()),
    )
}
