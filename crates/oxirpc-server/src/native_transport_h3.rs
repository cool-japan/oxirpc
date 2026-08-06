//! Native HTTP/3 (gRPC-over-QUIC) server transport.
//!
//! This module is the h3 analogue of [`crate::native_transport`]. It accepts
//! QUIC connections on a bound [`ServerEndpoint`], upgrades each to HTTP/3, and
//! dispatches every request to a `tower::Service<Request<NativeBody>>` — in
//! practice the [`crate::RegistryService`] produced by a
//! [`crate::NativeServiceRegistry`].
//!
//! # Request / response mapping
//!
//! - The client's request body (raw gRPC-framed bytes) is streamed verbatim
//!   into a channel-backed [`NativeBody`](oxirpc_core::wire::NativeBody) and
//!   handed to the service, exactly as
//!   the hyper transport hands it `hyper::body::Incoming`.
//! - The service's `Response<RespB>` is written back frame-by-frame: data frames
//!   become HTTP/3 DATA frames and the terminating trailer frame (carrying
//!   `grpc-status` / `grpc-message`) becomes the HTTP/3 trailers block.
//! - The `te` header (an HTTP/2 hop-by-hop construct) is stripped from the
//!   response headers; it is invalid over HTTP/3.
//!
//! # Graceful shutdown
//!
//! Pass a `tokio::sync::watch::Receiver<()>` as `shutdown`. When it fires the
//! accept loop stops taking new QUIC connections; in-flight connections and
//! streams (each handled in its own task) complete naturally.
//!
//! This whole module is gated behind the `http3` cargo feature.

use std::convert::Infallible;

use bytes::{Buf, Bytes};
use http::{HeaderMap, Request, Response};
use oxiquic_h3::{accept_h3, H3RecvStream, OxiQuicH3Connection};
use oxiquic_transport::{ServerEndpoint, TransportConfig};
use tower::Service;

use oxirpc_core::wire::{body_channel, NativeBody, NativeBodySender};
use oxirpc_core::OxiRpcError;

/// Bind a QUIC [`ServerEndpoint`] for HTTP/3 with the given TLS config.
///
/// The `server_cfg` **must** be built from
/// [`oxirpc_core::tls::server_config_h3`] (QUIC crypto provider + TLS 1.3 +
/// `"h3"` ALPN). Bind to port `0` for an OS-assigned port; read it back via
/// [`ServerEndpoint::local_addr`].
///
/// # Errors
///
/// Returns [`OxiRpcError::Transport`] if the UDP socket cannot be bound.
pub async fn bind_h3_endpoint(
    bind_addr: std::net::SocketAddr,
    server_cfg: std::sync::Arc<rustls::ServerConfig>,
    transport: TransportConfig,
) -> Result<ServerEndpoint, OxiRpcError> {
    ServerEndpoint::bind(bind_addr, server_cfg, transport)
        .await
        .map_err(|e| OxiRpcError::Transport(format!("h3 server bind: {e}")))
}

/// Serve gRPC-over-HTTP/3 requests from a bound [`ServerEndpoint`] using any
/// compatible `tower::Service`.
///
/// # Parameters
///
/// - `endpoint` — a pre-bound QUIC [`ServerEndpoint`] (see [`bind_h3_endpoint`]).
/// - `service`  — a `tower::Service<Request<NativeBody>>` returning
///   `Response<RespB>`; `Clone + Send + 'static`.
/// - `shutdown` — optional watch receiver; when it fires the accept loop exits.
///
/// # Errors
///
/// Returns [`OxiRpcError`] only on unexpected endpoint teardown; per-connection
/// and per-stream errors are logged and isolated to their task.
pub async fn serve_native_h3_with_service<S, RespB>(
    endpoint: ServerEndpoint,
    service: S,
    shutdown: Option<tokio::sync::watch::Receiver<()>>,
) -> Result<(), OxiRpcError>
where
    S: Service<Request<NativeBody>, Response = Response<RespB>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    RespB: http_body::Body<Data = Bytes> + Send + 'static,
    RespB::Error: std::fmt::Display + Send,
{
    loop {
        let accept = if let Some(mut rx) = shutdown.clone() {
            tokio::select! {
                biased;
                _ = rx.changed() => {
                    tracing::debug!(target: "oxirpc::native_transport_h3", "shutdown signalled");
                    break;
                }
                res = endpoint.accept() => res,
            }
        } else {
            endpoint.accept().await
        };

        let quic = match accept {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(target: "oxirpc::native_transport_h3", "quic accept error: {e}");
                continue;
            }
        };

        // Enforce that the client negotiated the HTTP/3 ALPN. A mismatch means
        // the peer is not speaking h3, so we reject the connection.
        match quic.negotiated_alpn() {
            Some(alpn) if alpn == b"h3" => {}
            other => {
                tracing::warn!(
                    target: "oxirpc::native_transport_h3",
                    "rejecting connection: expected h3 ALPN, got {:?}",
                    other.map(|a| String::from_utf8_lossy(&a).into_owned())
                );
                continue;
            }
        }

        let svc = service.clone();
        tokio::spawn(async move {
            let driven = quic.into_driven();
            let h3_conn = match accept_h3(driven).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        target: "oxirpc::native_transport_h3",
                        "h3 handshake error: {e}"
                    );
                    return;
                }
            };
            handle_h3_connection(h3_conn, svc).await;
        });
    }

    Ok(())
}

/// Drive one HTTP/3 connection: accept requests until GOAWAY / close, spawning a
/// task per request stream.
async fn handle_h3_connection<S, RespB>(
    mut h3_conn: h3::server::Connection<OxiQuicH3Connection, Bytes>,
    service: S,
) where
    S: Service<Request<NativeBody>, Response = Response<RespB>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    RespB: http_body::Body<Data = Bytes> + Send + 'static,
    RespB::Error: std::fmt::Display + Send,
{
    loop {
        match h3_conn.accept().await {
            Ok(Some(resolver)) => {
                let svc = service.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_request(resolver, svc).await {
                        tracing::debug!(
                            target: "oxirpc::native_transport_h3",
                            "h3 request handling ended: {e}"
                        );
                    }
                });
            }
            Ok(None) => break, // graceful close (GOAWAY, all requests done)
            Err(e) => {
                tracing::debug!(
                    target: "oxirpc::native_transport_h3",
                    "h3 connection accept ended: {e}"
                );
                break;
            }
        }
    }
}

/// Handle a single HTTP/3 request stream end-to-end.
async fn handle_request<S, RespB>(
    resolver: h3::server::RequestResolver<OxiQuicH3Connection, Bytes>,
    mut service: S,
) -> Result<(), OxiRpcError>
where
    S: Service<Request<NativeBody>, Response = Response<RespB>, Error = Infallible>,
    RespB: http_body::Body<Data = Bytes> + Send + 'static,
    RespB::Error: std::fmt::Display + Send,
{
    let (req_parts, stream) = resolver
        .resolve_request()
        .await
        .map_err(|e| OxiRpcError::Transport(format!("h3 resolve request: {e}")))?;
    let (mut send_stream, recv_stream) = stream.split();

    // ── Build the request body from the recv side ─────────────────────────────
    let (body_tx, req_body) = body_channel(16);
    tokio::spawn(pump_request_body(recv_stream, body_tx));

    let (parts, ()) = req_parts.into_parts();
    let request = Request::from_parts(parts, req_body);

    // ── Dispatch to the service ───────────────────────────────────────────────
    // The service error type is Infallible, so this cannot actually fail.
    let response = match service.call(request).await {
        Ok(r) => r,
        Err(_infallible) => return Ok(()),
    };
    let (resp_parts, resp_body) = response.into_parts();

    // ── Send response headers (strip `te`, keep status + gRPC headers) ────────
    let mut head = Response::new(());
    *head.status_mut() = resp_parts.status;
    for (name, value) in resp_parts.headers.iter() {
        if name == http::header::TE {
            continue;
        }
        head.headers_mut().append(name.clone(), value.clone());
    }
    send_stream
        .send_response(head)
        .await
        .map_err(|e| OxiRpcError::Transport(format!("h3 send response: {e}")))?;

    // ── Stream the response body: data frames → DATA, trailer frame → trailers ─
    let mut trailers: Option<HeaderMap> = None;
    let mut resp_body = std::pin::pin!(resp_body);
    loop {
        match std::future::poll_fn(|cx| resp_body.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if frame.is_data() {
                    if let Ok(data) = frame.into_data() {
                        if !data.has_remaining() {
                            continue;
                        }
                        send_stream
                            .send_data(data)
                            .await
                            .map_err(|e| OxiRpcError::Transport(format!("h3 send data: {e}")))?;
                    }
                } else if let Ok(t) = frame.into_trailers() {
                    // The last trailer frame wins (gRPC emits exactly one).
                    trailers = Some(t);
                }
            }
            Some(Err(e)) => {
                return Err(OxiRpcError::Transport(format!("h3 response body: {e}")));
            }
            None => break,
        }
    }

    if let Some(t) = trailers {
        send_stream
            .send_trailers(t)
            .await
            .map_err(|e| OxiRpcError::Transport(format!("h3 send trailers: {e}")))?;
    }
    // Always FIN the send side. `send_trailers` writes the trailers HEADERS frame
    // but does not close the stream (h3 0.0.8 requires an explicit `finish`);
    // without the FIN the client's `recv_data` never observes end-of-stream and
    // would block until the QUIC idle timeout.
    send_stream
        .finish()
        .await
        .map_err(|e| OxiRpcError::Transport(format!("h3 finish: {e}")))?;

    Ok(())
}

/// Pump the client's request body (raw gRPC-framed bytes) into a [`NativeBody`]
/// channel, forwarding every DATA chunk verbatim.
async fn pump_request_body(
    mut recv_stream: h3::server::RequestStream<H3RecvStream, Bytes>,
    body_tx: NativeBodySender,
) {
    loop {
        match recv_stream.recv_data().await {
            Ok(Some(mut chunk)) => {
                let bytes = chunk.copy_to_bytes(chunk.remaining());
                if body_tx.send_data(bytes).await.is_err() {
                    return; // receiver dropped
                }
            }
            Ok(None) => break,
            Err(e) => {
                body_tx
                    .send_error(OxiRpcError::Transport(format!("h3 recv request body: {e}")))
                    .await;
                return;
            }
        }
    }
    // Dropping body_tx closes the channel → EOF for the service.
    let _ = recv_stream;
}
