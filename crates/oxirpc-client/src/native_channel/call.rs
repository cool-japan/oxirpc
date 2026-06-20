//! Per-RPC execution for the NativeChannel.
//!
//! [`execute_unary`] handles a complete request/response cycle over one
//! HTTP/2 stream. The same function works for streaming RPCs — the body
//! channel carries multiple frames.

use std::sync::Arc;
use std::time::Instant;

use bytes::{BufMut, Bytes, BytesMut};
use http::HeaderMap;
use http_body::Body as _;
use tokio::pin;

use oxirpc_core::wire::FrameDecoder;
use oxirpc_core::wire::WireError;
use oxirpc_core::{OxiRpcError, StatusCode};
use tokio_util::codec::Decoder as _;

use super::body::{body_channel, NativeBody, NativeBodySender};
use super::connection::{h2_error_to_oxirpc, Connection, StreamSlot};
use super::intercept::{apply_metadata_to_headers, request_from_headers};
use oxirpc_core::interceptor::AsyncInterceptor;

// ── execute_unary ─────────────────────────────────────────────────────────────

/// Execute a single gRPC call over an existing connection.
///
/// # Protocol
///
/// 1. Split the request into headers and body.
/// 2. Open an H2 stream (send headers).
/// 3. If there is a request body, spawn a task to pump it.
/// 4. Await the response future.
/// 5. If `grpc-status` is present in the *initial* response headers
///    (trailers-only response), map it to an error immediately.
/// 6. Otherwise spawn a response-body pump task and return the response.
///
/// The `_slot` is moved in and dropped with the returned `NativeBody`,
/// ensuring the stream counter is decremented when the body is consumed.
pub async fn execute_unary(
    conn: Arc<Connection>,
    req: http::Request<NativeBody>,
    deadline: Option<Instant>,
    interceptor: Option<Arc<dyn AsyncInterceptor>>,
    _slot: StreamSlot,
) -> Result<http::Response<NativeBody>, OxiRpcError> {
    let remaining = deadline.map(|d| d.saturating_duration_since(Instant::now()));

    let fut = do_execute(conn, req, interceptor, _slot);

    match remaining {
        Some(dur) if !dur.is_zero() => tokio::time::timeout(dur, fut)
            .await
            .map_err(|_| OxiRpcError::Timeout)?,
        Some(_) => Err(OxiRpcError::Timeout),
        None => fut.await,
    }
}

/// Inner execution logic (without deadline wrapping).
async fn do_execute(
    conn: Arc<Connection>,
    req: http::Request<NativeBody>,
    interceptor: Option<Arc<dyn AsyncInterceptor>>,
    slot: StreamSlot,
) -> Result<http::Response<NativeBody>, OxiRpcError> {
    // ── Split request into headers and body ───────────────────────────────────
    let (mut parts, body) = req.into_parts();
    if let Some(ref interceptor) = interceptor {
        let irequest = request_from_headers(&parts.headers);
        match interceptor.intercept_async(irequest).await {
            Ok(updated) => apply_metadata_to_headers(&updated, &mut parts.headers),
            Err(status) => {
                return Err(OxiRpcError::from_status_code(status.code, status.message));
            }
        }
    }

    // Build a headers-only Request<()> for h2::open_stream.
    let h2_req = {
        let mut builder = http::Request::builder()
            .method(parts.method.clone())
            .uri(parts.uri.clone())
            .version(http::Version::HTTP_2);
        for (name, value) in &parts.headers {
            builder = builder.header(name, value);
        }
        builder
            .body(())
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?
    };

    // ── Open H2 stream ────────────────────────────────────────────────────────
    let (response_future, mut send_stream) = conn.open_stream(h2_req).await?;

    // ── Pump request body ─────────────────────────────────────────────────────
    // We spawn a task so that sending the request body and awaiting the response
    // can proceed concurrently — the server may start responding before we finish
    // sending (e.g. error reply).
    let body_task = tokio::spawn(async move { send_body(body, &mut send_stream).await });

    // ── Await response headers ────────────────────────────────────────────────
    let response = response_future.await.map_err(h2_error_to_oxirpc)?;
    let (head, recv_stream) = response.into_parts();

    // ── Trailers-only detection ───────────────────────────────────────────────
    if head.headers.contains_key("grpc-status") {
        // Server sent grpc-status in the initial HEADERS frame (trailers-only).
        // Abort the body sender (we don't care about the outcome at this point).
        body_task.abort();
        let status = parse_grpc_status(&head.headers)?;
        if status == 0 {
            // grpc-status:0 in a trailers-only response means OK with no body.
            // Preserve any other trailer headers (e.g. custom metadata) by
            // attaching them to the empty body so callers can inspect them.
            let trailer_headers = head.headers.clone();
            let body = NativeBody::empty().with_trailers(trailer_headers);
            return Ok(http::Response::from_parts(head, body));
        }
        let message = grpc_message(&head.headers);
        return Err(OxiRpcError::from_status_code(
            StatusCode::from_i32_lossy(status),
            message,
        ));
    }

    // ── Spawn response-body pump task ─────────────────────────────────────────
    let (body_tx, response_body) = body_channel(16);

    // `slot` is moved into the response pump task — it will be dropped when the
    // response body is fully consumed, decrementing the stream counter.
    tokio::spawn(async move {
        // Bind slot lifetime to body pump.
        let _slot = slot;
        // Drop body_task explicitly — we do NOT await it, but we hold it so that
        // the JoinHandle stays alive while we pump the response. When the pump
        // finishes, body_task drops and its internal task gets cancelled.
        std::mem::drop(body_task);
        pump_response(recv_stream, body_tx).await;
    });

    let response = http::Response::from_parts(head, response_body);
    Ok(response)
}

// ── send_body ─────────────────────────────────────────────────────────────────

/// Pump a [`NativeBody`] into an h2 `SendStream`, with backpressure.
async fn send_body(
    body: NativeBody,
    send_stream: &mut h2::SendStream<Bytes>,
) -> Result<(), OxiRpcError> {
    pin!(body);
    loop {
        match futures_util::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if frame.is_data() {
                    let chunk = frame
                        .into_data()
                        .map_err(|_| OxiRpcError::Transport("frame type error".to_owned()))?;
                    if chunk.is_empty() {
                        continue;
                    }
                    // Backpressure: wait for capacity.
                    let len = chunk.len();
                    send_stream.reserve_capacity(len);
                    futures_util::future::poll_fn(|cx| send_stream.poll_capacity(cx))
                        .await
                        .ok_or_else(|| {
                            OxiRpcError::Transport("send stream capacity closed".to_owned())
                        })?
                        .map_err(h2_error_to_oxirpc)?;

                    send_stream
                        .send_data(chunk, false)
                        .map_err(h2_error_to_oxirpc)?;
                }
                // Trailer frames from the request body are ignored on the
                // client side — gRPC trailers flow on the response stream.
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    // Signal end-of-stream on the request side.
    send_stream
        .send_data(Bytes::new(), true)
        .map_err(h2_error_to_oxirpc)?;
    Ok(())
}

// ── pump_response ─────────────────────────────────────────────────────────────

/// Drive an h2 `RecvStream` through a [`FrameDecoder`] and forward decoded
/// frames to `body_tx`.
///
/// At EOF, reads trailers and checks `grpc-status`. If status != 0, sends an
/// error on the channel.
async fn pump_response(mut recv_stream: h2::RecvStream, body_tx: NativeBodySender) {
    let mut decoder = FrameDecoder::default();
    let mut buf = BytesMut::new();

    loop {
        match recv_stream.data().await {
            Some(Ok(chunk)) => {
                // Release H2 flow-control window immediately.
                let _ = recv_stream.flow_control().release_capacity(chunk.len());
                buf.put_slice(&chunk);

                // Decode as many gRPC frames as are available.
                loop {
                    match decoder.decode(&mut buf) {
                        Ok(Some(frame)) => {
                            if body_tx.send_data(frame.payload).await.is_err() {
                                // Receiver dropped — abort.
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(WireError::FrameTooLarge { .. }) | Err(WireError::UnknownFlag(_)) => {
                            body_tx
                                .send_error(OxiRpcError::Transport(
                                    "invalid gRPC frame from server".to_owned(),
                                ))
                                .await;
                            return;
                        }
                        Err(e) => {
                            body_tx
                                .send_error(OxiRpcError::Transport(e.to_string()))
                                .await;
                            return;
                        }
                    }
                }
            }
            Some(Err(e)) => {
                body_tx.send_error(h2_error_to_oxirpc(e)).await;
                return;
            }
            None => break,
        }
    }

    // ── Read trailers ─────────────────────────────────────────────────────────
    let trailers: Option<HeaderMap> = recv_stream.trailers().await.unwrap_or(None);
    if let Some(ref t) = trailers {
        let status = parse_grpc_status(t).unwrap_or(2 /* UNKNOWN */);
        if status != 0 {
            let message = grpc_message(t);
            body_tx
                .send_error(OxiRpcError::from_status_code(
                    StatusCode::from_i32_lossy(status),
                    message,
                ))
                .await;
        }
        // status == 0: OK — channel close signals EOF to the consumer.
    }
    // body_tx drops here, closing the channel and signalling EOF to the consumer.
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Extract the `grpc-status` header value as an i32.
fn parse_grpc_status(headers: &HeaderMap) -> Result<i32, OxiRpcError> {
    headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| OxiRpcError::Transport("missing or invalid grpc-status".to_owned()))
}

/// Extract the `grpc-message` header, percent-decoded.
fn grpc_message(headers: &HeaderMap) -> String {
    headers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(percent_decode_simple)
        .unwrap_or_default()
}

/// Minimal percent-decoding for `grpc-message` values.
fn percent_decode_simple(s: &str) -> String {
    // Delegate to the wire module's implementation via the trailer helpers,
    // but since we can't easily call it from here we inline a simple version.
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[inline]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}
