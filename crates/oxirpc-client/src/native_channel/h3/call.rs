//! Per-RPC execution for the native HTTP/3 channel.
//!
//! [`execute_h3`] runs one gRPC call over an [`H3Connection`]. It is the h3
//! analogue of [`super::super::call::execute_unary`] and follows the same
//! protocol shape:
//!
//! 1. Split the request into headers and body; force an `https` scheme and copy
//!    the gRPC headers, **stripping the `te` header** (the `te: trailers`
//!    hop-by-hop header is an HTTP/2 construct forbidden over HTTP/3).
//! 2. Open a request stream and `split()` it so the request body and the
//!    response can be driven concurrently (full-duplex, for client/bidi streams).
//! 3. Spawn a task to pump the request body, finishing the send side at EOF.
//! 4. Await the response headers; validate the `content-type` and fail fast
//!    with a [`OxiRpcError::Transport`] if it does not indicate gRPC. Then, if
//!    `grpc-status` is present in the initial header block (a trailers-only
//!    response) map it to OK-empty or an error immediately.
//! 5. Otherwise spawn a response-body pump task that decodes gRPC frames and
//!    forwards the message payloads, then reads the `grpc-status` trailer.

use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use http::HeaderMap;
use http_body::Body as _;
use tokio::pin;
use tokio_util::codec::Decoder as _;

use oxiquic_h3::{H3RecvStream, H3SendStream};
use oxirpc_core::wire::{FrameDecoder, WireError};
use oxirpc_core::{OxiRpcError, StatusCode};

use super::super::body::{body_channel, NativeBody, NativeBodySender};
use super::super::content_type::validate_grpc_response_content_type;
use super::connection::{h3_stream_error_to_oxirpc, H3Connection};

// ── execute_h3 ────────────────────────────────────────────────────────────────

/// Execute a single gRPC call over an existing HTTP/3 connection.
///
/// When `deadline` is `Some`, the whole call is wrapped in a
/// [`tokio::time::timeout`]; an elapsed deadline yields [`OxiRpcError::Timeout`].
///
/// # Errors
///
/// Returns a typed [`OxiRpcError`] on transport failure, an error gRPC status,
/// or a malformed response; it never panics on peer-supplied data.
pub async fn execute_h3(
    conn: Arc<H3Connection>,
    req: http::Request<NativeBody>,
    deadline: Option<Instant>,
) -> Result<http::Response<NativeBody>, OxiRpcError> {
    let remaining = deadline.map(|d| d.saturating_duration_since(Instant::now()));
    let fut = do_execute(conn, req);
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
    conn: Arc<H3Connection>,
    req: http::Request<NativeBody>,
) -> Result<http::Response<NativeBody>, OxiRpcError> {
    let (parts, body) = req.into_parts();

    // ── Build a headers-only Request<()> with an https scheme + authority ──────
    // HTTP/3 derives `:scheme`, `:authority`, `:path` and `:method` from the URI
    // and method. We force `https` (QUIC is always TLS) and pin the authority to
    // the dialled endpoint so relative or http-scheme request URIs still work.
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let uri = format!("https://{}{}", conn.authority(), path_and_query);

    let mut builder = http::Request::builder()
        .method(parts.method.clone())
        .uri(uri)
        .version(http::Version::HTTP_3);
    for (name, value) in &parts.headers {
        // Strip `te`: `te: trailers` is an HTTP/2 hop-by-hop header that MUST NOT
        // be forwarded over HTTP/3 (RFC 9114 §4.2 forbids connection-specific
        // fields); h3 will reject it as a malformed header otherwise.
        if name == http::header::TE {
            continue;
        }
        builder = builder.header(name, value);
    }
    let h3_req = builder
        .body(())
        .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

    // ── Open the request stream, then split for full-duplex operation ─────────
    let req_stream = {
        let mut send = conn.send().lock().await;
        send.send_request(h3_req)
            .await
            .map_err(h3_stream_error_to_oxirpc)?
    };
    let (mut send_stream, mut recv_stream) = req_stream.split();

    // ── Pump the request body concurrently with awaiting the response ─────────
    let body_task = tokio::spawn(async move { send_body(body, &mut send_stream).await });

    // ── Await response headers ────────────────────────────────────────────────
    let response = match recv_stream.recv_response().await {
        Ok(r) => r,
        Err(e) => {
            body_task.abort();
            return Err(h3_stream_error_to_oxirpc(e));
        }
    };
    let (head, _) = response.into_parts();

    // ── Content-Type validation ───────────────────────────────────────────────
    // A response whose content-type does not indicate gRPC is not a gRPC
    // response at all — e.g. a reverse-proxy error page or a plain HTTP
    // endpoint reachable at the same address. Decoding its body as
    // length-prefixed gRPC frames would produce a confusing "invalid gRPC
    // frame" error; report the real cause instead.
    if let Err(e) = validate_grpc_response_content_type(&head.headers, head.status) {
        body_task.abort();
        return Err(e);
    }

    // ── Trailers-only detection ───────────────────────────────────────────────
    // Some servers put grpc-status directly in the initial HEADERS block.
    if head.headers.contains_key("grpc-status") {
        body_task.abort();
        let status = parse_grpc_status(&head.headers)?;
        if status == 0 {
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
    let initial_status = head.status;
    tokio::spawn(async move {
        // Keep the request-body sender task alive alongside the response pump; it
        // is aborted when this task ends (mirrors the h2 path).
        let _body_task = body_task;
        pump_response(recv_stream, body_tx, initial_status).await;
    });

    Ok(http::Response::from_parts(head, response_body))
}

// ── send_body ───────────────────────────────────────────────────────────────

/// Pump a [`NativeBody`] into an h3 send stream, finishing at EOF.
async fn send_body(
    body: NativeBody,
    send_stream: &mut h3::client::RequestStream<H3SendStream, Bytes>,
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
                    send_stream
                        .send_data(chunk)
                        .await
                        .map_err(h3_stream_error_to_oxirpc)?;
                }
                // Request-side trailer frames are ignored — gRPC trailers flow on
                // the response stream only.
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    // Signal end-of-stream on the request side.
    send_stream
        .finish()
        .await
        .map_err(h3_stream_error_to_oxirpc)?;
    Ok(())
}

// ── pump_response ─────────────────────────────────────────────────────────────

/// Drive an h3 response recv-stream through a [`FrameDecoder`] and forward the
/// decoded gRPC message payloads to `body_tx`; then read the `grpc-status`
/// trailer.
///
/// A stream that ends without ever delivering a `grpc-status` trailer is a
/// gRPC-protocol violation and is surfaced as an error, exactly as in the h2
/// path, rather than looking like a successful empty stream.
async fn pump_response(
    mut recv_stream: h3::client::RequestStream<H3RecvStream, Bytes>,
    body_tx: NativeBodySender,
    initial_status: http::StatusCode,
) {
    let mut decoder = FrameDecoder::default();
    let mut buf = BytesMut::new();

    loop {
        match recv_stream.recv_data().await {
            Ok(Some(mut chunk)) => {
                // Copy the received bytes into our decode buffer.
                while chunk.has_remaining() {
                    let piece = chunk.chunk();
                    buf.extend_from_slice(piece);
                    let n = piece.len();
                    chunk.advance(n);
                }

                loop {
                    match decoder.decode(&mut buf) {
                        Ok(Some(frame)) => {
                            if body_tx.send_data(frame.payload).await.is_err() {
                                return; // receiver dropped
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
            Ok(None) => break,
            Err(e) => {
                body_tx.send_error(h3_stream_error_to_oxirpc(e)).await;
                return;
            }
        }
    }

    // ── Read trailers ─────────────────────────────────────────────────────────
    // A genuine h3 stream error while reading the trailers block (reset,
    // connection error, malformed HEADERS) is distinct from a clean stream close
    // with no trailers frame at all (`Ok(None)`) — map it through
    // `h3_stream_error_to_oxirpc` so the caller sees the real transport cause
    // instead of a generic "missing trailer" message indistinguishable from a
    // non-compliant-but-alive server.
    let trailers: Option<HeaderMap> = match recv_stream.recv_trailers().await {
        Ok(t) => t,
        Err(e) => {
            body_tx.send_error(h3_stream_error_to_oxirpc(e)).await;
            return;
        }
    };
    match trailers {
        Some(t) => {
            let status = parse_grpc_status(&t).unwrap_or(2 /* UNKNOWN */);
            if status != 0 {
                let message = grpc_message(&t);
                body_tx
                    .send_error(OxiRpcError::from_status_code(
                        StatusCode::from_i32_lossy(status),
                        message,
                    ))
                    .await;
            }
            // status == 0: OK — closing the channel signals EOF.
        }
        None => {
            let message = if initial_status.is_success() {
                "stream ended without a grpc-status trailer".to_owned()
            } else {
                format!(
                    "stream ended without a grpc-status trailer (http status {})",
                    initial_status.as_u16()
                )
            };
            body_tx
                .send_error(OxiRpcError::from_status_code(StatusCode::Unknown, message))
                .await;
        }
    }
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
