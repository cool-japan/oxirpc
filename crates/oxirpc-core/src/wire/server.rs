//! Pure-native gRPC server-side codec helpers.
//!
//! Provides functions for encoding/decoding prost messages in gRPC wire format
//! and building response bodies for unary, server-streaming, and bidi-streaming
//! RPCs — without using `tonic::server::Grpc` or `tonic_prost::ProstCodec`.

use bytes::{BufMut, Bytes, BytesMut};
use http::HeaderMap;
use http_body_util::BodyExt as _;
use prost::Message;
use tokio::task;

use crate::encoding::CompressionEncoding;
use crate::wire::body::{body_channel, NativeBody, NativeBodySender};
use crate::wire::codec::MessagePipeline;
use crate::wire::frame::{
    Frame, FrameDecoder, FrameOptions, FLAG_COMPRESSED, FLAG_UNCOMPRESSED, GRPC_FRAME_HEADER_LEN,
    MAX_FRAME_SIZE_DEFAULT,
};
use crate::wire::trailer::percent_encode_message;
use crate::{OxiRpcError, StatusCode};
use tokio_util::codec::Decoder as _;

use super::WireError;

// ─────────────────────────────────────────────────────────────────────────────
// Encode / decode
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a prost message into gRPC-framed [`Bytes`] (5-byte prefix + payload).
///
/// The frame is always uncompressed (flag byte 0x00). For compressed frames,
/// use [`crate::wire::MessagePipeline`].
pub fn encode_grpc_message<T: Message>(msg: &T) -> Result<Bytes, WireError> {
    let payload_len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(GRPC_FRAME_HEADER_LEN + payload_len);
    buf.put_u8(FLAG_UNCOMPRESSED);
    buf.put_u32(payload_len as u32);
    msg.encode(&mut buf)?;
    Ok(buf.freeze())
}

/// Decode a single prost message from gRPC-framed [`Bytes`] (identity/uncompressed only).
///
/// Strips the 5-byte gRPC frame header, then decodes the payload with prost.
/// If the compressed flag (byte 0) is set, returns [`OxiRpcError::Compression`].
/// For frames that may be compressed, use [`decode_grpc_message_with_encoding`].
///
/// # Errors
///
/// - [`OxiRpcError::Transport`] — if the bytes are too short for a gRPC frame header or payload,
///   or if the claimed payload length exceeds [`MAX_FRAME_SIZE_DEFAULT`] (also covers the
///   arithmetic-overflow case on 32-bit/wasm32 targets, since the size guard runs first).
/// - [`OxiRpcError::Compression`] — if the compressed flag is set (flag byte != 0x00).
/// - [`OxiRpcError::Proto`] — if the prost decode fails.
pub fn decode_grpc_message<T: Message + Default>(data: Bytes) -> Result<T, OxiRpcError> {
    if data.len() < GRPC_FRAME_HEADER_LEN {
        return Err(OxiRpcError::Transport(format!(
            "gRPC frame too short: need {GRPC_FRAME_HEADER_LEN}, have {}",
            data.len()
        )));
    }
    let compressed = data[0] != FLAG_UNCOMPRESSED;
    if compressed {
        return Err(OxiRpcError::Compression(
            "client sent compressed request but server requires grpc-encoding identity; \
             use decode_grpc_message_with_encoding for decompression support"
                .to_owned(),
        ));
    }
    let payload_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    // DoS / overflow guard: reject frames claiming more than the configured maximum
    // BEFORE doing any arithmetic on the attacker-controlled length. This must run
    // before `GRPC_FRAME_HEADER_LEN + payload_len` — on a 32-bit or wasm32 target
    // `usize` is only 32 bits wide, so a maliciously large `payload_len` (up to
    // `u32::MAX`) could otherwise overflow the addition and yield a `total` smaller
    // than `GRPC_FRAME_HEADER_LEN`, which would then panic when slicing below.
    if payload_len > MAX_FRAME_SIZE_DEFAULT {
        return Err(OxiRpcError::from(WireError::FrameTooLarge {
            max: MAX_FRAME_SIZE_DEFAULT,
            got: payload_len,
        }));
    }
    let total = GRPC_FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| {
            OxiRpcError::Transport(format!(
            "gRPC frame length overflow: header {GRPC_FRAME_HEADER_LEN} + payload {payload_len}"
        ))
        })?;
    if data.len() < total {
        return Err(OxiRpcError::Transport(format!(
            "gRPC frame payload truncated: need {total}, have {}",
            data.len()
        )));
    }
    let payload = &data[GRPC_FRAME_HEADER_LEN..total];
    T::decode(payload).map_err(|e| OxiRpcError::Proto(e.to_string()))
}

/// Decode a prost message from gRPC-framed [`Bytes`], handling optional compression.
///
/// If the frame's compressed flag is set, decompresses using `encoding` before
/// prost-decoding. If the flag is set but `encoding` is
/// [`CompressionEncoding::Identity`], returns [`OxiRpcError::Compression`].
///
/// # Errors
///
/// - [`OxiRpcError::Transport`] — frame too short, payload truncated, or the claimed
///   payload length exceeds [`MAX_FRAME_SIZE_DEFAULT`].
/// - [`OxiRpcError::Compression`] — compressed flag set but encoding is identity,
///   or decompression backend failed.
/// - [`OxiRpcError::Proto`] — prost decode failure.
pub fn decode_grpc_message_with_encoding<T: Message + Default>(
    data: Bytes,
    encoding: CompressionEncoding,
) -> Result<T, OxiRpcError> {
    if data.len() < GRPC_FRAME_HEADER_LEN {
        return Err(OxiRpcError::Transport(format!(
            "gRPC frame too short: need {GRPC_FRAME_HEADER_LEN}, have {}",
            data.len()
        )));
    }
    let compressed = data[0] != FLAG_UNCOMPRESSED;
    let payload_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    // See the identical guard in `decode_grpc_message` above: reject oversized
    // frames before any arithmetic on the attacker-controlled length, so the
    // subsequent addition can never overflow `usize` on 32-bit/wasm32 targets.
    if payload_len > MAX_FRAME_SIZE_DEFAULT {
        return Err(OxiRpcError::from(WireError::FrameTooLarge {
            max: MAX_FRAME_SIZE_DEFAULT,
            got: payload_len,
        }));
    }
    let total = GRPC_FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| {
            OxiRpcError::Transport(format!(
            "gRPC frame length overflow: header {GRPC_FRAME_HEADER_LEN} + payload {payload_len}"
        ))
        })?;
    if data.len() < total {
        return Err(OxiRpcError::Transport(format!(
            "gRPC frame payload truncated: need {total}, have {}",
            data.len()
        )));
    }
    let payload = data.slice(GRPC_FRAME_HEADER_LEN..total);
    let frame = Frame {
        compressed,
        payload,
    };
    let pipeline = MessagePipeline::new(encoding, FrameOptions::default());
    let decoded_bytes = pipeline.decode_message(&frame).map_err(OxiRpcError::from)?;
    T::decode(decoded_bytes.as_ref()).map_err(|e| OxiRpcError::Proto(e.to_string()))
}

/// Encode a prost message into gRPC-framed [`Bytes`], with optional compression.
///
/// When `encoding` is [`CompressionEncoding::Identity`], this is equivalent to
/// calling [`encode_grpc_message`] (fast path, no allocation overhead).
///
/// # Errors
///
/// Returns [`WireError::Compression`] if the compression backend fails.
pub fn encode_grpc_message_with_encoding<T: Message>(
    msg: &T,
    encoding: CompressionEncoding,
) -> Result<Bytes, WireError> {
    if encoding == CompressionEncoding::Identity {
        return encode_grpc_message(msg);
    }
    let encoded = msg.encode_to_vec();
    let pipeline = MessagePipeline::new(encoding, FrameOptions::default());
    let frame = pipeline.encode_message(&encoded)?;
    let mut buf = BytesMut::with_capacity(GRPC_FRAME_HEADER_LEN + frame.payload.len());
    buf.put_u8(if frame.compressed {
        FLAG_COMPRESSED
    } else {
        FLAG_UNCOMPRESSED
    });
    buf.put_u32(frame.payload.len() as u32);
    buf.put_slice(&frame.payload);
    Ok(buf.freeze())
}

// ─────────────────────────────────────────────────────────────────────────────
// Request reading
// ─────────────────────────────────────────────────────────────────────────────

/// Collect an entire HTTP body and decode a single gRPC-framed prost message from it.
///
/// This is the canonical unary-request reader for identity (uncompressed) frames.
/// It drains the body, then calls [`decode_grpc_message`].
/// For compressed request bodies, use [`read_unary_request_with_encoding`].
///
/// # Errors
///
/// Propagates body collection errors and [`decode_grpc_message`] errors.
pub async fn read_unary_request<T, B>(body: B) -> Result<T, OxiRpcError>
where
    T: Message + Default,
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<OxiRpcError>,
{
    let collected = body.collect().await.map_err(|e| e.into())?;
    let all_bytes = collected.to_bytes();
    decode_grpc_message(all_bytes)
}

/// Collect an entire HTTP body and decode a single gRPC-framed prost message,
/// supporting optional compression via `encoding`.
///
/// Drains the body, then calls [`decode_grpc_message_with_encoding`].
///
/// # Errors
///
/// Propagates body collection errors and [`decode_grpc_message_with_encoding`] errors.
pub async fn read_unary_request_with_encoding<T, B>(
    body: B,
    encoding: CompressionEncoding,
) -> Result<T, OxiRpcError>
where
    T: Message + Default,
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<OxiRpcError>,
{
    let collected = body.collect().await.map_err(|e| e.into())?;
    let all_bytes = collected.to_bytes();
    decode_grpc_message_with_encoding(all_bytes, encoding)
}

// ─────────────────────────────────────────────────────────────────────────────
// Response headers / trailers
// ─────────────────────────────────────────────────────────────────────────────

/// Build initial response headers for a gRPC response (HTTP 200, content-type: application/grpc).
pub fn grpc_response_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/grpc"),
    );
    headers.insert(
        http::header::HeaderName::from_static("te"),
        http::HeaderValue::from_static("trailers"),
    );
    headers
}

/// Build `grpc-status: 0` OK trailers.
pub fn ok_grpc_trailers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::HeaderName::from_static("grpc-status"),
        http::HeaderValue::from_static("0"),
    );
    headers
}

/// Build error trailers with the given gRPC status code and message.
pub fn error_grpc_trailers(code: u32, message: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let code_str = code.to_string();
    headers.insert(
        http::header::HeaderName::from_static("grpc-status"),
        http::HeaderValue::from_str(&code_str)
            .unwrap_or_else(|_| http::HeaderValue::from_static("13")),
    );
    if !message.is_empty() {
        let encoded = percent_encode_message(message);
        if let Ok(v) = http::HeaderValue::from_str(&encoded) {
            headers.insert(http::header::HeaderName::from_static("grpc-message"), v);
        }
    }
    headers
}

// ─────────────────────────────────────────────────────────────────────────────
// Response body builders
// ─────────────────────────────────────────────────────────────────────────────

/// Build a [`NativeBody`] containing one gRPC-framed message followed by OK trailers.
///
/// Used for unary responses.
pub fn unary_response_body<T: Message>(msg: &T) -> Result<NativeBody, WireError> {
    let frame = encode_grpc_message(msg)?;
    let body = NativeBody::once(frame).with_trailers(ok_grpc_trailers());
    Ok(body)
}

/// Build a [`NativeBody`] containing one optionally-compressed gRPC-framed message
/// followed by OK trailers.
///
/// When `encoding` is [`CompressionEncoding::Identity`], this is identical to
/// [`unary_response_body`]. When a compressing encoding is provided, the response
/// payload is compressed and the compressed flag in the 5-byte frame header is set.
///
/// **Note**: callers are responsible for setting the `grpc-encoding` response header
/// to the negotiated encoding string (e.g. `"gzip"`); this helper only compresses
/// the body bytes.
pub fn unary_response_body_compressed<T: Message>(
    msg: &T,
    encoding: CompressionEncoding,
) -> Result<NativeBody, WireError> {
    let frame = encode_grpc_message_with_encoding(msg, encoding)?;
    Ok(NativeBody::once(frame).with_trailers(ok_grpc_trailers()))
}

/// Build a [`NativeBody`] emitting only error trailers (no data frames).
///
/// Used when the handler returns an error before any response data is sent.
pub fn error_response_body(code: u32, message: &str) -> NativeBody {
    NativeBody::empty().with_trailers(error_grpc_trailers(code, message))
}

/// Build a server-streaming response: returns a [`NativeBodySender`] and a [`NativeBody`].
///
/// The caller MUST send trailers via `sender.send_trailers(ok_grpc_trailers())` (or
/// error trailers) before dropping the sender, otherwise the client will not receive
/// proper termination.
pub fn streaming_response_body() -> (NativeBodySender, NativeBody) {
    body_channel(64)
}

// ─────────────────────────────────────────────────────────────────────────────
// Bidi sequential driver
// ─────────────────────────────────────────────────────────────────────────────

/// Drive a bidi sequential request body: spawns a task reading all request frames,
/// calling `handler` for each, sending response frames via channel, then sending
/// OK or error trailers at the end.
///
/// The handler is called once per decoded request, sequentially. On any error the
/// task sends error trailers and stops. On success it sends OK trailers.
///
/// Returns the response [`NativeBody`] immediately (the spawned task runs async).
pub fn bidi_sequential_response<B, ReqT, RespT, F>(req_body: B, mut handler: F) -> NativeBody
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<OxiRpcError> + Send,
    ReqT: Message + Default + 'static,
    RespT: Message + 'static,
    F: FnMut(ReqT) -> Result<RespT, OxiRpcError> + Send + 'static,
{
    let (sender, body) = streaming_response_body();

    task::spawn(async move {
        if let Err(e) = drive_bidi(req_body, &sender, &mut handler).await {
            let (code, msg) = oxirpc_error_to_grpc_status(&e);
            let _ = sender.send_trailers(error_grpc_trailers(code, &msg)).await;
        }
    });

    body
}

/// Internal driver for `bidi_sequential_response`.
///
/// Reads the request body incrementally frame-by-frame using `BodyExt::frame()` +
/// `FrameDecoder`. Each response is sent as soon as its corresponding request frame
/// is fully decoded — enabling interactive bidi (response N emitted before request
/// N+1 arrives).
///
/// # Behavioral notes
/// - **Frame-level errors** (malformed header, truncated payload, FLAG_COMPRESSED
///   mismatch): behavior is identical to the old buffered driver — errors surface
///   inside the loop after some responses may already have been sent.
/// - **Transport-level errors** (`B::Error`): behavioral change vs the old buffered
///   driver. Previously, `collect()` surfaced transport errors *before* any response
///   was sent. Now, a transport error surfaces *after* prior frames have already
///   produced and flushed responses. Error trailers still arrive as the last thing
///   the client sees.
async fn drive_bidi<B, ReqT, RespT, F>(
    mut req_body: B,
    sender: &NativeBodySender,
    handler: &mut F,
) -> Result<(), OxiRpcError>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<OxiRpcError>,
    ReqT: Message + Default,
    RespT: Message,
    F: FnMut(ReqT) -> Result<RespT, OxiRpcError>,
{
    let mut decoder = FrameDecoder::new(FrameOptions::default());
    let mut buf = BytesMut::new();

    loop {
        // Drain any complete gRPC frames from the accumulated buffer first.
        loop {
            match decoder.decode(&mut buf) {
                Err(e) => return Err(OxiRpcError::Transport(e.to_string())),
                Ok(None) => break, // need more data from the HTTP body
                Ok(Some(frame)) => {
                    if frame.compressed {
                        return Err(OxiRpcError::Compression(
                            "bidi stream received a compressed frame but server requires \
                             grpc-encoding identity; use a compression-aware bidi driver \
                             for decompression support"
                                .to_owned(),
                        ));
                    }
                    let req = ReqT::decode(frame.payload.as_ref())
                        .map_err(|e| OxiRpcError::Proto(e.to_string()))?;
                    let resp = handler(req)?;
                    let encoded = encode_grpc_message(&resp)
                        .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
                    sender.send_data(encoded).await?;
                }
            }
        }

        // Fetch the next HTTP/2 DATA frame from the body.
        match req_body.frame().await {
            None => break, // client half-closed the request stream
            Some(Err(e)) => return Err(e.into()),
            Some(Ok(http_frame)) => {
                match http_frame.into_data() {
                    Ok(data) => buf.put(data),
                    Err(_trailers_frame) => {
                        // Trailers or unknown frame — treat as end of data stream.
                        break;
                    }
                }
            }
        }
    }

    // Drain any complete frames remaining in buf after the body stream ended.
    loop {
        match decoder.decode(&mut buf) {
            Err(e) => return Err(OxiRpcError::Transport(e.to_string())),
            Ok(None) => break,
            Ok(Some(frame)) => {
                if frame.compressed {
                    return Err(OxiRpcError::Compression(
                        "bidi stream received a compressed frame but server requires \
                         grpc-encoding identity; use a compression-aware bidi driver \
                         for decompression support"
                            .to_owned(),
                    ));
                }
                let req = ReqT::decode(frame.payload.as_ref())
                    .map_err(|e| OxiRpcError::Proto(e.to_string()))?;
                let resp = handler(req)?;
                let encoded = encode_grpc_message(&resp)
                    .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
                sender.send_data(encoded).await?;
            }
        }
    }

    // Error if a partial gRPC frame was left unconsumed.
    if !buf.is_empty() {
        return Err(OxiRpcError::Transport(format!(
            "truncated gRPC frame at end of bidi stream: {} bytes remaining",
            buf.len()
        )));
    }

    // Send OK trailers.
    sender
        .send_trailers(ok_grpc_trailers())
        .await
        .map_err(|e| OxiRpcError::Transport(e.to_string()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Bidi sequential driver with compression negotiation
// ─────────────────────────────────────────────────────────────────────────────

/// Like [`bidi_sequential_response`] but with compression negotiation.
///
/// - Request frames are decompressed using `encoding` before prost-decode.
/// - Response frames are compressed using `encoding` before sending.
/// - When `encoding == Identity`, behavior is identical to `bidi_sequential_response`.
///
/// Callers are responsible for setting the `grpc-encoding` response header to
/// the negotiated encoding string (e.g. `"gzip"`) when a compressing encoding is
/// used; this helper only handles the wire bytes.
#[cfg(any(feature = "gzip", feature = "zstd"))]
pub fn bidi_sequential_response_with_encoding<B, ReqT, RespT, F>(
    req_body: B,
    encoding: CompressionEncoding,
    mut handler: F,
) -> NativeBody
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<OxiRpcError> + Send,
    ReqT: Message + Default + 'static,
    RespT: Message + 'static,
    F: FnMut(ReqT) -> Result<RespT, OxiRpcError> + Send + 'static,
{
    let (sender, body) = streaming_response_body();
    task::spawn(async move {
        if let Err(e) = drive_bidi_with_encoding(req_body, encoding, &sender, &mut handler).await {
            let (code, msg) = oxirpc_error_to_grpc_status(&e);
            let _ = sender.send_trailers(error_grpc_trailers(code, &msg)).await;
        }
    });
    body
}

/// Internal driver for `bidi_sequential_response_with_encoding`.
///
/// Reads the request body incrementally frame-by-frame using `BodyExt::frame()` +
/// `FrameDecoder`. Each response is sent as soon as its corresponding request frame
/// is fully decoded — enabling interactive bidi (response N emitted before request
/// N+1 arrives).
///
/// Mirrors `drive_bidi` except:
/// - Each incoming frame is decoded via `MessagePipeline::decode_message` (handles
///   the compressed flag with decompression when encoding != Identity).
/// - Each outgoing frame is encoded via `encode_grpc_message_with_encoding`.
/// - When `encoding == Identity` the encoding-aware paths still work correctly (they
///   fall through to the identity fast-path in the underlying helpers).
///
/// # Behavioral notes
/// - **Frame-level errors**: identical to buffered driver — surface inside the loop
///   after some responses may already have been sent.
/// - **Transport-level errors** (`B::Error`): behavioral change vs old driver.
///   Previously surfaced before any response; now surface after prior frames have
///   already produced and flushed responses. Error trailers still arrive last.
#[cfg(any(feature = "gzip", feature = "zstd"))]
async fn drive_bidi_with_encoding<B, ReqT, RespT, F>(
    mut req_body: B,
    encoding: CompressionEncoding,
    sender: &NativeBodySender,
    handler: &mut F,
) -> Result<(), OxiRpcError>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<OxiRpcError>,
    ReqT: Message + Default,
    RespT: Message,
    F: FnMut(ReqT) -> Result<RespT, OxiRpcError>,
{
    let pipeline = MessagePipeline::new(encoding, FrameOptions::default());
    let mut decoder = FrameDecoder::new(FrameOptions::default());
    let mut buf = BytesMut::new();

    loop {
        // Drain any complete gRPC frames from the accumulated buffer first.
        loop {
            match decoder.decode(&mut buf) {
                Err(e) => return Err(OxiRpcError::Transport(e.to_string())),
                Ok(None) => break, // need more data from the HTTP body
                Ok(Some(frame)) => {
                    // Use the pipeline to decompress if needed (handles gzip/zstd).
                    let raw = pipeline.decode_message(&frame).map_err(OxiRpcError::from)?;
                    let req = ReqT::decode(raw.as_ref())
                        .map_err(|e| OxiRpcError::Proto(e.to_string()))?;
                    let resp = handler(req)?;
                    let encoded = encode_grpc_message_with_encoding(&resp, encoding)
                        .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
                    sender.send_data(encoded).await?;
                }
            }
        }

        // Fetch the next HTTP/2 DATA frame from the body.
        match req_body.frame().await {
            None => break, // client half-closed the request stream
            Some(Err(e)) => return Err(e.into()),
            Some(Ok(http_frame)) => {
                match http_frame.into_data() {
                    Ok(data) => buf.put(data),
                    Err(_trailers_frame) => {
                        // Trailers or unknown frame — treat as end of data stream.
                        break;
                    }
                }
            }
        }
    }

    // Drain any complete frames remaining in buf after the body stream ended.
    loop {
        match decoder.decode(&mut buf) {
            Err(e) => return Err(OxiRpcError::Transport(e.to_string())),
            Ok(None) => break,
            Ok(Some(frame)) => {
                let raw = pipeline.decode_message(&frame).map_err(OxiRpcError::from)?;
                let req =
                    ReqT::decode(raw.as_ref()).map_err(|e| OxiRpcError::Proto(e.to_string()))?;
                let resp = handler(req)?;
                let encoded = encode_grpc_message_with_encoding(&resp, encoding)
                    .map_err(|e| OxiRpcError::Transport(e.to_string()))?;
                sender.send_data(encoded).await?;
            }
        }
    }

    // Error if a partial gRPC frame was left unconsumed.
    if !buf.is_empty() {
        return Err(OxiRpcError::Transport(format!(
            "truncated gRPC frame at end of bidi stream: {} bytes remaining",
            buf.len()
        )));
    }

    // Send OK trailers.
    sender
        .send_trailers(ok_grpc_trailers())
        .await
        .map_err(|e| OxiRpcError::Transport(e.to_string()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Status code mapping helper
// ─────────────────────────────────────────────────────────────────────────────

/// Map an `OxiRpcError` to a `(grpc_code, message)` pair for error trailers.
fn oxirpc_error_to_grpc_status(e: &OxiRpcError) -> (u32, String) {
    match e {
        OxiRpcError::Status(s) => (s.code() as u32, s.message().to_owned()),
        OxiRpcError::Transport(msg) => (StatusCode::Unavailable as u32, msg.clone()),
        OxiRpcError::Timeout => (
            StatusCode::DeadlineExceeded as u32,
            "deadline exceeded".to_owned(),
        ),
        OxiRpcError::Cancelled => (
            StatusCode::Cancelled as u32,
            "operation cancelled".to_owned(),
        ),
        OxiRpcError::Build(msg) | OxiRpcError::Tls(msg) | OxiRpcError::Compression(msg) => {
            (StatusCode::Internal as u32, msg.clone())
        }
        OxiRpcError::Proto(msg) => (StatusCode::Internal as u32, msg.clone()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal prost message used only to exercise the generic `T` parameter
    /// of `decode_grpc_message[_with_encoding]` in tests below.
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    /// Build a raw gRPC frame header (flag + big-endian u32 length) with no
    /// payload bytes following it — simulates an attacker sending only the
    /// 5-byte header with a claimed length that is never backed by data.
    fn header_only(payload_len: u32) -> Bytes {
        let mut buf = BytesMut::with_capacity(GRPC_FRAME_HEADER_LEN);
        buf.put_u8(FLAG_UNCOMPRESSED);
        buf.put_u32(payload_len);
        buf.freeze()
    }

    #[test]
    fn decode_grpc_message_round_trip_ok() {
        let msg = TestMessage {
            value: "hello".to_owned(),
        };
        let framed = encode_grpc_message(&msg).unwrap();
        let decoded: TestMessage = decode_grpc_message(framed).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn decode_grpc_message_rejects_oversized_length_without_panic() {
        // Claimed length is one byte over the configured maximum. Regression
        // guard: this must be rejected via the size check, not by computing
        // `GRPC_FRAME_HEADER_LEN + payload_len` and slicing on it.
        let data = header_only((MAX_FRAME_SIZE_DEFAULT + 1) as u32);
        let err = decode_grpc_message::<TestMessage>(data).unwrap_err();
        match err {
            OxiRpcError::Transport(msg) => assert!(
                msg.contains("too large"),
                "expected a 'too large' message, got: {msg}"
            ),
            other => panic!("expected OxiRpcError::Transport, got {other:?}"),
        }
    }

    #[test]
    fn decode_grpc_message_rejects_u32_max_length_without_panic() {
        // A maximal u32 length prefix (~4 GiB). On a 32-bit or wasm32 target,
        // `GRPC_FRAME_HEADER_LEN + payload_len` would overflow `usize` here if
        // computed without a prior size guard, producing a wrapped `total`
        // smaller than `GRPC_FRAME_HEADER_LEN` and panicking on the subsequent
        // slice. The size guard must reject this before any such arithmetic.
        let data = header_only(u32::MAX);
        let err = decode_grpc_message::<TestMessage>(data).unwrap_err();
        assert!(
            matches!(err, OxiRpcError::Transport(_)),
            "expected OxiRpcError::Transport, got {err:?}"
        );
    }

    #[test]
    fn decode_grpc_message_truncated_payload_still_reported_cleanly() {
        // Length is within the allowed maximum but the buffer doesn't actually
        // contain that many payload bytes — must be a clean truncation error,
        // never a panic.
        let mut data = BytesMut::from(header_only(16).as_ref());
        data.put_slice(&[0u8; 4]); // only 4 of the claimed 16 payload bytes
        let err = decode_grpc_message::<TestMessage>(data.freeze()).unwrap_err();
        match err {
            OxiRpcError::Transport(msg) => assert!(
                msg.contains("truncated"),
                "expected a 'truncated' message, got: {msg}"
            ),
            other => panic!("expected OxiRpcError::Transport, got {other:?}"),
        }
    }

    #[test]
    fn decode_grpc_message_with_encoding_rejects_oversized_length_without_panic() {
        let data = header_only((MAX_FRAME_SIZE_DEFAULT + 1) as u32);
        let err =
            decode_grpc_message_with_encoding::<TestMessage>(data, CompressionEncoding::Identity)
                .unwrap_err();
        match err {
            OxiRpcError::Transport(msg) => assert!(
                msg.contains("too large"),
                "expected a 'too large' message, got: {msg}"
            ),
            other => panic!("expected OxiRpcError::Transport, got {other:?}"),
        }
    }

    #[test]
    fn decode_grpc_message_with_encoding_rejects_u32_max_length_without_panic() {
        let data = header_only(u32::MAX);
        let err =
            decode_grpc_message_with_encoding::<TestMessage>(data, CompressionEncoding::Identity)
                .unwrap_err();
        assert!(
            matches!(err, OxiRpcError::Transport(_)),
            "expected OxiRpcError::Transport, got {err:?}"
        );
    }
}
