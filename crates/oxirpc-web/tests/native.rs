//! Tests for the native gRPC-Web translation layer.
//!
//! Covers:
//! - `GrpcWebContentType::from_header` detection
//! - `translate_request` (binary and text modes)
//! - `translate_response` (trailer embedding, text-mode base64)
//! - `NativeGrpcWebService` pass-through behaviour (non-gRPC-Web, OPTIONS)
//! - End-to-end gRPC-status in trailer frames
//! - Pass-through for plain `application/grpc` content-type (health check style)

use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::{BodyExt as _, Full};
use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_core::metadata::base64_encode;
use oxirpc_web::{
    codec::{decode_body, encode_body, encode_frame, Frame, FrameKind, FLAG_TRAILER},
    translate::{translate_request, translate_response, GrpcWebContentType},
};
use tonic::body::Body as TonicBody;
use tower::Layer;

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Build a minimal gRPC-Web binary request (5-byte prefix + payload).
fn grpc_web_binary_request(payload: &[u8]) -> Request<Full<Bytes>> {
    let frame = Frame::data(payload.to_vec());
    let body_bytes = encode_frame(&frame, CompressionEncoding::Identity).expect("encode frame");

    Request::builder()
        .method(Method::POST)
        .uri("/grpc.Test/Method")
        .header("content-type", "application/grpc-web")
        .header("origin", "https://example.com")
        .header("x-user-agent", "grpc-web-javascript/0.1")
        .body(Full::new(Bytes::from(body_bytes)))
        .expect("build request")
}

/// Build a minimal gRPC-Web text request (base64-encoded body).
fn grpc_web_text_request(payload: &[u8]) -> Request<Full<Bytes>> {
    let frame = Frame::data(payload.to_vec());
    let body_bytes = encode_frame(&frame, CompressionEncoding::Identity).expect("encode frame");
    let b64 = base64_encode(&body_bytes);

    Request::builder()
        .method(Method::POST)
        .uri("/grpc.Test/Method")
        .header("content-type", "application/grpc-web-text")
        .header("origin", "https://example.com")
        .body(Full::new(Bytes::from(b64.into_bytes())))
        .expect("build request")
}

/// Build a tonic `Response<TonicBody>` with a gRPC data frame as the body
/// and optional trailers in the HTTP trailer position.
fn grpc_response_with_trailer(
    payload: &[u8],
    grpc_status: u32,
    grpc_message: &str,
) -> Response<TonicBody> {
    // The body carries a single data frame.
    let frame = Frame::data(payload.to_vec());
    let body_bytes =
        encode_frame(&frame, CompressionEncoding::Identity).expect("encode data frame");

    // Build the body stream with HTTP trailers via `http_body_util::BodyExt::with_trailers`.
    let base_body = Full::new(Bytes::from(body_bytes));
    let mut trailer_map = http::HeaderMap::new();
    trailer_map.insert(
        http::header::HeaderName::from_static("grpc-status"),
        http::HeaderValue::from_str(&grpc_status.to_string()).expect("header value"),
    );
    trailer_map.insert(
        http::header::HeaderName::from_static("grpc-message"),
        http::HeaderValue::from_str(grpc_message).expect("header value"),
    );
    let body_with_trailers = base_body
        .with_trailers(async move { Some(Ok::<_, std::convert::Infallible>(trailer_map)) });
    let tonic_body = TonicBody::new(body_with_trailers);

    Response::builder()
        .status(200)
        .body(tonic_body)
        .expect("build response")
}

// ─── Test 1: content-type detection ──────────────────────────────────────────

#[test]
fn native_content_type_detection() {
    // Binary variants.
    assert_eq!(
        GrpcWebContentType::from_header("application/grpc-web"),
        Some(GrpcWebContentType::Binary)
    );
    assert_eq!(
        GrpcWebContentType::from_header("application/grpc-web+proto"),
        Some(GrpcWebContentType::Binary)
    );
    // Text variants.
    assert_eq!(
        GrpcWebContentType::from_header("application/grpc-web-text"),
        Some(GrpcWebContentType::Text)
    );
    assert_eq!(
        GrpcWebContentType::from_header("application/grpc-web-text+proto"),
        Some(GrpcWebContentType::Text)
    );
    // Parameters should be ignored.
    assert_eq!(
        GrpcWebContentType::from_header("application/grpc-web; charset=utf-8"),
        Some(GrpcWebContentType::Binary)
    );
    // Non-gRPC-Web types.
    assert_eq!(GrpcWebContentType::from_header("application/json"), None);
    assert_eq!(GrpcWebContentType::from_header("application/grpc"), None);
    assert_eq!(GrpcWebContentType::from_header(""), None);
}

// ─── Test 2: binary request translation ──────────────────────────────────────

#[tokio::test]
async fn native_translates_binary_request_to_grpc() {
    let payload = b"hello gRPC-Web";
    let req = grpc_web_binary_request(payload);

    let translated = translate_request(req, GrpcWebContentType::Binary)
        .await
        .expect("translate_request");

    // Content-Type must be native gRPC.
    let ct = translated
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok());
    assert_eq!(
        ct,
        Some("application/grpc+proto"),
        "content-type must be application/grpc+proto"
    );

    // TE: trailers must be set.
    let te = translated.headers().get("te").and_then(|v| v.to_str().ok());
    assert_eq!(te, Some("trailers"), "te: trailers must be set");

    // CORS-unsafe headers must be stripped.
    assert!(
        !translated.headers().contains_key("origin"),
        "origin header must be stripped"
    );
    assert!(
        !translated.headers().contains_key("x-user-agent"),
        "x-user-agent header must be stripped"
    );

    // Body should still contain the 5-byte-prefixed frame.
    let body_bytes = translated
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();

    let frames = decode_body(&body_bytes, CompressionEncoding::Identity).expect("decode frames");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].kind, FrameKind::Data);
    assert_eq!(frames[0].payload, payload);
}

// ─── Test 3: text request translation ────────────────────────────────────────

#[tokio::test]
async fn native_translates_text_request_to_grpc() {
    let payload = b"text-mode payload bytes";
    let req = grpc_web_text_request(payload);

    let translated = translate_request(req, GrpcWebContentType::Text)
        .await
        .expect("translate_request text");

    // Content-Type must be native gRPC.
    let ct = translated
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok());
    assert_eq!(
        ct,
        Some("application/grpc+proto"),
        "content-type must be application/grpc+proto"
    );

    // Body must be base64-decoded and contain the original 5-byte-prefixed frame.
    let body_bytes = translated
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();

    let frames = decode_body(&body_bytes, CompressionEncoding::Identity).expect("decode frames");
    assert_eq!(frames.len(), 1, "expected 1 data frame");
    assert_eq!(frames[0].kind, FrameKind::Data);
    assert_eq!(
        frames[0].payload, payload,
        "payload must match after base64-decode"
    );
}

// ─── Test 4: response trailers embedded in body ───────────────────────────────

#[tokio::test]
async fn native_translates_response_appends_trailers_frame() {
    let payload = b"response data";
    let resp = grpc_response_with_trailer(payload, 0, "OK");

    let web_resp = translate_response(resp, GrpcWebContentType::Binary).await;

    assert_eq!(web_resp.status(), 200);

    // Collect the gRPC-Web response body.
    let body_bytes = web_resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();

    // Decode all frames — must have a data frame and a trailer frame.
    let frames = decode_body(&body_bytes, CompressionEncoding::Identity).expect("decode frames");

    let has_trailer_frame = frames.iter().any(|f| f.kind == FrameKind::Trailer);
    assert!(
        has_trailer_frame,
        "response body must contain a trailer frame"
    );

    // The trailer frame must have FLAG_TRAILER bit set.
    // Re-encode to verify the flag byte directly.
    let trailer_frame = frames
        .iter()
        .find(|f| f.kind == FrameKind::Trailer)
        .expect("trailer frame");

    // Verify the frame is of Trailer kind (FLAG_TRAILER was set during encode).
    assert_eq!(trailer_frame.kind, FrameKind::Trailer);

    // Trailer must include grpc-status.
    let pairs = trailer_frame.parse_trailers();
    let has_grpc_status = pairs.iter().any(|(k, _)| k == "grpc-status");
    assert!(has_grpc_status, "trailer frame must contain grpc-status");
}

// ─── Test 5: text-mode response is base64-encoded ────────────────────────────

#[tokio::test]
async fn native_text_response_base64_encodes_whole_body() {
    let payload = b"text response payload";
    let resp = grpc_response_with_trailer(payload, 0, "");

    let web_resp = translate_response(resp, GrpcWebContentType::Text).await;

    assert_eq!(web_resp.status(), 200);

    // Content-Type must be text variant.
    let ct = web_resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok());
    assert_eq!(
        ct,
        Some("application/grpc-web-text+proto"),
        "content-type must be text variant"
    );

    let body_bytes = web_resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();

    // The body must be valid ASCII (base64).
    let body_str = std::str::from_utf8(&body_bytes).expect("body is UTF-8");
    assert!(
        body_str
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '='),
        "text-mode response body must be valid base64, got: {body_str:?}"
    );

    // Decode the base64 to verify structure.
    let decoded_binary = oxirpc_core::metadata::base64_decode(body_str).expect("base64 decode");
    let frames =
        decode_body(&decoded_binary, CompressionEncoding::Identity).expect("decode frames");
    assert!(
        frames.iter().any(|f| f.kind == FrameKind::Trailer),
        "base64-decoded body must contain a trailer frame"
    );
}

// ─── Test 6: pass-through for non-gRPC-Web content-type ──────────────────────

#[tokio::test]
async fn native_passes_through_non_grpc_web_requests() {
    use oxirpc_web::native::NativeGrpcWebLayer;
    use std::sync::{Arc, Mutex};

    let recorded_ct: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded_ct.clone();

    // Inner service records the content-type it received and returns a 200.
    let inner = tower::service_fn(move |req: Request<TonicBody>| {
        let ct = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        *recorded_clone.lock().expect("lock") = ct;
        async {
            Ok::<_, std::convert::Infallible>(
                Response::builder()
                    .status(200)
                    .body(TonicBody::default())
                    .expect("response"),
            )
        }
    });

    let mut svc = NativeGrpcWebLayer::new().layer(inner);

    let req: Request<Full<Bytes>> = Request::builder()
        .method(Method::POST)
        .uri("/foo")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(b"{}".to_vec())))
        .expect("request");

    // Use Service::call directly to sidestep type-inference ambiguity with
    // ServiceExt::ready (the compiler struggles to infer the Request type
    // through the generic Service impl).
    let fut = tower::Service::call(&mut svc, req);
    let resp = fut.await.expect("call");

    assert_eq!(resp.status(), 200, "non-gRPC-Web request must pass through");
    assert_eq!(
        *recorded_ct.lock().expect("lock"),
        "application/json",
        "inner service must see original content-type"
    );
}

// ─── Test 7: OPTIONS preflight passes through ─────────────────────────────────

#[tokio::test]
async fn native_passes_through_options_preflight() {
    use oxirpc_web::native::NativeGrpcWebLayer;
    use std::sync::{Arc, Mutex};

    let called: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let called_clone = called.clone();

    let inner = tower::service_fn(move |req: Request<TonicBody>| {
        *called_clone.lock().expect("lock") = true;
        let method = req.method().clone();
        async move {
            Ok::<_, std::convert::Infallible>(
                Response::builder()
                    .status(if method == Method::OPTIONS { 204 } else { 200 })
                    .body(TonicBody::default())
                    .expect("response"),
            )
        }
    });

    let mut svc = NativeGrpcWebLayer::new().layer(inner);

    let req: Request<Full<Bytes>> = Request::builder()
        .method(Method::OPTIONS)
        .uri("/grpc.Test/Method")
        .header("origin", "https://example.com")
        .header("access-control-request-method", "POST")
        .body(Full::new(Bytes::new()))
        .expect("options request");

    // Use Service::call directly to sidestep type-inference ambiguity with
    // ServiceExt::ready (the compiler struggles to infer the Request type
    // through the generic Service impl).
    let fut = tower::Service::call(&mut svc, req);
    let resp = fut.await.expect("call");

    // OPTIONS was passed to inner; inner returned 204.
    assert_eq!(
        resp.status(),
        204,
        "OPTIONS must be passed through to inner"
    );
    assert!(
        *called.lock().expect("lock"),
        "inner service must be called for OPTIONS"
    );
}

// ─── Test 8: grpc-status 2 in trailer ────────────────────────────────────────

#[tokio::test]
async fn native_handles_grpc_status_2_in_trailer() {
    let payload = b"";
    let resp = grpc_response_with_trailer(payload, 2, "UNKNOWN");

    let web_resp = translate_response(resp, GrpcWebContentType::Binary).await;

    let body_bytes = web_resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();

    let frames = decode_body(&body_bytes, CompressionEncoding::Identity).expect("decode frames");

    let trailer_frame = frames
        .iter()
        .find(|f| f.kind == FrameKind::Trailer)
        .expect("trailer frame must be present");

    let pairs = trailer_frame.parse_trailers();
    let status = pairs
        .iter()
        .find(|(k, _)| k == "grpc-status")
        .map(|(_, v)| v.as_str());
    assert_eq!(status, Some("2"), "grpc-status must be 2 in trailer frame");

    let message = pairs
        .iter()
        .find(|(k, _)| k == "grpc-message")
        .map(|(_, v)| v.as_str());
    assert_eq!(message, Some("UNKNOWN"), "grpc-message must be UNKNOWN");

    // Verify the FLAG_TRAILER bit is set by re-encoding the frame.
    let encoded = encode_body(
        std::slice::from_ref(trailer_frame),
        CompressionEncoding::Identity,
    )
    .expect("encode");
    assert_ne!(
        encoded[0] & FLAG_TRAILER,
        0,
        "FLAG_TRAILER (0x80) must be set on the trailer frame byte"
    );
}

// ─── Test 9: pass-through for application/grpc (health-check style) ──────────

/// When the request carries `Content-Type: application/grpc` (plain gRPC, not
/// gRPC-Web), `NativeGrpcWebService` must pass it through to the inner service
/// unchanged.  This is the shape a health-check client might use.
#[tokio::test]
async fn native_layer_with_health_content_type() {
    use oxirpc_web::native::NativeGrpcWebLayer;
    use std::sync::{Arc, Mutex};

    let recorded_ct: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded_ct.clone();

    // Inner service records the content-type it received.
    let inner = tower::service_fn(move |req: Request<TonicBody>| {
        let ct = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        *recorded_clone.lock().expect("lock") = ct;
        async {
            Ok::<_, std::convert::Infallible>(
                Response::builder()
                    .status(200)
                    .body(TonicBody::default())
                    .expect("response"),
            )
        }
    });

    let mut svc = NativeGrpcWebLayer::new().layer(inner);

    // `application/grpc` is NOT a gRPC-Web type → NativeGrpcWebService must pass through.
    let req: Request<Full<Bytes>> = Request::builder()
        .method(Method::POST)
        .uri("/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc+proto")
        .body(Full::new(Bytes::from(vec![0u8, 0, 0, 0, 0]))) // empty 5-byte gRPC frame
        .expect("request");

    let fut = tower::Service::call(&mut svc, req);
    let resp = fut.await.expect("call");

    assert_eq!(
        resp.status(),
        200,
        "plain gRPC health request must pass through"
    );
    assert_eq!(
        *recorded_ct.lock().expect("lock"),
        "application/grpc+proto",
        "inner service must see the original content-type unmodified"
    );
}
