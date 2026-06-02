//! Tests for the native gRPC-Web frame codec, CORS policy builder,
//! content-type negotiation, header translation, and stream sequencer.

use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_web::codec::{
    decode_body, decode_text_body, encode_body, encode_frame, encode_text_body, Frame, FrameError,
    FrameKind,
};
use oxirpc_web::cors::CorsPolicy;
use oxirpc_web::negotiate::{
    metadata_to_wire_headers, negotiate, response_content_type, wire_headers_to_metadata,
    GrpcWebConfig, SequencerError, StreamSequencer, WebMode,
};

// ─── frame codec (identity / no-compression) ────────────────────────────────

#[test]
fn frame_round_trip_single_data() {
    let frame = Frame::data(b"hello gRPC-Web".to_vec());
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode");
    // 5-byte prefix + payload
    assert_eq!(encoded.len(), 5 + 14);
    assert_eq!(encoded[0], 0x00, "data frame flag");
    let decoded = decode_body(&encoded, CompressionEncoding::Identity).expect("decode");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0], frame);
}

#[test]
fn frame_length_prefix_is_big_endian() {
    let frame = Frame::data(vec![0u8; 0x0102]);
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode");
    // length 0x0102 → bytes [0x00, 0x00, 0x01, 0x02]
    assert_eq!(&encoded[1..5], &[0x00, 0x00, 0x01, 0x02]);
}

#[test]
fn body_round_trip_data_plus_trailer() {
    let data = Frame::data(b"payload".to_vec());
    let trailers = Frame::trailers(&[("grpc-status", "0"), ("grpc-message", "ok")]);
    let body = encode_body(
        &[data.clone(), trailers.clone()],
        CompressionEncoding::Identity,
    )
    .expect("encode");

    let decoded = decode_body(&body, CompressionEncoding::Identity).expect("decode");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].kind, FrameKind::Data);
    assert_eq!(decoded[1].kind, FrameKind::Trailer);

    let parsed = decoded[1].parse_trailers();
    assert_eq!(
        parsed,
        vec![
            ("grpc-status".to_owned(), "0".to_owned()),
            ("grpc-message".to_owned(), "ok".to_owned()),
        ]
    );
}

#[test]
fn trailer_frame_flag_set() {
    let trailers = Frame::trailers(&[("grpc-status", "5")]);
    let encoded = encode_frame(&trailers, CompressionEncoding::Identity).expect("encode");
    assert_eq!(encoded[0] & 0x80, 0x80, "trailer flag must be set");
}

#[test]
fn text_mode_round_trip() {
    let data = Frame::data(b"text-mode message".to_vec());
    let trailers = Frame::trailers(&[("grpc-status", "0")]);
    let text = encode_text_body(
        &[data.clone(), trailers.clone()],
        CompressionEncoding::Identity,
    )
    .expect("encode");
    // base64 text should not contain raw NUL bytes
    assert!(text.is_ascii());

    let decoded = decode_text_body(&text, CompressionEncoding::Identity).expect("decode text");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0], data);
    assert_eq!(decoded[1].kind, FrameKind::Trailer);
}

#[test]
fn decode_truncated_header_errors() {
    // Fewer than 5 bytes — cannot read a full frame header.
    let body = [0x00, 0x00, 0x01];
    assert_eq!(
        decode_body(&body, CompressionEncoding::Identity),
        Err(FrameError::Truncated)
    );
}

#[test]
fn decode_length_overflow_errors() {
    // Frame claims 100 bytes but body has none.
    let mut body = vec![0x00];
    body.extend_from_slice(&100u32.to_be_bytes());
    assert_eq!(
        decode_body(&body, CompressionEncoding::Identity),
        Err(FrameError::LengthOverflow)
    );
}

#[test]
fn empty_body_decodes_to_no_frames() {
    let decoded = decode_body(&[], CompressionEncoding::Identity).expect("decode empty");
    assert!(decoded.is_empty());
}

// ─── CORS policy ────────────────────────────────────────────────────────────

#[test]
fn cors_explicit_origin_allowed() {
    let policy = CorsPolicy::new().allow_origin("https://app.example.com");
    assert!(policy.is_origin_allowed("https://app.example.com"));
    assert!(!policy.is_origin_allowed("https://evil.example.com"));

    let headers = policy.preflight_headers("https://app.example.com");
    let allow_origin = headers
        .iter()
        .find(|(k, _)| k == "access-control-allow-origin")
        .map(|(_, v)| v.as_str());
    assert_eq!(allow_origin, Some("https://app.example.com"));
}

#[test]
fn cors_disallowed_origin_yields_no_headers() {
    let policy = CorsPolicy::new().allow_origin("https://app.example.com");
    let headers = policy.preflight_headers("https://other.example.com");
    assert!(headers.is_empty());
}

#[test]
fn cors_any_origin_returns_wildcard() {
    let policy = CorsPolicy::new().allow_any_origin();
    let headers = policy.preflight_headers("https://anything.example.com");
    let allow_origin = headers
        .iter()
        .find(|(k, _)| k == "access-control-allow-origin")
        .map(|(_, v)| v.as_str());
    assert_eq!(allow_origin, Some("*"));
}

#[test]
fn cors_any_origin_with_credentials_echoes_origin() {
    let policy = CorsPolicy::new().allow_any_origin().allow_credentials(true);
    let headers = policy.preflight_headers("https://app.example.com");
    let allow_origin = headers
        .iter()
        .find(|(k, _)| k == "access-control-allow-origin")
        .map(|(_, v)| v.as_str());
    // `*` is illegal with credentials → concrete origin echoed.
    assert_eq!(allow_origin, Some("https://app.example.com"));
    assert!(headers
        .iter()
        .any(|(k, v)| k == "access-control-allow-credentials" && v == "true"));
}

#[test]
fn cors_preflight_includes_methods_and_max_age() {
    let policy = CorsPolicy::new()
        .allow_origin("https://app.example.com")
        .allow_method("POST")
        .max_age_secs(600);
    let headers = policy.preflight_headers("https://app.example.com");

    assert!(headers
        .iter()
        .any(|(k, v)| k == "access-control-allow-methods" && v.contains("POST")));
    assert!(headers
        .iter()
        .any(|(k, v)| k == "access-control-max-age" && v == "600"));
}

#[test]
fn cors_response_exposes_grpc_status() {
    let policy = CorsPolicy::new().allow_origin("https://app.example.com");
    let headers = policy.response_headers("https://app.example.com");
    let exposed = headers
        .iter()
        .find(|(k, _)| k == "access-control-expose-headers")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    assert!(exposed.contains("grpc-status"));
    assert!(exposed.contains("grpc-message"));
}

// ─── content-type negotiation ────────────────────────────────────────────────

#[test]
fn negotiate_binary_content_types() {
    assert_eq!(negotiate("application/grpc-web"), Some(WebMode::Binary));
    assert_eq!(
        negotiate("application/grpc-web+proto"),
        Some(WebMode::Binary)
    );
}

#[test]
fn negotiate_text_content_types() {
    assert_eq!(negotiate("application/grpc-web-text"), Some(WebMode::Text));
    assert_eq!(
        negotiate("application/grpc-web-text+proto"),
        Some(WebMode::Text)
    );
}

#[test]
fn negotiate_unknown_returns_none() {
    assert_eq!(negotiate("application/json"), None);
    assert_eq!(negotiate("text/plain"), None);
    assert_eq!(negotiate("application/grpc"), None);
    assert_eq!(negotiate(""), None);
}

#[test]
fn negotiate_ignores_parameters() {
    assert_eq!(
        negotiate("application/grpc-web; charset=utf-8"),
        Some(WebMode::Binary)
    );
}

#[test]
fn response_content_type_values() {
    assert_eq!(
        response_content_type(WebMode::Binary),
        "application/grpc-web+proto"
    );
    assert_eq!(
        response_content_type(WebMode::Text),
        "application/grpc-web-text+proto"
    );
}

// ─── header round-trip ───────────────────────────────────────────────────────

#[test]
fn header_round_trip_ascii_key() {
    use oxirpc_core::metadata::Metadata;

    let mut meta = Metadata::new();
    meta.insert("x-trace-id", "abc123").unwrap();

    let headers = metadata_to_wire_headers(&meta);
    let recovered = wire_headers_to_metadata(&headers).expect("parse headers");
    assert_eq!(recovered.get("x-trace-id"), Some("abc123"));
}

#[test]
fn header_round_trip_bin_key() {
    use oxirpc_core::metadata::Metadata;

    let binary_value: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
    let mut meta = Metadata::new();
    meta.insert_bin("token-bin", &binary_value).unwrap();

    let headers = metadata_to_wire_headers(&meta);
    // The wire value for a -bin key must be base64 (no raw binary).
    let bin_wire = headers
        .iter()
        .find(|(k, _)| k == "token-bin")
        .map(|(_, v)| v.as_str())
        .expect("token-bin must be present");
    // base64 characters only
    assert!(bin_wire
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '='));

    let recovered = wire_headers_to_metadata(&headers).expect("parse headers");
    assert_eq!(recovered.get_bin("token-bin"), Some(binary_value));
}

// ─── StreamSequencer ─────────────────────────────────────────────────────────

#[test]
fn stream_sequencer_single_push_complete_frame() {
    let frame = Frame::data(b"hello streaming".to_vec());
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode");

    let mut seq = StreamSequencer::new();
    let frames = seq.push(&encoded).expect("push");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload, b"hello streaming");
    assert_eq!(frames[0].kind, FrameKind::Data);
}

#[test]
fn stream_sequencer_split_mid_length_prefix() {
    // 20-byte payload; frame = 5-byte header + 20-byte payload = 25 bytes.
    let payload = b"12345678901234567890"; // exactly 20 bytes
    let frame = Frame::data(payload.to_vec());
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode");
    assert_eq!(encoded.len(), 25);

    // Split at byte 2 (inside the 4-byte length field).
    let (part1, part2) = encoded.split_at(2);

    let mut seq = StreamSequencer::new();
    let frames1 = seq.push(part1).expect("push part1");
    assert!(frames1.is_empty(), "incomplete frame should not be emitted");

    let frames2 = seq.push(part2).expect("push part2");
    assert_eq!(frames2.len(), 1);
    assert_eq!(frames2[0].payload, payload);
}

#[test]
fn stream_sequencer_two_frames_across_chunks() {
    let f1 = Frame::data(b"frame one".to_vec());
    let f2 = Frame::trailers(&[("grpc-status", "0")]);
    let mut encoded = encode_frame(&f1, CompressionEncoding::Identity).expect("encode f1");
    encoded
        .extend_from_slice(&encode_frame(&f2, CompressionEncoding::Identity).expect("encode f2"));

    // Split so that f1 is complete in part1 and f2 is incomplete.
    let split = encode_frame(&f1, CompressionEncoding::Identity)
        .expect("encode f1")
        .len()
        + 3; // 3 bytes into f2 header
    let (part1, part2) = encoded.split_at(split);

    let mut seq = StreamSequencer::new();
    let frames1 = seq.push(part1).expect("push part1");
    assert_eq!(frames1.len(), 1);
    assert_eq!(frames1[0].kind, FrameKind::Data);

    let frames2 = seq.push(part2).expect("push part2");
    assert_eq!(frames2.len(), 1);
    assert_eq!(frames2[0].kind, FrameKind::Trailer);
}

#[test]
fn stream_sequencer_max_message_size_rejected() {
    let big_payload = vec![0u8; 100];
    let frame = Frame::data(big_payload);
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode");

    // Allow only 50 bytes.
    let mut seq = StreamSequencer::with_max_message_bytes(50);
    let result = seq.push(&encoded);
    assert_eq!(
        result,
        Err(SequencerError::MessageTooLarge {
            length: 100,
            max: 50
        })
    );
}

// ─── GrpcWebConfig ───────────────────────────────────────────────────────────

#[test]
fn grpc_web_config_defaults() {
    let cfg = GrpcWebConfig::new(WebMode::Binary);
    assert_eq!(cfg.mode, WebMode::Binary);
    assert_eq!(cfg.compression, CompressionEncoding::Identity);
    assert_eq!(cfg.max_message_bytes, 4 * 1024 * 1024);
}

#[test]
fn grpc_web_config_builder() {
    let cfg = GrpcWebConfig::new(WebMode::Text)
        .with_compression(CompressionEncoding::Gzip)
        .with_max_message_bytes(1024);
    assert_eq!(cfg.mode, WebMode::Text);
    assert_eq!(cfg.compression, CompressionEncoding::Gzip);
    assert_eq!(cfg.max_message_bytes, 1024);
}

// ─── Compressed frame round-trip (gzip feature) ──────────────────────────────

#[cfg(feature = "gzip")]
#[test]
fn compressed_frame_roundtrip_gzip() {
    let payload = b"hello world repeated hello world hello world";
    let frame = Frame::data(payload.to_vec());
    let frame_bytes = encode_frame(&frame, CompressionEncoding::Gzip).expect("encode compressed");

    // First byte should have FLAG_COMPRESSED set.
    assert_ne!(
        frame_bytes[0] & oxirpc_web::codec::FLAG_COMPRESSED,
        0,
        "FLAG_COMPRESSED must be set"
    );

    let frames = decode_body(&frame_bytes, CompressionEncoding::Gzip).expect("decode compressed");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload, payload);
    // After decompression, compressed flag should be cleared.
    assert!(!frames[0].compressed);
}

#[cfg(feature = "gzip")]
#[test]
fn compressed_text_mode_roundtrip_gzip() {
    let data = Frame::data(b"compressed text mode payload".to_vec());
    let text = encode_text_body(std::slice::from_ref(&data), CompressionEncoding::Gzip)
        .expect("encode text");
    assert!(text.is_ascii());

    let decoded = decode_text_body(&text, CompressionEncoding::Gzip).expect("decode text");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].payload, data.payload);
}

#[cfg(feature = "gzip")]
#[test]
fn trailer_frames_not_compressed() {
    use oxirpc_web::codec::FLAG_COMPRESSED;

    let trailer = Frame::trailers(&[("grpc-status", "0")]);
    let encoded = encode_frame(&trailer, CompressionEncoding::Gzip).expect("encode trailer");
    // FLAG_COMPRESSED must NOT be set on a trailer frame, per the gRPC-Web spec.
    assert_eq!(
        encoded[0] & FLAG_COMPRESSED,
        0,
        "trailer frame must never have FLAG_COMPRESSED set"
    );
}

#[cfg(feature = "zstd")]
#[test]
fn compressed_frame_roundtrip_zstd() {
    let payload = b"hello world repeated hello world hello world";
    let frame = Frame::data(payload.to_vec());
    let frame_bytes = encode_frame(&frame, CompressionEncoding::Zstd).expect("encode zstd");

    assert_ne!(
        frame_bytes[0] & oxirpc_web::codec::FLAG_COMPRESSED,
        0,
        "FLAG_COMPRESSED must be set for zstd"
    );

    let frames = decode_body(&frame_bytes, CompressionEncoding::Zstd).expect("decode zstd");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload, payload);
    assert!(!frames[0].compressed);
}

// ─── CorsLayer / CorsService tower integration ───────────────────────────────

#[test]
fn cors_layer_type_checks() {
    use oxirpc_web::{
        cors::CorsPolicy,
        grpc_web_layer_with_config, grpc_web_layer_with_cors,
        negotiate::{GrpcWebConfig, WebMode},
    };
    // Verify the functions compile and produce usable values.
    let _layer1 = grpc_web_layer_with_cors(CorsPolicy::new().allow_any_origin());
    let _layer2 = grpc_web_layer_with_config(
        GrpcWebConfig::new(WebMode::Binary),
        Some(CorsPolicy::new().allow_any_origin()),
    );
    let _layer3 = grpc_web_layer_with_config(GrpcWebConfig::new(WebMode::Text), None);
}

#[tokio::test]
async fn cors_service_preflight_returns_204() {
    use http::{Method, Request};
    use oxirpc_web::cors::{CorsLayer, CorsPolicy};
    use tower::{Layer, Service, ServiceExt};

    // Inner service returns a plain 200.  Body type is `()` so that
    // `CorsService<_>` can satisfy `ResBody: Default` on the preflight path.
    let inner = tower::service_fn(|_req: Request<()>| async {
        Ok::<_, std::convert::Infallible>(
            http::Response::builder()
                .status(200)
                .body(())
                .expect("response build"),
        )
    });

    let policy = CorsPolicy::new().allow_any_origin();
    let mut svc = CorsLayer::new(policy).layer(inner);

    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/grpc.Foo/Bar")
        .header("origin", "https://example.com")
        .body(())
        .expect("request build");

    let resp = svc
        .ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(resp.status(), 204, "preflight must return 204 No Content");
    let has_cors_origin = resp.headers().contains_key("access-control-allow-origin");
    assert!(
        has_cors_origin,
        "preflight response must include access-control-allow-origin"
    );
}

#[tokio::test]
async fn cors_service_injects_headers_on_normal_request() {
    use http::{Method, Request};
    use oxirpc_web::cors::{CorsLayer, CorsPolicy};
    use tower::{Layer, Service, ServiceExt};

    let inner = tower::service_fn(|_req: Request<()>| async {
        Ok::<_, std::convert::Infallible>(
            http::Response::builder()
                .status(200)
                .body(())
                .expect("response build"),
        )
    });

    let policy = CorsPolicy::new().allow_origin("https://app.example.com");
    let mut svc = CorsLayer::new(policy).layer(inner);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/grpc.Foo/Bar")
        .header("origin", "https://app.example.com")
        .body(())
        .expect("request build");

    let resp = svc
        .ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().contains_key("access-control-allow-origin"),
        "non-preflight response must include access-control-allow-origin"
    );
}

#[tokio::test]
async fn cors_service_no_headers_for_disallowed_origin() {
    use http::{Method, Request};
    use oxirpc_web::cors::{CorsLayer, CorsPolicy};
    use tower::{Layer, Service, ServiceExt};

    let inner = tower::service_fn(|_req: Request<()>| async {
        Ok::<_, std::convert::Infallible>(
            http::Response::builder()
                .status(200)
                .body(())
                .expect("response build"),
        )
    });

    let policy = CorsPolicy::new().allow_origin("https://app.example.com");
    let mut svc = CorsLayer::new(policy).layer(inner);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/grpc.Foo/Bar")
        .header("origin", "https://evil.example.com")
        .body(())
        .expect("request build");

    let resp = svc
        .ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(resp.status(), 200);
    assert!(
        !resp.headers().contains_key("access-control-allow-origin"),
        "disallowed origin must NOT get CORS headers"
    );
}

// ─── Additional codec round-trip tests ──────────────────────────────────────

#[test]
fn base64_frame_roundtrip_multi() {
    let frames = vec![
        Frame::data(b"hello world".to_vec()),
        Frame::data(b"second message".to_vec()),
    ];
    let text = encode_text_body(&frames, CompressionEncoding::Identity).expect("encode text");
    let decoded = decode_text_body(&text, CompressionEncoding::Identity).expect("decode text");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].payload, b"hello world");
    assert_eq!(decoded[1].payload, b"second message");
}

#[test]
fn trailer_frame_roundtrip_binary() {
    let trailer = Frame::trailers(&[("grpc-status", "0"), ("grpc-message", "ok")]);
    assert_eq!(trailer.kind, FrameKind::Trailer);
    let encoded = encode_body(&[trailer], CompressionEncoding::Identity).expect("encode");
    let decoded = decode_body(&encoded, CompressionEncoding::Identity).expect("decode");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].kind, FrameKind::Trailer);
    let pairs = decoded[0].parse_trailers();
    assert!(pairs.iter().any(|(k, v)| k == "grpc-status" && v == "0"));
}

#[test]
fn error_status_in_trailer_roundtrip() {
    let trailer = Frame::trailers(&[
        ("grpc-status", "2"),
        ("grpc-message", "something went wrong"),
    ]);
    let encoded = encode_body(&[trailer], CompressionEncoding::Identity).expect("encode");
    let decoded = decode_body(&encoded, CompressionEncoding::Identity).expect("decode");
    assert_eq!(decoded.len(), 1);
    let pairs = decoded[0].parse_trailers();
    let status_val = pairs
        .iter()
        .find(|(k, _)| k == "grpc-status")
        .map(|(_, v)| v.as_str());
    assert_eq!(status_val, Some("2"));
    let msg_val = pairs
        .iter()
        .find(|(k, _)| k == "grpc-message")
        .map(|(_, v)| v.as_str());
    assert_eq!(msg_val, Some("something went wrong"));
}

// ─── GrpcWebPrefixLayer tests ────────────────────────────────────────────────

#[test]
fn grpc_web_prefix_layer_accessor() {
    use oxirpc_web::GrpcWebPrefixLayer;
    let layer = GrpcWebPrefixLayer::new("/api");
    assert_eq!(layer.prefix(), "/api");
}

#[test]
fn grpc_web_prefix_layer_with_prefix_fn() {
    use oxirpc_web::grpc_web_layer_with_prefix;
    // Verify the convenience function compiles and produces a usable value.
    let _layer = grpc_web_layer_with_prefix("/api");
}

#[tokio::test]
async fn grpc_web_prefix_strips_path() {
    use http::{Method, Request};
    use oxirpc_web::GrpcWebPrefixLayer;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};

    let recorded = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded.clone();

    let inner = tower::service_fn(move |req: Request<()>| {
        let path = req.uri().path().to_owned();
        *recorded_clone.lock().expect("lock") = path;
        async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(())
                    .expect("response build"),
            )
        }
    });

    let layer = GrpcWebPrefixLayer::new("/api");
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/mypackage.MyService/Method")
        .body(())
        .expect("request build");

    svc.ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(
        *recorded.lock().expect("lock"),
        "/mypackage.MyService/Method"
    );
}

#[tokio::test]
async fn grpc_web_prefix_no_match_passes_through() {
    use http::{Method, Request};
    use oxirpc_web::GrpcWebPrefixLayer;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};

    let recorded = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded.clone();

    let inner = tower::service_fn(move |req: Request<()>| {
        let path = req.uri().path().to_owned();
        *recorded_clone.lock().expect("lock") = path;
        async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(())
                    .expect("response build"),
            )
        }
    });

    let layer = GrpcWebPrefixLayer::new("/api");
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/other/mypackage.MyService/Method")
        .body(())
        .expect("request build");

    svc.ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    // Path without prefix is passed through unchanged.
    assert_eq!(
        *recorded.lock().expect("lock"),
        "/other/mypackage.MyService/Method"
    );
}

#[tokio::test]
async fn grpc_web_prefix_exact_match_yields_root() {
    use http::{Method, Request};
    use oxirpc_web::GrpcWebPrefixLayer;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};

    let recorded = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded.clone();

    let inner = tower::service_fn(move |req: Request<()>| {
        let path = req.uri().path().to_owned();
        *recorded_clone.lock().expect("lock") = path;
        async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(())
                    .expect("response build"),
            )
        }
    });

    let layer = GrpcWebPrefixLayer::new("/api");
    let mut svc = layer.layer(inner);

    // URI path exactly equals the prefix — should map to "/".
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api")
        .body(())
        .expect("request build");

    svc.ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(*recorded.lock().expect("lock"), "/");
}

#[tokio::test]
async fn grpc_web_prefix_substring_not_stripped() {
    use http::{Method, Request};
    use oxirpc_web::GrpcWebPrefixLayer;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};

    let recorded = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded.clone();

    let inner = tower::service_fn(move |req: Request<()>| {
        let path = req.uri().path().to_owned();
        *recorded_clone.lock().expect("lock") = path;
        async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(())
                    .expect("response build"),
            )
        }
    });

    // "/api" must NOT be stripped from "/apifoo" (boundary-safe match).
    let layer = GrpcWebPrefixLayer::new("/api");
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/apifoo/bar")
        .body(())
        .expect("request build");

    svc.ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(*recorded.lock().expect("lock"), "/apifoo/bar");
}

#[tokio::test]
async fn grpc_web_prefix_preserves_query_string() {
    use http::{Method, Request};
    use oxirpc_web::GrpcWebPrefixLayer;
    use std::sync::{Arc, Mutex};
    use tower::{Layer, Service, ServiceExt};

    let recorded = Arc::new(Mutex::new(String::new()));
    let recorded_clone = recorded.clone();

    let inner = tower::service_fn(move |req: Request<()>| {
        // Capture both path and query.
        let uri = req.uri().clone();
        let path_and_query = match uri.query() {
            Some(q) => format!("{}?{}", uri.path(), q),
            None => uri.path().to_owned(),
        };
        *recorded_clone.lock().expect("lock") = path_and_query;
        async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(())
                    .expect("response build"),
            )
        }
    });

    let layer = GrpcWebPrefixLayer::new("/api");
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/foo?bar=baz&x=1")
        .body(())
        .expect("request build");

    svc.ready()
        .await
        .expect("service ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(*recorded.lock().expect("lock"), "/foo?bar=baz&x=1");
}

// ─── Error response tests ────────────────────────────────────────────────────

/// Verify every gRPC status code (0-16) survives a trailer encode→decode cycle.
/// Codes 0 (OK) through 16 (UNAUTHENTICATED) must all be preserved bit-for-bit.
#[test]
fn all_grpc_status_codes_in_trailers() {
    for code in 0u32..=16 {
        let code_str = code.to_string();
        let trailer = Frame::trailers(&[("grpc-status", &code_str), ("grpc-message", "test")]);
        let encoded = encode_body(&[trailer], CompressionEncoding::Identity).expect("encode");
        let decoded = decode_body(&encoded, CompressionEncoding::Identity).expect("decode");
        assert_eq!(
            decoded.len(),
            1,
            "expected exactly one frame for code {code}"
        );
        let pairs = decoded[0].parse_trailers();
        let status = pairs
            .iter()
            .find(|(k, _)| k == "grpc-status")
            .map(|(_, v)| v.as_str());
        assert_eq!(
            status,
            Some(code_str.as_str()),
            "Status code {code} not preserved"
        );
    }
}

/// Server-streaming shape: two data frames followed by a trailer frame.
///
/// This is distinct from `body_round_trip_data_plus_trailer` (one data + trailer)
/// and covers the common server-streaming pattern where multiple response messages
/// precede the final trailer.
#[test]
fn two_data_frames_then_trailer_roundtrip() {
    let frame1 = Frame::data(b"first chunk".to_vec());
    let frame2 = Frame::data(b"second chunk".to_vec());
    let trailer_frame = Frame::trailers(&[("grpc-status", "0"), ("grpc-message", "OK")]);

    let encoded = encode_body(
        &[frame1, frame2, trailer_frame],
        CompressionEncoding::Identity,
    )
    .expect("encode");
    let decoded = decode_body(&encoded, CompressionEncoding::Identity).expect("decode");

    assert_eq!(decoded.len(), 3, "expected 3 frames: two data + trailer");
    assert_eq!(decoded[0].kind, FrameKind::Data);
    assert_eq!(decoded[0].payload, b"first chunk");
    assert_eq!(decoded[1].kind, FrameKind::Data);
    assert_eq!(decoded[1].payload, b"second chunk");
    assert_eq!(decoded[2].kind, FrameKind::Trailer);

    let trailers = decoded[2].parse_trailers();
    assert!(
        trailers.iter().any(|(k, v)| k == "grpc-status" && v == "0"),
        "grpc-status 0 must be present in final trailer"
    );
}

/// A UTF-8 gRPC error message in a trailer must survive encode→decode intact.
///
/// gRPC-Web encodes `grpc-message` as a percent-encoded UTF-8 string; the codec
/// must not corrupt multi-byte sequences.
#[test]
fn error_trailer_with_unicode_message() {
    let msg = "服务不可用"; // Chinese: "Service Unavailable"
    let trailer = Frame::trailers(&[("grpc-status", "14"), ("grpc-message", msg)]);
    let encoded = encode_body(&[trailer], CompressionEncoding::Identity).expect("encode");
    let decoded = decode_body(&encoded, CompressionEncoding::Identity).expect("decode");
    assert_eq!(decoded.len(), 1);
    let pairs = decoded[0].parse_trailers();
    let decoded_msg = pairs
        .iter()
        .find(|(k, _)| k == "grpc-message")
        .map(|(_, v)| v.as_str());
    assert_eq!(
        decoded_msg,
        Some(msg),
        "unicode grpc-message must be preserved"
    );
}

// ─── Integration tests with oxirpc-health ────────────────────────────────────

/// A `HealthCheckRequest` round-trips through the binary gRPC-Web codec
/// (5-byte prefix encode → decode) without corruption.
///
/// A `HealthCheckResponse` in text mode (base64) also round-trips, and the
/// accompanying trailer frame is decoded correctly.
#[test]
fn health_request_roundtrips_through_grpc_web_codec() {
    use oxirpc_health::proto::{HealthCheckRequest, HealthCheckResponse};
    use prost::Message as _;

    // ── binary request round-trip ─────────────────────────────────────────
    let req = HealthCheckRequest {
        service: "my-service".to_string(),
    };
    let req_bytes = req.encode_to_vec();

    let frame = Frame::data(req_bytes.clone());
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode ok");

    let frames = decode_body(&encoded, CompressionEncoding::Identity).expect("decode ok");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].kind, FrameKind::Data);

    let decoded_req =
        HealthCheckRequest::decode(frames[0].payload.as_slice()).expect("proto decode");
    assert_eq!(decoded_req.service, "my-service");

    // ── text-mode response round-trip ─────────────────────────────────────
    let resp = HealthCheckResponse { status: 1 }; // SERVING
    let resp_bytes = resp.encode_to_vec();

    let resp_frame = Frame::data(resp_bytes);
    let trailer_frame = Frame::trailers(&[("grpc-status", "0")]);
    let text_body = encode_text_body(&[resp_frame, trailer_frame], CompressionEncoding::Identity)
        .expect("encode text ok");

    let frames =
        decode_text_body(&text_body, CompressionEncoding::Identity).expect("decode text ok");
    assert_eq!(frames.len(), 2);

    let decoded_resp =
        HealthCheckResponse::decode(frames[0].payload.as_slice()).expect("proto decode resp");
    assert_eq!(decoded_resp.status, 1, "status must be SERVING (1)");

    assert_eq!(
        frames[1].kind,
        FrameKind::Trailer,
        "second frame must be a trailer"
    );
    let trailers = frames[1].parse_trailers();
    assert!(
        trailers.iter().any(|(k, v)| k == "grpc-status" && v == "0"),
        "trailer frame must contain grpc-status: 0"
    );
}

/// A `HealthCheckRequest` with an empty service field (server aggregate health)
/// encodes and decodes through the gRPC-Web binary codec without loss.
#[test]
fn health_request_empty_service_roundtrip() {
    use oxirpc_health::proto::HealthCheckRequest;
    use prost::Message as _;

    let req = HealthCheckRequest {
        service: String::new(),
    };
    let frame = Frame::data(req.encode_to_vec());
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode ok");

    let frames = decode_body(&encoded, CompressionEncoding::Identity).expect("decode ok");
    assert_eq!(frames.len(), 1);

    let decoded = HealthCheckRequest::decode(frames[0].payload.as_slice()).expect("proto decode");
    assert_eq!(decoded.service, "");
}

/// A `HealthCheckResponse` carrying `NOT_SERVING` (2) round-trips through the
/// gRPC-Web binary codec and the proto status field is preserved.
#[test]
fn health_response_not_serving_roundtrip() {
    use oxirpc_health::proto::{HealthCheckResponse, ServingStatusProto};
    use prost::Message as _;

    let resp = HealthCheckResponse::not_serving();
    assert_eq!(resp.status, ServingStatusProto::NotServing as i32);

    let frame = Frame::data(resp.encode_to_vec());
    let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode ok");

    let frames = decode_body(&encoded, CompressionEncoding::Identity).expect("decode ok");
    assert_eq!(frames.len(), 1);

    let decoded = HealthCheckResponse::decode(frames[0].payload.as_slice()).expect("proto decode");
    assert_eq!(decoded.status, ServingStatusProto::NotServing as i32);
}

/// A complete server-streaming health response (data frame + error trailer)
/// encodes to a text body and decodes back with the error status preserved.
#[test]
fn health_error_response_text_mode_roundtrip() {
    use oxirpc_health::proto::HealthCheckResponse;
    use prost::Message as _;

    // Simulate a NOT_SERVING response followed by a grpc-status 14 (UNAVAILABLE) trailer.
    let resp = HealthCheckResponse::not_serving();
    let data_frame = Frame::data(resp.encode_to_vec());
    let trailer_frame = Frame::trailers(&[
        ("grpc-status", "14"),
        ("grpc-message", "service unavailable"),
    ]);

    let text_body = encode_text_body(&[data_frame, trailer_frame], CompressionEncoding::Identity)
        .expect("encode text ok");

    // The text body must be ASCII (base64).
    assert!(text_body.is_ascii(), "text body must be valid ASCII base64");

    let frames =
        decode_text_body(&text_body, CompressionEncoding::Identity).expect("decode text ok");
    assert_eq!(frames.len(), 2, "must decode to 2 frames: data + trailer");

    let decoded_resp =
        HealthCheckResponse::decode(frames[0].payload.as_slice()).expect("proto decode");
    assert_eq!(decoded_resp.status, 2, "status must be NOT_SERVING (2)");

    let trailers = frames[1].parse_trailers();
    let status = trailers
        .iter()
        .find(|(k, _)| k == "grpc-status")
        .map(|(_, v)| v.as_str());
    assert_eq!(status, Some("14"), "grpc-status 14 must be present");
}
