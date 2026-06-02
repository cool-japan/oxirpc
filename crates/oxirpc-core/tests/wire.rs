//! Integration tests for the `oxirpc_core::wire` module.
//!
//! These tests exercise the full wire layer end-to-end: request headers →
//! frame encoding → response headers → trailer parsing.

use bytes::BytesMut;
use oxirpc_core::{
    metadata::Metadata,
    status::StatusCode,
    wire::{
        frame::encode_frame,
        header::{
            build_request_headers, build_response_headers, parse_response_headers,
            RequestHeaderSpec, ResponseHeaderSpec, CONTENT_TYPE_GRPC_PROTO,
        },
        trailer::{build_trailers, parse_trailers, GrpcResponseStatus},
        Frame, FrameDecoder, FrameEncoder, WireError,
    },
};
use tokio_util::codec::{Decoder, Encoder};

// ─── Test 1: end-to-end header + frame + trailer round-trip ─────────────────

#[test]
fn end_to_end_frame_header_trailer_round_trip() {
    // 1. Build request headers
    let md = Metadata::new();
    let spec = RequestHeaderSpec {
        scheme: "https",
        authority: "localhost:50051",
        path: "/helloworld.Greeter/SayHello",
        content_type: "",
        encoding: None,
        accept_encoding: &[],
        timeout: Some(std::time::Duration::from_secs(5)),
        user_agent: Some("oxirpc-test/0.1"),
        metadata: &md,
    };
    let built = build_request_headers(&spec).unwrap();
    assert_eq!(built.pseudo.path, "/helloworld.Greeter/SayHello");
    assert_eq!(built.pseudo.method, "POST");
    assert_eq!(
        built.headers.get("grpc-timeout").unwrap().to_str().unwrap(),
        "5S"
    );

    // 2. Encode a frame
    let payload = b"proto-encoded request bytes";
    let encoded = encode_frame(payload, false).unwrap();
    assert_eq!(encoded.len(), 5 + payload.len());

    // 3. Build response headers
    let resp_md = Metadata::new();
    let resp_spec = ResponseHeaderSpec {
        content_type: CONTENT_TYPE_GRPC_PROTO,
        encoding: None,
        metadata: &resp_md,
    };
    let resp_headers = build_response_headers(&resp_spec).unwrap();
    let parsed_resp = parse_response_headers(&resp_headers).unwrap();
    assert_eq!(
        parsed_resp.content_type.as_deref(),
        Some(CONTENT_TYPE_GRPC_PROTO)
    );

    // 4. Build and parse trailers
    let status = GrpcResponseStatus {
        code: StatusCode::Ok,
        message: String::new(),
        details_bin: None,
        metadata: Metadata::new(),
    };
    let trailers = build_trailers(&status).unwrap();
    let parsed_status = parse_trailers(&trailers).unwrap();
    assert_eq!(parsed_status.code, StatusCode::Ok);
}

// ─── Test 2: gzip compression round-trip ─────────────────────────────────────

#[cfg(feature = "gzip")]
#[test]
fn cross_feature_gzip_compression_round_trip() {
    use oxirpc_core::{
        encoding::CompressionEncoding,
        wire::{frame::FrameOptions, MessagePipeline},
    };
    let pipeline = MessagePipeline::new(CompressionEncoding::Gzip, FrameOptions::default());
    let original = b"this is the original message payload that will be gzip-compressed";

    // Encode into a Frame
    let frame = pipeline.encode_message(original).unwrap();
    assert!(frame.compressed, "gzip pipeline must set compressed=true");

    // The frame payload should differ from original (it is compressed)
    // Decode back
    let decoded = pipeline.decode_message(&frame).unwrap();
    assert_eq!(decoded.as_ref(), original);
}

// ─── Test 3: backward compat shims ───────────────────────────────────────────

#[allow(deprecated)]
#[test]
fn backward_compat_grpc_frame_fn_still_resolves() {
    // oxirpc_core::grpc::encode_grpc_frame and decode_grpc_frame must still work.
    use oxirpc_core::grpc::{decode_grpc_frame, encode_grpc_frame};
    let payload = b"compatibility check";
    let encoded = encode_grpc_frame(payload, false);
    let (compressed, decoded_payload) = decode_grpc_frame(&encoded).unwrap();
    assert!(!compressed);
    assert_eq!(decoded_payload, payload);
}

// ─── Test 4: FrameEncoder / FrameDecoder codec pair ─────────────────────────

#[test]
fn frame_encoder_decoder_codec_pair() {
    let mut enc = FrameEncoder::default();
    let mut dec = FrameDecoder::default();
    let mut buf = BytesMut::new();

    let input = Frame {
        compressed: false,
        payload: bytes::Bytes::from_static(b"codec integration test"),
    };

    enc.encode(input.clone(), &mut buf).unwrap();
    let output = dec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(output.payload, input.payload);
    assert_eq!(output.compressed, input.compressed);
}

// ─── Test 5: WireError converts to OxiRpcError ───────────────────────────────

#[test]
fn wire_error_converts_to_oxirpc_error() {
    use oxirpc_core::OxiRpcError;

    let wire_err = WireError::Compression("gzip exploded".to_owned());
    let oxirpc_err: OxiRpcError = wire_err.into();
    assert!(
        matches!(oxirpc_err, OxiRpcError::Compression(_)),
        "Compression WireError must map to OxiRpcError::Compression"
    );

    let transport_err = WireError::MissingStatusTrailer;
    let oxirpc_err2: OxiRpcError = transport_err.into();
    assert!(
        matches!(oxirpc_err2, OxiRpcError::Transport(_)),
        "non-Compression WireError must map to OxiRpcError::Transport"
    );
}
