//! Cross-crate integration tests: NativeGrpcWebLayer wrapping NativeReflectionServiceV1.
//!
//! These tests prove that the gRPC-Web translation layer works end-to-end with
//! a real native reflection service:
//!
//! 1. Binary-mode gRPC-Web ListServices roundtrip.
//! 2. Text-mode (base64) gRPC-Web ListServices roundtrip.
//! 3. FileByFilename lookup via gRPC-Web.
//! 4. Unknown file yields an error embedded in the proto response.
//! 5. NativeGrpcWebService<NativeReflectionServiceV1> is a valid tower::Service.

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt as _, Full};
use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_reflect::{DescriptorPoolBuilder, NativeReflectionServiceV1, ReflectionBuilder};
use oxirpc_web::codec::{
    decode_body, decode_text_body, encode_body, encode_text_body, Frame, FrameKind,
};
use oxirpc_web::native::{NativeGrpcWebLayer, NativeGrpcWebService};
use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet, ServiceDescriptorProto};
use tower::{Layer as _, Service};

// ─── test helpers ────────────────────────────────────────────────────────────

/// Build a [`NativeReflectionServiceV1`] backed by a minimal single-service pool.
///
/// Returns the service and the expected bare service name (`"TestService"`).
fn make_reflection_service() -> (NativeReflectionServiceV1, String) {
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".to_owned()),
            package: Some("test".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let pool = DescriptorPoolBuilder::new().register(fds).build();
    let svc = ReflectionBuilder::new()
        .register_pool(pool)
        .build_native_v1();
    (svc, "TestService".to_owned())
}

/// Encode a prost message as a gRPC-Web binary request body.
///
/// The proto bytes are wrapped in a single 5-byte gRPC data frame.
fn grpc_web_binary_body(msg: &impl Message) -> Bytes {
    let proto_bytes = msg.encode_to_vec();
    let frame = Frame::data(proto_bytes);
    let encoded = encode_body(&[frame], CompressionEncoding::Identity)
        .expect("frame encoding is infallible for identity compression");
    Bytes::from(encoded)
}

/// Encode a prost message as a gRPC-Web text (base64) request body.
fn grpc_web_text_body(msg: &impl Message) -> Bytes {
    let proto_bytes = msg.encode_to_vec();
    let frame = Frame::data(proto_bytes);
    let text = encode_text_body(&[frame], CompressionEncoding::Identity)
        .expect("text frame encoding is infallible for identity compression");
    Bytes::from(text.into_bytes())
}

/// Build a POST request for the reflection bidi-streaming endpoint.
fn build_reflection_request(body: Bytes, content_type: &'static str) -> Request<Full<Bytes>> {
    Request::builder()
        .method(http::Method::POST)
        .uri("/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")
        .header(http::header::CONTENT_TYPE, content_type)
        .body(Full::new(body))
        .expect("request builder produces a valid request")
}

/// Call a [`NativeGrpcWebService<NativeReflectionServiceV1>`] with the given request.
///
/// This helper avoids the type-inference issues that arise from `ServiceExt::ready`
/// when the request body type is not concretely constrained at the call site.
async fn call_svc(
    svc: &mut NativeGrpcWebService<NativeReflectionServiceV1>,
    req: Request<Full<Bytes>>,
) -> http::Response<tonic::body::Body> {
    use std::future::poll_fn;
    use std::task::Poll;

    // Poll the service to readiness (NativeReflectionServiceV1 is always ready).
    // The explicit turbofish fixes type-inference for the `Request` type parameter.
    poll_fn(|cx| {
        let poll = <NativeGrpcWebService<NativeReflectionServiceV1> as Service<
            Request<Full<Bytes>>,
        >>::poll_ready(svc, cx);
        assert!(
            matches!(poll, Poll::Ready(Ok(()))),
            "service must be immediately ready"
        );
        poll
    })
    .await
    .expect("poll_ready must not error");

    svc.call(req).await.expect("service call must not error")
}

// ─── Test 1: binary-mode ListServices roundtrip ──────────────────────────────

#[tokio::test]
async fn reflect_list_services_via_grpc_web_binary() {
    use oxirpc_reflect::proto::{
        server_reflection_request, server_reflection_response, ServerReflectionRequest,
        ServerReflectionResponse,
    };

    let (reflect_svc, _expected_bare_name) = make_reflection_service();
    let mut svc: NativeGrpcWebService<NativeReflectionServiceV1> =
        NativeGrpcWebLayer::new().layer(reflect_svc);

    // Build a ListServices request and encode it as binary gRPC-Web.
    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(server_reflection_request::MessageRequest::ListServices(
            String::new(),
        )),
    };
    let body = grpc_web_binary_body(&req_proto);
    let http_req = build_reflection_request(body, "application/grpc-web+proto");

    let resp = call_svc(&mut svc, http_req).await;
    assert_eq!(resp.status(), 200, "gRPC-Web response must be HTTP 200");

    // Collect and decode the gRPC-Web framed response body.
    let raw_bytes = resp
        .into_body()
        .collect()
        .await
        .expect("response body collection must succeed")
        .to_bytes();

    let frames = decode_body(&raw_bytes, CompressionEncoding::Identity)
        .expect("response must be valid gRPC-Web frames");

    let data_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == FrameKind::Data)
        .collect();
    assert!(
        !data_frames.is_empty(),
        "response must contain at least one data frame"
    );

    let resp_proto = ServerReflectionResponse::decode(data_frames[0].payload.as_slice())
        .expect("data frame payload must be a valid ServerReflectionResponse");

    match resp_proto.message_response {
        Some(server_reflection_response::MessageResponse::ListServicesResponse(list_resp)) => {
            let names: Vec<&str> = list_resp.service.iter().map(|s| s.name.as_str()).collect();
            assert!(
                names.iter().any(|n| n.contains("TestService")),
                "ListServicesResponse must include TestService; got: {names:?}"
            );
        }
        other => panic!("expected ListServicesResponse, got: {other:?}"),
    }
}

// ─── Test 2: text-mode gRPC-Web ListServices roundtrip ───────────────────────

#[tokio::test]
async fn reflect_via_grpc_web_text_mode() {
    use oxirpc_reflect::proto::{
        server_reflection_request, server_reflection_response, ServerReflectionRequest,
        ServerReflectionResponse,
    };

    let (reflect_svc, _) = make_reflection_service();
    let mut svc: NativeGrpcWebService<NativeReflectionServiceV1> =
        NativeGrpcWebLayer::new().layer(reflect_svc);

    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(server_reflection_request::MessageRequest::ListServices(
            String::new(),
        )),
    };
    // Text-mode: base64-encode the framed request body.
    let body = grpc_web_text_body(&req_proto);
    let http_req = build_reflection_request(body, "application/grpc-web-text+proto");

    let resp = call_svc(&mut svc, http_req).await;
    assert_eq!(resp.status(), 200);

    // Text-mode responses are base64-encoded.
    let raw_bytes = resp
        .into_body()
        .collect()
        .await
        .expect("body collect")
        .to_bytes();

    let text = std::str::from_utf8(&raw_bytes)
        .expect("text-mode response body must be valid UTF-8 (base64 ASCII)");
    let frames = decode_text_body(text, CompressionEncoding::Identity)
        .expect("must decode valid base64 gRPC-Web text body");

    let data_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == FrameKind::Data)
        .collect();
    assert!(
        !data_frames.is_empty(),
        "text-mode response must contain at least one data frame"
    );

    let resp_proto = ServerReflectionResponse::decode(data_frames[0].payload.as_slice())
        .expect("valid ServerReflectionResponse in text-mode data frame");

    match resp_proto.message_response {
        Some(server_reflection_response::MessageResponse::ListServicesResponse(list_resp)) => {
            let names: Vec<&str> = list_resp.service.iter().map(|s| s.name.as_str()).collect();
            assert!(
                names.iter().any(|n| n.contains("TestService")),
                "text-mode ListServices must include TestService; got: {names:?}"
            );
        }
        other => panic!("expected ListServicesResponse in text mode, got: {other:?}"),
    }
}

// ─── Test 3: FileByFilename via gRPC-Web ─────────────────────────────────────

#[tokio::test]
async fn reflect_file_by_name_via_grpc_web() {
    use oxirpc_reflect::proto::{
        server_reflection_request, server_reflection_response, ServerReflectionRequest,
        ServerReflectionResponse,
    };

    let (reflect_svc, _) = make_reflection_service();
    let mut svc: NativeGrpcWebService<NativeReflectionServiceV1> =
        NativeGrpcWebLayer::new().layer(reflect_svc);

    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(server_reflection_request::MessageRequest::FileByFilename(
            "test.proto".to_owned(),
        )),
    };
    let body = grpc_web_binary_body(&req_proto);
    let http_req = build_reflection_request(body, "application/grpc-web+proto");

    let resp = call_svc(&mut svc, http_req).await;
    assert_eq!(resp.status(), 200);

    let raw_bytes = resp
        .into_body()
        .collect()
        .await
        .expect("body collect")
        .to_bytes();

    let frames =
        decode_body(&raw_bytes, CompressionEncoding::Identity).expect("valid gRPC-Web frames");

    let data_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == FrameKind::Data)
        .collect();
    assert!(
        !data_frames.is_empty(),
        "response must contain at least one data frame"
    );

    let resp_proto = ServerReflectionResponse::decode(data_frames[0].payload.as_slice())
        .expect("valid ServerReflectionResponse");

    match resp_proto.message_response {
        Some(server_reflection_response::MessageResponse::FileDescriptorResponse(fdr)) => {
            assert!(
                !fdr.file_descriptor_proto.is_empty(),
                "FileDescriptorResponse must contain at least one FileDescriptorProto"
            );
            // Decode the returned file descriptor and verify it is "test.proto".
            let fdp = FileDescriptorProto::decode(fdr.file_descriptor_proto[0].as_slice())
                .expect("file_descriptor_proto bytes must decode to FileDescriptorProto");
            assert_eq!(
                fdp.name.as_deref(),
                Some("test.proto"),
                "returned FileDescriptorProto must have name 'test.proto'"
            );
        }
        other => panic!("expected FileDescriptorResponse, got: {other:?}"),
    }
}

// ─── Test 4: unknown file returns an embedded error response ──────────────────

#[tokio::test]
async fn reflect_unknown_service_returns_error_via_grpc_web() {
    use oxirpc_reflect::proto::{
        server_reflection_request, server_reflection_response, ServerReflectionRequest,
        ServerReflectionResponse,
    };

    let (reflect_svc, _) = make_reflection_service();
    let mut svc: NativeGrpcWebService<NativeReflectionServiceV1> =
        NativeGrpcWebLayer::new().layer(reflect_svc);

    // Request a file that was never registered.
    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(server_reflection_request::MessageRequest::FileByFilename(
            "nonexistent.proto".to_owned(),
        )),
    };
    let body = grpc_web_binary_body(&req_proto);
    let http_req = build_reflection_request(body, "application/grpc-web+proto");

    let resp = call_svc(&mut svc, http_req).await;

    // gRPC embeds errors in the proto payload inside a 200 HTTP response.
    assert_eq!(
        resp.status(),
        200,
        "gRPC error is embedded in the proto body; HTTP status stays 200"
    );

    let raw_bytes = resp
        .into_body()
        .collect()
        .await
        .expect("body collect")
        .to_bytes();

    let frames =
        decode_body(&raw_bytes, CompressionEncoding::Identity).expect("valid gRPC-Web frames");

    let data_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == FrameKind::Data)
        .collect();
    assert!(
        !data_frames.is_empty(),
        "response must contain at least one data frame"
    );

    let resp_proto = ServerReflectionResponse::decode(data_frames[0].payload.as_slice())
        .expect("valid ServerReflectionResponse");

    assert!(
        matches!(
            resp_proto.message_response,
            Some(server_reflection_response::MessageResponse::ErrorResponse(
                _
            ))
        ),
        "unknown file lookup must embed an ErrorResponse in the proto payload; got: {:?}",
        resp_proto.message_response
    );
}

// ─── Test 5: NativeGrpcWebService<NativeReflectionServiceV1> is callable ─────

#[tokio::test]
async fn grpc_web_layer_preserves_reflection_service_name() {
    use oxirpc_reflect::proto::{
        server_reflection_request, server_reflection_response, ServerReflectionRequest,
        ServerReflectionResponse,
    };

    // The NamedService constant must be the canonical v1 name on the inner type.
    assert_eq!(
        <NativeReflectionServiceV1 as tonic::server::NamedService>::NAME,
        "grpc.reflection.v1.ServerReflection",
        "NativeReflectionServiceV1 must advertise the v1 service name"
    );

    // Confirm the wrapped service is fully callable through NativeGrpcWebLayer.
    let (reflect_svc, _) = make_reflection_service();
    let mut svc: NativeGrpcWebService<NativeReflectionServiceV1> =
        NativeGrpcWebLayer::new().layer(reflect_svc);

    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(server_reflection_request::MessageRequest::ListServices(
            String::new(),
        )),
    };
    let body = grpc_web_binary_body(&req_proto);
    let http_req = build_reflection_request(body, "application/grpc-web+proto");

    let resp = call_svc(&mut svc, http_req).await;
    assert_eq!(resp.status(), 200);

    let raw_bytes = resp
        .into_body()
        .collect()
        .await
        .expect("body collect")
        .to_bytes();

    let frames =
        decode_body(&raw_bytes, CompressionEncoding::Identity).expect("valid gRPC-Web frames");

    let data_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == FrameKind::Data)
        .collect();
    assert!(
        !data_frames.is_empty(),
        "wrapped service must produce at least one data frame"
    );

    let resp_proto = ServerReflectionResponse::decode(data_frames[0].payload.as_slice())
        .expect("valid ServerReflectionResponse from wrapped service");

    assert!(
        matches!(
            resp_proto.message_response,
            Some(server_reflection_response::MessageResponse::ListServicesResponse(_))
        ),
        "wrapped service must return ListServicesResponse; got: {:?}",
        resp_proto.message_response
    );
}
