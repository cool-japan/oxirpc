//! Tests for `oxirpc-build`'s native service-stub code generator.
//!
//! These tests exercise [`ServiceCodegen`] at the string level (pattern matching
//! in the generated source) as well as a smoke-compilation test that writes a
//! generated file and verifies the on-disk result.

use oxirpc_build::codegen::{to_snake_case, ServiceCodegen};
use prost_types::{FileDescriptorProto, MethodDescriptorProto, ServiceDescriptorProto};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a [`ServiceDescriptorProto`] for the classic `Greeter` service.
///
/// Contains one method of each streaming kind:
/// - `SayHello` — unary
/// - `SayHelloServerStream` — server-streaming
/// - `SayHelloClientStream` — client-streaming
/// - `SayHelloBidi` — bidi-streaming
fn make_greeter_service() -> ServiceDescriptorProto {
    ServiceDescriptorProto {
        name: Some("Greeter".into()),
        method: vec![
            // Unary
            MethodDescriptorProto {
                name: Some("SayHello".into()),
                input_type: Some(".helloworld.HelloRequest".into()),
                output_type: Some(".helloworld.HelloReply".into()),
                client_streaming: Some(false),
                server_streaming: Some(false),
                ..Default::default()
            },
            // Server-streaming
            MethodDescriptorProto {
                name: Some("SayHelloServerStream".into()),
                input_type: Some(".helloworld.HelloRequest".into()),
                output_type: Some(".helloworld.HelloReply".into()),
                client_streaming: Some(false),
                server_streaming: Some(true),
                ..Default::default()
            },
            // Client-streaming
            MethodDescriptorProto {
                name: Some("SayHelloClientStream".into()),
                input_type: Some(".helloworld.HelloRequest".into()),
                output_type: Some(".helloworld.HelloReply".into()),
                client_streaming: Some(true),
                server_streaming: Some(false),
                ..Default::default()
            },
            // Bidi-streaming
            MethodDescriptorProto {
                name: Some("SayHelloBidi".into()),
                input_type: Some(".helloworld.HelloRequest".into()),
                output_type: Some(".helloworld.HelloReply".into()),
                client_streaming: Some(true),
                server_streaming: Some(true),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

/// Build a minimal [`FileDescriptorProto`] wrapping the greeter service.
fn make_greeter_file() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some("helloworld/greeter.proto".into()),
        package: Some("helloworld".into()),
        service: vec![make_greeter_service()],
        ..Default::default()
    }
}

/// Return a unique temp directory path for this test invocation.
fn unique_temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "oxirpc-build-codegen-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ))
}

// ---------------------------------------------------------------------------
// Test 1 – server module contains trait and struct
// ---------------------------------------------------------------------------

#[test]
fn test_generate_greeter_server_contains_trait() {
    let codegen = ServiceCodegen::new();
    let svc = make_greeter_service();
    let out = codegen.generate_server("helloworld", &svc);

    assert!(
        out.contains("trait Greeter"),
        "server output should contain `trait Greeter`; got:\n{out}"
    );
    assert!(
        out.contains("struct GreeterServer"),
        "server output should contain `struct GreeterServer`; got:\n{out}"
    );
    // Should also have the module name
    assert!(
        out.contains("pub mod greeter_server"),
        "server output should contain `pub mod greeter_server`; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 2 – client module contains struct
// ---------------------------------------------------------------------------

#[test]
fn test_generate_greeter_client_contains_struct() {
    let codegen = ServiceCodegen::new();
    let svc = make_greeter_service();
    let out = codegen.generate_client("helloworld", &svc);

    assert!(
        out.contains("struct GreeterClient"),
        "client output should contain `struct GreeterClient`; got:\n{out}"
    );
    assert!(
        out.contains("pub mod greeter_client"),
        "client output should contain `pub mod greeter_client`; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 3 – unary method is snake_case
// ---------------------------------------------------------------------------

#[test]
fn test_generate_unary_method_produces_snake_case() {
    let codegen = ServiceCodegen::new();
    let svc = make_greeter_service();

    // Check both server and client sides.
    let server_out = codegen.generate_server("helloworld", &svc);
    assert!(
        server_out.contains("async fn say_hello("),
        "server trait should contain `async fn say_hello(`; got:\n{server_out}"
    );

    let client_out = codegen.generate_client("helloworld", &svc);
    assert!(
        client_out.contains("async fn say_hello("),
        "client should contain `async fn say_hello(`; got:\n{client_out}"
    );
}

// ---------------------------------------------------------------------------
// Test 4 – server-streaming method signature
// ---------------------------------------------------------------------------

#[test]
fn test_generate_server_streaming_method() {
    let codegen = ServiceCodegen::new();
    // Only server-streaming method
    let svc = ServiceDescriptorProto {
        name: Some("StreamSvc".into()),
        method: vec![MethodDescriptorProto {
            name: Some("GetItems".into()),
            input_type: Some(".pkg.GetRequest".into()),
            output_type: Some(".pkg.Item".into()),
            client_streaming: Some(false),
            server_streaming: Some(true),
            ..Default::default()
        }],
        ..Default::default()
    };

    let out = codegen.generate_server("pkg", &svc);

    // Associated stream type should be present in the trait.
    assert!(
        out.contains("type GetItemsStream"),
        "server-streaming method must declare an associated stream type; got:\n{out}"
    );
    // The method should use the associated type in the return position.
    assert!(
        out.contains("Response<Self::GetItemsStream>"),
        "server-streaming method return type should use `Self::GetItemsStream`; got:\n{out}"
    );
    // The method parameter must be a single (non-streaming) request.
    assert!(
        out.contains("request: Request<super::GetRequest>"),
        "server-streaming method should take a plain `Request<…>`; got:\n{out}"
    );
    // dispatch arm must call server_streaming
    assert!(
        out.contains("grpc.server_streaming"),
        "dispatch arm must call `grpc.server_streaming`; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 5 – bidi-streaming method signature
// ---------------------------------------------------------------------------

#[test]
fn test_generate_bidi_streaming_method() {
    let codegen = ServiceCodegen::new();
    let svc = ServiceDescriptorProto {
        name: Some("ChatSvc".into()),
        method: vec![MethodDescriptorProto {
            name: Some("Chat".into()),
            input_type: Some(".chat.Msg".into()),
            output_type: Some(".chat.Msg".into()),
            client_streaming: Some(true),
            server_streaming: Some(true),
            ..Default::default()
        }],
        ..Default::default()
    };

    let out = codegen.generate_server("chat", &svc);

    assert!(
        out.contains("type ChatStream"),
        "bidi method must declare an associated stream type; got:\n{out}"
    );
    assert!(
        out.contains("tonic::Streaming<super::Msg>"),
        "bidi method must accept `tonic::Streaming<…>` request; got:\n{out}"
    );
    assert!(
        out.contains("grpc.streaming"),
        "dispatch arm must call `grpc.streaming`; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 6 – to_snake_case
// ---------------------------------------------------------------------------

#[test]
fn test_to_snake_case_converts_correctly() {
    let cases = [
        ("SayHello", "say_hello"),
        ("Hello", "hello"),
        ("SayHelloWorldResponse", "say_hello_world_response"),
        ("GetHTTP", "get_h_t_t_p"),
        ("GetItems", "get_items"),
        ("", ""),
        ("A", "a"),
        ("ABc", "a_bc"),
    ];

    for (input, expected) in cases {
        let got = to_snake_case(input);
        assert_eq!(
            got, expected,
            "to_snake_case({input:?}) should be {expected:?}, got {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 7 – generate_services writes files
// ---------------------------------------------------------------------------

#[test]
fn test_generate_services_writes_files() {
    let dir = unique_temp_dir("write-files");
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let fds = prost_types::FileDescriptorSet {
        file: vec![make_greeter_file()],
    };

    let codegen = ServiceCodegen::new();
    let written = oxirpc_build::generate_services(&fds, &dir, &codegen)
        .expect("generate_services should succeed");

    // At least one file should have been written.
    assert_eq!(
        written.len(),
        1,
        "expected one output file, got {written:?}"
    );

    let out_path = &written[0];
    assert!(
        out_path.exists(),
        "expected output file to exist: {out_path:?}"
    );

    let content = std::fs::read_to_string(out_path).expect("read generated file");
    assert!(
        content.contains("pub mod greeter_server"),
        "generated file should contain greeter_server module"
    );
    assert!(
        content.contains("pub mod greeter_client"),
        "generated file should contain greeter_client module"
    );

    // Filename convention: <stem>.services.rs
    assert!(
        out_path.file_name().and_then(|n| n.to_str()) == Some("greeter.services.rs"),
        "expected filename `greeter.services.rs`, got {:?}",
        out_path.file_name()
    );

    // Cleanup
    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Test 8 – NamedService constant matches package.Service
// ---------------------------------------------------------------------------

#[test]
fn test_named_service_constant_matches_package_service() {
    let codegen = ServiceCodegen::new();
    let svc = make_greeter_service();
    let out = codegen.generate_server("helloworld", &svc);

    // The SERVICE_NAME constant should be "helloworld.Greeter"
    assert!(
        out.contains("SERVICE_NAME: &str = \"helloworld.Greeter\""),
        "expected SERVICE_NAME = \"helloworld.Greeter\" in generated server; got:\n{out}"
    );
    // NamedService impl should reference SERVICE_NAME
    assert!(
        out.contains("const NAME: &'static str = SERVICE_NAME"),
        "NamedService impl should use SERVICE_NAME; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 9 – no-package service uses bare service name
// ---------------------------------------------------------------------------

#[test]
fn test_no_package_service_name() {
    let codegen = ServiceCodegen::new();
    let svc = ServiceDescriptorProto {
        name: Some("FooService".into()),
        method: vec![MethodDescriptorProto {
            name: Some("DoThing".into()),
            input_type: Some(".FooRequest".into()),
            output_type: Some(".FooResponse".into()),
            client_streaming: Some(false),
            server_streaming: Some(false),
            ..Default::default()
        }],
        ..Default::default()
    };

    // Empty package string
    let out = codegen.generate_server("", &svc);

    assert!(
        out.contains("SERVICE_NAME: &str = \"FooService\""),
        "no-package service name should be bare service name; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 10 – generate() with emit_server=false produces no server module
// ---------------------------------------------------------------------------

#[test]
fn test_emit_server_false_suppresses_server_module() {
    let mut codegen = ServiceCodegen::new();
    codegen.emit_server = false;
    let file = make_greeter_file();
    let out = codegen.generate(&file);

    assert!(
        !out.contains("greeter_server"),
        "emit_server=false should suppress the server module; got:\n{out}"
    );
    // Client should still be present
    assert!(
        out.contains("greeter_client"),
        "emit_server=false should still emit the client module; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 11 – generate() with emit_client=false produces no client module
// ---------------------------------------------------------------------------

#[test]
fn test_emit_client_false_suppresses_client_module() {
    let mut codegen = ServiceCodegen::new();
    codegen.emit_client = false;
    let file = make_greeter_file();
    let out = codegen.generate(&file);

    assert!(
        !out.contains("greeter_client"),
        "emit_client=false should suppress the client module; got:\n{out}"
    );
    assert!(
        out.contains("greeter_server"),
        "emit_client=false should still emit the server module; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 12 – client-streaming method signature
// ---------------------------------------------------------------------------

#[test]
fn test_generate_client_streaming_method() {
    let codegen = ServiceCodegen::new();
    let svc = ServiceDescriptorProto {
        name: Some("UploadSvc".into()),
        method: vec![MethodDescriptorProto {
            name: Some("Upload".into()),
            input_type: Some(".upload.Chunk".into()),
            output_type: Some(".upload.UploadResult".into()),
            client_streaming: Some(true),
            server_streaming: Some(false),
            ..Default::default()
        }],
        ..Default::default()
    };

    let server_out = codegen.generate_server("upload", &svc);
    // Server trait should accept Streaming<Chunk>
    assert!(
        server_out.contains("tonic::Streaming<super::Chunk>"),
        "client-streaming server method must accept Streaming<Chunk>; got:\n{server_out}"
    );
    assert!(
        server_out.contains("grpc.client_streaming"),
        "dispatch arm must call `grpc.client_streaming`; got:\n{server_out}"
    );

    let client_out = codegen.generate_client("upload", &svc);
    // Client method should use IntoStreamingRequest
    assert!(
        client_out.contains("IntoStreamingRequest"),
        "client-streaming client method must use IntoStreamingRequest; got:\n{client_out}"
    );
    assert!(
        client_out.contains("client_streaming("),
        "client method should call client_streaming; got:\n{client_out}"
    );
}

// ---------------------------------------------------------------------------
// Test 13 – native codegen generates both client and server
// ---------------------------------------------------------------------------

/// Verify that `ServiceCodegen` (the native backend) generates both a
/// `*_server` module and a `*_client` module when both `emit_server` and
/// `emit_client` are `true` (the defaults).
///
/// This test operates at the string level — it exercises the same path that
/// `Builder::compile()` uses by default (without `legacy-tonic-codegen`).
#[test]
fn native_codegen_generates_both_client_and_server() {
    let file = make_greeter_file();
    let codegen = ServiceCodegen::new();

    // generate() is the entry point used by file_gen::generate_services.
    let out = codegen.generate(&file);

    assert!(
        out.contains("pub mod greeter_server"),
        "native codegen must emit greeter_server module; got:\n{out}"
    );
    assert!(
        out.contains("pub mod greeter_client"),
        "native codegen must emit greeter_client module; got:\n{out}"
    );
    assert!(
        out.contains("trait Greeter"),
        "native codegen server module must contain the Greeter trait; got:\n{out}"
    );
    assert!(
        out.contains("struct GreeterClient"),
        "native codegen client module must contain GreeterClient struct; got:\n{out}"
    );
}

// ---------------------------------------------------------------------------
// Test 14 – legacy tonic-prost-build still works under feature
// ---------------------------------------------------------------------------

/// Verify that the legacy tonic-prost-build codegen backend compiles and
/// links correctly when the `legacy-tonic-codegen` feature is enabled.
///
/// This test only runs with `--features legacy-tonic-codegen`.
#[cfg(feature = "legacy-tonic-codegen")]
#[test]
fn legacy_tonic_codegen_still_works_under_feature() {
    use oxirpc_build::Builder;
    use std::fs;

    let proto_src = r#"
syntax = "proto3";
package legacytest;
service LegacySvc { rpc Call (LegacyMsg) returns (LegacyMsg); }
message LegacyMsg { string value = 1; }
"#;

    // Write the proto to a temp dir and compile using the legacy path.
    let tmp_dir = std::env::temp_dir().join(format!(
        "oxirpc-build-legacy-codegen-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    fs::create_dir_all(&tmp_dir).expect("create tmp dir");
    let proto_file = tmp_dir.join("legacytest.proto");
    fs::write(&proto_file, proto_src).expect("write proto");

    let out_dir = tmp_dir.join("out");
    fs::create_dir_all(&out_dir).expect("create out dir");

    let result = Builder::new()
        .out_dir(&out_dir)
        .compile(&[&proto_file], &[&tmp_dir]);

    fs::remove_dir_all(&tmp_dir).ok();

    // Legacy path should succeed.
    assert!(
        result.is_ok(),
        "legacy-tonic-codegen path must succeed; got: {:?}",
        result.err()
    );
}
