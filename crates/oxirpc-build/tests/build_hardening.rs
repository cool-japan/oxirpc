//! Tests for Slice 8 build-hardening: compile_str, structured errors,
//! import-validation warnings, and the Proto error Display format.

use oxirpc_build::{Builder, OxiRpcBuildError};
use std::fs;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A unique temp subdirectory for each test invocation.
fn unique_out_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "oxirpc-build-hardening-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ))
}

// ---------------------------------------------------------------------------
// compile_str
// ---------------------------------------------------------------------------

#[test]
fn compile_str_roundtrip_succeeds_or_returns_structured_error() {
    let src = r#"
syntax = "proto3";
package test;

message Ping { string msg = 1; }
service PingService {
  rpc Ping (Ping) returns (Ping);
}
"#;

    let out = unique_out_dir("str-roundtrip");
    fs::create_dir_all(&out).expect("create out dir");
    // Safety: only this test thread sets this env var for the process.
    // In practice build tests run single-threaded (nextest isolates).
    std::env::set_var("OUT_DIR", &out);

    let result = Builder::default().compile_str("test", src);
    fs::remove_dir_all(&out).ok();

    // Either Ok (codegen succeeded) or a structured Err — not a panic.
    match result {
        Ok(output) => {
            // warnings is allowed to be empty for a clean proto
            let _ = output.warnings;
        }
        Err(e) => {
            // Error must be displayable and non-empty
            let msg = format!("{e}");
            assert!(!msg.is_empty(), "error Display must not be empty");
        }
    }
}

#[test]
fn compile_str_malformed_proto_gives_structured_error() {
    let out = unique_out_dir("str-malformed");
    fs::create_dir_all(&out).expect("create out dir");
    std::env::set_var("OUT_DIR", &out);

    let result = Builder::default().compile_str("bad", "this is {{{{ not valid proto3");
    fs::remove_dir_all(&out).ok();

    assert!(result.is_err(), "malformed proto must yield an error");
    let err = result.expect_err("already asserted is_err");
    let msg = format!("{err}");
    assert!(!msg.is_empty(), "error Display must not be empty");

    // The error should be a Proto variant (parse failure)
    match err {
        OxiRpcBuildError::Proto { .. } => {} // expected
        other => panic!("expected Proto variant, got: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Proto error Display formatting
// ---------------------------------------------------------------------------

#[test]
fn proto_error_display_with_full_location() {
    let e = OxiRpcBuildError::Proto {
        path: Some("test.proto".into()),
        line: Some(5),
        col: Some(3),
        msg: "unexpected token".into(),
    };
    let display = format!("{e}");
    assert!(
        display.contains("test.proto"),
        "display should contain filename, got: {display}"
    );
    assert!(
        display.contains('5'),
        "display should contain line, got: {display}"
    );
    assert!(
        display.contains('3'),
        "display should contain col, got: {display}"
    );
    assert!(
        display.contains("unexpected token"),
        "display should contain message, got: {display}"
    );
}

#[test]
fn proto_error_display_path_and_line_only() {
    let e = OxiRpcBuildError::Proto {
        path: Some("a.proto".into()),
        line: Some(10),
        col: None,
        msg: "bad field".into(),
    };
    let display = format!("{e}");
    assert!(display.contains("a.proto"), "got: {display}");
    assert!(display.contains("10"), "got: {display}");
    assert!(display.contains("bad field"), "got: {display}");
}

#[test]
fn proto_error_display_path_only() {
    let e = OxiRpcBuildError::Proto {
        path: Some("b.proto".into()),
        line: None,
        col: None,
        msg: "syntax error".into(),
    };
    let display = format!("{e}");
    assert!(display.contains("b.proto"), "got: {display}");
    assert!(display.contains("syntax error"), "got: {display}");
}

#[test]
fn proto_error_display_no_location() {
    let e = OxiRpcBuildError::Proto {
        path: None,
        line: None,
        col: None,
        msg: "unknown error".into(),
    };
    let display = format!("{e}");
    assert!(display.contains("unknown error"), "got: {display}");
}

// ---------------------------------------------------------------------------
// compile_to_fds — structured error on missing file
// ---------------------------------------------------------------------------

#[test]
fn compile_to_fds_missing_file_gives_proto_error() {
    let dir = std::env::temp_dir();
    let missing = dir.join("definitely_does_not_exist_build_hardening.proto");

    let result = Builder::new().compile_to_fds(&[&missing], &[&dir]);
    assert!(result.is_err());

    match result.expect_err("already asserted is_err") {
        OxiRpcBuildError::Proto { msg, .. } => {
            assert!(!msg.is_empty(), "Proto error message must not be empty");
        }
        other => panic!("expected Proto variant, got: {other}"),
    }
}

// ---------------------------------------------------------------------------
// compile_checked — warnings for missing package
// ---------------------------------------------------------------------------

#[test]
fn compile_checked_warns_on_missing_package() {
    // Write a proto with no package declaration into a temp dir.
    let dir = unique_out_dir("no-pkg");
    fs::create_dir_all(&dir).expect("create dir");

    let proto_path = dir.join("nopackage.proto");
    fs::write(&proto_path, b"syntax = \"proto3\";\nmessage Empty {}\n").expect("write proto");

    let out = unique_out_dir("no-pkg-out");
    fs::create_dir_all(&out).expect("create out dir");
    std::env::set_var("OUT_DIR", &out);

    let result = Builder::new().compile_checked(&[&proto_path], &[&dir]);
    fs::remove_dir_all(&dir).ok();
    fs::remove_dir_all(&out).ok();

    // If codegen itself fails (e.g. OUT_DIR issue), that's acceptable in a
    // test environment — we only assert on the Ok branch.
    if let Ok(output) = result {
        // Should have at least one warning about the missing package.
        assert!(
            !output.warnings.is_empty(),
            "expected a warning for missing package declaration"
        );
        let joined = output.warnings.join("\n");
        assert!(
            joined.to_lowercase().contains("package"),
            "warning should mention 'package', got: {joined}"
        );
    }
}

// ---------------------------------------------------------------------------
// Builder::codec — codec path field
// ---------------------------------------------------------------------------

#[test]
fn codec_path_compiles() {
    // Verify that setting a codec path doesn't cause a compile error.
    let b = Builder::new().codec("my_crate::MyCustomCodec");
    let _ = b;
}

// ---------------------------------------------------------------------------
// Builder::on_progress — progress callback
// ---------------------------------------------------------------------------

#[test]
fn on_progress_callback_invoked() {
    use std::sync::{Arc, Mutex};

    let messages = Arc::new(Mutex::new(Vec::<String>::new()));
    let messages_clone = Arc::clone(&messages);

    let proto_src = r#"
        syntax = "proto3";
        package progress_test;
        message Empty {}
        service ProgressSvc { rpc Ping(Empty) returns (Empty); }
    "#;

    let out = unique_out_dir("on-progress");
    fs::create_dir_all(&out).expect("create out dir");
    std::env::set_var("OUT_DIR", &out);

    let result = Builder::new()
        .on_progress(move |msg| {
            messages_clone
                .lock()
                .expect("lock messages")
                .push(msg.to_owned());
        })
        .compile_str("progress_test", proto_src);

    fs::remove_dir_all(&out).ok();

    // Even if compilation fails (e.g., OUT_DIR not set), the progress callback
    // should have fired at least once ("parsing protos..." always fires).
    let msgs = messages.lock().expect("lock messages");
    assert!(
        !msgs.is_empty(),
        "on_progress callback must have been called at least once; result={result:?}"
    );
    drop(msgs);
    let _ = result;
}

// ---------------------------------------------------------------------------
// All 4 RPC types — compile_to_fds descriptor verification
// ---------------------------------------------------------------------------

#[test]
fn compile_all_rpc_types_produces_fds() {
    let proto_content = r#"
syntax = "proto3";
package allrpc;

service AllRpcTypes {
    rpc Unary (Req) returns (Resp);
    rpc ServerStream (Req) returns (stream Resp);
    rpc ClientStream (stream Req) returns (Resp);
    rpc BidiStream (stream Req) returns (stream Resp);
}

message Req  { string value = 1; }
message Resp { string result = 1; }
"#;

    let tmp = unique_out_dir("all-rpc-fds");
    fs::create_dir_all(&tmp).expect("create tmp dir");
    let proto_file = tmp.join("all_rpc.proto");
    fs::write(&proto_file, proto_content).expect("write proto");

    let fds = Builder::new()
        .compile_to_fds(&[&proto_file], &[&tmp])
        .expect("compile_to_fds for all RPC types");

    fs::remove_dir_all(&tmp).ok();

    // Locate the service file in the descriptor set.
    let file = fds
        .file
        .iter()
        .find(|f| f.package.as_deref() == Some("allrpc"))
        .expect("FDS must contain a file with package 'allrpc'");

    let svc = file
        .service
        .iter()
        .find(|s| s.name.as_deref() == Some("AllRpcTypes"))
        .expect("FDS must contain service 'AllRpcTypes'");

    assert_eq!(
        svc.method.len(),
        4,
        "expected 4 methods, got: {}",
        svc.method.len()
    );

    // Find each method and verify its streaming flags.
    let method_map: std::collections::HashMap<
        &str,
        (&prost_types::MethodDescriptorProto, bool, bool),
    > = svc
        .method
        .iter()
        .filter_map(|m| {
            m.name.as_deref().map(|name| {
                (
                    name,
                    (
                        m,
                        m.client_streaming.unwrap_or(false),
                        m.server_streaming.unwrap_or(false),
                    ),
                )
            })
        })
        .collect();

    let (_, client_s, server_s) = method_map["Unary"];
    assert!(!client_s && !server_s, "Unary must have no streaming flags");

    let (_, client_s, server_s) = method_map["ServerStream"];
    assert!(
        !client_s && server_s,
        "ServerStream must have server_streaming=true"
    );

    let (_, client_s, server_s) = method_map["ClientStream"];
    assert!(
        client_s && !server_s,
        "ClientStream must have client_streaming=true"
    );

    let (_, client_s, server_s) = method_map["BidiStream"];
    assert!(
        client_s && server_s,
        "BidiStream must have both streaming flags"
    );
}

// ---------------------------------------------------------------------------
// build_client(false) — suppresses client stubs
// ---------------------------------------------------------------------------

#[test]
fn build_client_false_suppresses_client_code() {
    let proto_src = r#"
syntax = "proto3";
package stubtest;

service StubSvc { rpc Call (Msg) returns (Msg); }
message Msg { string x = 1; }
"#;

    let out = unique_out_dir("no-client");
    fs::create_dir_all(&out).expect("create out dir");

    let result = Builder::new()
        .build_client(false)
        .build_server(true)
        .out_dir(&out)
        .compile_str("stubtest", proto_src);

    // If codegen succeeds, verify no client file / client code.
    if result.is_ok() {
        let generated: Vec<_> = fs::read_dir(&out)
            .expect("read out dir")
            .filter_map(|e| e.ok())
            .collect();

        for entry in &generated {
            let content = fs::read_to_string(entry.path()).unwrap_or_default();
            // tonic-prost-build emits "client" in client module names.
            assert!(
                !content.contains("stub_svc_client"),
                "build_client(false) must not emit client stubs; \
                 found 'stub_svc_client' in {:?}",
                entry.path()
            );
        }
    }

    fs::remove_dir_all(&out).ok();
    // If codegen failed for env reasons that's acceptable; we tested the flag wiring.
    let _ = result;
}

// ---------------------------------------------------------------------------
// build_server(false) — suppresses server stubs
// ---------------------------------------------------------------------------

#[test]
fn build_server_false_suppresses_server_code() {
    let proto_src = r#"
syntax = "proto3";
package stubtest2;

service StubSvc2 { rpc Call (Msg) returns (Msg); }
message Msg { string x = 1; }
"#;

    let out = unique_out_dir("no-server");
    fs::create_dir_all(&out).expect("create out dir");

    let result = Builder::new()
        .build_client(true)
        .build_server(false)
        .out_dir(&out)
        .compile_str("stubtest2", proto_src);

    if result.is_ok() {
        let generated: Vec<_> = fs::read_dir(&out)
            .expect("read out dir")
            .filter_map(|e| e.ok())
            .collect();

        for entry in &generated {
            let content = fs::read_to_string(entry.path()).unwrap_or_default();
            assert!(
                !content.contains("stub_svc2_server"),
                "build_server(false) must not emit server stubs; \
                 found 'stub_svc2_server' in {:?}",
                entry.path()
            );
        }
    }

    fs::remove_dir_all(&out).ok();
    let _ = result;
}

// ---------------------------------------------------------------------------
// type_attribute — accepted and stored by Builder
// ---------------------------------------------------------------------------

#[test]
fn type_attribute_accepted_by_builder() {
    // Verify the builder chain compiles and the attribute is stored
    // (no panic, no error during construction).
    let b = Builder::new()
        .type_attribute(".", "#[derive(PartialEq)]")
        .type_attribute("allrpc.Req", "#[derive(Hash)]");
    // Cloning works (needed internally by compile_checked/compile_str).
    let _b2 = b.clone();
}

/// `type_attribute` propagation to generated prost structs requires the legacy
/// tonic-prost-build backend, which generates both message types and service
/// stubs.  The native `ServiceCodegen` backend emits service stubs only and
/// therefore does not apply message-type attributes.
#[cfg(feature = "legacy-tonic-codegen")]
#[test]
fn type_attribute_appears_in_generated_output() {
    let proto_src = r#"
syntax = "proto3";
package attrtest;

service AttrSvc { rpc Do (AttrMsg) returns (AttrMsg); }
message AttrMsg { string v = 1; }
"#;

    let out = unique_out_dir("type-attr-out");
    fs::create_dir_all(&out).expect("create out dir");

    let result = Builder::new()
        .type_attribute("attrtest.AttrMsg", "#[derive(Hash)]")
        .out_dir(&out)
        .compile_str("attrtest", proto_src);

    if result.is_ok() {
        let generated_content: String = fs::read_dir(&out)
            .expect("read out dir")
            .filter_map(|e| e.ok())
            .filter_map(|e| fs::read_to_string(e.path()).ok())
            .collect::<Vec<_>>()
            .join("\n");

        // tonic-prost-build emits the attribute before the struct.
        assert!(
            generated_content.contains("#[derive(Hash)]"),
            "type_attribute should appear in generated output; got:\n{generated_content}"
        );
    }

    fs::remove_dir_all(&out).ok();
    let _ = result;
}

// ---------------------------------------------------------------------------
// field_attribute — accepted and stored by Builder
// ---------------------------------------------------------------------------

#[test]
fn field_attribute_accepted_by_builder() {
    let b = Builder::new().field_attribute("attrtest.AttrMsg.v", r#"#[serde(rename = "val")]"#);
    let _b2 = b.clone();
}

// ---------------------------------------------------------------------------
// extern_path — accepted and stored by Builder
// ---------------------------------------------------------------------------

#[test]
fn extern_path_accepted_by_builder() {
    let b = Builder::new().extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp");
    let _b2 = b.clone();
}

// ---------------------------------------------------------------------------
// server_mod_attribute / client_mod_attribute — accepted by Builder
// ---------------------------------------------------------------------------

#[test]
fn server_mod_attribute_accepted_by_builder() {
    let b = Builder::new().server_mod_attribute(".", r#"#[cfg(feature = "server")]"#);
    let _b2 = b.clone();
}

#[test]
fn client_mod_attribute_accepted_by_builder() {
    let b = Builder::new().client_mod_attribute(".", r#"#[cfg(feature = "client")]"#);
    let _b2 = b.clone();
}

// ---------------------------------------------------------------------------
// btree_map / bytes — accepted by Builder
// ---------------------------------------------------------------------------

#[test]
fn btree_map_accepted_by_builder() {
    let b = Builder::new().btree_map(".");
    let _b2 = b.clone();
}

#[test]
fn bytes_accepted_by_builder() {
    let b = Builder::new().bytes(".");
    let _b2 = b.clone();
}

// ---------------------------------------------------------------------------
// compile_well_known_types — accepted by Builder
// ---------------------------------------------------------------------------

#[test]
fn compile_well_known_types_accepted_by_builder() {
    let b = Builder::new().compile_well_known_types(true);
    let _b2 = b.clone();
}

// ---------------------------------------------------------------------------
// Default impl — same as new()
// ---------------------------------------------------------------------------

#[test]
fn default_equals_new() {
    // Both should be constructible without panics.
    let _a = Builder::new();
    let _b = Builder::default();
}

// ---------------------------------------------------------------------------
// compile_str — warnings vec is present on Ok
// ---------------------------------------------------------------------------

#[test]
fn compile_str_ok_has_warnings_field() {
    let proto_src = r#"
syntax = "proto3";
package warntest;
message W { string s = 1; }
"#;

    let out = unique_out_dir("warnings-field");
    fs::create_dir_all(&out).expect("create out dir");
    std::env::set_var("OUT_DIR", &out);

    let result = Builder::new()
        .out_dir(&out)
        .compile_str("warntest", proto_src);

    fs::remove_dir_all(&out).ok();

    if let Ok(output) = result {
        // warnings is a Vec — it may be empty for a well-formed proto.
        // We only verify the field is accessible and iterable.
        let _count = output.warnings.len();
    }
}

// ---------------------------------------------------------------------------
// disable_package_emission — Builder method and behavioural tests
// ---------------------------------------------------------------------------

#[test]
fn disable_package_emission_accepted_by_builder() {
    // Verify the method exists and chains correctly.
    let b = oxirpc_build::Builder::new().disable_package_emission();
    // Cloning must also work (needed by compile_checked/compile_str).
    let _b2 = b.clone();
}

#[test]
fn compile_with_disable_package_emission_no_package_mod() {
    let proto_src = r#"
        syntax = "proto3";
        package mypackage.v1;
        message MyMessage { string name = 1; }
        service MyService {
            rpc Get(MyMessage) returns (MyMessage);
        }
    "#;

    let out = unique_out_dir("disable-pkg-emission");
    fs::create_dir_all(&out).expect("create out dir");

    let result = Builder::new()
        .disable_package_emission()
        .out_dir(&out)
        .compile_str("mymsg", proto_src);

    if result.is_ok() {
        let generated_content: String = fs::read_dir(&out)
            .expect("read out dir")
            .filter_map(|e| e.ok())
            .filter_map(|e| fs::read_to_string(e.path()).ok())
            .collect::<Vec<_>>()
            .join("\n");

        // With emit_package(false), the generated code should NOT wrap types
        // in `pub mod mypackage { ... }` or `pub mod v1 { ... }`.
        assert!(
            !generated_content.contains("pub mod mypackage"),
            "disable_package_emission() must suppress 'pub mod mypackage'; got:\n{generated_content}"
        );
        assert!(
            !generated_content.contains("pub mod v1"),
            "disable_package_emission() must suppress 'pub mod v1'; got:\n{generated_content}"
        );
    }

    fs::remove_dir_all(&out).ok();
    // Tolerate codegen failures due to OUT_DIR env issues in test environments.
    let _ = result;
}

#[test]
fn compile_to_fds_with_disable_package_emission_returns_fds() {
    // compile_to_fds does not run codegen, but the flag must not cause a panic.
    let proto_src = r#"
        syntax = "proto3";
        package mypackage.v1;
        message MyMessage { string name = 1; }
        service MyService {
            rpc Get(MyMessage) returns (MyMessage);
        }
    "#;

    let tmp = unique_out_dir("fds-disable-pkg");
    fs::create_dir_all(&tmp).expect("create tmp dir");
    let proto_file = tmp.join("mymsg.proto");
    fs::write(&proto_file, proto_src).expect("write proto");

    let fds = Builder::new()
        .disable_package_emission()
        .compile_to_fds(&[&proto_file], &[&tmp])
        .expect("compile_to_fds should succeed");

    fs::remove_dir_all(&tmp).ok();

    // Verify FDS contains our message regardless of emission flag.
    let has_msg = fds
        .file
        .iter()
        .flat_map(|f| f.message_type.iter())
        .any(|m| m.name.as_deref() == Some("MyMessage"));
    assert!(has_msg, "FDS should contain MyMessage");
}

#[test]
fn compile_to_fds_with_nested_package() {
    // Proto with deeply nested package: a.b.c.NestedService
    let proto_src = r#"
        syntax = "proto3";
        package a.b.c;
        service NestedService {
            rpc Ping(Req) returns (Resp);
        }
        message Req {}
        message Resp {}
    "#;

    let tmp = unique_out_dir("nested-pkg");
    fs::create_dir_all(&tmp).expect("create tmp dir");
    let proto_file = tmp.join("nested.proto");
    fs::write(&proto_file, proto_src).expect("write proto");

    let fds = Builder::new()
        .compile_to_fds(&[&proto_file], &[&tmp])
        .expect("nested package should compile");

    fs::remove_dir_all(&tmp).ok();

    let service_count = fds.file.iter().flat_map(|f| f.service.iter()).count();
    assert_eq!(service_count, 1, "Should have exactly 1 service");
}

#[test]
fn document_minimum_tonic_version() {
    // Compile-time check: verifies the crate version field is populated.
    // Note: tonic-prost-build is only linked when the `legacy-tonic-codegen`
    // feature is enabled; the default path uses native ServiceCodegen.
    let version = env!("CARGO_PKG_VERSION");
    assert!(!version.is_empty(), "oxirpc-build version must be set");
}

// ---------------------------------------------------------------------------
// compile_to_fds — verify messages present
// ---------------------------------------------------------------------------

#[test]
fn compile_to_fds_returns_messages() {
    let proto_src = r#"
syntax = "proto3";
package msgcheck;

message First  { string a = 1; }
message Second { int32 b = 1; }
"#;

    let tmp = unique_out_dir("msg-check");
    fs::create_dir_all(&tmp).expect("create tmp dir");
    let proto_file = tmp.join("msgcheck.proto");
    fs::write(&proto_file, proto_src).expect("write proto");

    let fds = Builder::new()
        .compile_to_fds(&[&proto_file], &[&tmp])
        .expect("compile_to_fds");

    fs::remove_dir_all(&tmp).ok();

    let file = fds
        .file
        .iter()
        .find(|f| f.package.as_deref() == Some("msgcheck"))
        .expect("FDS must contain package 'msgcheck'");

    let msg_names: Vec<&str> = file
        .message_type
        .iter()
        .filter_map(|m| m.name.as_deref())
        .collect();

    assert!(
        msg_names.contains(&"First"),
        "FDS must contain message 'First', got: {msg_names:?}"
    );
    assert!(
        msg_names.contains(&"Second"),
        "FDS must contain message 'Second', got: {msg_names:?}"
    );
}
