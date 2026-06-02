//! Tests for oxirpc-build's protox-backed compilation (no protoc).

use std::io::Write;

use oxirpc_build::Builder;

/// Write a minimal greeter proto into a unique temp dir and return
/// `(dir, proto_path)`.
fn write_temp_proto() -> (std::path::PathBuf, std::path::PathBuf) {
    let mut dir = std::env::temp_dir();
    let unique = format!(
        "oxirpc_build_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    dir.push(unique);
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let proto_path = dir.join("greeter.proto");
    let mut f = std::fs::File::create(&proto_path).expect("create proto");
    f.write_all(
        br#"syntax = "proto3";
package greeter;

message HelloRequest { string name = 1; }
message HelloReply   { string message = 1; }

service Greeter {
  rpc Hello (HelloRequest) returns (HelloReply);
  rpc HelloServerStream (HelloRequest) returns (stream HelloReply);
  rpc HelloClientStream (stream HelloRequest) returns (HelloReply);
  rpc HelloBidi (stream HelloRequest) returns (stream HelloReply);
}
"#,
    )
    .expect("write proto");

    (dir, proto_path)
}

#[test]
fn compile_to_fds_returns_descriptor_set() {
    let (dir, proto) = write_temp_proto();

    let fds = Builder::new()
        .compile_to_fds(&[&proto], &[&dir])
        .expect("compile_to_fds");

    // The descriptor set must contain our single file with the Greeter service.
    assert_eq!(fds.file.len(), 1);
    let file = &fds.file[0];
    assert_eq!(file.package.as_deref(), Some("greeter"));
    let svc_names: Vec<&str> = file
        .service
        .iter()
        .filter_map(|s| s.name.as_deref())
        .collect();
    assert_eq!(svc_names, vec!["Greeter"]);

    // All four RPC method kinds should be present.
    let methods: Vec<&str> = file.service[0]
        .method
        .iter()
        .filter_map(|m| m.name.as_deref())
        .collect();
    assert_eq!(
        methods,
        vec![
            "Hello",
            "HelloServerStream",
            "HelloClientStream",
            "HelloBidi"
        ]
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compile_to_fds_errors_on_missing_proto() {
    let dir = std::env::temp_dir();
    let missing = dir.join("definitely_does_not_exist_oxirpc.proto");
    let result = Builder::new().compile_to_fds(&[&missing], &[&dir]);
    assert!(result.is_err(), "missing proto must error");
}
