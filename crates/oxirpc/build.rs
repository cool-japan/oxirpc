fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only generate test protos if the test fixture exists.
    let proto = "tests/proto/greeter.proto";
    if std::path::Path::new(proto).exists() {
        // Step 1: Parse .proto once with protox (no protoc) to get the FDS.
        let fds = oxirpc_build::Builder::new().compile_to_fds(&[proto], &["tests/proto/"])?;

        // Step 2: Generate prost message types → $OUT_DIR/greeter.rs
        // (messages only; build_client/build_server are false to avoid duplicate stubs)
        tonic_prost_build::configure()
            .build_client(false)
            .build_server(false)
            .compile_fds(fds.clone())
            .map_err(|e| format!("tonic-prost-build: {e}"))?;

        // Step 3: Generate native service stubs → $OUT_DIR/greeter.services.rs
        oxirpc_build::Builder::new().compile(&[proto], &["tests/proto/"])?;
    }

    // gRPC interop conformance fixture (grpc.testing.TestService).
    let testing_proto = "tests/proto/grpc_testing.proto";
    if std::path::Path::new(testing_proto).exists() {
        // Step 1: Parse .proto once with protox (no protoc) to get the FDS.
        let fds =
            oxirpc_build::Builder::new().compile_to_fds(&[testing_proto], &["tests/proto/"])?;

        // Step 2: Generate prost message types → $OUT_DIR/grpc.testing.rs
        // (messages only; build_client/build_server are false to avoid duplicate stubs)
        tonic_prost_build::configure()
            .build_client(false)
            .build_server(false)
            .compile_fds(fds.clone())
            .map_err(|e| format!("tonic-prost-build: {e}"))?;

        // Step 3: Generate native service stubs → $OUT_DIR/grpc_testing.services.rs
        oxirpc_build::Builder::new().compile(&[testing_proto], &["tests/proto/"])?;

        // tonic-prost-build names the message file by proto PACKAGE → grpc.testing.rs.
        // The test includes grpc_testing.rs, so copy it across.
        let out_dir = std::env::var("OUT_DIR")?;
        std::fs::copy(
            format!("{out_dir}/grpc.testing.rs"),
            format!("{out_dir}/grpc_testing.rs"),
        )?;
    }
    Ok(())
}
