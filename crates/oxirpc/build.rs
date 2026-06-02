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
    Ok(())
}
