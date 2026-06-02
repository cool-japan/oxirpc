//! gRPC conformance tests against serve_native_registry.
//! These tests require Go toolchain + grpc-go interop client.
//! Run with: RUN_CONFORMANCE=1 cargo nextest run -p oxirpc --test conformance

fn should_run() -> bool {
    std::env::var("RUN_CONFORMANCE")
        .map(|v| v == "1")
        .unwrap_or(false)
}

macro_rules! conformance_test {
    ($name:ident, $test_case:expr) => {
        #[tokio::test]
        #[ignore = "requires RUN_CONFORMANCE=1 and grpc-go interop binary"]
        async fn $name() {
            if !should_run() {
                return;
            }
            run_conformance_test($test_case).await;
        }
    };
}

async fn run_conformance_test(test_case: &str) {
    // Start a server with serve_native_registry
    // Run: grpc-go/interop/client --server_host=localhost --server_port=PORT --test_case=TEST
    // Assert exit code 0
    // This is a scaffold — actual grpc-go binary integration is deferred
    let _ = test_case; // placeholder
}

conformance_test!(empty_unary, "empty_unary");
conformance_test!(large_unary, "large_unary");
conformance_test!(client_streaming, "client_streaming");
conformance_test!(server_streaming, "server_streaming");
conformance_test!(ping_pong, "ping_pong");
conformance_test!(empty_stream, "empty_stream");
