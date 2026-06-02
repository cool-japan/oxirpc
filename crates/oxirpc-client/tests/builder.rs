//! Tests for the ClientBuilder configuration surface.
//!
//! These use `connect_lazy()` so no live server is required: lazy connection
//! validates the endpoint + applied config without performing a TCP handshake.

use std::time::Duration;

use oxirpc_client::ClientBuilder;

#[test]
fn builder_is_clone() {
    let builder = ClientBuilder::new("http://127.0.0.1:50051")
        .timeout(Duration::from_secs(5))
        .user_agent("oxirpc-test/1.0");
    let _clone = builder.clone();
}

#[tokio::test]
async fn connect_lazy_with_full_config_succeeds() {
    let channel = ClientBuilder::new("http://127.0.0.1:50051")
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(2))
        .user_agent("oxirpc-test/1.0")
        .concurrency_limit(64)
        .initial_connection_window_size(1 << 20)
        .initial_stream_window_size(1 << 18)
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(30))
        .connect_lazy();
    assert!(
        channel.is_ok(),
        "connect_lazy should succeed: {:?}",
        channel.err()
    );
}

#[test]
fn connect_lazy_rejects_invalid_endpoint() {
    let channel = ClientBuilder::new("not a valid uri at all !!").connect_lazy();
    assert!(channel.is_err(), "invalid endpoint must error");
}

#[tokio::test]
async fn origin_override_parses() {
    let channel = ClientBuilder::new("http://127.0.0.1:50051")
        .origin("http://service.internal")
        .connect_lazy();
    assert!(channel.is_ok());
}

#[test]
fn origin_override_rejects_invalid_uri() {
    let channel = ClientBuilder::new("http://127.0.0.1:50051")
        .origin("::::not a uri")
        .connect_lazy();
    assert!(channel.is_err(), "invalid origin must error");
}

#[test]
fn new_transport_knobs_compile() {
    let b = ClientBuilder::new("http://localhost:50051")
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(5))
        .keep_alive_while_idle(true)
        .buffer_size(Some(1024))
        .rate_limit(100, Duration::from_secs(1))
        .http2_adaptive_window(true)
        .max_frame_size(Some(65536));
    // Just verify it compiles and chains correctly
    let _ = b;
}

#[tokio::test]
async fn connect_lazy_with_interceptor_compiles() {
    let result = ClientBuilder::new("http://localhost:50051")
        .connect_lazy_with_interceptor(|req: tonic::Request<()>| Ok(req));
    assert!(result.is_ok());
}
