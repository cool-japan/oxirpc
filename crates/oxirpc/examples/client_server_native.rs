//! Example: a real, self-contained native (pure-`h2`, no tonic transport)
//! gRPC client + server round trip.
//!
//! Spins up a [`oxirpc::ServerBuilder`] serving the built-in
//! `grpc.health.v1.Health` service via [`oxirpc_server::NativeServiceRegistry`]
//! on a background task, then drives it with a native
//! [`oxirpc_client::native_channel::NativeChannelBuilder`] channel — the same
//! transport used internally by `oxirpc-client`/`oxirpc-server`, with no proto
//! codegen required (the health service's types come pre-built from the
//! `tonic-health` crate). Prints the decoded response and exits.
//!
//! Also demonstrates graceful server shutdown via a `tokio::sync::oneshot`
//! signal.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example client_server_native --features "client,server,native,health"
//! ```

use std::time::Duration;

use http_body::Body as _;
use prost::Message as _;

use oxirpc::ServerBuilder;
use oxirpc_client::balance::{Endpoint, StaticResolver};
use oxirpc_client::native_channel::NativeChannelBuilder;
use oxirpc_core::wire::{encode_grpc_message, NativeBody};
use oxirpc_health::HealthBuilder;
use oxirpc_server::NativeServiceRegistry;

use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Server: build a Health service and serve it natively ───────────────
    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("greeter.Greeter").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("[server] listening on {addr}");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener_shutdown(listener, registry, async {
                shutdown_rx.await.ok();
            })
            .await
    });

    // ── Client: dial the server over plain HTTP/2 (no TLS) ──────────────────
    let uri: http::Uri = format!("http://{addr}").parse()?;
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?;
    channel.ready().await;

    // ── Issue a Health/Check RPC by hand (no generated client stub) ────────
    let request = HealthCheckRequest {
        service: "greeter.Greeter".to_string(),
    };
    let framed = encode_grpc_message(&request)?;
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/grpc.health.v1.Health/Check"))
        .version(http::Version::HTTP_2)
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(NativeBody::once(framed))?;

    let resp = channel.call(req).await?;
    println!("[client] response status: {}", resp.status());

    let mut body = std::pin::pin!(resp.into_body());
    let mut payload = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        if let Ok(f) = frame {
            if f.is_data() {
                if let Ok(data) = f.into_data() {
                    payload.extend_from_slice(&data);
                }
            }
        }
    }
    let decoded = HealthCheckResponse::decode(payload.as_slice())?;
    println!(
        "[client] greeter.Greeter serving status = {} (1 = SERVING)",
        decoded.status
    );
    assert_eq!(decoded.status, 1, "expected SERVING");

    // ── Graceful shutdown ────────────────────────────────────────────────
    shutdown_tx.send(()).ok();
    server.await??;
    println!("[server] shut down cleanly");

    Ok(())
}
