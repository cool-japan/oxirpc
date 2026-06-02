//! Sustained-concurrency load tests for the native `serve_native_registry` path.
//!
//! These tests validate that the native server handles many concurrent RPCs
//! without errors or panics.  All assertions are on correctness (status codes,
//! response values) — no timing/latency assertions are made.
//!
//! Test layout:
//!
//! 1. `sustained_concurrent_unary_no_errors`
//!    — 256 concurrent `Health/Check` calls must all succeed with SERVING (1)
//!
//! 2. `sustained_concurrent_streams_no_errors`
//!    — 64 concurrent `Health/Watch` streams; the first frame from each must
//!    be SERVING (1)
//!
//! 3. `soak_high_volume` (ignored by default)
//!    — 2048 concurrent calls × 10 batches; enable with `OXIRPC_SOAK=1`
//!
//! Requires the `native` and `health` cargo features.

use oxirpc_health::HealthBuilder;
use oxirpc_server::{NativeServiceRegistry, ServerBuilder};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn bind_random() -> (tokio::net::TcpListener, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

async fn connect_channel(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect channel")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: sustained_concurrent_unary_no_errors
//
// Spin up a native registry server, then fire 256 concurrent Health/Check RPCs.
// All 256 must return Ok with ServingStatus::Serving (== 1).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sustained_concurrent_unary_no_errors() {
    const CONCURRENCY: usize = 256;
    const SERVICE: &str = "load.TestService";

    // Build NativeHealthService and register the service.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving(SERVICE).await;

    let (listener, addr) = bind_random().await;
    let registry = NativeServiceRegistry::new().add_service(native_svc);

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    // Allow the server to start accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // Build a shared channel (tonic channels are cheap to clone).
    let channel = connect_channel(addr).await;

    // Fire 256 concurrent Health/Check calls.
    let futures: Vec<_> = (0..CONCURRENCY)
        .map(|_| {
            let ch = channel.clone();
            async move {
                let mut client = HealthClient::new(ch);
                client
                    .check(HealthCheckRequest {
                        service: SERVICE.to_string(),
                    })
                    .await
            }
        })
        .collect();

    let results = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        futures_util::future::join_all(futures),
    )
    .await
    .expect("all 256 concurrent Check RPCs must complete within 10 s");

    // Validate: 0 errors, all SERVING.
    let mut error_count = 0usize;
    let mut not_serving_count = 0usize;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(resp) => {
                if resp.into_inner().status != 1 {
                    not_serving_count += 1;
                }
            }
            Err(e) => {
                eprintln!("request {i} failed: {e}");
                error_count += 1;
            }
        }
    }

    assert_eq!(
        error_count, 0,
        "{error_count}/{CONCURRENCY} concurrent Check RPCs failed with transport errors"
    );
    assert_eq!(
        not_serving_count, 0,
        "{not_serving_count}/{CONCURRENCY} concurrent Check RPCs returned non-SERVING status"
    );

    // Keep handle alive until after all assertions.
    drop(handle);

    server_handle.abort();
    server_handle.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: sustained_concurrent_streams_no_errors
//
// Open 64 concurrent Watch streams on the native server.
// Read the first frame from each stream.
// All 64 first frames must be ServingStatus::Serving (== 1).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sustained_concurrent_streams_no_errors() {
    const CONCURRENCY: usize = 64;
    const SERVICE: &str = "load.StreamService";

    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving(SERVICE).await;

    let (listener, addr) = bind_random().await;
    let registry = NativeServiceRegistry::new().add_service(native_svc);

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let channel = connect_channel(addr).await;

    // Open 64 concurrent Watch streams, reading just the first frame from each.
    let stream_futures: Vec<_> = (0..CONCURRENCY)
        .map(|i| {
            let ch = channel.clone();
            async move {
                use tokio_stream::StreamExt as _;
                let mut client = HealthClient::new(ch);
                let stream_result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.watch(HealthCheckRequest {
                        service: SERVICE.to_string(),
                    }),
                )
                .await;

                let stream_response = match stream_result {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        return Err(format!("stream {i}: Watch RPC failed: {e}"));
                    }
                    Err(_) => {
                        return Err(format!("stream {i}: Watch RPC timed out"));
                    }
                };

                let mut stream = stream_response.into_inner();
                let first_frame =
                    tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await;

                match first_frame {
                    Ok(Some(Ok(resp))) => Ok(resp.status),
                    Ok(Some(Err(e))) => Err(format!("stream {i}: frame error: {e}")),
                    Ok(None) => Err(format!("stream {i}: stream ended immediately")),
                    Err(_) => Err(format!("stream {i}: timed out waiting for first frame")),
                }
            }
        })
        .collect();

    let results = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        futures_util::future::join_all(stream_futures),
    )
    .await
    .expect("all 64 concurrent Watch streams must complete within 15 s");

    assert_eq!(
        results.len(),
        CONCURRENCY,
        "expected {CONCURRENCY} results, got {}",
        results.len()
    );

    let mut error_count = 0usize;
    let mut not_serving_count = 0usize;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(status) => {
                if status != 1 {
                    eprintln!("stream {i}: expected SERVING (1), got {status}");
                    not_serving_count += 1;
                }
            }
            Err(msg) => {
                eprintln!("{msg}");
                error_count += 1;
            }
        }
    }

    assert_eq!(
        error_count, 0,
        "{error_count}/{CONCURRENCY} concurrent Watch streams encountered errors"
    );
    assert_eq!(
        not_serving_count, 0,
        "{not_serving_count}/{CONCURRENCY} concurrent Watch first frames were not SERVING"
    );

    // Keep handle alive until after all assertions.
    drop(handle);

    server_handle.abort();
    server_handle.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: soak_high_volume
//
// Heavy soak: 2048 concurrent Check RPCs × 10 batches.
// Gated behind `OXIRPC_SOAK=1` environment variable.
// Run with: OXIRPC_SOAK=1 cargo nextest run -p oxirpc soak_high_volume
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn soak_high_volume() {
    if std::env::var("OXIRPC_SOAK").is_err() {
        eprintln!("Skipping soak test: set OXIRPC_SOAK=1 to enable");
        return;
    }

    const CONCURRENCY: usize = 2048;
    const BATCHES: usize = 10;
    const SERVICE: &str = "load.SoakService";

    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving(SERVICE).await;

    let (listener, addr) = bind_random().await;
    let registry = NativeServiceRegistry::new().add_service(native_svc);

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let channel = connect_channel(addr).await;
    let mut total_errors = 0usize;

    for batch in 0..BATCHES {
        let futures: Vec<_> = (0..CONCURRENCY)
            .map(|_| {
                let ch = channel.clone();
                async move {
                    let mut client = HealthClient::new(ch);
                    client
                        .check(HealthCheckRequest {
                            service: SERVICE.to_string(),
                        })
                        .await
                }
            })
            .collect();

        let results = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            futures_util::future::join_all(futures),
        )
        .await
        .unwrap_or_else(|_| panic!("soak batch {batch}: timed out"));

        let batch_errors = results
            .into_iter()
            .filter(|r| r.is_err() || r.as_ref().ok().map(|v| v.get_ref().status) != Some(1))
            .count();

        if batch_errors > 0 {
            eprintln!(
                "soak batch {batch}: {batch_errors}/{CONCURRENCY} calls failed or returned non-SERVING"
            );
        }
        total_errors += batch_errors;
    }

    assert_eq!(
        total_errors, 0,
        "{total_errors} errors across {BATCHES} batches of {CONCURRENCY} concurrent calls"
    );

    // Keep handle alive until after all assertions.
    drop(handle);

    server_handle.abort();
    server_handle.await.ok();
}
