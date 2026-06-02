//! Benchmarks for NativeHealthService Check RPC latency and Watch notification
//! propagation.
//!
//! Run with:
//!   cargo bench -p oxirpc-health --bench health_bench --all-features

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_health::{HealthBuilder, HealthState, NativeHealthService, ServingStatus};
use tokio::runtime::Runtime;
use tower::Service as _;

// ---------------------------------------------------------------------------
// gRPC request helpers (mirrors tests/health.rs)
// ---------------------------------------------------------------------------

/// Encode a `HealthCheckRequest { service }` into a gRPC-framed `tonic::body::Body`.
fn make_bench_request(path: &str, service: &str) -> http::Request<tonic::body::Body> {
    use bytes::Bytes;
    use http_body_util::BodyExt as _;
    use oxirpc_health::proto::HealthCheckRequest as NativeReq;
    use prost::Message as _;

    let proto_bytes = NativeReq {
        service: service.to_owned(),
    }
    .encode_to_vec();
    let mut frame = Vec::with_capacity(5 + proto_bytes.len());
    frame.push(0u8); // uncompressed
    let len = proto_bytes.len() as u32;
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&proto_bytes);

    let body = tonic::body::Body::new(
        http_body_util::Full::new(Bytes::from(frame))
            .map_err(|e| tonic::Status::internal(e.to_string())),
    );
    http::Request::builder()
        .uri(path)
        .header("content-type", "application/grpc+proto")
        .body(body)
        .expect("request builder")
}

// ---------------------------------------------------------------------------
// Benchmark 1: Check RPC latency (single service lookup)
// ---------------------------------------------------------------------------

/// Bench a single Check RPC call end-to-end (decode → state lookup → encode).
/// This measures the pure in-memory latency of the health check path.
fn bench_check_latency(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    // Pre-build state with 100 pre-registered services.
    let state = rt.block_on(async {
        let s = HealthState::new();
        for i in 0..100 {
            s.set(format!("bench.Service{i}"), ServingStatus::Serving)
                .await;
        }
        s
    });

    let svc_100 = NativeHealthService::new(std::sync::Arc::clone(&state));

    c.bench_function("check_rpc_latency_100_services", |b| {
        b.iter(|| {
            let mut svc_clone = svc_100.clone();
            rt.block_on(async move {
                let req = make_bench_request("/grpc.health.v1.Health/Check", "bench.Service50");
                svc_clone.call(req).await.expect("call")
            })
        });
    });

    // Also bench worst-case single-service lookup.
    let state_single = rt.block_on(async {
        let s = HealthState::new();
        s.set("bench.Single", ServingStatus::Serving).await;
        s
    });
    let svc_1 = NativeHealthService::new(std::sync::Arc::clone(&state_single));

    c.bench_function("check_rpc_latency_1_service", |b| {
        b.iter(|| {
            let mut svc_clone = svc_1.clone();
            rt.block_on(async move {
                let req = make_bench_request("/grpc.health.v1.Health/Check", "bench.Single");
                svc_clone.call(req).await.expect("call")
            })
        });
    });
}

// ---------------------------------------------------------------------------
// Benchmark 2: Watch notification propagation
// ---------------------------------------------------------------------------

/// Bench: state change → watcher receives the notification.
///
/// Alternates Serving ↔ NotServing each iteration to prevent dedup suppression
/// (HealthState uses `send_if_modified` which no-ops on identical consecutive
/// values).
fn bench_watch_propagation(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("watch_notification_propagation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let state = HealthState::new();
                state.set("bench.Watch", ServingStatus::Serving).await;
                let mut rx = state.watcher("bench.Watch").await;
                // Consume the initial value so the next `changed()` is purely for the flip.
                let _ = rx.borrow_and_update();

                // Flip → NotServing, wait for notification.
                state.set("bench.Watch", ServingStatus::NotServing).await;
                rx.changed().await.expect("changed");
                let _ = rx.borrow_and_update();

                // Flip back → Serving, wait for notification.
                state.set("bench.Watch", ServingStatus::Serving).await;
                rx.changed().await.expect("changed");
                let _ = rx.borrow_and_update();
            });
        });
    });
}

// ---------------------------------------------------------------------------
// Benchmark 3: HealthState::set with N watchers
// ---------------------------------------------------------------------------

/// Bench `HealthState::set()` with N = 1, 10, 100 watchers registered for the
/// same service.  Measures the cost of broadcasting a status change to all
/// watchers.
fn bench_state_set(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("state_set_with_watchers");

    for n_watchers in [1usize, 10, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_watchers),
            &n_watchers,
            |b, &n| {
                b.iter(|| {
                    rt.block_on(async move {
                        let state = HealthState::new();
                        state.set("bench.State", ServingStatus::Serving).await;

                        // Register n watchers for the service.
                        let mut receivers = Vec::with_capacity(n);
                        for _ in 0..n {
                            receivers.push(state.watcher("bench.State").await);
                        }
                        // Consume initial values.
                        for rx in &mut receivers {
                            let _ = rx.borrow_and_update();
                        }

                        // Bench a single status flip, broadcasting to all watchers.
                        state.set("bench.State", ServingStatus::NotServing).await;

                        // Verify all receivers can see the new value.
                        for rx in &receivers {
                            assert_eq!(
                                *rx.borrow(),
                                oxirpc_health::proto::ServingStatusProto::NotServing as i32
                            );
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 4: HealthBuilder::build_native (construction cost)
// ---------------------------------------------------------------------------

/// Bench the cost of constructing a NativeHealthService via the builder.
fn bench_build_native(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("build_native_10_services", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut builder = HealthBuilder::new();
                for i in 0..10 {
                    builder = builder.register(format!("svc.Service{i}"), ServingStatus::Serving);
                }
                let (_svc, _handle) = builder.build_native().await;
            });
        });
    });
}

criterion_group!(
    benches,
    bench_check_latency,
    bench_watch_propagation,
    bench_state_set,
    bench_build_native,
);
criterion_main!(benches);
