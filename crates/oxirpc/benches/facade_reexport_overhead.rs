//! Benchmark: verify zero overhead of `oxirpc` facade re-exports vs direct tonic.
//!
//! Both call paths resolve to the same underlying type at the end of the chain
//! (`tonic::Status`).  Any non-zero delta between the two benchmark arms is
//! measurement noise, not real overhead.
//!
//! Covers:
//! - `Status` construction (most frequently-used path in gRPC error handling)
//! - `Request` wrapping
//! - `StatusCode` → `tonic::Code` conversion

use criterion::{criterion_group, criterion_main, Criterion};

// ---------------------------------------------------------------------------
// Status construction
// ---------------------------------------------------------------------------

fn bench_status_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("status_create");

    group.bench_function("via_oxirpc", |b| {
        b.iter(|| {
            std::hint::black_box(oxirpc::Status::not_found(std::hint::black_box(
                "resource not found",
            )))
        })
    });

    group.bench_function("via_tonic_direct", |b| {
        b.iter(|| {
            std::hint::black_box(tonic::Status::not_found(std::hint::black_box(
                "resource not found",
            )))
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Request wrapping
// ---------------------------------------------------------------------------

fn bench_request_wrap(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_wrap");

    group.bench_function("via_oxirpc", |b| {
        b.iter(|| std::hint::black_box(oxirpc::Request::new(std::hint::black_box(42u64))))
    });

    group.bench_function("via_tonic_direct", |b| {
        b.iter(|| std::hint::black_box(tonic::Request::new(std::hint::black_box(42u64))))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Code/StatusCode lookup
// ---------------------------------------------------------------------------

fn bench_code_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("code_lookup");

    group.bench_function("status_code_ok_via_oxirpc", |b| {
        b.iter(|| {
            let code = oxirpc::StatusCode::Ok;
            std::hint::black_box(code)
        })
    });

    group.bench_function("code_ok_via_tonic_direct", |b| {
        b.iter(|| {
            let code = tonic::Code::Ok;
            std::hint::black_box(code)
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// version() call
// ---------------------------------------------------------------------------

fn bench_version(c: &mut Criterion) {
    c.bench_function("oxirpc_version", |b| {
        b.iter(|| std::hint::black_box(oxirpc::version()))
    });
}

criterion_group!(
    benches,
    bench_status_create,
    bench_request_wrap,
    bench_code_lookup,
    bench_version
);
criterion_main!(benches);
