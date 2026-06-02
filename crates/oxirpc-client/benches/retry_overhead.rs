//! Retry policy overhead benchmarks.
//!
//! Measures the in-process cost of constructing `RetryPolicy` and of calling
//! `should_retry` / `Backoff::next_delay` — the code paths that execute on
//! every failure without any I/O.  This establishes a baseline for the
//! pure-logic overhead of the resilience layer.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_client::resilience::{Backoff, RetryPolicy};
use oxirpc_core::status::StatusCode;
use std::time::Duration;

fn bench_retry_policy_new(c: &mut Criterion) {
    c.bench_function("retry_policy_new", |b| {
        b.iter(|| RetryPolicy::new(3));
    });
}

fn bench_should_retry_retryable(c: &mut Criterion) {
    let policy = RetryPolicy::new(3);
    c.bench_function("should_retry_retryable", |b| {
        b.iter(|| policy.should_retry(0, StatusCode::Unavailable));
    });
}

fn bench_should_retry_not_retryable(c: &mut Criterion) {
    let policy = RetryPolicy::new(3);
    c.bench_function("should_retry_not_retryable", |b| {
        b.iter(|| policy.should_retry(0, StatusCode::NotFound));
    });
}

fn bench_should_retry_max_attempts_exceeded(c: &mut Criterion) {
    let policy = RetryPolicy::new(3);
    c.bench_function("should_retry_max_attempts_exceeded", |b| {
        b.iter(|| policy.should_retry(3, StatusCode::Unavailable));
    });
}

fn bench_backoff_next_delay(c: &mut Criterion) {
    let backoff = Backoff::exponential(Duration::from_millis(100));
    let mut group = c.benchmark_group("backoff_next_delay");
    for attempt in [0_u32, 1, 3, 8] {
        group.bench_with_input(BenchmarkId::from_parameter(attempt), &backoff, |b, bf| {
            b.iter(|| bf.next_delay(attempt));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_retry_policy_new,
    bench_should_retry_retryable,
    bench_should_retry_not_retryable,
    bench_should_retry_max_attempts_exceeded,
    bench_backoff_next_delay,
);
criterion_main!(benches);
