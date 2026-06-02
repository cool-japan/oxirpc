//! Benchmark: cost of calling through an interceptor chain of varying depth.
//!
//! Measures the overhead of dispatching through N interceptors sequentially
//! via [`oxirpc::interceptors::InterceptorChain`] for N ∈ {0, 1, 4, 16}.
//! All interceptors are identity (pass-through), so the measurement captures
//! only chain dispatch overhead with no business logic cost.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc::interceptors::{InterceptorChain, TracingInterceptor};
use tonic::service::Interceptor;
use tonic::Request;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an `InterceptorChain` pre-loaded with `n` pass-through steps.
///
/// Uses [`TracingInterceptor`] (cheapest stateful interceptor: one atomic
/// increment + one header insert) so the measurement is representative of a
/// real-world interceptor cost rather than the theoretical zero-cost floor.
fn build_tracing_chain(n: usize) -> InterceptorChain {
    let mut chain = InterceptorChain::new();
    for _ in 0..n {
        chain = chain.push(TracingInterceptor);
    }
    chain
}

/// Build an `InterceptorChain` pre-loaded with `n` pure no-op steps.
///
/// Captures the lowest possible chain overhead: just the `try_fold` iteration
/// with trivial `Ok(req)` returns.
fn build_noop_chain(n: usize) -> InterceptorChain {
    /// Minimal interceptor that just passes the request through unchanged.
    #[derive(Clone)]
    struct Noop;

    impl Interceptor for Noop {
        fn call(&mut self, req: Request<()>) -> Result<Request<()>, tonic::Status> {
            Ok(req)
        }
    }

    let mut chain = InterceptorChain::new();
    for _ in 0..n {
        chain = chain.push(Noop);
    }
    chain
}

// ---------------------------------------------------------------------------
// Benchmark: TracingInterceptor chain
// ---------------------------------------------------------------------------

fn bench_tracing_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("interceptor_chain_tracing");

    for &depth in &[0usize, 1, 4, 16] {
        group.bench_with_input(BenchmarkId::new("depth", depth), &depth, |b, &n| {
            let mut chain = build_tracing_chain(n);
            b.iter(|| {
                let req: Request<()> = Request::new(());
                let _ = std::hint::black_box(chain.call(std::hint::black_box(req)));
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: no-op chain (baseline)
// ---------------------------------------------------------------------------

fn bench_noop_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("interceptor_chain_noop");

    for &depth in &[0usize, 1, 4, 16] {
        group.bench_with_input(BenchmarkId::new("depth", depth), &depth, |b, &n| {
            let mut chain = build_noop_chain(n);
            b.iter(|| {
                let req: Request<()> = Request::new(());
                let _ = std::hint::black_box(chain.call(std::hint::black_box(req)));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_tracing_chain, bench_noop_chain);
criterion_main!(benches);
