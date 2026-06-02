//! ChannelPool round-robin `get` benchmark.
//!
//! Measures the hot-path cost of `ChannelPool::get()` — a load-balancer index
//! pick followed by a slice lookup — for pools of N = 2 / 8 / 32 channels.
//!
//! Channels are created with `connect_lazy` so no TCP handshake happens during
//! setup; the bench measures only the in-process dispatch logic.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_client::{balance::RoundRobin, ChannelPool};
use tonic::transport::Endpoint as TonicEndpoint;

fn build_pool(n: usize) -> ChannelPool {
    let pairs: Vec<(TonicEndpoint, tonic::transport::Channel)> = (0..n)
        .map(|i| {
            let ep = TonicEndpoint::from_shared(format!("http://127.0.0.{i}:50051"))
                .expect("valid endpoint URI");
            let ch = ep.connect_lazy();
            (ep, ch)
        })
        .collect();
    ChannelPool::new(pairs, RoundRobin::new())
}

fn bench_pool_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_pool_get");
    for n in [2_usize, 8, 32] {
        let pool = build_pool(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &pool, |b, p| {
            b.iter(|| {
                let _ch = p.get();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pool_get);
criterion_main!(benches);
