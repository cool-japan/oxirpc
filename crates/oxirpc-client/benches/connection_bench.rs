//! Benchmarks: connection establishment (lazy channel creation) and
//! ChannelMonitor state-machine throughput.
//!
//! # Design
//!
//! **`lazy_channel_create`** — measures the in-process cost of building a
//! `tonic::transport::Endpoint` and calling `connect_lazy()`.  No TCP
//! connection is established; this is pure struct-allocation and URI parsing.
//!
//! **`channel_monitor_set_state`** — measures the cost of advancing a
//! `ChannelMonitor` through a state transition (AtomicU8 swap + callback
//! dispatch through a `Mutex<Vec<_>>`).
//!
//! **`channel_pool_dispatch`** — pool-based channel dispatch (N = 8) as a
//! representative RPC throughput baseline.  No server needed; measures only
//! the dispatch path through the load balancer.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_client::{
    balance::RoundRobin,
    monitor::{ChannelMonitor, ConnectionState},
    ChannelPool,
};
use tonic::transport::Endpoint;

// ── lazy_channel_create ───────────────────────────────────────────────────────

fn bench_lazy_channel_create(c: &mut Criterion) {
    c.bench_function("lazy_channel_create", |b| {
        b.iter(|| Endpoint::from_static("http://127.0.0.1:9999").connect_lazy());
    });
}

// ── channel_monitor_new ───────────────────────────────────────────────────────

fn bench_channel_monitor_new(c: &mut Criterion) {
    c.bench_function("channel_monitor_new", |b| {
        b.iter(|| {
            let ch = Endpoint::from_static("http://127.0.0.1:9999").connect_lazy();
            ChannelMonitor::new(ch)
        });
    });
}

// ── channel_monitor_set_state ─────────────────────────────────────────────────

fn bench_channel_monitor_set_state(c: &mut Criterion) {
    // Pre-create; bench only the state-machine transition cost.
    let ch = Endpoint::from_static("http://127.0.0.1:9999").connect_lazy();
    let monitor = ChannelMonitor::new(ch);

    let mut group = c.benchmark_group("channel_monitor_set_state");

    group.bench_function("no_callbacks", |b| {
        b.iter(|| {
            // Alternate states to always cause a transition (no same-state no-op).
            monitor.set_state(ConnectionState::Connecting);
            monitor.set_state(ConnectionState::Ready);
        });
    });

    // With one callback registered.
    let ch2 = Endpoint::from_static("http://127.0.0.1:9999").connect_lazy();
    let monitor_cb = ChannelMonitor::new(ch2);
    monitor_cb.on_state_change(|_s| {
        // no-op callback — measures dispatch overhead
    });
    group.bench_function("one_callback", |b| {
        b.iter(|| {
            monitor_cb.set_state(ConnectionState::Connecting);
            monitor_cb.set_state(ConnectionState::Ready);
        });
    });

    group.finish();
}

// ── channel_pool_dispatch ─────────────────────────────────────────────────────

fn bench_channel_pool_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_pool_dispatch");

    for n in [2_usize, 8, 32] {
        let pairs: Vec<(tonic::transport::Endpoint, tonic::transport::Channel)> = (0..n)
            .map(|i| {
                let ep = Endpoint::from_shared(format!("http://10.0.0.{i}:50051"))
                    .expect("valid endpoint URI");
                let ch = ep.connect_lazy();
                (ep, ch)
            })
            .collect();
        let pool = ChannelPool::new(pairs, RoundRobin::new());

        group.bench_with_input(BenchmarkId::new("pool_size", n), &pool, |b, p| {
            b.iter(|| {
                let _ch = p.get();
            });
        });
    }

    group.finish();
}

// ── healthy_count ─────────────────────────────────────────────────────────────

fn bench_healthy_count(c: &mut Criterion) {
    let pairs: Vec<(tonic::transport::Endpoint, tonic::transport::Channel)> = (0..32)
        .map(|i| {
            let ep = Endpoint::from_shared(format!("http://10.0.0.{i}:50051"))
                .expect("valid endpoint URI");
            let ch = ep.connect_lazy();
            (ep, ch)
        })
        .collect();
    let pool = ChannelPool::new(pairs, RoundRobin::new());

    c.bench_function("healthy_count_32", |b| {
        b.iter(|| pool.healthy_count());
    });
}

// ── criterion entry ───────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_lazy_channel_create,
    bench_channel_monitor_new,
    bench_channel_monitor_set_state,
    bench_channel_pool_dispatch,
    bench_healthy_count,
);
criterion_main!(benches);
