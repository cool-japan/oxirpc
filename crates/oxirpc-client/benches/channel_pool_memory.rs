//! Memory-profiling benchmark for [`ChannelPool`].
//!
//! Uses a custom [`CountingAllocator`] to measure live heap bytes allocated
//! when constructing [`ChannelPool`] instances with N endpoints.  The
//! benchmark avoids establishing real TCP connections — `ChannelPool` is
//! populated with `connect_lazy` channels so no I/O occurs.
//!
//! Run with:
//!   cargo bench -p oxirpc-client --bench channel_pool_memory

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_client::balance::RoundRobin;
use oxirpc_client::ChannelPool;

// ─── CountingAllocator ───────────────────────────────────────────────────────

struct CountingAllocator {
    allocated: AtomicI64,
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            self.allocated
                .fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        self.allocated
            .fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            let diff = new_size as i64 - layout.size() as i64;
            self.allocated.fetch_add(diff, Ordering::Relaxed);
        }
        new_ptr
    }
}

impl CountingAllocator {
    const fn new() -> Self {
        Self {
            allocated: AtomicI64::new(0),
        }
    }

    fn live_bytes(&self) -> i64 {
        self.allocated.load(Ordering::Relaxed)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ─── Helper ──────────────────────────────────────────────────────────────────

/// Build N lazy tonic channel pairs `(tonic::transport::Endpoint, Channel)`.
fn make_lazy_pairs(n: usize) -> Vec<(tonic::transport::Endpoint, tonic::transport::Channel)> {
    (0..n)
        .map(|i| {
            let uri = format!("http://127.0.0.1:{}", 50_000 + i);
            let ep = tonic::transport::Endpoint::from_shared(uri).expect("valid endpoint URI");
            let chan = ep.connect_lazy();
            (ep, chan)
        })
        .collect()
}

// ─── Benchmark ───────────────────────────────────────────────────────────────

/// Measure live heap bytes per N-endpoint [`ChannelPool`].
///
/// For each N in {1, 10, 100}: snapshot before construction, build the pool,
/// snapshot after, report `(after - before) / N` as the per-endpoint cost.
fn bench_memory_per_pool_endpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_per_pool_endpoint");

    for n in [1usize, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let pairs = make_lazy_pairs(n);
                let baseline = ALLOCATOR.live_bytes();
                let pool = ChannelPool::new(pairs, RoundRobin::new());
                let after = ALLOCATOR.live_bytes();
                let delta = after - baseline;
                let per_endpoint = if n > 0 { delta / n as i64 } else { 0 };

                // Keep pool alive until after the snapshot.
                std::hint::black_box(per_endpoint);
                drop(pool);
                per_endpoint
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_memory_per_pool_endpoint);
criterion_main!(benches);
