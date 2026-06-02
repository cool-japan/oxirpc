//! Memory-profiling benchmarks for [`HealthState`].
//!
//! Uses a custom [`CountingAllocator`] backed by `std::alloc::System` to
//! measure live heap bytes before and after each operation.  Because
//! `#[global_allocator]` intercepts **all** allocations (Tokio, Criterion, …),
//! we snapshot `ALLOCATOR.live_bytes()` immediately before and after the code
//! under measurement so that framework overhead cancels out.
//!
//! Run with:
//!   cargo bench -p oxirpc-health --bench memory_bench --all-features

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_health::{HealthState, ServingStatus};

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

// ─── Benchmarks ──────────────────────────────────────────────────────────────

/// Measure live heap bytes after registering N services into `HealthState`.
///
/// For each N in {1, 10, 100, 1000}: snapshot before construction, build the
/// state, register N services, snapshot after, report `(after - before) / N`
/// as the per-service cost.
fn bench_memory_per_service(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("memory_per_service");

    for n in [1usize, 10, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let baseline = ALLOCATOR.live_bytes();
                    let state = HealthState::new();
                    for i in 0..n {
                        state
                            .set(format!("svc.Service{i}"), ServingStatus::Serving)
                            .await;
                    }
                    let after = ALLOCATOR.live_bytes();
                    let delta = after - baseline;
                    let per_service = if n > 0 { delta / n as i64 } else { 0 };
                    std::hint::black_box(per_service)
                })
            });
        });
    }

    group.finish();
}

/// Measure live heap bytes after registering N watchers for a single service.
///
/// For each N in {1, 10, 100}: create a `HealthState` with one service, then
/// subscribe N `watch::Receiver` handles, snapshot the delta, and report
/// `delta / N` as the per-watcher cost.
fn bench_memory_per_watcher(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("memory_per_watcher");

    for n in [1usize, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let state = HealthState::new();
                    state
                        .set("my.Service".to_owned(), ServingStatus::Serving)
                        .await;

                    let baseline = ALLOCATOR.live_bytes();
                    let mut watchers = Vec::with_capacity(n);
                    for _ in 0..n {
                        watchers.push(state.watcher("my.Service").await);
                    }
                    let after = ALLOCATOR.live_bytes();
                    let delta = after - baseline;
                    let per_watcher = if n > 0 { delta / n as i64 } else { 0 };

                    // Keep watchers alive until after the snapshot.
                    std::hint::black_box(per_watcher);
                    drop(watchers);
                    per_watcher
                })
            });
        });
    }

    group.finish();
}

/// Measure heap churn from 1 000 status-flip cycles on a single service.
///
/// Because `HealthState::set` uses `send_if_modified` (dedup), alternating
/// Serving ↔ NotServing forces a real send each iteration.  The live-byte
/// delta at the end should be near zero for a non-leaking implementation.
fn bench_memory_set_churn(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    c.bench_function("memory_set_churn_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let state = HealthState::new();
                state
                    .set("my.Service".to_owned(), ServingStatus::Serving)
                    .await;

                let baseline = ALLOCATOR.live_bytes();
                for _ in 0..1000 {
                    state
                        .set("my.Service".to_owned(), ServingStatus::NotServing)
                        .await;
                    state
                        .set("my.Service".to_owned(), ServingStatus::Serving)
                        .await;
                }
                let after = ALLOCATOR.live_bytes();
                std::hint::black_box(after - baseline)
            })
        })
    });
}

criterion_group!(
    benches,
    bench_memory_per_service,
    bench_memory_per_watcher,
    bench_memory_set_churn,
);
criterion_main!(benches);
