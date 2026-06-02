//! Memory-profiling benchmark for [`NativeServiceRegistry`].
//!
//! Uses a custom [`CountingAllocator`] (same pattern as `oxirpc-health`) to
//! measure live heap bytes before and after constructing N registry instances.
//! Because [`NativeServiceRegistry`] is `cfg(feature = "native")` and the bench
//! binary therefore requires that feature, add `required-features = ["native"]`
//! to the `[[bench]]` entry in `Cargo.toml`.
//!
//! Run with:
//!   cargo bench -p oxirpc-server --bench high_conn_memory --features native

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{criterion_group, criterion_main, Criterion};

#[cfg(feature = "native")]
use criterion::BenchmarkId;

#[cfg(feature = "native")]
use oxirpc_server::NativeServiceRegistry;

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

    #[cfg(feature = "native")]
    fn live_bytes(&self) -> i64 {
        self.allocated.load(Ordering::Relaxed)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

// ─── Benchmark ───────────────────────────────────────────────────────────────

/// Measure live heap bytes per [`NativeServiceRegistry`] instance.
///
/// For each N in {1, 10, 100}: snapshot live bytes before allocating N registry
/// instances, allocate them, snapshot after, and report `(after - before) / N`
/// as the per-registry cost.  The registries are kept alive until after the
/// snapshot so the allocator sees them as live.
#[cfg(feature = "native")]
fn bench_memory_per_registry(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_per_registry");

    for n in [1usize, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let baseline = ALLOCATOR.live_bytes();
                let mut registries: Vec<NativeServiceRegistry> = Vec::with_capacity(n);
                for _ in 0..n {
                    registries.push(NativeServiceRegistry::new());
                }
                let after = ALLOCATOR.live_bytes();
                let delta = after - baseline;
                let per_registry = if n > 0 { delta / n as i64 } else { 0 };

                // Keep registries alive until after the snapshot.
                std::hint::black_box(per_registry);
                drop(registries);
                per_registry
            });
        });
    }

    group.finish();
}

// ─── Fallback for non-native builds ──────────────────────────────────────────

#[cfg(not(feature = "native"))]
fn bench_memory_per_registry(_c: &mut Criterion) {
    // NativeServiceRegistry is only available with the `native` feature.
    // This stub keeps the bench binary compilable without it.
}

criterion_group!(benches, bench_memory_per_registry);
criterion_main!(benches);
