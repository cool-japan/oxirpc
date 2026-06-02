//! Memory-profiling benchmark for native reflection service construction.
//!
//! Measures the live heap bytes allocated per [`NativeReflectionServiceV1`]
//! instance built from a `FileDescriptorSet` with a single service.  The main
//! cost is the `Arc<DescriptorPool>` + `Arc<[u8]>` owned by each service.
//!
//! Uses a custom [`CountingAllocator`] following the same pattern as
//! `oxirpc-health/benches/memory_bench.rs`.
//!
//! Run with:
//!   cargo bench -p oxirpc-reflect --bench leaked_fds_memory

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_reflect::ReflectionBuilder;
use prost::Message as _;
use prost_types::{FileDescriptorProto, FileDescriptorSet, ServiceDescriptorProto};

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

/// Encode a [`FileDescriptorSet`] with one service named `"Svc{i}"`.
fn make_fds_bytes(i: usize) -> Vec<u8> {
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some(format!("svc_{i}.proto")),
            package: Some("bench.reflect".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some(format!("Svc{i}")),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    fds.encode_to_vec()
}

// ─── Benchmark ───────────────────────────────────────────────────────────────

/// Measure live heap bytes per [`NativeReflectionServiceV1`] instance.
///
/// For each N in {1, 10, 100}: snapshot before building N services, build
/// them, snapshot after, and report `(after - before) / N` as the per-service
/// cost.  Services are kept alive until after the snapshot.
fn bench_memory_per_reflection_service(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_per_reflection_service");

    for n in [1usize, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let baseline = ALLOCATOR.live_bytes();
                let services: Vec<_> = (0..n)
                    .map(|i| {
                        let bytes = make_fds_bytes(i);
                        ReflectionBuilder::new()
                            .register_file_descriptor_set(bytes)
                            .build_native_v1()
                    })
                    .collect();
                let after = ALLOCATOR.live_bytes();
                let delta = after - baseline;
                let per_service = if n > 0 { delta / n as i64 } else { 0 };

                // Keep services alive until after the snapshot.
                std::hint::black_box(per_service);
                drop(services);
                per_service
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_memory_per_reflection_service);
criterion_main!(benches);
