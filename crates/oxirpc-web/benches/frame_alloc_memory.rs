//! Memory-profiling benchmark for gRPC-Web frame encoding.
//!
//! Measures bytes allocated to encode 1 000 gRPC-Web binary frames of varying
//! sizes using the [`oxirpc_web::codec::encode_frame`] function.
//!
//! Uses a custom [`CountingAllocator`] following the same pattern as
//! `oxirpc-health/benches/memory_bench.rs`.
//!
//! Run with:
//!   cargo bench -p oxirpc-web --bench frame_alloc_memory

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_web::codec::{encode_frame, Frame};

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

// ─── Benchmark ───────────────────────────────────────────────────────────────

/// Measure bytes allocated to encode 1 000 gRPC-Web frames of a given size.
///
/// For each frame_size in {64, 1 024, 65 536}: snapshot live bytes before
/// encoding the 1 000 frames, encode them (collecting into a `Vec` to keep
/// allocations alive), snapshot after, and report `(after - before) / 1000`
/// as the per-frame allocation cost.
fn bench_frame_alloc_1000(c: &mut Criterion) {
    const ITER: usize = 1_000;
    let mut group = c.benchmark_group("frame_alloc_1000_frames");

    for frame_size in [64usize, 1_024, 65_536] {
        let payload = vec![0xCDu8; frame_size];
        let frame = Frame::data(payload);

        group.bench_with_input(BenchmarkId::from_parameter(frame_size), &frame, |b, f| {
            b.iter(|| {
                let baseline = ALLOCATOR.live_bytes();
                let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(ITER);
                for _ in 0..ITER {
                    let bytes = encode_frame(f, CompressionEncoding::Identity).expect("encode ok");
                    encoded.push(bytes);
                }
                let after = ALLOCATOR.live_bytes();
                let delta = after - baseline;
                let per_frame = delta / ITER as i64;

                // Keep encoded buffers alive until after the snapshot.
                std::hint::black_box(per_frame);
                drop(encoded);
                per_frame
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_frame_alloc_1000);
criterion_main!(benches);
