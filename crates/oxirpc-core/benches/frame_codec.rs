//! Benchmarks for gRPC wire-frame encode/decode throughput.
//!
//! Exercises `encode_grpc_frame` and `decode_grpc_frame` across four payload
//! sizes (64 B, 1 KiB, 64 KiB, 1 MiB) to surface any per-call overhead and
//! to establish a baseline for future regression detection.
//!
//! These functions are deprecated; the `#[allow(deprecated)]` attribute here is
//! intentional because this benchmark exists specifically to track their behaviour.
#![allow(deprecated)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxirpc_core::grpc::{decode_grpc_frame, encode_grpc_frame};

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_grpc_frame");
    for size in [64_usize, 1024, 65536, 1_048_576] {
        let payload = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, p| {
            b.iter(|| encode_grpc_frame(p, false));
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_grpc_frame");
    for size in [64_usize, 1024, 65536, 1_048_576] {
        let payload = vec![0xABu8; size];
        let frame = encode_grpc_frame(&payload, false);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &frame, |b, f| {
            b.iter(|| decode_grpc_frame(f).expect("decode_grpc_frame ok"));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
