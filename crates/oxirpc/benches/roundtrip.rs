//! Benchmark: encode/decode round-trip via the oxirpc-core gRPC framing layer.
//!
//! Spins no real network — measures the CPU cost of the gRPC 5-byte frame
//! encode + decode cycle using [`oxirpc_core::grpc::encode_grpc_frame`] and
//! [`oxirpc_core::grpc::decode_grpc_frame`].
//!
//! This is a proxy for the minimum per-RPC serialisation overhead on the
//! hot path.  A future milestone that introduces a native transport can replace
//! this with a full in-process loopback roundtrip.
//!
//! These functions are deprecated; the `#[allow(deprecated)]` here is intentional
//! because this benchmark exists to track their historical behaviour.
#![allow(deprecated)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_core::grpc::{decode_grpc_frame, encode_grpc_frame};

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_frame_roundtrip");

    // Payload sizes representative of small, medium, and large gRPC messages.
    for &payload_bytes in &[16usize, 64, 512, 4096] {
        let payload: Vec<u8> = (0..payload_bytes).map(|i| (i % 251) as u8).collect();

        group.bench_with_input(
            BenchmarkId::new("encode_uncompressed", payload_bytes),
            &payload,
            |b, p| {
                b.iter(|| std::hint::black_box(encode_grpc_frame(std::hint::black_box(p), false)));
            },
        );

        let framed = encode_grpc_frame(&payload, false);
        group.bench_with_input(
            BenchmarkId::new("decode_uncompressed", payload_bytes),
            &framed,
            |b, f| {
                b.iter(|| {
                    let _ = std::hint::black_box(decode_grpc_frame(std::hint::black_box(f)));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("encode_decode_roundtrip", payload_bytes),
            &payload,
            |b, p| {
                b.iter(|| {
                    let framed = encode_grpc_frame(std::hint::black_box(p), false);
                    let _ = std::hint::black_box(
                        decode_grpc_frame(&framed).expect("decode must succeed"),
                    );
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip);
criterion_main!(benches);
