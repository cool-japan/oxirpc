//! OxiARC gzip compression/decompression throughput benchmarks.
//!
//! Requires the `gzip` feature (`--features gzip` or `--all-features`).
//!
//! Payload sizes: 64 B, 1 KiB, 64 KiB, 1 MiB.  Data is filled with a
//! pseudo-random pattern via a simple LCG (no external dependency) to avoid
//! artificially inflated throughput from trivially compressible constant bytes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxirpc_core::encoding::{compress, decompress, CompressionEncoding};

/// Fill `len` bytes with a deterministic pseudo-random sequence using a 64-bit
/// LCG.  This avoids trivially compressible constant payloads while keeping the
/// bench setup free of external dependencies.
fn lcg_payload(len: usize) -> Vec<u8> {
    let mut state: u64 = 0x853c_49e6_748f_ea9b;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect()
}

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compress_gzip");
    for size in [64_usize, 1024, 65536, 1_048_576] {
        let data = lcg_payload(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, d| {
            b.iter(|| compress(CompressionEncoding::Gzip, d).expect("compress ok"));
        });
    }
    group.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompress_gzip");
    for size in [64_usize, 1024, 65536, 1_048_576] {
        let data = lcg_payload(size);
        let compressed =
            compress(CompressionEncoding::Gzip, &data).expect("compress for decompress bench");
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &compressed,
            |b, c_data| {
                b.iter(|| decompress(CompressionEncoding::Gzip, c_data).expect("decompress ok"));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_compress, bench_decompress);
criterion_main!(benches);
