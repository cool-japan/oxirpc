//! Metadata HashMap lookup benchmarks.
//!
//! Measures the cost of `Metadata::get` (case-insensitive ASCII header lookup)
//! for maps of N = 8 / 64 / 512 entries.  Each bench hits the same key
//! ("key-0") every iteration so the map size reflects realistic cache pressure.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_core::metadata::Metadata;

fn build_metadata(n: usize) -> Metadata {
    let mut meta = Metadata::new();
    for i in 0..n {
        // key and value are valid ASCII; unwrap is safe in bench setup.
        meta.insert(
            Box::leak(format!("key-{i}").into_boxed_str()),
            Box::leak(format!("value-{i}").into_boxed_str()),
        )
        .expect("insert ok");
    }
    meta
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_get");
    for n in [8_usize, 64, 512] {
        let meta = build_metadata(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &meta, |b, m| {
            b.iter(|| m.get("key-0"));
        });
    }
    group.finish();
}

fn bench_contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_contains_key");
    for n in [8_usize, 64, 512] {
        let meta = build_metadata(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &meta, |b, m| {
            b.iter(|| m.contains_key("key-0"));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_get, bench_contains_key);
criterion_main!(benches);
