//! Benchmark: protox parse time on synthetic `.proto` inputs.
//!
//! Measures wall time for `protox::compile` on small (10-service),
//! medium (50-service), and large (200-service) synthetic proto files
//! written to `std::env::temp_dir()`.
//!
//! Pure Rust — no `protoc` invocation.  This benchmark documents the
//! protox parse cost; comparison against protoc requires an external
//! toolchain and is not included here.
//!
//! Run with:
//!   cargo bench -p oxirpc-build --bench protox_parse

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::path::PathBuf;

// ─── Proto generator ────────────────────────────────────────────────────────

/// Generate a synthetic `.proto` source string with `n_services` services,
/// each containing 2 unary RPCs.
///
/// The generated string is deterministic — no timestamps or random content.
fn generate_proto(n_services: usize) -> String {
    let mut out = String::with_capacity(n_services * 128 + 128);
    out.push_str("syntax = \"proto3\";\n");
    out.push_str("package bench;\n\n");
    out.push_str("message Req  { string data = 1; }\n");
    out.push_str("message Resp { string data = 1; }\n\n");

    for i in 0..n_services {
        out.push_str(&format!("service S{i} {{\n"));
        out.push_str("  rpc M0(Req) returns (Resp);\n");
        out.push_str("  rpc M1(Req) returns (Resp);\n");
        out.push_str("}\n\n");
    }

    out
}

// ─── Temp-dir helpers ───────────────────────────────────────────────────────

/// Return the temp directory path for a given service count.
///
/// Reuses a fixed path per `n` so the directory (and its `.proto` file) can be
/// created once in the setup closure and reused across iterations without
/// re-creating it on every call.
fn bench_dir(n: usize) -> PathBuf {
    std::env::temp_dir().join(format!("oxirpc_protox_bench_{n}"))
}

// ─── Benchmark ──────────────────────────────────────────────────────────────

/// Measure `protox::compile` wall time for small / medium / large proto inputs.
///
/// Each iteration re-parses the proto file to capture the true parse cost
/// including file I/O.  The proto file itself is written once per parameter
/// value in the setup closure so that set-up overhead is excluded from the
/// measurement.
fn bench_protox_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("protox_parse");

    for &n in &[10_usize, 50, 200] {
        // --- Setup: write the proto file once per n ---
        let dir = bench_dir(n);
        std::fs::create_dir_all(&dir).expect("create bench temp dir");
        let proto_file = dir.join("bench.proto");
        let proto_content = generate_proto(n);
        std::fs::write(&proto_file, &proto_content).expect("write bench proto");

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _n| {
            let proto_file = proto_file.clone();
            let dir = dir.clone();
            b.iter(|| {
                // Re-parse on every iteration; result is black-boxed so the
                // compiler cannot elide the work.
                let result = protox::compile(
                    std::hint::black_box(std::slice::from_ref(&proto_file)),
                    std::hint::black_box(std::slice::from_ref(&dir)),
                );
                std::hint::black_box(result)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_protox_parse);
criterion_main!(benches);
