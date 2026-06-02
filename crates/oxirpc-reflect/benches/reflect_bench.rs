//! Benchmarks for `DescriptorPool` descriptor lookup.
//!
//! Exercises the three hot paths:
//! - `list_services()` — iterates all registered service names.
//! - `find_file_by_name()` — scans by proto filename.
//! - `find_file_containing_symbol()` — walks packages/messages/services by FQN.
//!
//! Pool sizes of 1, 10, and 100 registered files are tested so Criterion can
//! plot scaling behaviour across the range of realistic deployments.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxirpc_reflect::{DescriptorPoolBuilder, ReflectionBuilder};
use prost::Message as _;
use prost_types::{
    field_descriptor_proto::Type, DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto,
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build a pool with `n` file descriptors.
///
/// Each file contains:
/// - one service named `"Svc{i}"` (package `"bench.pkg"`)
/// - one top-level message named `"Msg{i}"` (package `"bench.pkg"`)
///
/// File names follow the pattern `"bench_{i}.proto"`.
fn build_pool(n: usize) -> oxirpc_reflect::DescriptorPool {
    let mut builder = DescriptorPoolBuilder::new();
    for i in 0..n {
        let fd = FileDescriptorProto {
            name: Some(format!("bench_{i}.proto")),
            package: Some("bench.pkg".to_owned()),
            syntax: Some("proto3".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some(format!("Svc{i}")),
                ..Default::default()
            }],
            message_type: vec![DescriptorProto {
                name: Some(format!("Msg{i}")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let fds = FileDescriptorSet { file: vec![fd] };
        builder = builder.register(fds);
    }
    builder.build()
}

// ─── list_services ────────────────────────────────────────────────────────────

fn bench_list_services(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_services");

    for &n in &[1usize, 10, 100] {
        let pool = build_pool(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &pool, |b, p| {
            b.iter(|| {
                let names = p.list_services();
                std::hint::black_box(names);
            });
        });
    }

    group.finish();
}

// ─── find_file_by_name ───────────────────────────────────────────────────────

fn bench_find_file_by_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_file_by_name");
    // Search for the last registered file to exercise the worst-case linear scan.
    let pool = build_pool(100);
    let target = "bench_99.proto";

    group.bench_function("100_files_last", |b| {
        b.iter(|| {
            let result = pool.find_file_by_name(std::hint::black_box(target));
            std::hint::black_box(result);
        });
    });

    group.finish();
}

// ─── find_file_containing_symbol ─────────────────────────────────────────────

fn bench_find_file_containing_symbol(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_file_containing_symbol");
    let pool = build_pool(100);

    // Bench message symbol lookup for the last registered message.
    let msg_symbol = "bench.pkg.Msg99";
    group.bench_function("message_100_last", |b| {
        b.iter(|| {
            let result = pool.find_file_containing_symbol(std::hint::black_box(msg_symbol));
            std::hint::black_box(result);
        });
    });

    // Bench service symbol lookup for the last registered service.
    let svc_symbol = "bench.pkg.Svc99";
    group.bench_function("service_100_last", |b| {
        b.iter(|| {
            let result = pool.find_file_containing_symbol(std::hint::black_box(svc_symbol));
            std::hint::black_box(result);
        });
    });

    // Bench a miss (symbol not present anywhere).
    let miss_symbol = "bench.pkg.NonExistent";
    group.bench_function("miss_100", |b| {
        b.iter(|| {
            let result = pool.find_file_containing_symbol(std::hint::black_box(miss_symbol));
            std::hint::black_box(result);
        });
    });

    group.finish();
}

// ─── Helpers for extension benchmarks ────────────────────────────────────────

/// Build a pool with `n` file descriptors, each contributing one extension field on
/// `"TestMessage"`.
///
/// File `i` registers extension number `1000 + i` so that searching for the last
/// extension (`1000 + n - 1`) exercises a worst-case linear scan — mirroring the
/// `bench_find_file_by_name` pattern.
fn build_pool_with_extensions(n: usize) -> oxirpc_reflect::DescriptorPool {
    let mut builder = DescriptorPoolBuilder::new();
    for i in 0..n {
        let ext = FieldDescriptorProto {
            name: Some(format!("ext_{i}")),
            number: Some(1000 + i as i32),
            extendee: Some(".TestMessage".to_owned()),
            r#type: Some(Type::Int32 as i32),
            ..Default::default()
        };
        let fd = FileDescriptorProto {
            name: Some(format!("ext_{i}.proto")),
            package: Some("bench.ext".to_owned()),
            extension: vec![ext],
            ..Default::default()
        };
        let fds = FileDescriptorSet { file: vec![fd] };
        builder = builder.register(fds);
    }
    builder.build()
}

// ─── bench_find_file_containing_extension ────────────────────────────────────

/// Benchmark `pool.find_file_containing_extension("TestMessage", last_ext_number)`.
///
/// Each pool size N has a distinct worst-case last extension (`1000 + N - 1`) so
/// the lookup scans all N files before finding a match.
fn bench_find_file_containing_extension(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_file_containing_extension");

    for &n in &[1usize, 10, 100] {
        let pool = build_pool_with_extensions(n);
        // Search for the last extension to force a worst-case scan.
        let target_ext = 1000 + n as i32 - 1;

        group.bench_with_input(
            BenchmarkId::from_parameter(n),
            &(pool, target_ext),
            |b, (p, ext)| {
                b.iter(|| {
                    let result = p.find_file_containing_extension(
                        std::hint::black_box("TestMessage"),
                        std::hint::black_box(*ext),
                    );
                    std::hint::black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ─── bench_extension_numbers_of_type ─────────────────────────────────────────

/// Benchmark `pool.extension_numbers("TestMessage")` — collects all extension
/// numbers across N files targeting the same type.
fn bench_extension_numbers_of_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("extension_numbers_of_type");

    for &n in &[1usize, 10, 100] {
        let pool = build_pool_with_extensions(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &pool, |b, p| {
            b.iter(|| {
                let nums = p.extension_numbers(std::hint::black_box("TestMessage"));
                std::hint::black_box(nums);
            });
        });
    }

    group.finish();
}

// ─── bench_build_native_pair ──────────────────────────────────────────────────

/// Benchmark `ReflectionBuilder::new().register_file_descriptor_set(fds_bytes).build_native()`.
///
/// `fds_bytes` encodes a single `FileDescriptorSet` containing 10 service descriptors.
/// This measures the construction overhead of building both the v1 and v1alpha native
/// reflection service wrappers.
fn bench_build_native_pair(c: &mut Criterion) {
    // Pre-compute the FDS bytes outside the benchmark loop.
    let fds = FileDescriptorSet {
        file: (0..10)
            .map(|i| FileDescriptorProto {
                name: Some(format!("native_{i}.proto")),
                package: Some("bench.native".to_owned()),
                service: vec![ServiceDescriptorProto {
                    name: Some(format!("NativeSvc{i}")),
                    method: vec![MethodDescriptorProto {
                        name: Some("Call".to_owned()),
                        input_type: Some(".bench.native.Req".to_owned()),
                        output_type: Some(".bench.native.Resp".to_owned()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
    };
    let fds_bytes = fds.encode_to_vec();

    let mut group = c.benchmark_group("build_native_pair");
    group.bench_function("10_services", |b| {
        b.iter(|| {
            let (v1, v1alpha) = ReflectionBuilder::new()
                .register_file_descriptor_set(fds_bytes.clone())
                .build_native();
            std::hint::black_box((v1, v1alpha));
        });
    });
    group.finish();
}

// ─── Criterion entry-points ───────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_list_services,
    bench_find_file_by_name,
    bench_find_file_containing_symbol,
    bench_find_file_containing_extension,
    bench_extension_numbers_of_type,
    bench_build_native_pair,
);
criterion_main!(benches);
