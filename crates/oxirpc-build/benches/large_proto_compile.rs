//! Benchmark: code generation time for large synthesised proto schemas.
//!
//! Constructs a [`prost_types::FileDescriptorProto`] with 50 services (each
//! having 2 unary methods) and measures the wall time for
//! [`ServiceCodegen::generate`] to produce the Rust source for all services.
//!
//! Run with:
//!   cargo bench -p oxirpc-build --bench large_proto_compile

use criterion::{criterion_group, criterion_main, Criterion};
use oxirpc_build::codegen::ServiceCodegen;
use prost_types::{FileDescriptorProto, MethodDescriptorProto, ServiceDescriptorProto};

// ─── Helper ──────────────────────────────────────────────────────────────────

/// Build a synthetic [`FileDescriptorProto`] with `n` services, each
/// containing `methods_per_svc` unary RPCs.
fn make_large_file(n: usize, methods_per_svc: usize) -> FileDescriptorProto {
    let services = (0..n)
        .map(|i| {
            let methods = (0..methods_per_svc)
                .map(|j| MethodDescriptorProto {
                    name: Some(format!("Method{j}")),
                    input_type: Some(format!(".bench.Req{i}_{j}")),
                    output_type: Some(format!(".bench.Resp{i}_{j}")),
                    client_streaming: Some(false),
                    server_streaming: Some(false),
                    ..Default::default()
                })
                .collect();
            ServiceDescriptorProto {
                name: Some(format!("Service{i}")),
                method: methods,
                ..Default::default()
            }
        })
        .collect();

    FileDescriptorProto {
        name: Some("bench_large.proto".to_owned()),
        package: Some("bench".to_owned()),
        syntax: Some("proto3".to_owned()),
        service: services,
        ..Default::default()
    }
}

// ─── Benchmark ───────────────────────────────────────────────────────────────

/// Measure wall time for [`ServiceCodegen::generate`] on a file with 50 services.
fn bench_generate_50_services(c: &mut Criterion) {
    let file = make_large_file(50, 2);
    let codegen = ServiceCodegen::new();

    c.bench_function("generate_50_services", |b| {
        b.iter(|| {
            let result = codegen.generate(std::hint::black_box(&file));
            std::hint::black_box(result)
        })
    });
}

/// Measure wall time for [`ServiceCodegen::generate`] on a file with 10 services,
/// each with 10 methods — tests method-count scaling.
fn bench_generate_10x10(c: &mut Criterion) {
    let file = make_large_file(10, 10);
    let codegen = ServiceCodegen::new();

    c.bench_function("generate_10_services_10_methods", |b| {
        b.iter(|| {
            let result = codegen.generate(std::hint::black_box(&file));
            std::hint::black_box(result)
        })
    });
}

criterion_group!(benches, bench_generate_50_services, bench_generate_10x10);
criterion_main!(benches);
