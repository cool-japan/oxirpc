//! Benchmarks for oxirpc-web: base64 encode/decode, gRPC-Web frame encode/decode,
//! and translation overhead.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_web::codec::{decode_body, decode_text_body, encode_frame, encode_text_body, Frame};

fn bench_base64_encode_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("base64");
    for size in [64usize, 1024, 65536] {
        let data = vec![0xABu8; size];
        let frame = Frame::data(data);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("encode_text", size), &frame, |b, f| {
            b.iter(|| {
                encode_text_body(std::slice::from_ref(f), CompressionEncoding::Identity)
                    .expect("encode ok")
            });
        });

        // Pre-encode once to get a stable base64 string to benchmark decoding.
        let encoded = encode_text_body(std::slice::from_ref(&frame), CompressionEncoding::Identity)
            .expect("encode ok");
        group.bench_with_input(BenchmarkId::new("decode_text", size), &encoded, |b, e| {
            b.iter(|| decode_text_body(e, CompressionEncoding::Identity).expect("decode ok"));
        });
    }
    group.finish();
}

fn bench_binary_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_frame");
    for size in [64usize, 1024, 65536] {
        let data = vec![0u8; size];
        let frame = Frame::data(data);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &frame, |b, f| {
            b.iter(|| encode_frame(f, CompressionEncoding::Identity).expect("encode ok"));
        });
    }
    group.finish();
}

fn bench_binary_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_frame_decode");
    for size in [64usize, 1024, 65536] {
        let frame = Frame::data(vec![0u8; size]);
        let encoded = encode_frame(&frame, CompressionEncoding::Identity).expect("encode ok");
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &encoded, |b, e| {
            b.iter(|| decode_body(e, CompressionEncoding::Identity).expect("decode ok"));
        });
    }
    group.finish();
}

fn bench_trailer_frame_encode(c: &mut Criterion) {
    // Bench encoding a trailer frame with N=5 key/value pairs.
    c.bench_function("trailer_frame_5_pairs", |b| {
        let pairs = [
            ("grpc-status", "0"),
            ("grpc-message", "OK"),
            ("x-custom-1", "value1"),
            ("x-custom-2", "value2"),
            ("x-custom-3", "value3"),
        ];
        b.iter(|| {
            let frame = Frame::trailers(&pairs);
            encode_frame(&frame, CompressionEncoding::Identity).expect("encode ok")
        });
    });
}

criterion_group!(
    benches,
    bench_base64_encode_decode,
    bench_binary_frame_encode,
    bench_binary_frame_decode,
    bench_trailer_frame_encode,
);
criterion_main!(benches);
