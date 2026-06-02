//! Benchmarks for the live gRPC wire hot paths.
//!
//! Exercises `server::encode_grpc_message`, `server::decode_grpc_message`,
//! `wire::frame::encode_frame`, `FrameDecoder`, and `MessagePipeline` across
//! four payload sizes (64 B, 1 KiB, 64 KiB, 1 MiB) to surface per-call
//! overhead and establish a regression baseline.
//!
//! Uses `std::hint::black_box` (criterion 0.8 drops its own black_box).
//! No deprecated `grpc::*` functions are used; all paths are the live `wire::*`
//! APIs.

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio_util::codec::Decoder;

use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_core::wire::codec::MessagePipeline;
use oxirpc_core::wire::frame::{encode_frame, FrameDecoder, FrameOptions};
use oxirpc_core::wire::server::{decode_grpc_message, encode_grpc_message};

/// Minimal prost message used as the bench payload.
///
/// Serialised size ≈ `payload.len() + 2` bytes (tag/varint overhead).
#[derive(Clone, PartialEq, prost::Message)]
struct BenchMsg {
    #[prost(bytes = "bytes", tag = "1")]
    payload: bytes::Bytes,
}

const SIZES: &[usize] = &[64, 1024, 64 * 1024, 1024 * 1024];

// ─── 1. encode_grpc_message throughput ───────────────────────────────────────

fn bench_encode_grpc_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_grpc_message");
    for &size in SIZES {
        let msg = BenchMsg {
            payload: bytes::Bytes::from(vec![0u8; size]),
        };
        let encoded_size = prost::Message::encoded_len(&msg) as u64;
        group.throughput(Throughput::Bytes(encoded_size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &msg, |b, m| {
            b.iter(|| std::hint::black_box(encode_grpc_message(m).unwrap()))
        });
    }
    group.finish();
}

// ─── 2. decode_grpc_message throughput ───────────────────────────────────────

fn bench_decode_grpc_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_grpc_message");
    for &size in SIZES {
        let msg = BenchMsg {
            payload: bytes::Bytes::from(vec![0u8; size]),
        };
        // Pre-build the framed bytes once; clone is O(1) refcount bump.
        let framed = encode_grpc_message(&msg).unwrap();
        let framed_len = framed.len() as u64;
        group.throughput(Throughput::Bytes(framed_len));
        group.bench_with_input(BenchmarkId::from_parameter(size), &framed, |b, f| {
            b.iter(|| std::hint::black_box(decode_grpc_message::<BenchMsg>(f.clone()).unwrap()))
        });
    }
    group.finish();
}

// ─── 3. encode_frame (free function) throughput ──────────────────────────────

fn bench_frame_encoder(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_frame");
    for &size in SIZES {
        let payload = bytes::Bytes::from(vec![0u8; size]);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, p| {
            b.iter(|| std::hint::black_box(encode_frame(p, false).unwrap()))
        });
    }
    group.finish();
}

// ─── 4. FrameDecoder decode throughput ───────────────────────────────────────

fn bench_frame_decoder(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_frame");
    for &size in SIZES {
        let payload = vec![0u8; size];
        // Pre-build the framed bytes as a template for BytesMut each iteration.
        let framed_bytes = encode_frame(&payload, false).unwrap();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &framed_bytes,
            |b, template| {
                // Reuse one decoder across iterations; after a successful decode
                // `pending` is None so state is clean.
                let mut decoder = FrameDecoder::default();
                b.iter(|| {
                    let mut buf = BytesMut::from(template.as_ref());
                    std::hint::black_box(decoder.decode(&mut buf).unwrap().unwrap())
                })
            },
        );
    }
    group.finish();
}

// ─── 5. MessagePipeline identity round-trip throughput ───────────────────────

fn bench_message_pipeline_identity(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_pipeline_identity");
    for &size in SIZES {
        let raw = vec![0u8; size];
        let pipeline = MessagePipeline::new(CompressionEncoding::Identity, FrameOptions::default());
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &raw, |b, r| {
            b.iter(|| {
                let frame = std::hint::black_box(pipeline.encode_message(r).unwrap());
                std::hint::black_box(pipeline.decode_message(&frame).unwrap())
            })
        });
    }
    group.finish();
}

// ─── Criterion harness ───────────────────────────────────────────────────────

criterion_group!(
    wire_codec,
    bench_encode_grpc_message,
    bench_decode_grpc_message,
    bench_frame_encoder,
    bench_frame_decoder,
    bench_message_pipeline_identity,
);
criterion_main!(wire_codec);
