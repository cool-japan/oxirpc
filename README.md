# OxiRPC

**Pure-Rust gRPC stack — no protoc, no openssl, no ring by default.**

OxiRPC is a production-ready facade over [tonic 0.14](https://docs.rs/tonic/0.14)
that wires in [OxiProto](https://github.com/cool-japan/oxiproto) (descriptor parsing, replaces
protoc), [OxiTLS](https://github.com/cool-japan/oxitls) (Pure-Rust TLS via rustls, replaces
openssl/ring), and [OxiARC](https://github.com/cool-japan/oxiarc) (Pure-Rust gzip/zstd, replaces
flate2/zstd-sys). The default feature closure is 100% Pure Rust and FFI-free.

## Status: 0.2.1 — Pure Rust Policy v2 L1 compliant

702 tests pass across 9 crates (default features; 801 with `--all-features`,
including the HTTP/3 loopback E2E suite). clippy clean
(`-D warnings`). All milestones M0–M8 complete, including HTTP/3 (gRPC-over-QUIC)
behind the opt-in `http3` feature — see [HTTP/3](#http3-grpc-over-quic) below.
**Breaking change (0.2.0):** the `aws-lc` feature and `oxirpc::aws_lc` re-export
have been removed from the facade. Use `oxirpc-adapter-aws-lc` directly if you need
aws-lc-rs crypto.

```
cargo add oxirpc --features "client,server,tls,health,reflect,web,gzip,zstd"
```

## Feature flags

| Feature | What it enables |
|---------|----------------|
| `client` | `ClientBuilder`, channel pool, load balancing (round-robin, weighted, pick-first), resilience |
| `server` | `ServerBuilder`, middleware, compression layers |
| `native` | Native HTTP/2 server with `NativeServiceRegistry` and streaming bidi |
| `tls` | Pure-Rust TLS via OxiTLS 0.3.0 (no ring, no openssl) |
| `http3` | Pure-Rust HTTP/3 (gRPC-over-QUIC) client + server via OxiQUIC + `h3` (implies `client`+`server`+`native`+`tls`) |
| `gzip` | gRPC gzip message compression via `oxiarc-deflate` |
| `zstd` | gRPC Zstandard compression via `oxiarc-zstd` |
| `compression` | Legacy `OxiArcGzip` compress/decompress API |
| `reflect` | gRPC server reflection v1 + v1alpha |
| `health` | gRPC health checking protocol v1 |
| `web` | gRPC-Web bridge (binary + base64 text mode, CORS) |
| `oxiproto` | OxiProto type system integration (path-dep, not on crates.io default) |
| `full` | All Pure-Rust features (client + server + native + tls + http3 + reflect + web + health + gzip + zstd) |

Default features: `[]` (zero deps beyond tonic + prost + tokio).

> **Note (0.2.0):** The `aws-lc` feature has been removed from `oxirpc` facade.
> If you need aws-lc-rs–backed TLS, add `oxirpc-adapter-aws-lc` as a direct
> dependency with its `aws-lc` feature. This change makes every feature reachable
> via `oxirpc --all-features` 100% Pure Rust (Pure Rust Policy v2 L1).

## HTTP/3 (gRPC-over-QUIC)

Enable the `http3` feature for a 100% Pure-Rust gRPC-over-QUIC client and server,
built on [OxiQUIC](https://github.com/cool-japan/oxiquic) and the hyperium `h3`
crate. HTTP/3 rides on QUIC, which is TLS-1.3-only and negotiates the `"h3"` ALPN,
so its TLS configs **must** come from the `*_h3` helpers — they use OxiQUIC's
crypto provider (the generic pure provider cannot derive QUIC packet keys).

```rust,no_run
use std::sync::Arc;
use rustls::RootCertStore;
use oxirpc::http3::{client_config_h3_arc, H3ChannelBuilder};

# async fn ex(roots: RootCertStore) -> Result<(), oxirpc::OxiRpcError> {
// Client: dial one endpoint over QUIC and issue gRPC calls.
let tls = client_config_h3_arc(roots)?;
let channel = H3ChannelBuilder::new()
    .addr("127.0.0.1:4433".parse().unwrap())
    .server_name("localhost")
    .tls(tls)
    .build()?;
// channel.call(request).await ...
# let _ = channel; Ok(()) }
```

Server side, serve a `NativeServiceRegistry` over HTTP/3 with
`ServerBuilder::serve_native_registry_h3` (or `_with_endpoint` to observe the
bound port and wire graceful shutdown). The default build stays QUIC-free —
`cargo tree -p oxirpc --edges normal | grep oxiquic` is empty without `http3`.

## Quick start

```toml
# Cargo.toml
[dependencies]
oxirpc = { version = "0.2", features = ["client", "server", "tls", "health"] }

[build-dependencies]
oxirpc-build = "0.2"
```

```rust,no_run
// build.rs — no protoc required
fn main() -> Result<(), Box<dyn std::error::Error>> {
    oxirpc_build::compile_protos(&["proto/greeter.proto"], &["proto/"])?;
    Ok(())
}
```

```rust,no_run
// Server
use oxirpc::{ServerBuilder, OxiRpcError};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), OxiRpcError> {
    let addr: SocketAddr = "[::1]:50051".parse().expect("valid addr");
    ServerBuilder::new()
        .add_service(my_service())
        .serve(addr)
        .await
}
# fn my_service() -> () { () }
```

```rust,no_run
// Client
use oxirpc::ClientBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _channel = ClientBuilder::new("http://[::1]:50051")
        .connect()
        .await?;
    // let client = MyServiceClient::new(_channel);
    Ok(())
}
```

## Crate map

| Crate | Description |
|-------|-------------|
| `oxirpc` | Facade — re-exports everything under one dependency |
| `oxirpc-core` | Core types: errors, status codes, metadata, timeout, encoding, wire layer, TLS |
| `oxirpc-build` | Build-time proto compiler (no protoc) via oxiproto-build + protox |
| `oxirpc-client` | Channel builder, pool, load balancing, resilience, xDS/ADS |
| `oxirpc-server` | Server builder, routing, native registry, compression, TLS |
| `oxirpc-reflect` | gRPC server reflection v1 + v1alpha |
| `oxirpc-web` | gRPC-Web bridge + CORS policy |
| `oxirpc-health` | gRPC health checking protocol v1 |
| `oxirpc-adapter-aws-lc` | Optional aws-lc-rs TLS adapter (non-default) |

## Implementation highlights

**Wire layer (oxirpc-core::wire)**

Native HTTP/2 gRPC wire format: `FrameEncoder`/`FrameDecoder` (tokio-util codecs),
`GrpcRequestHeaders`/`GrpcResponseHeaders`, gRPC status trailer encoding with
percent-encoding, `MessagePipeline` for compression-aware encode/decode,
`Deadline` + timeout codec. Truly-streaming bidi driver (`drive_bidi` /
`drive_bidi_with_encoding`) using incremental `FrameDecoder` — response N
before request N+1.

**TLS (oxirpc-core::tls + oxirpc-client::tls_connector)**

`PureRustTlsConnector` wires `rustls` + OxiTLS's `pure_provider()` into
tonic-transport's `connect_with_connector`. End-to-end Pure-Rust TLS on both
client and server without forking tonic.

**Compression (oxirpc-core::encoding)**

`CompressionEncoding` (Identity / Gzip / Zstd) backed by `oxiarc-deflate` and
`oxiarc-zstd`. Wire-level `FLAG_COMPRESSED` framing in gRPC-Web codec. No
`flate2`, no `zstd` C crate on any feature edge.

**Async Interceptors (oxirpc-core, oxirpc-client, oxirpc-server) — v0.1.3**

`AsyncInterceptor` is now fully wired on both sides. Client-side:
`NativeChannelBuilder::with_async_interceptor` runs before every H2 stream is
opened — inject metadata or abort with a `Status`. Server-side:
`NativeServiceRegistry::with_async_interceptor` runs before the matched service
handler — mutate incoming headers or return a gRPC error response. Closures
returning a future implement `AsyncInterceptor` via a blanket impl, so no
manual struct is needed.

**Build cache isolation (oxirpc-build) — v0.1.3**

`compile_to_fds` now stores each proto set's descriptor under
`$OUT_DIR/.oxirpc-cache/fds-<hash>.bin` where `<hash>` is a stable hash of the
sorted input paths. Multiple independent `compile_to_fds` calls sharing one
`$OUT_DIR` no longer collide.

**gRPC conformance harness (oxirpc) — v0.1.3**

`tests/conformance.rs` implements `grpc.testing.TestService` (empty, unary,
server/client/full-duplex streaming) and drives the upstream grpc-go interop
client when `GRPC_GO_INTEROP_CLIENT` is set. CI without the Go toolchain skips
gracefully.

**Interceptors (oxirpc::interceptors)**

Full interceptor library: `BearerAuth`, `Tracing`, `Deadline`, rate limiting,
metrics, logging, retry, circuit breaker. Composable via `InterceptorChain`.

**Reflection + Health**

`NativeReflectionService` with optional `oxiproto-reflect` `DescriptorPool`
backend (`oxiproto` feature). `HealthBuilder` with per-service status mirror,
bulk `set_all_serving` / `set_all_not_serving`, and Watch streaming.

**gRPC-Web**

`GrpcWebLayer` wraps any tonic `Router`. Native frame codec handles binary and
base64 text mode, compression-aware data frames, trailer frame synthesis.
`CorsPolicy` supports wildcard, per-origin, and `allow_credentials` modes.

## FFI audit

Default features and `--all-features` on `cargo tree -p oxirpc --edges normal`
contain zero occurrences of: `protoc`, `openssl`, `openssl-sys`, `ring`,
`aws-lc-sys`, `native-tls`, `flate2`, `zstd` (C crate), `bzip2-sys`, `xz2`.

**0.2.0 change:** The `aws-lc` feature has been removed from the facade entirely.
The entire `--all-features` closure of `oxirpc` is now 100% Pure Rust
(Pure Rust Policy v2 L1 compliant). To use aws-lc-rs, depend directly on
`oxirpc-adapter-aws-lc` with its `aws-lc` feature.

## FFI eliminated

| Old dependency | Replaced by |
|----------------|-------------|
| `protoc` binary | `oxiproto-build` + `protox` (pure Rust) |
| `openssl-sys` | OxiTLS (`rustls` + `rustls-rustcrypto`) |
| `ring` | OxiTLS `pure_provider()` |
| `native-tls` | Banned from default closure |
| `flate2` | `oxiarc-deflate` (pure Rust) |
| `zstd` C crate | `oxiarc-zstd` (pure Rust) |

## Testing

```bash
cargo nextest run                     # 702 tests (default features)
cargo nextest run --all-features      # 801 tests: full suite including HTTP/3 E2E + aws-lc adapter purity check
```

Includes: unit tests, integration tests (TLS round-trip, gRPC-Web transport,
health/reflect streaming), conformance scaffold, cross-validation against
tonic, load-concurrency (256-concurrent unary, 64-concurrent streams), and a
fuzz target for the `FrameDecoder` (`cargo fuzz run wire_frame`).

## Inter-Oxi

**Depends on:** OxiProto (descriptor parsing, replaces protoc), OxiTLS (CryptoProvider + cert
types, replaces openssl/ring), OxiARC (compression, replaces flate2/zstd-sys).

**Depended on by:** `oxirouter` (gateway gRPC ingress + service-to-service), `oxigenai` (LLM
inference server gRPC API), `oxigdal-cluster` (geospatial distributed coordination),
`oxionnx` (model serving), `oxirs` (SPARQL-over-gRPC).

## License

Apache-2.0. Copyright COOLJAPAN OU (Team Kitasan).
