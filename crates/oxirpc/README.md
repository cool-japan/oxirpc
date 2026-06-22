# oxirpc — The COOLJAPAN Pure-Rust gRPC facade

[![Crates.io](https://img.shields.io/crates/v/oxirpc.svg)](https://crates.io/crates/oxirpc)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc` is the top-level **façade** for the OxiRPC gRPC stack. It is a transparent wrapper over [tonic 0.14](https://docs.rs/tonic/0.14) that aggregates the OxiRPC sub-crates behind one dependency, adds ergonomic re-exports, ready-to-use interceptor patterns, and native gRPC primitives — all with **COOLJAPAN Pure-Rust defaults**: no ring, no openssl, no native-tls on the default feature set. Existing tonic-generated stubs work unmodified; migrating from tonic is a dependency and import change, not a rewrite.

Because it is a façade, almost everything you import from `oxirpc` is re-exported from a focused sub-crate, gated behind a Cargo feature. The crate root re-exports the common types (`Status`, `Metadata`, `Request`, `Response`, `OxiRpcError`, …) from [`oxirpc-core`] unconditionally; `ClientBuilder`, `ServerBuilder`, reflection, health, gRPC-Web, compression, and TLS each arrive through their own feature and live both at the crate root and under a namespaced module (`oxirpc::client`, `oxirpc::server`, `oxirpc::reflect`, …). The crate is `#![forbid(unsafe_code)]`. Honest note on TLS: tonic 0.14's own `ServerTlsConfig`/`ClientTlsConfig` require its `tls-ring`/`tls-aws-lc` features (which pull FFI); `oxirpc`'s `tls` feature instead surfaces OxiTLS's Pure-Rust rustls configs via `oxirpc::tls`.

## Installation

```toml
[dependencies]
# Client + server + Pure-Rust TLS
oxirpc = { version = "0.2.0", features = ["client", "server", "tls"] }

# Everything Pure Rust (no aws-lc, no oxiproto)
oxirpc = { version = "0.2.0", features = ["full"] }

# Server with the native (zero-tonic-transport) path + health
oxirpc = { version = "0.2.0", features = ["native", "health"] }
```

Add proto codegen to your `build.rs` (no `protoc` required — see [`oxirpc-build`]):

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    oxirpc_build::compile_protos(&["proto/service.proto"], &["proto/"])?;
    Ok(())
}
```

## Quick Start

### Client

```rust,no_run
# #[cfg(feature = "client")]
# {
async fn run_client() -> Result<(), oxirpc::OxiRpcError> {
    let _channel = oxirpc::ClientBuilder::new("http://127.0.0.1:50051")
        .connect()
        .await?;
    // let client = MyServiceClient::new(_channel);  // generated client
    Ok(())
}
# }
```

### Server (one-liner)

```rust,no_run
# #[cfg(feature = "server")]
# async fn run<S>(addr: std::net::SocketAddr, my_service: S) -> Result<(), oxirpc::OxiRpcError>
# where
#     S: tower::Service<
#             http::Request<tonic::body::Body>,
#             Response = http::Response<tonic::body::Body>,
#             Error = std::convert::Infallible,
#         > + tonic::server::NamedService + Clone + Send + Sync + 'static,
#     <S as tower::Service<http::Request<tonic::body::Body>>>::Future: Send + 'static,
#     <S as tower::Service<http::Request<tonic::body::Body>>>::Response: axum::response::IntoResponse,
# {
// Mounts `my_service` and serves until killed.
oxirpc::serve(addr, my_service).await
# }
```

For TLS, keepalive, multiple services, or middleware layers, use [`ServerBuilder`] (re-exported at `oxirpc::ServerBuilder`) directly.

## Migrating from tonic

`oxirpc` is a thin wrapper over tonic 0.14. If you already use tonic, migration is import-only:

```text
Before (tonic):
  use tonic::transport::Server;
  use tonic::{Request, Response, Status};

After (oxirpc):
  use oxirpc::ServerBuilder;            // wrapper with ergonomic extras
  use oxirpc::{Request, Response, Status};  // same types, re-exported
```

All tonic-generated stubs work with `oxirpc` without modification.

## Architecture — sub-crates this façade aggregates

```text
oxirpc (facade)
├── oxirpc-core          — native types: Status, Metadata, Codec, Timeout, Encoding (always on)
├── oxirpc-client        — ClientBuilder, channel pool, load balancing, resilience   [client]
├── oxirpc-server        — ServerBuilder, middleware layers, native transport        [server, native]
├── oxirpc-reflect       — gRPC server reflection services (v1 / v1alpha)            [reflect]
├── oxirpc-health        — health checking + K8s probes                              [health]
├── oxirpc-web           — gRPC-Web codec + CORS tower layer                         [web]
├── oxirpc-adapter-aws-lc— aws-lc-rs rustls provider (opt-in, NOT Pure Rust)         [aws-lc]
└── oxirpc-build         — build-time proto compilation, no protoc (build-dependency)
```

## API Overview

### Crate-root re-exports (always available, from `oxirpc-core`)

| Item | Description |
|------|-------------|
| `Status`, `StatusCode`, `Code` | gRPC status type and the 17 status codes |
| `Metadata` | ASCII + binary gRPC metadata |
| `Request`, `Response` | gRPC request/response wrappers |
| `OxiRpcError`, `OxiRpcResult` | OxiRPC error type and result alias |
| `version() -> &'static str` | The `oxirpc` crate version (`CARGO_PKG_VERSION`) |

### Convenience functions

| Function | Feature | Description |
|----------|---------|-------------|
| `serve(addr, service)` | `server` | Start a server serving one `service` until killed |
| `serve_with_shutdown(addr, service, signal)` | `server` | Serve until the `signal` future resolves |
| `connect(endpoint)` | `client` | Connect (immediate TCP/H2 handshake) → `tonic::transport::Channel` |
| `connect_lazy(endpoint)` | `client` | Connect lazily — no handshake until the first RPC |

### Namespaced modules

| Module | Feature | Re-exports |
|--------|---------|-----------|
| `core` | always | `encoding`, `metadata`, `status`, `timeout` from `oxirpc-core` |
| `client` | `client` | `ClientBuilder`, `ChannelPool`, `TypedChannel`, `RpcMetrics`, `balance`, `resilience` |
| `server` | `server` | `ServerBuilder`, `MethodInterceptorBuilder/Layer/Service`, `IpRateLimiterLayer/Service` |
| `reflect` | `reflect` | All of [`oxirpc-reflect`] (`ReflectionBuilder`, `DescriptorPool`, native services, `proto`, …) |
| `web` | `web` | All of [`oxirpc-web`] (`grpc_web_layer`, `GrpcWebLayer`, codec, cors, native, …) |
| `health` | `health` | `HealthBuilder`, `HealthHandle`, `ServingStatus` |
| `compression` | `compression` | `OxiArcGzip`, `CompressionError` from `oxirpc-core::compression` |
| `tls` | `tls` | Pure-Rust rustls `ClientConfig`/`ServerConfig` helpers (OxiTLS RustCrypto, `h2` ALPN) |
| `aws_lc` | `aws-lc` | `aws_lc_provider` from [`oxirpc-adapter-aws-lc`] — **opt-in, brings in C/FFI** |
| `proto` | `oxiproto` | OxiProto wire traits, `OxiProtoError`, prost compat types (path-dep; not on crates.io) |
| `build` | always | Documentation-only; `oxirpc-build` is a build-dependency, called directly in `build.rs` |
| `interceptors` | always | Ready-to-use interceptor patterns (see below) |

### `interceptors` module

Composable [`tonic::service::Interceptor`] implementations for common cross-cutting concerns. All single-concern interceptors are `Clone`.

| Type | Description |
|------|-------------|
| `BearerAuthInterceptor` | Validates `Authorization: Bearer <token>`; rejects with `Unauthenticated`. `new(token)` |
| `TracingInterceptor` | Attaches a monotonic `x-request-id` to every request (unit struct, `Default`) |
| `DeadlineInterceptor` | Injects a default `grpc-timeout` (millisecond encoding, back-compat) when absent. `new(Duration)` |
| `TimeoutInterceptor` | Injects `grpc-timeout` using the coarsest exact unit (e.g. `5S`) via `format_grpc_timeout`. `new(Duration)` |
| `RateLimitInterceptor` | Thread-safe token-bucket limiter; rejects with `ResourceExhausted`. `new(capacity, refill_per_sec)` |
| `LoggingInterceptor` | Callback-based logger; sink gets `&Request<()>`. `new(sink)`, `noop()` |
| `MetricsInterceptor` | Atomic total + per-path counters; `new()`, `snapshot() -> MetricsSnapshot` |
| `MetricsSnapshot` | `{ total: u64, by_path: HashMap<String, u64> }` |
| `InterceptorChain` | Ordered composition, short-circuiting on first `Err`; `new()`, `push(interceptor)` |

### `prelude`

```rust
use oxirpc::prelude::*;
let _ok = StatusCode::Ok;
```

Imports `Code`, `Metadata`, `OxiRpcError`, `OxiRpcResult`, `Request`, `Response`, `Status`, `StatusCode`, `CompressionEncoding`, `version`, plus `ClientBuilder` (feature `client`) and `ServerBuilder` (feature `server`).

## Feature Flags

| Feature | Enables | Pure Rust |
|---------|---------|-----------|
| `client` | `oxirpc-client`: `ClientBuilder`, channel pool, load balancing, resilience | Yes |
| `server` | `oxirpc-server`: `ServerBuilder`, middleware layers (+ `tower`/`http`/`axum`) | Yes |
| `native` | `server` + the native zero-tonic-transport server path (`oxirpc-server/native`) | Yes |
| `tls` | Pure-Rust TLS via OxiTLS (`oxirpc-core/tls` + `rustls`), no ring | Yes |
| `reflect` | `oxirpc-reflect`: gRPC server reflection (v1 + v1alpha) | Yes |
| `health` | `oxirpc-health`: gRPC health-checking protocol | Yes |
| `web` | `oxirpc-web`: gRPC-Web bridge (HTTP/1.1 → gRPC) + CORS layer | Yes |
| `compression` | OxiARC-backed compress/decompress API (`OxiArcGzip`) | Yes |
| `gzip` | gzip message compression via OxiARC (`oxiarc-deflate`) | Yes |
| `zstd` | Zstandard message compression via OxiARC (`oxiarc-zstd`) | Yes |
| `aws-lc` | `oxirpc-adapter-aws-lc`: aws-lc-rs rustls provider — **opt-in, enables FFI** | **No (C/FFI)** |
| `oxiproto` | OxiProto type-system bridge (`oxirpc::proto`) — path-dep, **not on crates.io** | Yes |
| `full` | All Pure-Rust sub-features: `client`, `server`, `native`, `tls`, `reflect`, `web`, `health`, `compression`, `gzip`, `zstd` (no `aws-lc`, no `oxiproto`) | Yes |

The default feature set is **empty**, keeping the dependency footprint minimal and 100% Pure Rust until you opt in.

## TLS notes

`oxirpc::tls` (feature `tls`) constructs `rustls::ClientConfig` / `rustls::ServerConfig` through the OxiTLS RustCrypto provider — Pure Rust, no ring, no FFI — with `h2` ALPN set automatically. For an aws-lc-rs backend instead, enable `aws-lc` and use `oxirpc::aws_lc::aws_lc_provider()` (this brings in C/FFI via `aws-lc-sys`). Note that full tonic-native TLS round-trip wiring is deferred (tonic 0.14's TLS features pull FFI crates onto normal edges).

## Compression

```rust,no_run
# #[cfg(feature = "compression")]
# {
use oxirpc::compression::OxiArcGzip;

# fn run() -> Result<(), oxirpc::compression::CompressionError> {
let data = b"hello gRPC!";
let compressed = OxiArcGzip::compress(data)?;
let decompressed = OxiArcGzip::decompress(&compressed)?;
assert_eq!(&decompressed, data);
# Ok(())
# }
# }
```

## Cross-References

- [`oxirpc-core`](https://crates.io/crates/oxirpc-core) — native `Status`, `Metadata`, `Codec`, `Timeout`, `Encoding`, and the wire helpers (always pulled in).
- [`oxirpc-client`](https://crates.io/crates/oxirpc-client) — `ClientBuilder`, channel pool, load balancing, resilience (feature `client`).
- [`oxirpc-server`](https://crates.io/crates/oxirpc-server) — `ServerBuilder`, middleware layers, native transport (feature `server` / `native`).
- [`oxirpc-reflect`](https://crates.io/crates/oxirpc-reflect) — gRPC server reflection (feature `reflect`).
- [`oxirpc-health`](https://crates.io/crates/oxirpc-health) — gRPC health checking + K8s probes (feature `health`).
- [`oxirpc-web`](https://crates.io/crates/oxirpc-web) — gRPC-Web bridge + CORS layer (feature `web`).
- [`oxirpc-adapter-aws-lc`](https://crates.io/crates/oxirpc-adapter-aws-lc) — aws-lc-rs rustls provider (feature `aws-lc`; not Pure Rust).
- [`oxirpc-build`](https://crates.io/crates/oxirpc-build) — build-time proto compilation, no `protoc` (a `[build-dependencies]` entry).

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
