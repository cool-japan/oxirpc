# oxirpc-web — gRPC-Web bridge for OxiRPC (browser / HTTP1 support)

[![Crates.io](https://img.shields.io/crates/v/oxirpc-web.svg)](https://crates.io/crates/oxirpc-web)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-web` brings **gRPC-Web** to the OxiRPC stack — the protocol that lets browser clients call gRPC services over HTTP/1.1. Browsers cannot speak native gRPC (which requires HTTP/2 trailers and raw frame access), so gRPC-Web reframes each message with a 5-byte length prefix, embeds the trailing `grpc-status`/`grpc-message` metadata as an in-body trailer frame, optionally base64-encodes the whole body (text mode), and answers CORS preflights. This crate translates between that wire format and the native gRPC format tonic services expect.

Like the rest of OxiRPC, the crate ships **two paths**. The simple path wraps [`tonic_web`]'s [`GrpcWebLayer`] as an OxiRPC-flavored Tower layer ([`grpc_web_layer`] and CORS/prefix-composed variants). The **native** path is a fully Pure-Rust, dependency-light implementation — frame [`codec`], [`cors`] policy, content-type [`negotiate`]-iation, request/response [`translate`]-ion, a path-stripping [`prefix`] layer, the [`native`] Tower layer, and a stand-alone [`transport`] server that serves HTTP/1.1 (browsers) and HTTP/2 (native gRPC) on one port via hyper auto-negotiation. The whole crate is `#![forbid(unsafe_code)]`; compression, when enabled, routes through OxiARC (no flate2/zstd C deps).

## Installation

```toml
[dependencies]
oxirpc-web = "0.1.0"
```

With message compression enabled (OxiARC-backed):

```toml
[dependencies]
oxirpc-web = { version = "0.1.0", features = ["gzip", "zstd"] }
```

## HTTP/1.1 requirement

Every gRPC-Web layer requires the server to accept HTTP/1.1 — browser gRPC-Web requests arrive over HTTP/1.1 by default. With a tonic server that means calling `.accept_http1(true)`; the native [`GrpcWebServer`] handles both protocols automatically.

## Quick Start

### tonic-web layer

```rust,no_run
use oxirpc_web::grpc_web_layer;

let layer = grpc_web_layer();
// tonic::transport::Server::builder()
//     .accept_http1(true)
//     .layer(layer)
//     .add_service(my_service)
//     .serve(addr)
//     .await?;
```

### Layer with CORS

```rust,no_run
use oxirpc_web::{grpc_web_layer_with_cors, cors::CorsPolicy};

let layer = grpc_web_layer_with_cors(CorsPolicy::new().allow_any_origin());
// CORS is outermost: OPTIONS preflights are answered before gRPC-Web translation.
```

### Native stand-alone server (HTTP/1.1 + HTTP/2 on one port)

```rust,no_run
use std::net::SocketAddr;
use oxirpc_web::transport::GrpcWebServer;
use oxirpc_web::cors::CorsPolicy;

async fn run<S>(my_service: S) -> Result<(), oxirpc_core::OxiRpcError>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
            Future: Send + 'static,
        > + Clone + Send + 'static,
{
    let addr: SocketAddr = "0.0.0.0:50051".parse().expect("addr");
    GrpcWebServer::new(my_service)
        .cors(CorsPolicy::new().allow_any_origin())
        .serve(addr)
        .await
}
```

## API Overview

### Top-level layer constructors

| Function | Returns | Description |
|----------|---------|-------------|
| `grpc_web_layer()` | `GrpcWebLayer` | The bare tonic-web gRPC-Web translation layer |
| `grpc_web_layer_with_cors(CorsPolicy)` | `Stack<CorsLayer, GrpcWebLayer>` | CORS (outermost) + gRPC-Web translation |
| `grpc_web_layer_with_config(GrpcWebConfig, Option<CorsPolicy>)` | `Stack<CorsLayer, GrpcWebLayer>` | Config-driven; `None` CORS defaults to allow-any-origin |
| `grpc_web_layer_with_prefix(impl Into<String>)` | `Stack<GrpcWebLayer, GrpcWebPrefixLayer>` | Strip a URI path prefix, then translate (sub-path mounting) |

### Re-exports at the crate root

| Item | Source | Description |
|------|--------|-------------|
| `GrpcWebLayer`, `GrpcWebService` | `tonic_web` | The upstream tonic-web layer/service |
| `GrpcWebPrefixLayer`, `GrpcWebPrefixService` | [`prefix`] | Path-prefix stripping layer/service |
| `GrpcWebConfig`, `WebMode`, `StreamSequencer` | [`negotiate`] | Config bundle, transfer-mode enum, incremental decoder |
| `negotiate`, `response_content_type` | [`negotiate`] | Content-type negotiation helpers |
| `metadata_to_wire_headers`, `wire_headers_to_metadata` | [`negotiate`] | `Metadata` ↔ header-pair translation |
| `NativeGrpcWebLayer`, `NativeGrpcWebService` | [`native`] | Pure-Rust translation Tower layer/service |
| `native_grpc_web`, `native_grpc_web_layer` | [`native`] | Native layer constructors |
| `GrpcWebContentType` | [`translate`] | gRPC-Web content-type detection |
| `GrpcWebServer` | [`transport`] | Stand-alone HTTP/1.1 + HTTP/2 server |

### `codec` module — frame encode/decode

gRPC-Web framing: 1-byte flag + 4-byte big-endian length + payload; the `0x80` flag bit marks a trailer frame, `0x01` marks a compressed payload.

| Item | Description |
|------|-------------|
| `FLAG_TRAILER: u8` (`0x80`) | Frame-flag bit for trailer frames |
| `FLAG_COMPRESSED: u8` (`0x01`) | Frame-flag bit for compressed payloads |
| `FrameKind` | `Data` or `Trailer` (`Copy + Eq`) |
| `Frame` | Decoded frame: `kind`, `compressed`, `payload`; constructors `Frame::data(..)`, `Frame::trailers(&[(&str,&str)])`; `parse_trailers(&self)` |
| `encode_frame(&Frame, CompressionEncoding) -> Result<Vec<u8>, FrameError>` | Encode one frame, optionally compressing data payloads |
| `encode_body(&[Frame], CompressionEncoding) -> Result<Vec<u8>, FrameError>` | Encode a frame sequence into one binary body |
| `encode_text_body(&[Frame], CompressionEncoding) -> Result<String, FrameError>` | Encode into a base64 text-mode body |
| `decode_body(&[u8], CompressionEncoding) -> Result<Vec<Frame>, FrameError>` | Decode a binary body into frames (decompresses when requested) |
| `decode_text_body(&str, CompressionEncoding) -> Result<Vec<Frame>, FrameError>` | Decode a base64 text-mode body into frames |
| `FrameError` | `Truncated`, `LengthOverflow`, `InvalidBase64`, `Codec(String)` |

### `cors` module — CORS policy + layer

| Item | Description |
|------|-------------|
| `CorsPolicy` | Configurable CORS policy (`Debug + Clone`) |
| `CorsPolicy::new()` | Allows the standard gRPC-Web method/headers but **no** origins until added |
| `allow_any_origin()` / `allow_origin(..)` | Permit any origin (`*`) or an explicit origin |
| `allow_methods(..)` / `allow_method(..)` / `allow_header(..)` / `expose_header(..)` | Tune allowed methods, request headers, exposed response headers |
| `allow_credentials(bool)` / `max_age_secs(u64)` | Credentials toggle and preflight cache lifetime |
| `is_origin_allowed(&self, origin) -> bool` | Whether an origin is permitted |
| `preflight_headers(&self, origin)` / `response_headers(&self, origin)` | Header sets for an `OPTIONS` preflight / an actual response |
| `CorsLayer` / `CorsService<S>` | Tower layer/service: answers `OPTIONS` with `204` + CORS headers, injects headers on responses |

### `negotiate` module — content-type, headers, sequencing

| Item | Description |
|------|-------------|
| `WebMode` | `Binary` or `Text` (base64) transfer mode (`Copy + Eq`) |
| `negotiate(content_type) -> Option<WebMode>` | Detect the gRPC-Web mode from a `Content-Type` value |
| `response_content_type(WebMode) -> &'static str` | The response content-type for a mode |
| `GrpcWebConfig` | Session config: `mode`, `compression`, `max_message_bytes` (default 4 MiB); builder methods `new`, `with_compression`, `with_max_message_bytes` |
| `metadata_to_wire_headers(&Metadata) -> Vec<(String, String)>` | `Metadata` → header pairs (base64-encodes `-bin` keys) |
| `wire_headers_to_metadata(&[(String, String)]) -> Result<Metadata, MetadataError>` | Header pairs → `Metadata` (base64-decodes `-bin` keys) |
| `StreamSequencer` | Stateful incremental decoder reassembling frames from arbitrarily-split chunks; `new`, `with_max_message_bytes`, `push(&mut self, &[u8]) -> Result<Vec<Frame>, SequencerError>` |
| `SequencerError` | `MessageTooLarge { length, max }` |

### `translate` module — request/response translation

| Item | Description |
|------|-------------|
| `GrpcWebContentType` | `Binary` or `Text`; `from_header(&str)`, `grpc_content_type()`, `response_content_type()` |
| `translate_request(Request<B>, GrpcWebContentType) -> Result<Request<TonicBody>, FrameError>` | gRPC-Web → native gRPC: collects body, base64-decodes text mode, strips CORS-unsafe headers, sets `content-type`/`te: trailers` |
| `translate_response(Response<B>, GrpcWebContentType) -> Response<TonicBody>` | gRPC → gRPC-Web: collects body + HTTP trailers, appends a trailer frame, optionally base64-encodes (text mode) |

### `prefix` module — sub-path mounting

| Item | Description |
|------|-------------|
| `GrpcWebPrefixLayer` | Tower layer that strips a path prefix from incoming requests; `new(prefix)`, `prefix(&self)` |
| `GrpcWebPrefixService<S>` | The produced service; boundary-safe match (`/api` strips `/api/foo`, not `/apifoo`) |

### `native` module — Pure-Rust translation layer

| Item | Description |
|------|-------------|
| `NativeGrpcWebLayer` | Tower layer translating gRPC-Web ↔ gRPC, passing through non-gRPC-Web and `OPTIONS` requests; `Default`, `new()` |
| `NativeGrpcWebService<S>` | The produced service; `new(inner)` |
| `native_grpc_web_layer()` | Construct a `NativeGrpcWebLayer` |
| `native_grpc_web(inner)` | Wrap a service directly: `NativeGrpcWebLayer::new().layer(inner)` |

### `transport` module — stand-alone server

| Item | Description |
|------|-------------|
| `GrpcWebServer<S>` | Native TCP server serving gRPC-Web over HTTP/1.1 **and** HTTP/2 (hyper auto-negotiation). Layering: `CorsLayer (optional) → NativeGrpcWebLayer → S` |
| `GrpcWebServer::new(S)` | Wrap a service (no CORS by default) |
| `cors(CorsPolicy)` / `config(GrpcWebConfig)` | Attach a CORS policy / override negotiation config |
| `serve(addr)` | Bind and serve until the process is killed |
| `serve_with_shutdown(addr, shutdown)` | Bind and serve until the `shutdown` future resolves |

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `gzip` | off | gzip message compression via `oxirpc-core/gzip` (OxiARC DEFLATE) |
| `zstd` | off | Zstandard message compression via `oxirpc-core/zstd` (OxiARC) |
| `compression` | off | Enable the full OxiARC compression API via `oxirpc-core/compression` |

All features are off by default and keep the crate 100% Pure Rust.

## Error Types

| Type | Variants | Notes |
|------|----------|-------|
| `codec::FrameError` | `Truncated`, `LengthOverflow`, `InvalidBase64`, `Codec(String)` | Frame encode/decode errors; implements `Error + Display` |
| `negotiate::SequencerError` | `MessageTooLarge { length, max }` | Raised when a streamed frame exceeds the configured size cap; implements `Error + Display` |
| `negotiate::MetadataError` | (from `oxirpc-core`) | Returned by `wire_headers_to_metadata` for illegal keys/values or bad base64 |

Transport binding failures surface as `oxirpc_core::OxiRpcError::Transport`.

## Cross-References

- [`oxirpc`](https://crates.io/crates/oxirpc) — the facade; re-exports this crate under `oxirpc::web` via the `web` feature.
- [`oxirpc-core`](https://crates.io/crates/oxirpc-core) — `Metadata`, `encoding` (OxiARC compression), `OxiRpcError`, and base64 helpers used throughout.
- [`oxirpc-server`](https://crates.io/crates/oxirpc-server) — mount these layers on a tonic-style server (remember `accept_http1(true)`).
- [`oxirpc-reflect`](https://crates.io/crates/oxirpc-reflect) — gRPC server reflection; pairs well with a gRPC-Web endpoint for browser tooling.
- [`oxirpc-health`](https://crates.io/crates/oxirpc-health) — gRPC health-checking service.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
