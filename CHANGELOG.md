# Changelog

All notable changes to OxiRPC are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-06-10

### Changed

- Upgraded `oxiarc-deflate` from 0.3.2 to 0.3.3 and `oxiarc-zstd` from 0.3.2
  to 0.3.3 to pick up the latest Pure-Rust compression improvements from the
  OxiARC ecosystem.
- All workspace crates bumped to version 0.1.2.

## [0.1.1] - 2026-06-04

### Changed

- `tls` feature in the `oxirpc` facade crate now also activates
  `oxirpc-client?/tls`; previously enabling `tls` on the facade left the
  client crate's TLS support disabled, requiring callers to opt in separately.
- All intra-workspace dev-dependencies that were temporarily commented out for
  the initial publish (`oxirpc-reflect` in `oxirpc-build`, `oxirpc-server` +
  `oxirpc-health` in `oxirpc-client`, `oxirpc-reflect` + `oxirpc-web` in
  `oxirpc-health`) have been restored so the full integration test suite runs
  against the published crates.
- All workspace crates bumped to version 0.1.1.

### Fixed

- Race condition in `oxirpc-build` integration tests: concurrent tests that
  set the `OUT_DIR` environment variable could corrupt each other's build cache
  paths. A process-wide `OUT_DIR_LOCK` mutex now serialises all tests that
  read or write `OUT_DIR`.

## [0.1.0] — 2026-06-01

### Summary

First functional release. OxiRPC is a Pure-Rust gRPC stack built on top of
tonic 0.14 that eliminates protoc, openssl-sys, ring, and aws-lc-sys from the
default feature closure. 758 tests pass across 9 crates; clippy clean with
`-D warnings`; no `unsafe` in production code; no `unwrap()` in production
paths.

### Crates

- **oxirpc-core 0.1.0** — Core types: `OxiRpcError`, `StatusCode` (17 codes),
  `Metadata` (ascii + `-bin`/base64), `Timeout` parse/format, `CompressionEncoding`
  (Identity/Gzip/Zstd via OxiARC), HTTP/2 gRPC wire layer (`FrameEncoder`,
  `FrameDecoder`, header/trailer codec with percent-encoding, `MessagePipeline`,
  `Deadline`, timeout codec, 35 unit + 5 integration tests), TLS helpers via
  OxiTLS (`PureRustTlsConnector`, client/server config builders with h2 ALPN).

- **oxirpc-build 0.1.0** — Build-time proto compiler. Uses `oxiproto-build`
  (which delegates to `protox`) to compile `.proto` files to file descriptor
  sets without spawning `protoc`. Supports `compile_to_fds`, `file_descriptor_set_path`,
  `include_file`, module attributes, `btree_map`/`bytes` overrides, and
  well-known types. Optional `legacy-tonic-codegen` feature for the old
  `tonic-prost-build` backend.

- **oxirpc-client 0.1.0** — gRPC client builder: timeout, user-agent, origin,
  HTTP/2 flow-control windows, TCP tuning. Cloneable config. Channel pool,
  load balancing (round-robin, weighted, pick-first), resilience (retry,
  circuit breaker, hedging), xDS/ADS streaming client for service-mesh
  integration.

- **oxirpc-server 0.1.0** — gRPC server builder: concurrency limits, stream
  limits, connection timeout, TCP tuning, HTTP/2 keepalive. Bound-listener
  serving, graceful shutdown, native `NativeServiceRegistry` with HTTP/2
  framing, Pure-Rust TLS via OxiTLS, UNIX domain socket support.

- **oxirpc-reflect 0.1.0** — gRPC server reflection v1 + v1alpha. Static
  file descriptor set registration and optional `oxiproto-reflect`
  `DescriptorPool` backend (`oxiproto` feature).

- **oxirpc-web 0.1.0** — gRPC-Web bridge. Native frame codec (5-byte framing,
  trailer frames, base64 text mode, compression-aware encode/decode via OxiARC).
  `CorsPolicy` builder with wildcard, credential, and per-origin allow lists.
  `GrpcWebConfig` with path prefix routing.

- **oxirpc-health 0.1.0** — gRPC health checking protocol v1 (`Check` +
  `Watch`). Local status mirror: `get_status`, `list_services`,
  `set_all_serving` / `set_all_not_serving`, bulk operations. Compression
  negotiation for unary and streaming.

- **oxirpc-adapter-aws-lc 0.1.0** — Optional `aws-lc-rs`-backed rustls
  `CryptoProvider` adapter. Gated behind `aws-lc` feature; default features
  are 100% Pure Rust.

- **oxirpc 0.1.0** — Facade crate. Re-exports all sub-crates under a single
  dependency. `full` feature enables all Pure-Rust sub-features. Includes
  interceptor library (auth, tracing, deadline, rate-limiting, metrics, retry,
  circuit breaker), `prelude`, `version()`.

### FFI closure (default features)

`cargo tree -p oxirpc --edges normal` contains zero occurrences of:
`protoc`, `openssl`, `openssl-sys`, `ring`, `aws-lc-sys`, `native-tls`,
`flate2`, `zstd` (C crate).

### Notes

- Publishing requires `oxiproto >= 0.1.0` and `oxitls >= 0.1.0` on crates.io.
  Publish oxiproto and oxitls first, then oxirpc-core, then the remaining
  crates in the order listed in `pub_oxirpc.sh`.
- The `oxiproto` feature on oxirpc-core / oxirpc-reflect / oxirpc is gated and
  safe to ignore for plain gRPC usage.
- HTTP/3 support is deferred to OxiQuic.

[0.1.2]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.2
[0.1.1]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.1
[0.1.0]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.0

