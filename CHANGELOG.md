# Changelog

All notable changes to OxiRPC are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-22

### Removed

- **`aws-lc` feature removed from the `oxirpc` facade** (`oxirpc`): the `aws-lc`
  feature flag and its re-export module `oxirpc::aws_lc` have been removed from the
  facade crate. The `oxirpc-adapter-aws-lc` sub-crate remains in the workspace for
  downstream consumers that explicitly need aws-lc-rs, but it is no longer reachable
  via the facade's feature graph. This eliminates the last path by which enabling a
  facade feature could pull C/FFI code through a normal dependency edge.
- **`oxirpc-adapter-aws-lc` optional dependency removed from `oxirpc`**: the
  `oxirpc-adapter-aws-lc` entry has been removed from `[dependencies]` in
  `crates/oxirpc/Cargo.toml`. The adapter crate continues to exist as a standalone
  workspace member but is no longer transitively reachable from the facade.
- **`pkcs11` feature path closed**: because the `aws-lc` gate in the facade was the
  only supported path for hardware-token (PKCS#11) crypto in prior releases, its
  removal closes that surface area at the facade level. Native HSM support remains
  deferred to a dedicated adapter crate outside this workspace.

### Changed

- **Pure Rust Policy v2 L1 compliance** (`oxirpc`): the facade now passes the
  COOLJAPAN `--all-features` FFI audit. Running `cargo tree -p oxirpc --edges normal
  --all-features` produces zero occurrences of `aws-lc-sys`, `aws-lc-rs`, `ring`,
  `openssl-sys`, `native-tls`, `pkcs11-sys`. Every feature reachable from `oxirpc`
  with `--all-features` is 100% Pure Rust.
- **`full` feature comment updated** (`oxirpc`): the doc comment on `full` now reads
  "All Pure-Rust sub-features (no FFI)" (previously "no aws-lc / no FFI"), reflecting
  that the aws-lc gate has been removed rather than merely excluded.
- **`oxitls` upgraded to 0.2.0**: workspace dependency updated from 0.1.x to 0.2.0
  to align with the latest OxiTLS Pure Rust TLS release.
- All workspace crates bumped to version 0.2.0.

### Security

- Removing the `aws-lc` facade feature eliminates a class of supply-chain risk:
  users enabling `full` (or any named feature set) on the facade could previously
  unknowingly pull in C-compiled AWS-LC binaries. With 0.2.0 that path no longer
  exists; opting in to FFI crypto requires an explicit direct dependency on
  `oxirpc-adapter-aws-lc`.

## [0.1.3] - 2026-06-19

### Added

- **`AsyncInterceptor` blanket impl for closures** (`oxirpc-core`): any `Fn(Request<()>) ->
  impl Future<Output = Result<Request<()>, Status>>` can now be used as an `AsyncInterceptor`
  directly, without a manual struct implementation.
- **Client-side async interceptor** (`oxirpc-client`): `NativeChannelBuilder::with_async_interceptor`
  wires an `Arc<dyn AsyncInterceptor>` into every outgoing unary call. The interceptor runs just
  before the H2 stream is opened; it may inject or mutate request metadata, or abort the call with
  a `Status` error that surfaces as `OxiRpcError::Status`.
- **Server-side async interceptor** (`oxirpc-server`): `NativeServiceRegistry::with_async_interceptor`
  wires an `Arc<dyn AsyncInterceptor>` into the dispatch path. The interceptor runs before the
  matched service handler; metadata mutations are merged back into the request headers, and a
  returned `Status` short-circuits the call with a gRPC error response (grpc-status trailer).
- **Hash-keyed build-cache filenames** (`oxirpc-build`): the incremental FDS cache is now stored
  as `$OUT_DIR/.oxirpc-cache/fds-<hash>.bin` where `<hash>` is a stable hash of the sorted proto
  file paths. Multiple independent `compile_to_fds` calls sharing the same `$OUT_DIR` (e.g. two
  proto sets in one build script) no longer collide.
- **gRPC interop conformance fixture** (`oxirpc`): a new `tests/proto/grpc_testing.proto` and
  `tests/conformance.rs` implement the upstream `grpc.testing.TestService` (empty call, unary
  call, server/client/full-duplex streaming) against the tonic HTTP/2 transport. When the
  `GRPC_GO_INTEROP_CLIENT` env var points to the grpc-go interop binary the conformance suite
  drives it; otherwise tests are skipped so CI without the Go toolchain stays green.

### Changed

- `oxirpc_core::interceptor::AsyncInterceptor`: doc comment updated to reflect that the trait is
  now wired into both the native client channel and the native server registry (previously marked
  "reserved stub for future use").
- `ChannelConfig` (`oxirpc-client`): the `#[derive(Debug)]` macro is replaced by a manual
  `fmt::Debug` impl so the `Arc<dyn AsyncInterceptor>` field (not `Debug`) no longer blocks
  derivation; the debug output shows `async_interceptor_set: bool` instead.
- All workspace crates bumped to version 0.1.3.

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

[0.2.0]: https://github.com/cool-japan/oxirpc/releases/tag/v0.2.0
[0.1.3]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.3
[0.1.2]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.2
[0.1.1]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.1
[0.1.0]: https://github.com/cool-japan/oxirpc/releases/tag/v0.1.0

