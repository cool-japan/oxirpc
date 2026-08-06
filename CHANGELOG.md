# Changelog

All notable changes to OxiRPC are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - Unreleased

## [0.2.1] - 2026-08-07

### Added

- **HTTP/3 (gRPC-over-QUIC) support** behind the new opt-in `http3` feature —
  100% Pure Rust via [OxiQUIC](https://github.com/cool-japan/oxiquic)
  (`oxiquic-h3`, `oxiquic-transport`, `oxiquic-crypto`) and the hyperium `h3`
  crate. The default feature closure remains QUIC-free (verified with
  `cargo tree -p oxirpc --edges normal | grep oxiquic` → empty):
  - `oxirpc-core`: `tls::{client_config_h3, server_config_h3, client_config_h3_arc,
    server_config_h3_arc}` — TLS 1.3-only rustls configs with the `"h3"` ALPN,
    built on `oxiquic_crypto::quic_crypto_provider()` (whose cipher suites carry
    the `quic: Some(..)` key schedule required to derive QUIC packet keys).
  - `oxirpc-client`: `native_channel::h3::{H3Channel, H3ChannelBuilder, H3Connection,
    execute_h3}` — a pure-native gRPC-over-QUIC client channel. Full-duplex via
    `RequestStream::split()`, strips the HTTP/2-only `te` header, detects
    trailers-only responses, and maps `grpc-status` trailers to typed errors.
  - `oxirpc-server`: `native_transport_h3::{serve_native_h3_with_service,
    bind_h3_endpoint}` plus `ServerBuilder::{serve_native_registry_h3,
    serve_native_registry_h3_with_endpoint}` — a QUIC accept loop that reuses the
    `NativeServiceRegistry`, sends `grpc-status`/`grpc-message` response trailers,
    enforces the `"h3"` ALPN, and supports graceful shutdown.
  - `oxirpc` facade: new `http3` module re-exporting the above, `http3` added to
    the `full` feature set, and a real loopback E2E suite (`tests/h3_e2e.rs`)
    covering unary, server-streaming, client-streaming/bidi, deadline/timeout,
    graceful shutdown, and ALPN-mismatch rejection.
- **`SECURITY.md`** and **`CONTRIBUTING.md`** added to the repository root.
- **gRPC `Content-Type` validation** on the native transport paths
  (`oxirpc-client/src/native_channel/content_type.rs`, new): the native H2
  client, the native H3 client, and the native server dispatch path
  (`oxirpc-server/src/native_registry/dispatch.rs`) now reject requests/responses
  whose `content-type` is not a gRPC variant (`application/grpc`,
  `application/grpc+proto`, `application/grpc+json`) instead of feeding
  arbitrary bytes (e.g. a reverse-proxy HTML error page) into the gRPC frame
  decoder. The server responds `415 Unsupported Media Type`; the client
  surfaces a clear `OxiRpcError::Transport` naming the actual content-type.
- `rustfmt.toml` and `clippy.toml` added at the workspace root (`clippy.toml`
  pins `msrv = "1.89"` to match `workspace.package.rust-version`).
- `deny.toml` extended with the full COOLJAPAN Pure-Rust replacement ban list
  (`bincode`→`oxicode`, `rustfft`→`oxifft`, `quick-xml`→`oxixml-*`,
  `zip`/`zstd`/`bzip2`/`lz4`/`tar`/`snap`/`brotli`/`miniz_oxide`→`oxiarc-*`,
  `openblas-src`→`oxiblas`, `rusqlite`→`oxisql-sqlite-compat`) plus
  `[advisories]`, `[licenses]`, and `[sources]` sections; all four `cargo deny
  check` categories pass.
- New examples under `crates/oxirpc/examples/`: a native H2 client+server
  round trip (`client_server_native.rs`) and, behind `http3`, a client+server
  round trip driven entirely through the `oxirpc::http3` facade module
  (`http3_client_server.rs`) — the README's HTTP/3 snippet is now backed by a
  compiled, runnable example rather than only the lower-level `h3_e2e.rs`
  test suite.
- New fuzz targets under `fuzz/fuzz_targets/`: `grpc_web_frame` (the
  gRPC-Web `StreamSequencer` length-prefixed frame parser) and
  `trailer_and_metadata` (gRPC status-trailer percent-decoding and
  `-bin`/base64 metadata decoding), alongside the existing `wire_frame` target.
  `fuzz/Cargo.toml` also gained the `[package.metadata] cargo-fuzz = true`
  marker required by current `cargo-fuzz` to recognise the crate at all; all
  three targets were smoke-run for 20k iterations each with no crash.

### Known issues

- **Suspected `H3Channel`/`H3Connection` staleness after an idle gap**: an
  `h3-vs-h2` benchmark was attempted (`crates/oxirpc/benches/`) and dropped
  after reproducing, deterministically, `channel.ready().await` succeeding
  and a *later*, separate `channel.call(..)` on the same `H3Channel` failing
  with `Transport("h3 stream: failed to open bidi stream: connection error:
  driven connection driver closed before stream was opened")` — even with
  `TransportConfig::idle_timeout` raised to 300s, ruling out QUIC idle
  timeout as the cause. Every `tests/h3_e2e.rs` case chains connection
  establishment directly into its first stream-open (no separate `.ready()`
  call, no scheduling gap in between), which may explain why the E2E suite
  has never caught this — but that connection is circumstantial, not
  confirmed. Not investigated further (out of scope for this hygiene pass);
  flagged here for the functional-bug backlog. The originally-suspected lead
  has since been **ruled out**: `H3Connection::connect`
  (`oxirpc-client/src/native_channel/h3/connection.rs`) does not store its
  local `ClientEndpoint` in the returned `Self`, but `ClientEndpoint` holds
  its UDP socket as `Arc<UdpSocket>` (`oxiquic-transport`'s
  `endpoint::mod.rs`) and `connect()` clones that `Arc` into the spawned
  `ConnectionDriver` — so the driver task keeps the socket alive independently
  of the endpoint, and dropping the endpoint after `connect()` returns cannot
  be the cause. Root cause remains open — it has not been localized to either
  `oxirpc`'s own `H3Channel` connection-caching logic
  (`native_channel/h3/channel.rs`) or to `oxiquic` itself; if it does turn out
  to live in `oxiquic`, a fix there is out of scope for this repo.

### Changed

- **`Cargo.lock` re-resolved** against the current manifests (no manifest
  version pins changed): the lockfile previously predated the "bump oxiproto"
  / "bump oxiarc" commits and resolved versions (e.g. `oxiproto` 0.1.3,
  `oxiarc-deflate` 0.3.6, `oxitls`/`oxitls-rcgen` 0.2.0) that no longer
  satisfied the manifests' own requirements. `crossbeam-epoch` and `spin`
  were additionally updated to their latest compatible versions to clear
  RUSTSEC-2026-0204 and a yanked-version warning respectively (both dev-only,
  transitive via `criterion`/`protox`).
- **Sibling COOLJAPAN manifest dependencies bumped** to their latest published
  releases (net change since the `0.2.0` release tag):
  - `oxiproto` / `oxiproto-reflect` / `oxiproto-build` / `oxiproto-core`: `0.1.3` →
    `0.1.5` (via an intermediate `0.1.4` hop).
  - `oxiarc-deflate` / `oxiarc-zstd`: `0.3.3` → `0.4.1` (via `0.3.4`, `0.3.5`, `0.3.6`,
    `0.4.0`).
  - `oxitls`: `0.2.0` → `0.3.0` (via an intermediate `0.2.1` hop, landed alongside the
    `http3` feature work below). `oxitls-rcgen` (dev-only, self-signed cert generation
    for H3 tests/examples) was introduced at `0.2.1` and bumped the same way to `0.3.0`.
    Informational: the previously-published `oxitls 0.2.0` floor resolved a
    `rustls-webpki` 0.102.x edge affected by **RUSTSEC-2026-0104** (a CRL-parsing panic),
    fixed upstream at `oxitls 0.2.1` (see oxitls's own `CHANGELOG.md` `[0.2.1]`); this
    bump past that floor to `^0.3.0` clears the advisory path for `oxirpc` once this
    release publishes.
  - `oxiquic-h3` / `oxiquic-transport` / `oxiquic-crypto`: introduced at `0.2.0` (new,
    for the `http3` feature — see Added above) and bumped to `0.2.1`.

### Fixed

- **Missing `grpc-status` trailer no longer looks like a successful empty
  stream** (`oxirpc-client/src/native_channel/call.rs`,
  `native_channel/h3/call.rs`): `pump_response` now takes the initial HTTP
  response status and, if the stream ends without ever sending a
  `grpc-status` trailer, surfaces `OxiRpcError::from_status_code(StatusCode::Unknown,
  ..)` (naming the initial HTTP status when it was itself non-2xx) instead of
  closing the body channel as if the RPC had completed successfully. Fixed
  identically on both the native H2 and native H3 response pumps.
- **`decode_grpc_message` / `decode_grpc_message_with_encoding` frame-size
  overflow guard** (`oxirpc-core/src/wire/server.rs`): an attacker-controlled
  32-bit length prefix is now bounds-checked against `MAX_FRAME_SIZE_DEFAULT`
  and added via `checked_add` *before* any arithmetic on it, closing a
  usize-overflow / out-of-bounds-slice panic that was reachable on 32-bit and
  `wasm32` targets.
- **Trailer-read transport errors were coerced to "no trailers"**
  (`native_channel/call.rs`, `native_channel/h3/call.rs`): a genuine h2/h3
  error while reading the trailers block (e.g. `RST_STREAM` mid-response) is
  now mapped through `h2_error_to_oxirpc` / `h3_stream_error_to_oxirpc` and
  surfaced as the real transport error, rather than being flattened by
  `.unwrap_or(None)` into the generic "stream ended without a grpc-status
  trailer" message used for a clean close with no trailers frame at all. The
  two cases were previously byte-identical to callers.

### Removed

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

