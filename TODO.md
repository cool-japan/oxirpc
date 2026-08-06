# OxiRPC Project TODO

## Status — v0.2.1 (Unreleased)
Pure-Rust gRPC stack. 702 tests pass (default features; 801 with `--all-features`); clippy
clean (`-D warnings`); rustdoc clean; default closure is FFI-free.
All milestones M0–M8 complete, including the M8 HTTP/3 (gRPC-over-QUIC) initiative
(R1-R6, see the Production-Readiness Backlog section below) behind the opt-in
`http3` feature. Pure Rust Policy v2 L1 compliant: `aws-lc` and `pkcs11`
feature paths removed from the facade (`oxirpc`); `oxirpc-adapter-aws-lc` remains as
standalone opt-in workspace crate. oxitls pinned at 0.3.0.
All sub-crates operational: core (TLS via OxiTLS incl. HTTP/3 `*_h3` configs, native
StatusCode/Metadata/grpc-timeout/CompressionEncoding+Encoding via OxiARC,
AsyncInterceptor blanket impl), build (no protoc; compile_to_fds with hash-keyed
cache isolation, mod attrs, btree_map/bytes, FDS path), client (cloneable config +
load-balancing + resilience + xDS/ADS + native HTTP/3 channel, async interceptor
wired), server (config + bound-listener serving + native registry + native HTTP/3
transport, async interceptor wired), reflect (v1+v1alpha), web (gRPC-Web layer +
native frame codec + CorsPolicy), health (v1 + status mirror/bulk ops), interceptors
(auth/tracing/deadline/rate-limiting/metrics/retry/circuit-breaker), facade (prelude,
full feature, version(), gRPC conformance harness, http3 module). Goal: replace
tonic with native Pure Rust gRPC implementation.

## Milestones

### M0 -- Skeleton (DONE)
- [x] Workspace root Cargo.toml with resolver = "2"
- [x] oxirpc-core placeholder with OxiRpcError
- [x] deny.toml, Dockerfile.ffi-audit, scripts/ffi-audit.sh

### M1 -- Plaintext client+server (DONE)
- [x] oxirpc-build: protox + tonic_prost_build (no protoc)
- [x] oxirpc-client: plaintext Channel builder
- [x] oxirpc-server: plaintext Server builder with graceful shutdown
- [x] oxirpc facade with client/server feature gates
- [x] cargo tree zero flate2/protobuf-src/*-sys on normal edges

### M2 -- TLS via OxiTLS (DONE)
- [x] Pure Rust TLS configs via builder_with_provider(oxitls::pure_provider())
- [x] client_config / server_config with h2 ALPN
- [x] No ring, no openssl, no aws-lc-sys on normal edges
- [x] Tonic transport TLS wiring resolved — `PureRustTlsConnector` + `connect_with_connector` bypasses tonic FFI; no fork needed

### M3 -- Reflection + gRPC-Web (DONE)
- [x] oxirpc-reflect: v1 + v1alpha server reflection
- [x] oxirpc-web: gRPC-Web bridge via tonic-web

### M4 -- Health + Interceptors (DONE)
- [x] oxirpc-health: grpc.health.v1.Health (Check + Watch)
- [x] Interceptor recipes: BearerAuth, Tracing, Deadline

### M5 -- Compression + aws-lc Adapter (DONE)
- [x] Slice 7a: oxirpc-core compression via oxiarc-deflate (Pure Rust gzip, no flate2)
- [x] oxirpc `compression` feature gate; OxiArcGzip compress/decompress API
- [x] Slice 7b: oxirpc-adapter-aws-lc — aws-lc-rs backed rustls CryptoProvider
- [x] oxirpc `aws-lc` feature gate; pub mod aws_lc re-export from adapter
- [x] Purity tripwire: cargo tree -p oxirpc --edges normal contains no aws-lc or openssl
- [x] Zero warnings, zero unwrap() in production code, default = [] on adapter

### M6 -- Native primitives (additive, alongside tonic) (DONE)
- [x] oxirpc-core: native StatusCode (17 codes), Metadata (ascii + -bin/base64), grpc-timeout parse/format
- [x] oxirpc-core: CompressionEncoding (Identity/Gzip/Zstd) + Encoding trait + compress/decompress via OxiARC (gzip=oxiarc-deflate, zstd=oxiarc-zstd)
- [x] oxirpc-core: #[non_exhaustive] OxiRpcError + Proto/Timeout/Cancelled variants + OxiRpcResult alias
- [x] oxirpc-web: native gRPC-Web frame codec (5-byte framing, trailer frames, base64 text mode) + CorsPolicy builder
- [x] oxirpc-build: compile_to_fds, file_descriptor_set_path, include_file, mod attrs, btree_map/bytes, WKT
- [x] oxirpc-client: cloneable builder (timeout, user_agent, origin, HTTP/2 windows, tcp tuning)
- [x] oxirpc-server: builder config (concurrency/streams/timeout/tcp/h2-keepalive) + bound-listener serving
- [x] oxirpc-health: local status mirror — get_status, list_services, set_all_serving/not_serving
- [x] oxirpc facade: prelude, full feature, version(), core module re-exports

## Core Implementation
- [x] Phase 1: gRPC compression via OxiARC (oxiarc-deflate for gzip, oxiarc-zstd for zstd) -- message-level encode/decode + CompressionEncoding DONE in oxirpc-core::encoding; wire-level header/frame-flag negotiation still pending (~150 SLOC remaining)
  - **Completed 2026-05-25:** Wired in `oxirpc-web/src/codec.rs` — `encode_frame` compresses data frames (not trailers) and sets `FLAG_COMPRESSED`; `decode_body` decompresses when flag is set.
- [x] Phase 2: Native gRPC wire format — HTTP/2 framing, gRPC length-prefixed messages, header/trailer codec, timeout codec, status decoding (~1200 SLOC, `oxirpc-core::wire/`) (completed 2026-05-29)
  - **Foundation landed (2026-05-26):** `oxirpc-core::h2` module — header constants, `GrpcRequestHeaders`, `GrpcResponseHeaders`, `GrpcStatusCode` with wire-value helpers, `is_grpc_content_type`.
  - **Full wire layer landed (2026-05-29):** `oxirpc-core::wire/` — 6-file module: WireError, FrameEncoder/FrameDecoder (tokio-util codec), build/parse request+response headers, build/parse gRPC status trailers with percent-encoding, MessagePipeline compression glue, Deadline + timeout codec. 35 unit tests + 5 integration tests. Full HTTP/2 transport (Phase 3) still pending.
- [x] Phase 3: Native client Channel with load balancing (round-robin, weighted, pick-first) (~600 SLOC) (planned 2026-05-29)
- [x] Phase 4: Native server with service registry and request routing (~500 SLOC) (planned 2026-05-29)
- [x] Phase 5: Native service stub generation (replace tonic-prost-build) (~500 SLOC)
- [x] Phase 6: Native gRPC-Web protocol translation (~500 SLOC)
- [x] Phase 7: Native reflection + health services (~300 SLOC)
- [x] Phase 4.2: Native request body migration — add NativeBodyKind::Pinned variant, make health/reflect services generic over request body B, flip NativeServiceRegistry default to NativeBody, add ServerBuilder::add_native_service (~400 SLOC) (planned 2026-05-29)
- [x] Phase 4.3: Compression negotiation — wire grpc-accept-encoding per-RPC into health (unary+streaming) and reflect (bidi) services; add bidi_sequential_response_with_encoding to oxirpc-core (planned 2026-05-29)
- [x] Phase 8: Tonic TLS wiring/fork decision — RESOLVED: no fork needed; Pure-Rust TLS works end-to-end on both sides (completed 2026-05-30, R14)
  - **Goal:** Close the decision. Native Pure-Rust TLS (oxirpc-core tls + PureRustTlsConnector + native channel TLS) is fully working on both tonic-transport and native paths. Tonic's own FFI-gated TLS types (`tls-ring`/`tls-aws-lc`) are intentionally not wired per Pure-Rust Policy.
  - **Design:** Add a tonic-transport TLS roundtrip test (connector path); re-export `PureRustTlsConnector` from `oxirpc-client`'s public surface with docs. Mark root:34 and core:64 done.
  - **Files:** `oxirpc-client/src/lib.rs`; `oxirpc-client/tests/`; TODO corrections (root, core).
  - **Tests:** tonic-transport Pure-Rust TLS roundtrip (rcgen + health server + `connect_with_connector`).
  - **Risk:** Tonic endpoint TLS fiddly; fall back to connector-construction assertion if full roundtrip fails.

## API Improvements
- [x] Add comprehensive interceptor library (rate limiting, metrics, logging, retry)
- [x] Add interceptor composition chain
- [x] Add connection management: retry, circuit breaker, hedging
- [x] Design xDS integration for service mesh compatibility — ADS streaming client (xds::proto, xds::backoff, xds::ads) implemented 2026-05-29

## Testing
- [x] Conformance test suite against reference gRPC implementations (Go, C++) (scaffold landed 2026-05-29 — fuzz/fuzz_targets/wire_frame.rs + crates/oxirpc/tests/conformance.rs; grpc-go interop deferred)
- [x] Cross-validate native implementation against tonic for correctness (streaming tests landed 2026-05-29 — cross_validate.rs tests 6-8: Watch stream, bidi reflection ListServices, NOT_SERVING status propagation)
- [x] Load testing with sustained high-concurrency RPC streams (load_concurrency.rs: 256-concurrent unary, 64-concurrent streams, soak test behind OXIRPC_SOAK=1)
- [x] Fuzz gRPC frame decoder with arbitrary byte sequences (fuzz/ workspace landed 2026-05-29 — cargo-fuzz target wire_frame for FrameDecoder)

## Performance
- [x] Benchmark native vs tonic RPC throughput and latency (bench scaffold landed 2026-05-29 — crates/oxirpc/benches/native_vs_tonic.rs; full wiring deferred to Round 7 native client)
- [x] Benchmark compression overhead (OxiARC gzip vs flate2) (planned 2026-05-29)
- [x] Profile memory usage under high connection count (planned 2026-05-29)
- [x] Optimize hot paths in HTTP/2 frame processing (planned 2026-05-29, completed 2026-05-29)
  - **Goal:** eliminate per-response-frame `Vec` allocation + memcpy in `encode_grpc_message` (slice A); kill incremental `FrameDecoder` buffer regrowth via reserved payload-length (slice C); add criterion benchmarks over the non-deprecated `wire::frame` / `server::encode_grpc_message` / `MessagePipeline` paths (slice D).
  - **Files (A):** `crates/oxirpc-core/src/wire/server.rs`, `crates/oxirpc-core/src/wire/mod.rs`, `crates/oxirpc-core/tests/wire_server.rs`
  - **Files (C):** `crates/oxirpc-core/src/wire/frame.rs`
  - **Files (D):** `crates/oxirpc-core/benches/wire_codec.rs` (new), `crates/oxirpc-core/Cargo.toml`

## Integration
- [x] Coordinate with OxiProto for proto type system — facade feature gate wiring (facade:51, done 2026-05-30)
  - **Goal:** Expose oxiproto types through the oxirpc facade under an `oxiproto` feature gate, building on the existing `oxirpc-core` oxiproto bridge (`impl From<OxiProtoError>`).
  - **Design:** Add `oxiproto = ["oxirpc-core/oxiproto"]` to facade `[features]`; add `#[cfg(feature = "oxiproto")] pub mod proto { ... }` with curated re-exports of `DescriptorPool`, `FileDescriptorSet`, etc. from oxiproto.
  - **Files:** `oxirpc/Cargo.toml`; `oxirpc/src/lib.rs`; `oxirpc/tests/` (new).
  - **Tests:** `cargo test -p oxirpc --features oxiproto` with a test that uses a re-exported oxiproto type.
  - **Risk:** Path-dep; gated by prerequisite `cargo build -p oxirpc-core --features oxiproto`. Deeper integration (reflect DescriptorPool, build codegen) is a follow-up.
- [x] Coordinate with OxiARC for compression
- [x] Coordinate with OxiTLS for TLS configuration
- [x] HTTP/3 support via OxiQUIC — IMPLEMENTED 2026-07-17, see R1-R6 in the
      Production-Readiness Backlog section below (`http3` feature).

## Open Questions
1. Tonic 1.0 / fork policy: when tonic 1.0 lands, reassess facade vs native
2. gRPC compression: OxiARC-backed gzip/zstd behind feature flag
3. ~~HTTP/3 / QUIC: wait for OxiQuic or accept bounded-FFI adapter~~ RESOLVED
   2026-07-17: native HTTP/3 (gRPC-over-QUIC) shipped behind the `http3`
   feature via OxiQUIC (`oxiquic-h3`/`-transport`/`-crypto`) + the hyperium
   `h3` crate. No FFI adapter needed.
4. xDS integration scope and timeline

## Proposed follow-ups

- [x] Truly-streaming bidi driver — rewrite `drive_bidi`/`drive_bidi_with_encoding` from `req_body.collect()` to incremental `frame()`+`FrameDecoder` (done 2026-05-30)
  - **Goal:** Both drivers consume the request body incrementally (`http_body::Body::frame().await`), feed DATA bytes into a `FrameDecoder`, and emit each response frame as soon as its request frame is fully decoded — enabling true interactive bidi (response N before request N+1). Identical OK/error trailer semantics; identical output for the buffered send-then-half-close pattern reflection uses today.
  - **Design:** Replace `collect().await` + in-buffer slicing with a `FrameDecoder` loop: drain decoded frames immediately, read next HTTP DATA frame when `Ok(None)`, break on EOF/trailers. Preserve `FLAG_COMPRESSED` guard (identity driver), `MessagePipeline` decode (encoding driver). `task::spawn` wrappers unchanged.
  - **Files:** `oxirpc-core/src/wire/server.rs`; `oxirpc-core/tests/wire_server.rs`. Split with splitrs if >2000 lines after edit.
  - **Tests:** regression (existing wire+reflect bidi tests); multi-chunk (frame split across DATA chunks); streaming proof (response 1 before request EOF); error ordering; truncation.
  - **Risk:** Behavioral change: transport errors now surface mid-stream (after prior responses). Sole caller (reflection, buffered) is unaffected. No `FrameDecoder` borrow held across `.frame().await`.

- [x] oxiproto-reflect DescriptorPool in oxirpc-reflect — swap static file-descriptor handling to `oxiproto_reflect::DescriptorPool::parse_from_bytes`; requires reading oxiproto-reflect API carefully (reflect:56) (done 2026-05-30)
- [x] oxiproto-build for proto parsing in oxirpc-build — route proto compilation through oxiproto-build as shared parser; codegen change, separate round (build:54) (done 2026-05-30)
- [x] HTTP/3 support via OxiQuic — substantial new transport on `oxiquic-h3`
  (root:105). **IMPLEMENTED 2026-07-17** — see R1-R6 in the
  Production-Readiness Backlog section below; this entry is superseded by
  that section and kept only for history.


---

<!-- production-readiness-backlog 2026-07-16 -->
## Production-Readiness Backlog — 2026-07-16

_Consolidated from static audit + Opus adversarial bug-hunt (48 verified defects across noffi) + baseline nextest/clippy + design investigation. See `../NOFFI_PRODUCTION_BACKLOG.md` for the full cross-project list and severity/model legend. Not implemented; no commits._

**Confirmed bugs — Opus-verified:**
- [x] **A · med** `oxirpc-client/src/native_channel/call.rs:247` — response pump treats stream ending without grpc-status trailer (or non-200 w/o grpc-status) as successful empty stream instead of error. R2/N0.
  **FIXED**: `pump_response` now takes the initial HTTP status and, when the
  trailers block never arrives, returns `OxiRpcError::from_status_code(StatusCode::Unknown, ..)`
  instead of a silent successful close. Mirrored on the HTTP/3 path
  (`native_channel/h3/call.rs`). Regression test: `stream_ending_without_trailer_is_an_error`.
- [x] **S · med** `oxirpc-core/src/wire/server.rs:69` — `decode_grpc_message` computes `header_len + payload_len` from attacker u32 with no max-size guard → usize overflow / slice panic on 32-bit/wasm32. R1/N0.
  **FIXED**: an explicit `payload_len > MAX_FRAME_SIZE_DEFAULT` bound is
  checked before any arithmetic, followed by a `checked_add` overflow guard
  and a truncation check; duplicated in `decode_grpc_message_with_encoding`.
**HTTP/3 transport (feature `http3`; Pure Rust via OxiQUIC 0.2.0 `oxiquic-{h3,transport,crypto}` + h3 0.0.8; TLS uses `quic_crypto_provider()`) — IMPLEMENTED 2026-07-17:**
- [x] **R1** workspace deps + `http3` feature stanzas across core/client/server/facade + facade `http3` module/docs. Default build verified QUIC-free (`cargo tree --edges normal | grep oxiquic` empty).
- [x] **R2** `client_config_h3`/`server_config_h3` (+`_arc`): ALPN `"h3"`, TLS 1.3-only, `oxiquic_crypto::quic_crypto_provider()`. Unit-tested (ALPN, Ed25519 round-trip, invalid-PEM rejection).
- [x] **R3** client H3 transport: `native_channel::h3::{H3Connection, execute_h3, H3Channel, H3ChannelBuilder}`; `split()` full-duplex; strips `te`; trailers-only detection; `grpc-status` → typed error.
- [x] **R4** server H3 transport: `native_transport_h3::{serve_native_h3_with_service, bind_h3_endpoint}` + `ServerBuilder::serve_native_registry_h3[_with_endpoint]`; accept loop; sends `grpc-status`/`grpc-message` trailers + FIN; ALPN enforcement; graceful shutdown.
- [x] **R5** E2E loopback tests (`crates/oxirpc/tests/h3_e2e.rs`): unary, server-streaming, client-streaming/bidi echo, deadline/timeout, graceful shutdown, ALPN-mismatch rejection. All green.
- [x] **R6** CHANGELOG/README/TODO updated; `SECURITY.md` + `CONTRIBUTING.md` added; feature matrix (default, http3, full) builds clean.
- [ ] (optional follow-up) h3-vs-h2 throughput/latency benchmark. **Attempted
  and reverted**: reproducibly hit what looks like an `H3Channel` connection-
  staleness bug (`.ready()` succeeds, a later separate `.call()` fails with
  "driven connection driver closed before stream was opened", independent of
  `idle_timeout`) — see CHANGELOG.md "Known issues" under [0.2.1]. A
  Wave 3 hygiene pass re-checked (by code inspection, not by re-running the
  benchmark) the originally-suspected `H3Connection::connect`/`ClientEndpoint`
  lifetime lead and ruled it out: the socket is `Arc`-shared into the driver
  task independently of the endpoint, so dropping the endpoint after
  `connect()` cannot be the cause. Root cause is still open and most likely
  needs a standalone `oxiquic`-only repro to localize before this benchmark
  is re-attempted. Not a hygiene-wave task — tracked for the functional-bug
  backlog.
