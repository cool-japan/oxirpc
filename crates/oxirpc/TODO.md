# oxirpc TODO (facade)

## Status
Facade crate re-exporting Code, OxiRpcError, OxiRpcResult, Request, Response, Status, StatusCode, Metadata from oxirpc-core, plus a `core` module re-exporting status/metadata/timeout/encoding. Feature-gated modules: `client` (ClientBuilder), `server` (ServerBuilder), `tls` (OxiTLS configs), `reflect`, `web`, `health`, `compression`, `gzip`, `zstd`, and a `full` umbrella feature (all Pure-Rust sub-features). Provides `prelude`, `version()`, and an interceptors module (BearerAuth/Tracing/Deadline). ~155 SLOC production code.

## Core Implementation
- [x] Add `prelude` module for glob import of common types
- [x] Add `compression` feature and module for OxiARC-backed gRPC compression (plus `gzip`/`zstd` encoding features)
- [x] Add `full` feature that enables all sub-features (client, server, tls, reflect, web, health, compression, gzip, zstd)
- [x] Add `oxirpc::version()` returning crate version string
- [x] Populate `build` module docs (oxirpc-build is a build-dependency; documented integration contract)
- [x] Facade interceptors: `RateLimit`, `Logging`, `Metrics`, `Timeout` + `InterceptorChain` (done 2026-05-25)
  - **Goal:** Round out `oxirpc::interceptors` with production-grade tonic `Interceptor` impls and a composable chain, matching the existing BearerAuth/Tracing/Deadline style.
  - **Design:** `RateLimitInterceptor` — token-bucket with `Arc<Mutex<f64>>` tokens + refill-by-elapsed; rejects with `resource_exhausted`. `LoggingInterceptor` — callback-based `Arc<dyn Fn(&Request<()>)+Send+Sync>`. `MetricsInterceptor` — `Arc<AtomicU64>` total + `Arc<Mutex<HashMap<String,u64>>>` by_path with `snapshot()`. `TimeoutInterceptor` — injects `grpc-timeout` using `oxirpc_core::timeout::format_grpc_timeout` (preferred over existing `DeadlineInterceptor` hardcoded-`m`). `InterceptorChain` holds `Vec<Box<dyn FnMut(Request<()>)->Result<Request<()>,Status>+Send>>`, runs in order, short-circuits on first `Err`. Note: `RetryInterceptor` is NOT a sync interceptor — reframed as `resilience::RetryPolicy` in oxirpc-client.
  - **Files:** `src/interceptors.rs` (extend; split via `splitrs` if approaching 2000 lines). No new Cargo.toml deps expected.
  - **Prerequisites:** none (std + tonic already deps).
  - **Tests:** rate-limit allows N then rejects; logging sink observes call; metrics increments by_path; timeout injects and is no-op when present; chain runs in order and short-circuits.
  - **Risk:** sync interceptor cannot rate-limit across awaits (documented). Arc<Mutex> contention acceptable at facade level.
- [x] `interceptors::RetryInterceptor` — Superseded — reframed as `resilience::RetryPolicy` in oxirpc-client (see that crate's TODO).

## API Improvements
- [ ] Remove feature gates where possible once native implementations replace tonic
  - **INVESTIGATED 2026-06-03 — DEFERRED (not yet actionable)**
  - Each feature gate is independently justified and cannot be removed yet:
    - `client` / `server`: gate optional sub-crates (`oxirpc-client`, `oxirpc-server`). Removing would force every user to compile these heavy crates regardless of need.
    - `tls`: gates `rustls`, `rustls-pki-types`, `tokio-rustls` — optional but non-trivial deps. Must stay. Fixed bug: the facade's `tls` feature was not forwarding `oxirpc-client?/tls`, causing `--all-features` compile failure in `native_server_e2e` TLS test (fixed in same pass).
    - `reflect`, `web`, `health`: gate optional protocol-extension sub-crates. No native alternatives exist yet.
    - `compression` / `gzip` / `zstd`: gate OxiARC compression backends which are optional runtime features.
    - `aws-lc`: explicitly gates FFI (C code via aws-lc-sys). Must always be opt-in per Pure Rust Policy.
    - `oxiproto`: path dependency not on crates.io; explicitly excluded from `full`.
    - `native`: tonic is still load-bearing — `serve_native()` in `native_transport.rs` accepts `tonic::service::Routes`; `ClientBuilder::connect()` and `connect_lazy()` return `tonic::transport::Channel`. The native transport is a hybrid, not a full tonic replacement.
  - **Pre-condition for re-evaluation:** when `oxirpc-server` and `oxirpc-client` provide complete tonic-free paths (no `tonic::service::Routes`, no `tonic::transport::Channel` in public APIs), revisit `client`/`server`/`native` gates.
- [x] Add top-level convenience functions: `oxirpc::serve(addr, service)`, `oxirpc::connect(endpoint)`
- [x] Add comprehensive crate-level documentation with end-to-end example
- [x] Document feature flag matrix showing what each feature enables
- [x] Add migration guide from tonic to oxirpc

## Testing
- [x] Integration test: compile proto, start server, connect client, make RPC, verify response -- all through facade
- [x] Test each feature flag enables exactly the expected module
- [x] Test interceptor composition: auth + tracing + deadline chain
- [x] Test BearerAuthInterceptor: correct token passes, wrong token rejected
- [x] Test TracingInterceptor: x-request-id header is monotonically increasing
- [x] Test DeadlineInterceptor: grpc-timeout header injected when absent
- [x] Test that default features (empty) compiles and provides core types
- [x] Native server e2e integration test (done 2026-05-27) — native HTTP/2 server + tonic Channel client roundtrip; 4 tests: `native_server_unary_roundtrip`, `native_server_multi_service`, `native_server_graceful_shutdown`, `native_server_tls_roundtrip` — all passing

## Performance
- [x] `MethodRouter` tower layer + facade-level benchmarks (interceptor chain + full round-trip) (done 2026-05-27)
  - **Goal:** (a) `MethodRouter` for per-method dispatch. (b) Criterion benchmarks at facade level.
  - **Design (facade part):**
    - new `crates/oxirpc/benches/interceptor_chain.rs`: wrap no-op service in 0/1/4/16 interceptors; bench unary call cost.
    - new `crates/oxirpc/benches/roundtrip.rs`: spin up Pinger fixture, call Ping in loop, report ns/RPC and RPS.
    - new `crates/oxirpc/benches/facade_reexport_overhead.rs`: bench identical calls via `oxirpc::*` vs `tonic::*` import paths.
    - extend `crates/oxirpc/Cargo.toml`: criterion dev-dep + [[bench]] harness=false sections.
  - **Files:** new benches/*.rs (3 files), extend Cargo.toml
  - **Tests:** N/A — benchmarks. Verify `cargo bench -p oxirpc --no-run` compiles.

## Integration
- [x] Ensure generated stubs from oxirpc-build work seamlessly with facade types (verified: tonic re-exports are pass-through; migration guide added)
- [x] Ensure oxiproto types are accessible through oxirpc for proto operations — facade `oxiproto` feature gate (done 2026-05-30)
  - **Goal:** Add a new `oxiproto = ["oxirpc-core/oxiproto"]` feature to the facade. Under it, expose a `pub mod proto { ... }` with curated re-exports of oxiproto types (`DescriptorPool`, `FileDescriptorSet`, descriptor/message types). Builds on the existing `oxirpc-core` oxiproto bridge (`From<OxiProtoError>`).
  - **Design:** Facade `Cargo.toml` adds `oxiproto` to `[features]` (NOT to `default` or `full` — path-dep). `src/lib.rs` adds `#[cfg(feature = "oxiproto")] pub mod proto { pub use oxiproto::...; }` (curated, not glob if surface is large). Feature-matrix doc table updated.
  - **Files:** `oxirpc/Cargo.toml`; `oxirpc/src/lib.rs`; `oxirpc/tests/` (new test).
  - **Tests:** `cargo test -p oxirpc --features oxiproto` — test constructs/uses an oxiproto type via the re-export.
  - **Risk:** Path-dep — prerequisite: `cargo build -p oxirpc-core --features oxiproto` passes. Stop and return deviated if not.
- [x] Document recommended dependency configuration for downstream users (migration guide + feature flag matrix in crate docs)

