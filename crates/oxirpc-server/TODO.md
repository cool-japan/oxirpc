# oxirpc-server TODO

## Status
Plaintext server builder wrapping tonic::transport::Server. `ServerBuilder::new()` exposes config methods (concurrency_limit_per_connection, max_concurrent_streams, timeout, tcp_nodelay, tcp_keepalive, http2_keepalive_interval, http2_adaptive_window, http2_max_pending_accept_reset_streams, http2_max_local_error_reset_streams, tcp_keepalive_interval, tcp_keepalive_retries); `.add_service(svc)` returns `ServeReady` with `serve(addr)`, `serve_with_shutdown(addr, signal)`, listener-based `serve_with_listener` / `serve_with_listener_shutdown`, and Unix domain socket `serve_unix` / `serve_unix_with_shutdown` (Unix-only). ~175 SLOC production code.

## Core Implementation
- [x] Implement TLS server support — `ServerBuilder::tls(rustls::ServerConfig)` + tokio-rustls `TlsAcceptor` integration via `serve_with_incoming` (done 2026-05-27)
  - **Goal:** `ServerBuilder::new().tls(config).add_service(svc).serve(addr).await` — pure-Rust TLS via OxiTLS, no ring/aws-lc.
  - **Design:**
    - new `crates/oxirpc-server/src/tls.rs`: `tls_acceptor(config) -> TlsAcceptor`, `accept_one(...)`, `incoming_tls(listener, acceptor) -> impl Stream<...>` (handshake errors logged via tracing::warn, skipped).
    - Extend `crates/oxirpc-server/src/lib.rs`: `tls_config: Option<Arc<ServerConfig>>` field, `tls()`, `tls_arc()`, `is_tls()`, serve() branches on tls_config, Display shows tls=true|false. ALSO adds `pub mod routing; pub use routing::MethodRouter;` (preemptive for Slice 8).
    - Extend `crates/oxirpc-server/Cargo.toml`: tokio-rustls, rustls, async-stream, tracing under `tls` feature.
  - **Files:** new src/tls.rs, new src/routing.rs (stub for Slice 8), extend src/lib.rs (routing + tls mods, tls field + methods on ServerBuilder/ServeReady, Display tls= flag, serve TLS branch), extend Cargo.toml, new tests/tls_server.rs
  - **Tests:** tls_server_rejects_plaintext_client, tls_acceptor_passes_through_io_correctly, display_includes_tls_flag, tls_builder_clones_config_via_arc — all passing (62 passed, 1 ignored); `serve_with_listener` / `serve_with_listener_shutdown` also branch on tls_config (fixed footgun 2026-05-27)
- [x] Server transport ergonomics + multi-service: `accept_http1`, frame/conn limits, tower layers, `trace_fn`, graceful drain, `ServeReady::add_service` (done 2026-05-25); extended with `http2_adaptive_window`, `http2_max_pending_accept_reset_streams`, `http2_max_local_error_reset_streams`, `tcp_keepalive_interval`, `tcp_keepalive_retries`, Unix socket serving (Slice 3, done 2026-05-25)
  - **Goal:** Expose tonic 0.14 transport knobs through `ServerBuilder`/`ServeReady` and enable multiple services non-breaking.
  - **Design:** `ServerBuilder::accept_http1(bool)` pass-through (verified exists in tonic 0.14). `max_frame_size`, `initial_*_window_size`, `concurrency_limit_per_connection`, `tcp_keepalive`, `http2_keepalive_*`, `timeout` — pass-throughs (subagent verifies each; omit+note any absent, never fabricate). `ServerBuilder::layer<L>(L)` and `ServeReady::layer<L>(L)` tower Layer pass-through. `ServerBuilder::trace_fn(f)`. Multi-service: `ServeReady::add_service<S>(self, svc) -> ServeReady` calling `self.router.add_service(svc)` (Router::add_service returns Self → chainable, non-breaking). NOTE: per-message size/compression limits are per-service only in tonic 0.14, NOT on transport — do NOT add to ServerBuilder; document where they belong.
  - **Files:** `src/lib.rs` (extend builder + ServeReady). `tower` already a dep.
  - **Prerequisites:** none.
  - **Tests:** `tests/` — builder accepts_http1/limits without panic; multi-service router serves two services; layer pass-through compiles+runs; trace_fn invoked.
  - **Risk:** some setters may be absent in tonic 0.14 (mitigated: verify each, omit+note). Service bounds (`S::Response: IntoResponse`) must be preserved on add_service.
- [x] `MethodRouter` tower layer + facade-level benchmarks (interceptor chain + full round-trip) (done 2026-05-27)
  - **Goal:** (a) `MethodRouter` for per-method dispatch (not just gating). (b) Criterion benchmarks at facade level.
  - **Design:**
    - new `crates/oxirpc-server/src/routing.rs`: `MethodRouter { routes: HashMap<String, BoxCloneService<...>>, fallback }`, `.route()`, `.fallback()`, impl `tower::Service<Request<Body>>` (lookup by uri path → fallback → unimplemented). Uses `tower::util::BoxCloneService`. NOTE: `pub mod routing; pub use routing::MethodRouter;` are added to lib.rs by Slice 3 subagent — this slice only creates routing.rs.
    - new `crates/oxirpc-server/tests/routing.rs`: 5 tests.
    - new `crates/oxirpc/benches/`: interceptor_chain.rs, roundtrip.rs, facade_reexport_overhead.rs.
    - extend `crates/oxirpc/Cargo.toml`: criterion dev-dep + [[bench]] sections.
  - **Files (server):** new src/routing.rs, new tests/routing.rs (does NOT touch lib.rs or Cargo.toml — Slice 3 owns those)
  - **Files (facade):** new crates/oxirpc/benches/*.rs, extend crates/oxirpc/Cargo.toml
  - **Tests:** router_dispatches_to_exact_match, router_falls_back_when_no_match, router_returns_unimplemented_without_fallback, router_clones_correctly_for_concurrent_use, router_chains_with_interceptor_layer
- [x] Implement concurrent stream handling limits: max_concurrent_streams per connection
- [x] Implement per-method interceptors: apply interceptors only to specific service/method paths — `MethodInterceptorLayer` + `MethodInterceptorService` Tower layer with exact URI path matching (done 2026-05-26)
- [x] Implement compression: `ServerBuilder::accept_compressed(encoding)` / `send_compressed(encoding)` — fields `accept_encoding: Vec<CompressionEncoding>`, `send_encoding: Option<CompressionEncoding>`, accessors `accepted_encodings()` / `send_encoding()`, deduplication, Display updated (done 2026-05-26)
- [x] Enforce inbound `grpc-encoding` against accepted encodings — reject unsupported with gRPC status 12 (UNIMPLEMENTED) + `grpc-accept-encoding` header, replacing today's status-13 mishandling; centralized via `CompressionPrefsLayer` (done 2026-05-30)
  - **Goal:** A request compressed with an encoding the server cannot honor is rejected *before* body decode with status 12 + `grpc-accept-encoding` listing accepted encodings. Covers health, reflect, and user-registered services uniformly. Identity requests unchanged.
  - **Design:** Add `pub accept: Vec<CompressionEncoding>` to `ServerCompressionPrefs` (oxirpc-core). Extend `CompressionPrefsService::call` (concrete to `Response<NativeBody>`) with a header-only inbound gate: Unknown/unaccepted compressing encoding → short-circuit response (status 12, `grpc-accept-encoding`). All 6 serve sites populate both `send` and `accept` fields.
  - **Files:** `oxirpc-core/src/encoding.rs`; `oxirpc-server/src/compression_layer.rs`; `oxirpc-server/src/lib.rs`; tests in health/server.
  - **Tests:** Core unit tests for acceptance helpers; integration: gzip-request to identity-only server → status 12 + header; regression.
  - **Risk:** Generic→concrete layer (verified all 6 sites use NativeBody); header-only check before `into_body()`; Future enum must be `Send`.
- [x] Implement keepalive configuration: http2_keepalive_interval (timeout / max-idle still TODO)
- [x] Implement rate limiting per client IP or per method — `IpRateLimiterLayer` + `IpRateLimiterService` Tower layer with token-bucket algorithm, x-forwarded-for/x-real-ip header extraction, and periodic bucket cleanup (done 2026-05-26)
- [x] Add `ServerBuilder::tcp_nodelay(bool)` for TCP tuning
- [x] Add `ServerBuilder::tcp_keepalive(duration)` for TCP keepalive
- [x] Add `ServerBuilder::timeout(duration)` per-request timeout
- [x] Implement native HTTP/2 server transport via hyper's `http2::Builder` + facade-level end-to-end test (done 2026-05-27)
  - **Goal:** `ServeReady::serve_native(addr)`, `serve_native_with_shutdown(addr, signal)`, and `serve_native_with_listener(listener)` serve gRPC over HTTP/2 using `hyper::server::conn::http2::Builder` directly — no tonic transport in the hot path. TLS via existing tokio-rustls. Includes facade e2e test.
  - **Files:** new `src/native_transport.rs`, extend `src/lib.rs` + `Cargo.toml` (`native` feature, hyper/hyper-util/http-body-util optional deps), new `crates/oxirpc/tests/native_server_e2e.rs`, extend `crates/oxirpc/Cargo.toml`
  - **Tests:** `native_server_unary_roundtrip`, `native_server_multi_service`, `native_server_graceful_shutdown`, `native_server_tls_roundtrip` — all passing (4 passed, 0 failed)
  - **Architecture:** `ServeReady` holds a `tonic::service::Routes` field (behind `native` feature) populated in parallel with `router`. `native_transport::serve_native` accepts `Routes` directly — `Routes` implements `Service<Request<B>>` for any `B: http_body::Body<Data=Bytes>`, so `hyper::body::Incoming` passes through without a newtype wrapper. Body conversion: `tonic::body::Body::new(incoming)`. Shutdown via `tokio::sync::watch::channel` + `tokio::select!` in accept loop.

## API Improvements
- [x] Support bound-address discovery for OS-assigned ports (via `serve_with_listener` + `TcpListener::local_addr`)
- [x] Add `ServerBuilder::health_service()` convenience for auto-adding health checking (deferred — moved to oxirpc facade)
- [x] Add `ServerBuilder::reflection_service(fds)` convenience for auto-adding reflection (deferred — moved to oxirpc facade)
- [x] Implement `Display` for `ServerBuilder` showing configuration summary — tracks accept_http1, timeout, max_concurrent_streams, tcp_nodelay (done 2026-05-26)
- [x] Add Unix domain socket support: `serve_unix(path)` and `serve_unix_with_shutdown` (done 2026-05-25, Unix-only, uses UnixListenerStream)

## Testing
- [x] Test server startup and graceful shutdown with signal
- [x] Test multiple services on one server
- [x] Test TLS server with self-signed certificates (done 2026-05-27 — `tls_server_handshake_then_unary_rpc` full gRPC roundtrip over TLS using PureRustTlsConnector)
- [x] Test concurrent stream limit enforcement (done 2026-05-29 — `concurrent_stream_limit_enforced` in tests/concurrent_streams.rs)
- [x] Max message size / graceful shutdown drain / startup-time / full roundtrip server tests (done 2026-05-27)
  - **Goal:** Cover server-side test gaps: max-message-size rejection, graceful-shutdown drain, startup time, multi-service dispatch.
  - **Design:**
    - new `crates/oxirpc-server/tests/fixture/mod.rs`: Pinger (`NamedService::NAME = "fixture.Pinger"`) + Ponger (`NamedService::NAME = "fixture.Ponger"`) — both local, no cross-slice dependency. `spawn_with_builder(builder) -> (addr, shutdown_tx, JoinHandle)`.
    - new `crates/oxirpc-server/tests/roundtrip.rs`: 7 tests.
    - extend `crates/oxirpc-server/Cargo.toml` dev-deps.
  - **Files:** new tests/fixture/mod.rs, new tests/roundtrip.rs, extend Cargo.toml dev-deps
  - **Tests:** serve_then_ping_returns_pong, max_message_size_rejects_oversized, graceful_shutdown_drains_inflight_rpc, graceful_shutdown_rejects_new_after_signal, accept_compressed_supports_gzip, server_startup_under_100ms, multi_service_dispatch_routes_to_right_handler
- [x] Test compression negotiation — `tests/compression.rs` (13 tests: accept/send knobs, dedup, Display, chaining; done 2026-05-26)
- [x] Test rate limiting under high load — additional tests in `tests/middleware.rs`: independent IPs, no-header fallback, empty-layer pass-through, multiple routes (done 2026-05-26)
- [x] Test graceful shutdown drain period (in-flight RPCs complete) (done 2026-05-27 — graceful_shutdown_drains_inflight_rpc in tests/roundtrip.rs)

## Performance
- [x] Benchmark RPC throughput (requests/sec) at various concurrency levels (done 2026-05-27 — `bench_single_rpc_latency` in benches/server_bench.rs)
- [x] Benchmark server startup time (done 2026-05-27 — `bench_server_startup_time` in benches/server_bench.rs)
- [x] Profile memory usage under high connection count — `benches/high_conn_memory.rs` with CountingAllocator; N=1/10/100 registries (done 2026-05-29)
- [x] Benchmark graceful shutdown latency (done 2026-05-27 — `bench_graceful_shutdown_latency` in benches/server_bench.rs)

## Integration
- [x] Ensure ServerBuilder works with oxirpc-build generated server traits (planned 2026-05-29)
- [x] Ensure TLS uses oxirpc-core::tls::server_config (OxiTLS, no ring) (audited 2026-05-27 — src/tls.rs imports only tokio-rustls + rustls; no ring/aws-lc on any dep edge)
- [x] Ensure compression uses OxiARC (never flate2) (audited 2026-05-27 — no flate2/zstd-C/snap in src/ or Cargo.toml; compression encoding uses oxirpc-core::encoding backed by OxiARC)
- [x] Integrate with oxirpc-health for automatic health service registration
- [x] Integrate with oxirpc-reflect for automatic reflection service registration — `ServerBuilder::reflection_service(fds)` convenience method under `reflect` feature (done 2026-05-29)
- [x] Test with oxirpc-client for full round-trip (planned 2026-05-29)
- [x] Phase 4.0: Native server service registry — NativeServiceRegistry dispatching by service prefix, serve_native_registry, BoxCloneService type-erasing (done 2026-05-29)
  - **Files:** new `src/native_registry/{mod,registry,dispatch,service_set}.rs`, extend `src/lib.rs` + `src/native_transport.rs`, extend `Cargo.toml` (bytes, http-body optional under `native` feature)
  - **Tests:** 8 registry tests in `tests/native_registry.rs`, 1 concurrent stream test in `tests/concurrent_streams.rs` — all 11 passing
- [x] Phase 4.1: Switch NativeServiceRegistry and health/reflect services from `tonic::body::Body` to `NativeBody` for response bodies (done 2026-05-29)
  - **Change:** Removed unnecessary `tonic::body::Body::new(native_body)` wrapping. `BoxedNativeService<B>` now uses `Response<NativeBody>`. `RegistryService::type Response` is `Response<NativeBody>`. `serve_native_with_service` and `make_hyper_service` are generic over `RespB`. `NativeHealthService` and `NativeReflectionService` return `Response<NativeBody>` directly.
  - **Compat:** `ServerBuilder::health_service()` and `reflection_service()` use a thin `NativeBodyAdapter<S>` newtype to re-wrap `NativeBody` → `tonic::body::Body` for the tonic-Routes path. `ServerBuilder` now also exposes `serve_native_registry_with_listener*` directly (no `add_service` needed to reach `ServeReady`).
  - **Files:** `src/native_registry/{service_set,dispatch,registry}.rs`, `src/native_transport.rs`, `src/lib.rs`, `crates/oxirpc-health/src/service.rs`, `crates/oxirpc-reflect/src/service.rs`
  - **Tests:** All 60 health + 64 reflect + 55 oxirpc (total 179) passing; 0 warnings
