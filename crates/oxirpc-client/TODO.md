# oxirpc-client TODO

## Status
Plaintext client builder: `ClientBuilder::new(endpoint)` with a cloneable config surface (timeout, connect_timeout, user_agent, origin, concurrency_limit, HTTP/2 window sizes, tcp_nodelay, tcp_keepalive), then `.connect().await` or `.connect_lazy()` creating a tonic `Channel`. ~130 SLOC production code.

## Core Implementation
- [x] Phase 3: Native client Channel — `NativeChannel` / `NativeChannelBuilder` / `NativeBody` in `src/native_channel/` using pure `h2` crate; multi-connection pool per endpoint; `DynResolver`-driven background refresh; `tower::Service` impl; 10 integration tests with h2 server fixture (2026-05-29)
- [x] Implement TLS channel support: `ClientBuilder::tls(config, server_name)` accepting OxiTLS-built ClientConfig behind `tls` feature; `PureRustTlsConnector` in `src/tls_connector.rs` using tokio-rustls + hyper_util::rt::TokioIo; no ring, no aws-lc-rs on normal edges (2026-05-26)
- [x] Client `balance::*` (load balancers + resolvers) and `resilience::*` (retry / circuit-breaker / hedging) (completed 2026-05-25)
  - **Goal:** Transport-agnostic load-balancing and resilience state machines in `oxirpc-client`, ready to drive the future native channel and usable today as policy objects.
  - **Design:** `balance::Endpoint { uri: http::Uri, weight: u32 }`. `balance::Resolver` trait (async, returns `Vec<Endpoint>`); `StaticResolver(Vec<Endpoint>)`; `DnsResolver { host, port }` via `tokio::net::lookup_host`. `balance::LoadBalancer` trait; `PickFirst`, `RoundRobin { idx: AtomicUsize }`, `Weighted` (deterministic running-counter, no `rand` dep). `resilience::RetryPolicy { max_attempts, backoff: Backoff::Exponential{base,factor,max,jitter}, retryable: fn(StatusCode)->bool }` — jitter via LCG seeded from Instant. `resilience::CircuitBreaker` 3-state (Closed/Open/HalfOpen) with failure/success thresholds + cooldown. `resilience::Hedging { max_hedges, delay }` — emits schedule; fan-out is caller's job. All reuse `oxirpc_core::status::StatusCode::is_retryable`.
  - **Files:** new `src/balance.rs`, `src/resilience.rs`; wire into `src/lib.rs`. `Cargo.toml` — `futures-core` only if Resolver returns Stream (else skip).
  - **Prerequisites:** `oxirpc_core::status::StatusCode::is_retryable` (verified exists).
  - **Tests:** `tests/` — round-robin cycles; weighted ratios; pick-first stable; retry backoff sequence + max-attempts + retryable filter; CB opens/half-opens/closes; static resolver; hedging schedule. DNS test gated `#[ignore]`.
  - **Risk:** DnsResolver needs network (mitigated: ignore-gated). Deterministic jitter avoids rand dep. No tonic wiring yet (policies only — documented).
- [x] Implement minimal xDS endpoint types and `XdsResolver` implementing `Resolver` trait (done 2026-05-27) + ADS streaming client (done 2026-05-29)
  - **Goal:** Hand-authored `ClusterLoadAssignment`, `LocalityLbEndpoints`, `LbEndpoint`, `SocketAddress`, `HealthStatus` types + push-based `XdsResolver` holding `watch::Receiver<Vec<Endpoint>>`. `XdsWatcher` API for control-plane updates.
  - **Round 7 (2026-05-29):** `xds::proto` — hand-authored prost types for Envoy xDS v3; `xds::backoff` — deterministic exponential backoff; `xds::ads` — `AdsClient` + `AdsConfig` + pure state-machine logic (factored for testability); `encode_proto_frame`, `initial_request`, `build_ack`, `build_nack`, `process_response` all public for unit testing.
  - **Files:** `src/xds/proto.rs`, `src/xds/backoff.rs`, `src/xds/ads.rs`, updated `src/xds/mod.rs`, new `tests/xds_ads.rs`; `prost` added to production deps.
  - **Tests (7):** `proto_roundtrip_discovery_request`, `proto_decode_cluster_load_assignment`, `state_machine_initial_request_empty_version_nonce`, `state_machine_ack_advances_version`, `state_machine_nack_preserves_old_version`, `backoff_resets_after_ack`, `ads_client_builds_without_panic`
- [x] Implement connection management: keepalive pings, idle timeout, max connection age — `http2_keep_alive_interval`, `keep_alive_timeout`, `keep_alive_while_idle`, `buffer_size`, `rate_limit`, `http2_adaptive_window`, `max_frame_size` added to `ClientBuilder` (2026-05-25)
- [x] Implement client-side load reporting for backends (60-80 SLOC) — `CallTelemetry` + `LoadReporter` in `src/load_reporting.rs`; 6 tests in `tests/load_reporting.rs` (done 2026-05-29)
- [x] Implement per-call timeout/deadline setting via `ClientBuilder::timeout(duration)`
- [x] Implement compression settings: note — compression is configured per-service stub (`GreeterClient::new(channel).send_compressed(...)`), not on the channel; documented in `ClientBuilder` doc comment (2026-05-25)
- [x] Implement interceptor chain: `ClientBuilder::connect_with_interceptor(f)` and `connect_lazy_with_interceptor(f)` wrapping `Channel` with `tonic::service::interceptor::InterceptedService` (2026-05-25)
- [x] Implement channel pooling: `ChannelPool` (load-balancer-driven, `new`/`from_endpoints`/`get`/`len`/`is_empty`/`Clone`) and `TypedChannel<S>` (compile-time service binding, `Deref<Target=Channel>`) in `src/pool.rs` (2026-05-26)
- [x] Add `ClientBuilder::user_agent(s)` setting
- [x] Add `ClientBuilder::origin(uri)` for HTTP/2 :authority header override
- [x] Add `ClientBuilder::initial_connection_window_size(bytes)` HTTP/2 tuning
- [x] Add `ClientBuilder::initial_stream_window_size(bytes)` HTTP/2 tuning
- [x] Add `ClientBuilder::concurrency_limit`, `connect_timeout`, `tcp_nodelay`, `tcp_keepalive`

## API Improvements
- [x] Add `ClientBuilder::connect_with_connector(connector)` for custom transports (Unix sockets, in-process); uses correct `hyper::rt::Read + hyper::rt::Write` bounds matching tonic 0.14's Endpoint API (2026-05-26)
- [x] Add typed channel wrapper: `TypedChannel<S>` that only accepts requests for service S (completed 2026-05-26, see `src/pool.rs`)
- [x] Implement `Clone` for `ClientBuilder` (pre-connect config reuse)
- [x] Add connection state monitoring: `ChannelMonitor` + `ConnectionState` cooperative state machine in `src/monitor.rs`; `set_state()`, `on_state_change()`, `is_ready()`, clone-shared state via Arc (2026-05-27)
- [x] Add metrics hooks: `RpcMetrics` (started/completed/failed atomic counters, Arc-shared, Clone) in `src/metrics.rs`; `ClientBuilder::with_metrics(m)` accessor (2026-05-26)

## Testing
- [x] Plaintext / lazy / TLS / retry / timeout / compression integration tests + full client-server roundtrip (completed 2026-05-27)
  - **Goal:** Eliminate client test gaps by building a self-contained Pinger fixture and exercising the full client API.
  - **Design:**
    - new `crates/oxirpc-client/tests/fixture/mod.rs`: Pinger service (Ping/Stream/Sink/Echo RPCs), `spawn_fixture()`, `spawn_tls_fixture()`, `spawn_flaky_fixture(n)`, `spawn_slow_fixture(delay_ms)`. Uses `tonic::server::Grpc<ProstCodec<T>>` — no codegen.
    - new `crates/oxirpc-client/tests/roundtrip.rs`: 9 tests.
    - extend `crates/oxirpc-client/Cargo.toml` dev-deps.
  - **Files:** new tests/fixture/mod.rs, new tests/roundtrip.rs, extend Cargo.toml dev-deps
  - **Tests:** plaintext_unary_roundtrip, lazy_connect_no_tcp_until_first_rpc, tls_unary_roundtrip, retry_recovers_from_unavailable, timeout_triggers_deadline_exceeded, compression_roundtrip_gzip, channel_pool_load_balances_across_two_endpoints, client_metrics_count_rpcs, client_builder_clones_for_concurrent_use
- [x] Test circuit breaker: trip after N failures, reject without connecting
- [x] Test load balancing: round-robin distributes across 3 backends evenly

## Performance
- [x] Benchmark connection establishment time (plaintext vs TLS): `lazy_channel_create`, `channel_monitor_new` in `benches/connection_bench.rs` (2026-05-27)
- [x] Benchmark RPC throughput (unary calls/sec) at various concurrency levels: `channel_pool_dispatch` pool-size N=2/8/32 in `benches/connection_bench.rs` (2026-05-27)
- [x] Profile memory usage of channel pool with many connections — `benches/channel_pool_memory.rs` with CountingAllocator; N=1/10/100 endpoints (done 2026-05-29)
- [x] Criterion-based benchmarks for codec / compression / metadata / channel pool / retry (planned 2026-05-26)
  - **Goal:** Stand up criterion benchmark harnesses. OxiArc throughput baseline (flate2 banned by COOLJAPAN policy).
  - **Design (client part):**
    - new `crates/oxirpc-client/benches/channel_pool_get.rs`: bench `ChannelPool::get` N=2/8/32 endpoints.
    - new `crates/oxirpc-client/benches/retry_overhead.rs`: bench no-op call with retry off vs `RetryPolicy::new(3, ...)`.
    - extend `crates/oxirpc-client/Cargo.toml`: criterion = { workspace = true } dev-dep + [[bench]] harness=false sections.
  - **Files:** new benches/channel_pool_get.rs, new benches/retry_overhead.rs, extend Cargo.toml
  - **Tests:** N/A — benchmarks. Verify `cargo bench -p oxirpc-client --no-run` compiles.

## Integration
- [x] Ensure ClientBuilder produces channels compatible with oxirpc-build generated client stubs (planned 2026-05-29)
- [x] Ensure TLS uses oxirpc-core::tls::client_config (OxiTLS, no ring) — audited via doc comment in `src/tls_connector.rs` (done 2026-05-29)
- [x] Ensure compression uses OxiARC (never flate2) — audited via policy comment in `src/lib.rs` (done 2026-05-29)
- [x] Test with oxirpc-server for full client-server round-trip (planned 2026-05-29)
