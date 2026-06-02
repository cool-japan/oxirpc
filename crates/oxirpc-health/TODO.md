# oxirpc-health TODO

## Status
Functional gRPC health checking (v1): `health_service()` returns `(HealthServer, HealthHandle)` pair. HealthHandle supports set_serving, set_not_serving, clear, set_status, plus a local status mirror enabling get_status, list_services, set_all_serving, and set_all_not_serving. Wraps tonic-health. Native `grpc.health.v1` prost types in `proto.rs` (HealthCheckRequest, HealthCheckResponse, ServingStatusProto) for zero-protoc proto encode/decode. Watch notification tests added (on_change callback propagation). K8s liveness/readiness probe docs in module-level rustdoc.

## Core Implementation
- [x] Implement native Health service (Check unary + Watch server-streaming) replacing tonic-health internals (planned 2026-05-26)
  - **Goal:** A native `grpc.health.v1.Health` service implementation that handles both Check (unary) and Watch (server-streaming) RPCs without depending on `tonic_health::server::*` internals.
  - **Design:**
    - new `crates/oxirpc-health/src/state.rs`: `HealthState` with `Arc<RwLock<HashMap<String, ServingStatus>>>` + per-service `tokio::sync::watch::Sender<ServingStatusProto>`, `set(name, status)` broadcasts, `watcher(name)` creates lazily, `shutdown()` flips all to NOT_SERVING.
    - new `crates/oxirpc-health/src/service.rs`: `NativeHealthService { state: Arc<HealthState> }`, impl `NamedService` (NAME = "grpc.health.v1.Health"), impl `tower::Service<Request<Body>>` dispatching /Check (unary via `Grpc::unary`) and /Watch (server-streaming via `Grpc::server_streaming` + `WatchStream`).
    - Update `crates/oxirpc-health/src/lib.rs`: add `mod state; mod service;`, `HealthBuilder::build_native(self) -> (NativeHealthService, HealthHandle)`, deprecate old `serve_health()` / `health_pair()`.
    - Update `crates/oxirpc-health/Cargo.toml`: add `tokio-stream`, `pin-project-lite`, `http` workspace deps.
  - **Files:** new state.rs, new service.rs, update lib.rs, update Cargo.toml, update tests/health.rs (~8 new tests)
  - **Tests:** native_check_serving_returns_serving, native_check_unknown_returns_not_found, native_watch_streams_current_status_immediately, native_watch_propagates_state_change, native_watch_dedups_consecutive_duplicates, native_shutdown_flips_all_to_not_serving, native_service_named_constant_is_health_v1, native_check_unknown_method_returns_unimplemented
- [x] Implement native `grpc.health.v1` prost types: `HealthCheckRequest`, `HealthCheckResponse`, `ServingStatusProto` in `proto.rs` (no protoc required)
- [x] Implement Check RPC: unary health status query per service name (done 2026-05-27 — NativeHealthService::check in service.rs)
- [x] Implement Watch RPC: server-streaming health status updates with change notification (done 2026-05-27 — NativeHealthService::watch in service.rs)
- [x] Implement per-service health state machine: Unknown -> Serving | NotServing, transitions trigger Watch notifications (done 2026-05-27 — HealthState in state.rs)
- [x] Implement overall server health (empty string service name convention) (20-30 SLOC)
- [x] Implement health check aggregation: derive overall status from individual service statuses (60-80 SLOC)
- [x] Implement periodic health probe: background task checking service liveness at intervals (80-100 SLOC)
- [x] Implement health check with custom probe function: `HealthHandle::register_probe(service, async fn -> bool)` (60-80 SLOC)
- [x] Implement startup/liveness/readiness probe distinction (Kubernetes model) (60-80 SLOC)
- [x] Add `HealthHandle::set_all_serving()` / `set_all_not_serving()` for bulk status updates
- [x] Add `HealthHandle::get_status(service)` for querying current status programmatically
- [x] Add `HealthHandle::list_services()` returning all registered service names
- [x] Health: `HealthBuilder`, `on_change` callback, graceful-drain helper (planned 2026-05-25)
  - **Goal:** Ergonomic health-service construction and status-change observation, preserving the single `set_status` choke point.
  - **Design:** `HealthBuilder { initial: BTreeMap<String, ServingStatus>, on_change: Option<Arc<dyn Fn(&str, ServingStatus)+Send+Sync>> }` → `build() -> (HealthServer<..>, HealthHandle)`. `HealthHandle::on_change(cb)` stores callback; existing `set_status` choke point calls it after `reporter.set_service_status().await` + `statuses.insert()`. **BYPASS WARNING:** re-exported `HealthReporter` lets callers mutate status without on_change — document this; `set_status` is the only observed path. `set_all(NotServing)` drain helper flips every known service to NOT_SERVING.
  - **Files:** `src/lib.rs` (extend). `tests/health.rs` (extend).
  - **Prerequisites:** none.
  - **Tests:** builder seeds initial statuses; on_change fires on `set_status`; drain flips all to NOT_SERVING; doctest notes reporter bypass.
  - **Risk:** on_change bypass via raw HealthReporter (mitigated by documentation; keeping re-export to avoid breaking change).

## API Improvements
- [x] Implement `Debug` for `HealthHandle` showing registered services and their statuses
- [x] Add typed service registration: `register_named::<S: NamedService>()` and `status_for::<S>()` on `HealthHandle`; `register_named::<S>()` on `HealthBuilder` using tonic's NamedService
- [x] Document Kubernetes health check endpoint integration patterns

## Testing
- [x] Test Check returns SERVING for a service set to serving
- [x] Test Check returns NOT_SERVING for a service set to not-serving
- [x] Test Check returns NOT_FOUND for unknown service name
- [x] Test Watch receives updates when status changes
- [x] Test clear removes service, subsequent Check returns NOT_FOUND
- [x] Test overall server health (empty string service name)
- [x] Test health probe function: returns Serving when probe succeeds, NotServing when probe fails
- [ ] Integration test with grpc-health-probe binary

## Performance
- [x] Benchmark Check RPC latency (should be <1ms)
- [x] Benchmark Watch notification propagation time
- [x] Profile memory usage with many registered services: `CountingAllocator` benchmark for N=1/10/100/1000 services and N watchers (done 2026-05-27 — CountingAllocator benchmark in benches/memory_bench.rs)
  - **Goal:** Quantify per-service heap cost of `HealthState` (HashMap entry + watch channel). Custom `CountingAllocator` wrapping `std::alloc::System` with `AtomicI64` tracking.
  - **Files:** new `benches/memory_bench.rs`, extend `Cargo.toml` ([[bench]] section)
  - **Tests:** N/A — benchmarks. `cargo bench -p oxirpc-health --no-run` must compile.

## Integration
- [x] Ensure health service integrates with oxirpc-server graceful shutdown
- [x] Ensure health service works through oxirpc-web gRPC-Web bridge
- [x] Ensure health service is discoverable via oxirpc-reflect reflection
- [ ] Test with Kubernetes liveness/readiness probe configuration
