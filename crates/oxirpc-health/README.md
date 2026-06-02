# oxirpc-health — gRPC Health Checking (v1) service for OxiRPC

[![Crates.io](https://img.shields.io/crates/v/oxirpc-health.svg)](https://crates.io/crates/oxirpc-health)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-health` implements the standard **gRPC Health Checking Protocol** (`grpc.health.v1.Health`, with `Check` and `Watch`) for **OxiRPC**, the COOLJAPAN Pure-Rust gRPC stack. It exposes an ergonomic [`HealthHandle`] / service API on top of `tonic-health`, plus a fully **native** health service ([`NativeHealthService`]) backed by an async [`HealthState`] that returns `oxirpc_core::wire::NativeBody` — mountable directly on the native transport in `oxirpc-server`.

Beyond raw status reporting, the crate models **Kubernetes probes** ([`ProbeType::Startup`] / `Liveness` / `Readiness`) with registerable async [`ProbeFn`] checks and a background probe loop, exposes hand-written prost types for `grpc.health.v1` (no `protoc` required), and supports graceful drain (flip every service to `NOT_SERVING` while in-flight RPCs complete). The crate is `#![forbid(unsafe_code)]`.

## Installation

```toml
[dependencies]
oxirpc-health = "0.1.0"

# With OxiARC-backed compression encodings on the health responses:
oxirpc-health = { version = "0.1.0", features = ["gzip", "zstd"] }
```

## Quick Start

```rust,no_run
use oxirpc_health::{HealthBuilder, ServingStatus};

# #[tokio::main]
# async fn main() {
// Build a native health service + handle, seeded with one Serving service.
let (native_svc, mut handle) = HealthBuilder::new()
    .register("my.package.MyService", ServingStatus::Serving)
    .on_change(|svc, status| eprintln!("health: {svc} → {status:?}"))
    .build_native()
    .await;

// Flip status at runtime:
handle.set_not_serving("my.package.MyService").await;

// Mount `native_svc` on an oxirpc-server native router, or any
// tower::Service<Request<Body>>-compatible router.
# let _ = native_svc;
# }
```

### Kubernetes probes

```rust,no_run
use oxirpc_health::{HealthBuilder, ProbeFn, ProbeType};
use std::sync::Arc;

# #[tokio::main]
# async fn main() {
let (_svc, mut handle) = HealthBuilder::new().build_native().await;

// Readiness probe — e.g. a DB ping. Returns true → Serving, false → NotServing.
let readiness: ProbeFn = Arc::new(|| Box::pin(async { true }));
handle.register_k8s_probe(ProbeType::Readiness, "my.Service", readiness);

// Optionally run all probes every second in the background.
let _jh = handle.start_probe_loop(tokio::time::Duration::from_secs(1));
# }
```

Configure the matching probes in your Kubernetes deployment:

```yaml
readinessProbe:
  grpc:
    port: 50051
    service: my.package.MyService
  initialDelaySeconds: 5
```

## API Overview

### Crate-root re-exports

| Item | Origin | Description |
|------|--------|-------------|
| `ServingStatus` | re-export of `tonic_health::ServingStatus` | `Serving` / `NotServing` / `Unknown` |
| `HealthReporter` | re-export of `tonic_health::server::HealthReporter` | Raw tonic-health reporter (bypasses `on_change`) |
| `NativeHealthService` | `service` | Native health service returning `Response<NativeBody>` |
| `HealthState` | `state` | Shared async health-status store |

### `HealthBuilder`

Ergonomic constructor for a health service + [`HealthHandle`] pair (`Default + Debug`).

| Method | Description |
|--------|-------------|
| `new()` | Empty builder, no callback |
| `register(service, status)` | Seed an initial status for `service` |
| `register_named::<S>(status)` | Seed by `S::NAME` (`tonic::server::NamedService`) |
| `on_change(cb)` | Fire `Fn(&str, ServingStatus)` after every `set_status` |
| `build()` | Build a `HealthServer<impl Health>` + `HealthHandle` |
| `build_native()` | Build a `NativeHealthService` + `HealthHandle` (supports `shutdown()` + auto-drain on drop) |

### `HealthHandle`

The runtime control surface (`Debug`). Local mirror of per-service status enables querying without round-tripping the wire.

| Method | Description |
|--------|-------------|
| `set_serving(svc)` / `set_not_serving(svc)` | Convenience status setters |
| `set_status(svc, status)` | Single choke point; fires `on_change`, updates mirror + native state |
| `clear(svc)` | Remove a service (`Check` → `NOT_FOUND` afterward) |
| `on_change(cb)` | Register/replace the status-change callback |
| `get_status(svc)` / `status_for::<S>()` | Read last-known status (`Option<ServingStatus>`) |
| `list_services()` / `statuses()` | List registered services / borrow the full map |
| `set_all(status)` / `set_all_serving()` / `set_all_not_serving()` | Bulk updates (primary graceful-drain helper) |
| `register_named::<S>(status)` | Set status by `S::NAME` |
| `register_probe(svc, probe_fn)` | Register an async [`ProbeFn`] |
| `register_k8s_probe(probe_type, svc, probe_fn)` | Register a probe tagged with a [`ProbeType`] |
| `start_probe_loop(interval)` | Spawn a background probe loop → `JoinHandle<()>` |
| `aggregate_status()` | Serving iff all registered services are Serving |
| `check_probe(probe_type)` | Aggregate status for one probe type (opt-in: empty = Serving) |
| `shutdown()` | Flip all services to `NOT_SERVING` (native handles; idempotent; also runs on `Drop`) |

### Probe types

| Item | Description |
|------|-------------|
| `ProbeFn` | `Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>` |
| `ProbeType` | `Startup` / `Liveness` / `Readiness` |

### `service::NativeHealthService`

| Item | Description |
|------|-------------|
| `NativeHealthService` | Native `grpc.health.v1` service returning `Response<NativeBody>` |
| `NativeHealthService::new(state: Arc<HealthState>)` | Construct from a shared state; `Default` yields a fresh empty state |

### `state::HealthState`

Async, `Arc`-shared status store driving the native service and `Watch` streams.

| Method | Description |
|--------|-------------|
| `new() -> Arc<Self>` | Create a shared state |
| `set(name, status)` | Update a service's status (notifies watchers) |
| `get_status(name)` | Read current status (`Option<ServingStatus>`) |
| `watcher(name)` | A `watch::Receiver` for the service's status changes |
| `shutdown()` | Flip all services to NOT_SERVING and notify watchers |

### `proto` — hand-written `grpc.health.v1` types

Wire-accurate prost types (no `protoc` needed); suitable for a custom health server.

| Item | Description |
|------|-------------|
| `HealthCheckRequest` | `{ service: String }` (empty string = overall server health) |
| `HealthCheckResponse` | `{ status: i32 }`; `serving()`, `not_serving()`, `serving_status() -> Option<ServingStatusProto>` |
| `ServingStatusProto` | `Unknown = 0` / `Serving = 1` / `NotServing = 2` / `ServiceUnknown = 3` |

### Deprecation note

`health_service()` (the free function returning `(HealthServer<impl Health>, HealthHandle)`) is **deprecated** — prefer `HealthBuilder::build_native()` for the native service.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `gzip` | off | Enables `oxirpc-core/gzip` (OxiARC-backed gzip on health responses) |
| `zstd` | off | Enables `oxirpc-core/zstd` (OxiARC-backed zstd on health responses) |

`default = []`. Compression is Pure Rust via OxiARC — never `flate2` or a C-backed zstd crate.

## Cross-references

- [`oxirpc-server`](../oxirpc-server) — `ServerBuilder::health_service()` mounts this crate's native service (feature `health`).
- [`oxirpc-core`](../oxirpc-core) — the `wire::NativeBody` type the native service returns, plus the shared error type.
- [`oxirpc`](../oxirpc) — the top-level facade.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
