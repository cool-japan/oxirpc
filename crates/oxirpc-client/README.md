# oxirpc-client — gRPC channel/client builder with load-balancing, resilience, and metrics

[![Crates.io](https://img.shields.io/crates/v/oxirpc-client.svg)](https://crates.io/crates/oxirpc-client)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-client` is the client half of **OxiRPC**, the COOLJAPAN Pure-Rust gRPC stack. Its [`ClientBuilder`] wraps `tonic::transport::Endpoint` with an ergonomic, cloneable configuration surface — timeouts, user-agent, HTTP/2 flow-control windows, keep-alive, TCP tuning, rate limiting — and produces a connected or lazy [`Channel`] that any generated client stub can consume.

Beyond the basic builder, the crate provides endpoint **resolution and load-balancing** policies ([`balance`]), client-side **resilience** (retry/backoff, circuit-breaking, hedging — [`resilience`]), `Arc`-shared **RPC metrics** ([`metrics`]), cooperative **connection-state monitoring** ([`monitor`]), a [`ChannelPool`], a native HTTP/2 channel ([`native_channel`]), and an xDS resolver ([`xds`]). With the `tls` feature, [`PureRustTlsConnector`] performs TLS negotiation (ALPN `h2`) via `tokio-rustls` + `rustls-rustcrypto` — Pure Rust, no `ring`, no `aws-lc`. Compression, when used, goes through `oxirpc-core`'s OxiARC-backed encodings (`oxiarc-deflate` / `oxiarc-zstd`), never `flate2`. The crate is `#![forbid(unsafe_code)]`.

## Installation

```toml
[dependencies]
oxirpc-client = "0.2.0"

# With Pure-Rust TLS:
oxirpc-client = { version = "0.2.0", features = ["tls"] }
```

## Quick Start

```rust,no_run
use oxirpc_client::{ClientBuilder, RpcMetrics};
use std::time::Duration;

# async fn run() -> Result<(), oxirpc_client::OxiRpcError> {
let metrics = RpcMetrics::new();

let channel = ClientBuilder::new("http://127.0.0.1:50051")
    .timeout(Duration::from_secs(5))
    .connect_timeout(Duration::from_secs(2))
    .user_agent("my-app/1.0")
    .tcp_nodelay(true)
    .with_metrics(metrics.clone())
    .connect()
    .await?;

// Hand `channel` to any generated tonic client stub:
//   let mut client = GreeterClient::new(channel);
//   let reply = client.say_hello(request).await?;
# let _ = channel;
# Ok(())
# }
```

### Pure-Rust TLS (feature `tls`)

```rust,no_run
# #[cfg(feature = "tls")]
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
use oxirpc_client::ClientBuilder;
use oxirpc_core::tls::client_config;
use rustls::RootCertStore;
use rustls_pki_types::ServerName;

let roots = RootCertStore::empty();           // populate with trusted roots
let cfg = client_config(roots)?;              // Pure-Rust rustls::ClientConfig
let server_name = ServerName::try_from("example.com")?;

let channel = ClientBuilder::new("https://example.com:443")
    .tls(cfg, server_name)
    .connect()
    .await?;
# let _ = channel;
# Ok(())
# }
```

## API Overview

### `ClientBuilder`

`Clone + Debug`. Configuration is applied when `connect` / `connect_lazy` is called.

| Method | Description |
|--------|-------------|
| `new(endpoint)` | Point at a URI (e.g. `"http://127.0.0.1:50051"`) |
| `timeout(dur)` / `connect_timeout(dur)` | Per-RPC timeout / TCP-connect timeout |
| `user_agent(s)` / `origin(s)` | `user-agent` header / `:authority` override |
| `concurrency_limit(n)` | Cap concurrent in-flight requests |
| `initial_connection_window_size(b)` / `initial_stream_window_size(b)` | HTTP/2 flow-control windows |
| `tcp_nodelay(bool)` / `tcp_keepalive(dur)` | TCP tuning |
| `http2_keep_alive_interval(dur)` / `keep_alive_timeout(dur)` / `keep_alive_while_idle(bool)` | HTTP/2 keep-alive |
| `buffer_size(Option<usize>)` | Outbound request buffer size |
| `rate_limit(limit, per)` | Outbound rate limit |
| `http2_adaptive_window(bool)` / `max_frame_size(Option<u32>)` | HTTP/2 adaptive flow control / max frame size |
| `with_metrics(RpcMetrics)` | Attach `Arc`-shared RPC counters |
| `tls(config, server_name)` | Use Pure-Rust TLS (feature `tls`) |
| `connect()` | Connect now → `Channel` |
| `connect_lazy()` | No handshake until the first RPC → `Channel` |
| `connect_with_interceptor(f)` | Connect and wrap in an `InterceptedService` |
| `connect_lazy_with_interceptor(f)` | Lazy connect + interceptor |

### Crate-root re-exports

| Item | Module | Description |
|------|--------|-------------|
| `Channel` | re-export of `tonic::transport::Channel` | The connected channel type |
| `OxiRpcError` | re-export of `oxirpc_core::OxiRpcError` | Shared error type |
| `RpcMetrics` | `metrics` | RPC counters |
| `ChannelMonitor`, `ConnectionState` | `monitor` | Connection-state tracking |
| `CallTelemetry` | `load_reporting` | Per-call telemetry record |
| `ChannelPool`, `TypedChannel` | `pool` | Channel pooling |
| `NativeChannel`, `NativeChannelBuilder`, `NativeBody` | `native_channel` | Native HTTP/2 channel |
| `XdsResolver`, `XdsWatcher` | `xds` | xDS endpoint resolution |
| `PureRustTlsConnector` | `tls_connector` (feature `tls`) | `tower::Service<Uri>` TLS connector |

### `balance` — resolution & load-balancing

| Item | Description |
|------|-------------|
| `Endpoint` | A resolvable target endpoint |
| `Resolver` / `DynResolver` traits | Synchronous and dynamic endpoint resolution |
| `StaticResolver(Vec<Endpoint>)` | Fixed endpoint list |
| `DnsResolver` | DNS-based resolution |
| `ResolveError` | Resolution failure |
| `LoadBalancer` trait | Pick-policy abstraction |
| `PickFirst` / `RoundRobin` / `Weighted` | Built-in load-balancing policies |

### `resilience` — retry, circuit-breaking, hedging

| Item | Description |
|------|-------------|
| `Backoff` | `exponential(base)`, `next_delay(attempt)` |
| `RetryPolicy` | `new(max_attempts)`, `with_backoff`, `with_retryable`, `should_retry(attempt, code) -> Option<Duration>` |
| `CircuitBreaker` | `new(failure_threshold, success_threshold, open_cooldown)`, `allow_request`, `on_success`, `on_failure`, `is_open` |
| `Hedging` | `new(max_hedges, delay)`, `delay_for(idx)`, `schedule() -> Vec<Duration>` |

### `metrics` — `RpcMetrics`

`Arc`-shared counters; clones observe the same totals.

| Method | Description |
|--------|-------------|
| `new()` | Construct |
| `record_started()` / `record_completed()` / `record_failed()` | Increment counters |
| `started()` / `completed()` / `failed()` | Read totals (`u64`) |

### `monitor` — connection state

| Item | Description |
|------|-------------|
| `ConnectionState` | Connectivity state enum |
| `ChannelMonitor` | Cooperative state tracker for a channel |

### `pool` — channel pooling

| Item | Description |
|------|-------------|
| `ChannelPool` | `new(...)`, `from_endpoints(...)`, `get()`, `len()`, `is_empty()`, `healthy_count()` |
| `TypedChannel<S>` | `new(channel)`, `channel()`, `into_inner()` |

### `native_channel` — native HTTP/2 channel

| Item | Description |
|------|-------------|
| `NativeChannel` | `ready()`, `call(...)`, `call_with_deadline(...)`, `builder()` |
| `NativeChannelBuilder` | Fluent config (resolver, stream/connection limits, windows, keep-alive, user-agent, `tls`, `build()`) |
| `NativeBody` | `http_body`-compatible response body |

### `xds` — xDS resolution

| Item | Description |
|------|-------------|
| `XdsResolver`, `XdsWatcher`, `xds_resolver(...)` | xDS-driven endpoint resolution |
| `AdsClient`, `AdsConfig` | Aggregated Discovery Service client + config |

### `load_reporting`

| Item | Description |
|------|-------------|
| `CallTelemetry` | Per-call telemetry record |
| `LoadReporter` | Aggregates and reports call load |

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tls` | off | Enables `PureRustTlsConnector`, `ClientBuilder::tls`, and `NativeChannelBuilder::tls` (`tokio-rustls` + `rustls-rustcrypto`); also enables `oxirpc-core/tls` |

`default = []`.

## Errors

All fallible methods return `Result<_, OxiRpcError>` (re-exported from `oxirpc-core`). Transport and URI failures surface as `OxiRpcError::Transport`. See [`oxirpc-core`](../oxirpc-core) for the full variant table.

## Cross-references

- [`oxirpc-core`](../oxirpc-core) — error type, TLS config helpers, encodings, wire format.
- [`oxirpc-server`](../oxirpc-server) — the server counterpart.
- [`oxirpc-build`](../oxirpc-build) — generates the client stubs that consume a `Channel`.
- [`oxirpc`](../oxirpc) — the top-level facade.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
