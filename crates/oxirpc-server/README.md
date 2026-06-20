# oxirpc-server — gRPC server builder, router, and native HTTP/2 transport

[![Crates.io](https://img.shields.io/crates/v/oxirpc-server.svg)](https://crates.io/crates/oxirpc-server)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-server` is the server half of **OxiRPC**, the COOLJAPAN Pure-Rust gRPC stack. Its [`ServerBuilder`] is an ergonomic wrapper over `tonic::transport::Server`: configure HTTP/2 tuning, timeouts, keep-alive, compression preferences, and TLS, then `add_service(...)` to obtain a [`ServeReady`] that can be bound to a `SocketAddr`, a pre-bound `TcpListener`, or a Unix socket — with graceful-shutdown variants throughout.

The crate also ships a **native hyper-backed HTTP/2 transport** (feature `native`) that drives connections directly via `hyper::server::conn::http2::Builder` — keeping tonic's transport layer out of the hot path — plus a [`NativeServiceRegistry`] for hosting services that return `oxirpc_core::wire::NativeBody`. Optional convenience methods mount a gRPC **health-checking** service (feature `health`) or a **server-reflection** service (feature `reflect`). TLS is Pure Rust via `tokio-rustls` + `rustls-rustcrypto` through `oxirpc_core::tls` — no `ring`, no `aws-lc` on normal dependency edges. Server-side compression uses OxiARC-backed encodings (`oxiarc-deflate` / `oxiarc-zstd`), never `flate2`. The crate is `#![forbid(unsafe_code)]`.

## Installation

```toml
[dependencies]
oxirpc-server = "0.1.0"

# Native hyper H2 transport + TLS + health service:
oxirpc-server = { version = "0.1.3", features = ["native", "tls", "health"] }
```

## Quick Start

```rust,no_run
use oxirpc_server::ServerBuilder;

# async fn run<S>(my_svc: S) -> Result<(), oxirpc_server::OxiRpcError>
# where
#     S: tower::Service<http::Request<tonic::body::Body>,
#             Response = http::Response<tonic::body::Body>,
#             Error = std::convert::Infallible>
#         + tonic::server::NamedService + Clone + Send + Sync + 'static,
#     S::Future: Send + 'static,
#     S::Response: axum::response::IntoResponse,
# {
ServerBuilder::new()
    .timeout(std::time::Duration::from_secs(30))
    .add_service(my_svc)
    .serve("0.0.0.0:50051".parse().expect("valid addr"))
    .await?;
# Ok(())
# }
```

### Multiple services + graceful shutdown

```rust,no_run
# use oxirpc_server::ServerBuilder;
# async fn run(ready: oxirpc_server::ServeReady) -> Result<(), oxirpc_server::OxiRpcError> {
ready
    .serve_with_shutdown(
        "0.0.0.0:50051".parse().expect("valid addr"),
        async { let _ = tokio::signal::ctrl_c().await; },
    )
    .await?;
# Ok(())
# }
```

### Mount the health service (feature `health`)

```rust,no_run
# #[cfg(feature = "health")]
# async fn run() -> Result<(), oxirpc_server::OxiRpcError> {
oxirpc_server::ServerBuilder::new()
    .health_service()
    .serve("0.0.0.0:50051".parse().expect("valid addr"))
    .await?;
# Ok(())
# }
```

## API Overview

### Crate-root re-exports

| Item | Origin | Description |
|------|--------|-------------|
| `Server` | re-export of `tonic::transport::Server` | The underlying tonic server |
| `OxiRpcError` | re-export of `oxirpc_core::OxiRpcError` | Shared error type |
| `OxiNamedService` | `named_service` | OxiRPC-native service-name trait (decouples the registry from `tonic::server::NamedService`) |
| `MethodRouter` | `routing` | Per-method routing table |
| `IpRateLimiterLayer`, `IpRateLimiterService` | `middleware` | Per-IP rate-limiting tower layer/service |
| `MethodInterceptorBuilder`, `MethodInterceptorLayer`, `MethodInterceptorService` | `middleware` | Per-method interceptor layer |
| `NativeServiceRegistry`, `RegistryService` | `native_registry` (feature `native`) | Native service registry + type-erased service |

### `ServerBuilder`

`Default + Display`. Construct with `new()`, then chain configuration before `add_service`.

| Method | Description |
|--------|-------------|
| `accept_compressed(enc)` / `send_compressed(enc)` | Advertise an accepted encoding / set the default send encoding |
| `accepted_encodings()` / `send_encoding()` | Inspect the configured encodings |
| `inner()` / `into_inner()` | Borrow / take the underlying `tonic::transport::Server` |
| `accept_http1(bool)` | Accept HTTP/1.1 (useful behind grpc-web proxies) |
| `concurrency_limit_per_connection(n)` / `max_concurrent_streams(n)` | Concurrency limits |
| `timeout(dur)` | Per-RPC timeout |
| `tcp_nodelay(bool)` / `tcp_keepalive(dur)` / `tcp_keepalive_interval(...)` / `tcp_keepalive_retries(...)` | TCP tuning |
| `http2_keepalive_interval(dur)` / `http2_keepalive_timeout(dur)` | HTTP/2 keep-alive |
| `initial_stream_window_size(b)` / `initial_connection_window_size(b)` | HTTP/2 flow-control windows |
| `max_frame_size(b)` / `http2_max_header_list_size(b)` / `http2_adaptive_window(Option<bool>)` | HTTP/2 frame/header/adaptive settings |
| `http2_max_pending_accept_reset_streams(...)` / `http2_max_local_error_reset_streams(...)` | Reset-stream limits |
| `load_shed(bool)` | Reject (rather than buffer) when not ready |
| `max_connection_age(dur)` / `max_connection_age_grace(dur)` | Connection-age limits |
| `tls(config)` / `tls_arc(config)` / `is_tls()` | Pure-Rust TLS (feature `tls`) |
| `add_service(svc)` | Add a tonic service → `ServeReady` |
| `add_native_service(svc)` | Add a service returning `Response<NativeBody>` → `ServeReady` |
| `health_service()` | Mount the native health service (feature `health`) → `ServeReady` |
| `reflection_service(fds)` | Mount server reflection v1 from `FileDescriptorSet` bytes (feature `reflect`) → `ServeReady` |
| `serve_native_registry(addr, registry)` / `..._with_listener(..)` / `..._with_listener_shutdown(..)` | Serve a `NativeServiceRegistry` directly (feature `native`) |

> Intentionally **absent**: `ServerBuilder::layer` and `trace_fn` (would force the builder generic / pull in `tracing`); per-message size limits and per-service compression are configured on the generated `*Server<T>` types in tonic 0.14. Use `into_inner()` for those.

### `ServeReady`

Returned by `add_service`. Bind and serve, or add more services first.

| Method | Description |
|--------|-------------|
| `add_service(svc)` / `add_native_service(svc)` | Register additional services (multi-service on one port) |
| `serve(addr)` / `serve_with_shutdown(addr, signal)` | Serve on a `SocketAddr` (TLS path used if configured) |
| `serve_with_listener(listener)` / `serve_with_listener_shutdown(..)` | Serve over a pre-bound `TcpListener` (learn the bound port first) |
| `serve_unix(path)` / `serve_unix_with_shutdown(path, signal)` | Serve on a Unix domain socket (Unix only) |
| `serve_native(addr)` / `serve_native_with_shutdown(addr, signal)` | Native hyper H2 transport (feature `native`) |
| `serve_native_with_listener(..)` / `serve_native_with_listener_shutdown(..)` | Native H2 over a pre-bound listener (feature `native`) |
| `serve_native_registry(addr, registry)` (+ `_with_shutdown` / `_with_listener` / `_with_listener_shutdown`) | Native H2 driven by a `NativeServiceRegistry` (feature `native`) |

### `middleware`

| Item | Description |
|------|-------------|
| `MethodInterceptorLayer` / `MethodInterceptorService<S>` | Tower layer/service applying per-method interceptors |
| `MethodInterceptorBuilder` | `new()`, `route(...)`, `build()` — build a per-method interceptor layer |
| `IpRateLimiterLayer` / `IpRateLimiterService<S>` | `new(max_per_sec, burst)` token-bucket rate limiting per client IP |

### `routing` and `named_service`

| Item | Description |
|------|-------------|
| `routing::MethodRouter` | `new()`, `route(path, service)`, `fallback(service)` — path-based method routing |
| `named_service::OxiNamedService` | OxiRPC-native service-name trait used by the native registry |

### Native transport & registry (feature `native`)

| Item | Description |
|------|-------------|
| `native_transport` | Native hyper `http2::Builder`-driven serving path |
| `native_registry::NativeServiceRegistry` | Collects named native services and type-erases them |
| `native_registry::RegistryService` | The type-erased dispatch service produced by the registry |

### TLS acceptor (feature `tls`)

The `tls` module provides the TLS-acceptor incoming-stream helper used by the `serve*` TLS paths. Build the `rustls::ServerConfig` with `oxirpc_core::tls::server_config`.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `native` | off | Native hyper HTTP/2 transport + `NativeServiceRegistry` / `RegistryService` |
| `tls` | off | Pure-Rust TLS serving (`tokio-rustls` + `rustls-rustcrypto`); enables `oxirpc-core/tls` and `tonic/tls-connect-info` |
| `reflect` | off | `ServerBuilder::reflection_service` via `oxirpc-reflect` |
| `health` | off | `ServerBuilder::health_service` via `oxirpc-health` |

`default = []`.

## Errors

All `serve*` methods return `Result<(), OxiRpcError>` (re-exported from `oxirpc-core`); bind/transport failures surface as `OxiRpcError::Transport`. `reflection_service` returns `oxirpc_reflect::ReflectError` on undecodable `FileDescriptorSet` bytes. See [`oxirpc-core`](../oxirpc-core) for the full error-variant table.

## Cross-references

- [`oxirpc-core`](../oxirpc-core) — error type, TLS config, `ServerCompressionPrefs`, wire format.
- [`oxirpc-client`](../oxirpc-client) — the client counterpart.
- [`oxirpc-health`](../oxirpc-health) — backs `health_service()`.
- `oxirpc-reflect` — backs `reflection_service()`.
- [`oxirpc-build`](../oxirpc-build) — generates the server stubs passed to `add_service`.
- [`oxirpc`](../oxirpc) — the top-level facade.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
