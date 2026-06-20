# oxirpc-adapter-aws-lc — aws-lc-rs backed rustls CryptoProvider for OxiRPC

[![Crates.io](https://img.shields.io/crates/v/oxirpc-adapter-aws-lc.svg)](https://crates.io/crates/oxirpc-adapter-aws-lc)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-adapter-aws-lc` is an **optional TLS adapter** for the OxiRPC stack. It exposes a single function that returns a `rustls::crypto::CryptoProvider` backed by [aws-lc-rs](https://crates.io/crates/aws-lc-rs), for deployments that require an aws-lc-rs (BoringSSL-derived) cryptographic backend — for example, to satisfy FIPS-oriented build requirements or to match an existing platform crypto policy. It pairs with `oxirpc`'s `tls` feature, which by default uses the Pure-Rust RustCrypto provider from OxiTLS.

> **⚠️ Not Pure Rust.** This crate is the one deliberate exception to the COOLJAPAN Pure-Rust policy in the OxiRPC tree. Enabling its `aws-lc` feature pulls in `aws-lc-sys`, which compiles **C** (and assembly) code via a build script. The adapter therefore exists as a **separate, opt-in, non-default** crate so that the rest of OxiRPC stays 100% Pure Rust. **The default feature set is intentionally empty** — merely depending on this crate (without enabling `aws-lc`) pulls **no** C code into the build closure, and the entire `provider` module is gated behind `#[cfg(feature = "aws-lc")]`. A purity tripwire in `tests/purity.rs` enforces this automatically. The crate itself is `#![forbid(unsafe_code)]`.

## Installation

```toml
[dependencies]
# Default: empty feature set — no C/FFI is compiled. `aws_lc_provider` is NOT available.
oxirpc-adapter-aws-lc = "0.1.0"
```

To actually obtain the provider you must opt in to the `aws-lc` feature (which brings in C/FFI):

```toml
[dependencies]
oxirpc-adapter-aws-lc = { version = "0.1.3", features = ["aws-lc"] }
```

In practice you usually enable it transitively through the `oxirpc` facade, which wires it together with the `tls` feature:

```toml
[dependencies]
oxirpc = { version = "0.1.3", features = ["tls", "aws-lc"] }
```

## Quick Start

```rust,no_run
# #[cfg(feature = "aws-lc")]
# {
use oxirpc_adapter_aws_lc::aws_lc_provider;
use rustls::ServerConfig;

fn main() -> Result<(), rustls::Error> {
    // Obtain an aws-lc-rs backed CryptoProvider (an Arc).
    let provider = aws_lc_provider();

    // Inject it per-config — this crate does NOT call install_default().
    let _builder = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?;
    Ok(())
}
# }
```

## API Overview

The crate's public surface is a single function, available **only** when the `aws-lc` feature is enabled.

| Item | Signature | Description |
|------|-----------|-------------|
| `aws_lc_provider` | `fn() -> std::sync::Arc<rustls::crypto::CryptoProvider>` | Returns a `CryptoProvider` backed by aws-lc-rs. Does **not** call `CryptoProvider::install_default()` — inject it per-config via `ServerConfig::builder_with_provider` / `ClientConfig::builder_with_provider`. |

### Why per-config, not `install_default`?

`aws_lc_provider()` deliberately avoids `install_default()` so that taking this adapter as a dependency never mutates the process-global rustls default provider. This keeps it composable with crates that install their own provider, and lets a single process mix the Pure-Rust default provider with the aws-lc-rs provider on a per-`Config` basis.

## Feature Flags

| Feature | Default | Pure Rust | Description |
|---------|---------|-----------|-------------|
| `aws-lc` | off | **No** (C/FFI) | Enables the aws-lc-rs provider via `rustls/aws_lc_rs`. Pulls in `aws-lc-sys` (compiles C/assembly) and exposes `aws_lc_provider()`. |

With **no** features enabled (the default), the crate compiles no C code and exposes no public items.

## Cross-References

- [`oxirpc`](https://crates.io/crates/oxirpc) — the facade; re-exports this crate under `oxirpc::aws_lc` via the `aws-lc` feature, and provides the `tls` feature this adapter plugs into.
- [`oxirpc-core`](https://crates.io/crates/oxirpc-core) — exposes the Pure-Rust default `tls` helpers (RustCrypto provider via OxiTLS) that this adapter is an alternative to.
- [`rustls`](https://crates.io/crates/rustls) — the TLS library whose `CryptoProvider` this adapter supplies.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
