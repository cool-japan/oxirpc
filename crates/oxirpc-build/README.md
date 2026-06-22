# oxirpc-build — Build-time `.proto` → service-stub code generation for OxiRPC

[![Crates.io](https://img.shields.io/crates/v/oxirpc-build.svg)](https://crates.io/crates/oxirpc-build)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-build` is the build-time half of **OxiRPC**, the COOLJAPAN Pure-Rust gRPC stack. It compiles `.proto` files into async Rust gRPC **service stubs** (`*_server` / `*_client` modules) from your `build.rs` — with **no `protoc` binary required**. Parsing and resolution are handled by `oxiproto-build`, which delegates to [`protox`](https://crates.io/crates/protox), a pure-Rust `.proto` parser.

By default, `oxirpc-build` routes through its **native** code generator ([`ServiceCodegen`](src/codegen.rs)), which emits service stubs only — it does **not** generate prost message types. Generate message types separately with `prost_build`, or opt into the `legacy-tonic-codegen` feature to restore the all-in-one path via `tonic-prost-build` (message types *and* service stubs in one pass). The crate is `#![forbid(unsafe_code)]` and ships an incremental build cache keyed on `$OUT_DIR/.oxirpc-cache/fds.bin`.

## Installation

Add to `[build-dependencies]` (this is a build-time crate):

```toml
[build-dependencies]
oxirpc-build = "0.2.0"

# All-in-one legacy codegen (message types + service stubs):
oxirpc-build = { version = "0.2.0", features = ["legacy-tonic-codegen"] }
```

## Quick Start

`build.rs` — default native path (emits service stubs into `$OUT_DIR`):

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    oxirpc_build::compile_protos(&["proto/greeter.proto"], &["proto/"])?;
    Ok(())
}
```

### Configuring with `Builder`

```rust,ignore
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = oxirpc_build::Builder::new()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path("greeter.fds.bin") // for oxirpc-reflect
        .on_progress(|stage| eprintln!("oxirpc-build: {stage}"))
        .compile_checked(&["proto/greeter.proto"], &["proto/"])?;

    for warning in &out.warnings {
        println!("cargo:warning={warning}");
    }
    Ok(())
}
```

### Compiling from a string (no file on disk)

```rust,ignore
let out = oxirpc_build::Builder::new().compile_str(
    "greeter",
    r#"
        syntax = "proto3";
        package greeter;
        service Greeter { rpc SayHello (HelloRequest) returns (HelloReply); }
        message HelloRequest { string name = 1; }
        message HelloReply   { string message = 1; }
    "#,
)?;
# Ok::<(), oxirpc_build::OxiRpcBuildError>(())
```

## API Overview

### Top-level functions

| Function | Description |
|----------|-------------|
| `compile_protos(protos, includes)` | Convenience wrapper around `Builder::new().compile(...)`; writes to `$OUT_DIR` |

### `Builder`

`Clone`-able configuration for proto compilation.

> **Receiver asymmetry:** `compile()` consumes `self` (avoids cloning in the hot build-script path). `compile_to_fds()`, `compile_checked()`, and `compile_str()` take `&self` and clone internally — prefer these in new code.

| Method | Description |
|--------|-------------|
| `new()` / `Default` | Construct with defaults (`build_client = build_server = true`) |
| `out_dir(path)` | Override the output directory (otherwise `$OUT_DIR`) |
| `build_client(bool)` / `build_server(bool)` | Toggle client / server stub emission |
| `codec(path)` | Use a custom codec instead of `tonic::codec::ProstCodec` |
| `file_descriptor_set_path(path)` | Write the serialized `FileDescriptorSet` (feed to `oxirpc-reflect`) |
| `include_file(path)` | Emit a single `include!`-aggregating file (legacy path) |
| `on_progress(cb)` | Register a `Fn(&str)` progress callback (`"parsing protos..."`, …) |
| `compile_to_fds(protos, includes)` | Parse only → `prost_types::FileDescriptorSet` (uses the cache) |
| `compile(protos, includes)` | Parse + codegen (consumes `self`) |
| `compile_checked(protos, includes)` | Parse + codegen (`&self`); returns `CompileOutput` warnings |
| `compile_str(name, src)` | Compile an in-memory `.proto` string via a temp dir |

#### Legacy-path settings (`legacy-tonic-codegen` only)

The native path emits service stubs without prost message types, so these prost message-side knobs apply **only** when the `legacy-tonic-codegen` feature is active (otherwise silently ignored):

| Method | Description |
|--------|-------------|
| `type_attribute(path, attr)` / `field_attribute(path, attr)` | Attach attributes to generated types / fields |
| `extern_path(proto, rust)` | Map a proto path to an external Rust type |
| `server_mod_attribute(path, attr)` / `client_mod_attribute(path, attr)` | Attributes on generated server / client modules |
| `btree_map(path)` / `bytes(path)` | Generate maps as `BTreeMap` / bytes as `bytes::Bytes` |
| `compile_well_known_types(bool)` | Generate types for `google.protobuf.*` instead of extern |
| `disable_package_emission()` | Don't wrap types in proto-package modules (forwarded to `emit_package(false)`) |

### Native codegen (public)

| Item | Description |
|------|-------------|
| `codegen::ServiceCodegen` | Native async client/server stub generator from `prost_types::ServiceDescriptorProto`; fields `emit_client`, `emit_server`, `codec_path` |
| `file_gen::generate_services(fds, out_dir, codegen)` | Generate stubs from a `FileDescriptorSet` and write them to `out_dir` |

(Both re-exported at the crate root as `ServiceCodegen` and `generate_services`.)

### `CompileOutput` and `ProgressCallback`

| Item | Description |
|------|-------------|
| `CompileOutput` | `{ warnings: Vec<String> }` — non-fatal import-validation warnings |
| `ProgressCallback` | `Arc<dyn Fn(&str) + Send + Sync>` — the `on_progress` callback type |

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `legacy-tonic-codegen` | off | Route `Builder::compile` through `tonic_prost_build::compile_fds` (generates prost message types **and** service stubs, and activates the legacy-path `Builder` settings) |

The default (native) path uses `oxiproto-build` + `protox` and never requires `protoc`.

## Error-variant reference

`OxiRpcBuildError` (implements `std::error::Error`, `Display`, and `From<std::io::Error>`):

| Variant | Description |
|---------|-------------|
| `Proto { path, line, col, msg }` | Protobuf schema/parse error with optional source location (`file:line:col: msg` formatting) |
| `Codegen(String)` | Code-generation failure |
| `Io(std::io::Error)` | Filesystem I/O failure |

Import validation produces non-fatal warnings (in `CompileOutput::warnings`) for files lacking a `package` declaration and for likely-unused `google/protobuf/*` imports.

## Cross-references

- [`oxirpc-core`](../oxirpc-core) — the runtime types the generated stubs target.
- [`oxirpc-client`](../oxirpc-client) / [`oxirpc-server`](../oxirpc-server) — host the generated client and server stubs.
- [`oxirpc`](../oxirpc) — the top-level facade.
- `oxirpc-reflect` — consumes the `FileDescriptorSet` written via `file_descriptor_set_path`.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
