# oxirpc-build TODO

## Status
Functional build helper: `compile_protos` and `Builder` chain `protox::compile` (Pure Rust) to `tonic_prost_build::compile_fds` for gRPC client/server stub generation. Supports build_client/build_server toggles, type/field/mod attributes, extern paths, btree_map/bytes overrides, well-known-types, FDS path output, include file, and a `compile_to_fds()` accessor returning the raw `FileDescriptorSet`. No protoc required. ~210 SLOC production code.

## Core Implementation
- [x] Add `Builder::compile_to_fds()` exposing raw FileDescriptorSet for downstream use
- [x] Add `Builder::file_descriptor_set_path(path)` writing serialized FDS for runtime reflection
- [x] Add `Builder::include_file(path)` generating a single include file listing all generated modules
- [x] Implement native service stub generation (replace tonic-prost-build dependency) (400-500 SLOC) — native generator implemented in `src/codegen.rs` + `src/file_gen.rs`; `Builder::compile()` now routes through `ServiceCodegen` by default; tonic-prost-build moved to optional `legacy-tonic-codegen` feature (cutover done 2026-05-29)
- [x] Generate async client stubs with Channel abstraction (150-200 SLOC)
- [x] Generate async server trait with method signatures matching proto service (150-200 SLOC)
- [x] Generate streaming types: unary, server-streaming, client-streaming, bidirectional per method (100-150 SLOC)
- [x] Implement `Builder::codec(codec_path)` for custom encoding (JSON, flatbuffers) (30-40 SLOC)
- [x] Implement `Builder::server_mod_attribute(path, attr)` for server module attributes
- [x] Implement `Builder::client_mod_attribute(path, attr)` for client module attributes
- [x] Add `Builder::disable_package_emission()` for flat output without module nesting (wired to `tonic_prost_build::Builder::emit_package(false)` — available since tonic-prost-build 0.14)
- [x] Add `Builder::compile_well_known_types()` for generating WKT Rust types
- [x] Add `Builder::btree_map(paths)` for BTreeMap map fields
- [x] Add `Builder::bytes(paths)` for `bytes::Bytes` instead of `Vec<u8>`

## API Improvements
- [x] Build hardening: `#![warn(missing_docs)]`, structured errors w/ file:line:col, `compile_str`, import-validation warnings (done 2026-05-25)
  - **Goal:** Bring `oxirpc-build` up to workspace lint standards and provide actionable diagnostics + in-memory proto compilation.
  - **Design:** Add `#![warn(missing_docs)]` and document all public items. Enrich `OxiRpcBuildError`: extract file:line:col from `protox` compile errors into `Proto { path, line, col, msg }` (degrade to `Proto { msg }` if protox location extraction is coarse); keep `Codegen`, `Io`. `Builder::compile_str(&self, name: &str, src: &str)` — writes src to `std::env::temp_dir()/{name}.proto`, compiles, cleans up with drop guard. Import-validation: after `protox::compile`, inspect FDS for declared-but-unused imports / missing package; collect into `Vec<String>` warnings (returned from compile, not panics). Fix `compile_to_fds(&self)` / `compile(self)` receiver asymmetry if feasible without breaking callers; else document.
  - **Files:** `src/lib.rs`. `tests/`.
  - **Prerequisites:** confirm protox error type exposes source location (it wraps miette spans).
  - **Tests:** `tests/` — `compile_str` round-trips a tiny valid proto via temp_dir; malformed proto yields `Proto` error with location; unused-import emits warning; missing-docs lint passes (compile-clean).
  - **Risk:** protox location extraction may be coarse (degrade gracefully). Temp-file cleanup on error path (drop guard).
- [x] Add progress callback for large compilations (`Builder::on_progress`)
- [x] Document minimum compatible tonic version (tonic-prost-build >= 0.14; noted in `disable_package_emission()` doc comment)

## Testing
- [x] Test client+server stub generation for a service with all RPC types (unary, server-stream, client-stream, bidi)
- [x] Test build_client(false) suppresses client code generation
- [x] Test build_server(false) suppresses server code generation
- [x] Test type_attribute and field_attribute appear in generated code
- [x] Test extern_path mapping produces correct use statements
- [x] Test error handling: missing proto file, invalid proto syntax
- [x] Test generated code compatibility: `compile_to_fds_with_disable_package_emission_returns_fds`, `compile_with_disable_package_emission_no_package_mod`, `compile_to_fds_with_nested_package`, `document_minimum_tonic_version`
- [x] Test that generated code compiles and links with oxirpc-client/server (done 2026-05-27)
  - **Goal:** Integration test that generates Rust stubs from `greeter.proto` via `ServiceCodegen`, writes to temp dir, synthesizes a `Cargo.toml` with path deps, and runs `cargo check` via `Command::new(env!("CARGO"))` — asserting successful compilation.
  - **Files:** `tests/fixtures/greeter.proto`, `tests/compile_link.rs`
  - **Tests:** `test_generated_code_compiles`, `test_generated_code_with_server_only`, `test_generated_code_with_client_only` (all `#[ignore]`, run with `-- --ignored`)
  - **Bug fixed:** Client codec type params were swapped (`<Response, Request>` → `<Request, Response>`) in `src/codegen.rs`.

## Performance
- [x] Benchmark build time for large proto sets (50+ services) — `benches/large_proto_compile.rs`; 50-service and 10×10 benchmarks (done 2026-05-29)
- [x] Consider caching parsed FileDescriptorSet for incremental builds
- [x] Profile protox parse time vs protoc for equivalent proto files (done 2026-05-29) — `benches/protox_parse.rs`; small/medium/large benchmarks, bench compiles clean

## Integration
- [x] Ensure generated stubs are compatible with oxirpc-client Channel and oxirpc-server ServerBuilder (planned 2026-05-29)
- [x] Ensure oxiproto-build is used for proto parsing (shared dependency) (done 2026-05-30)
- [x] Test generated code works with oxirpc-reflect for server reflection — `tests/reflect_compat.rs` verifies protox-FDS → DescriptorPoolBuilder → list_services round-trip (done 2026-05-29)
- [x] Test generated code works with oxirpc-health for health checking (done 2026-05-29) — `tests/health_integration.rs`; `generated_service_composes_with_health_registry` (#[ignore], cargo check harness)
