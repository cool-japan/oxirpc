# oxirpc-core TODO

## Status
Re-exports tonic core types (Code, Request, Response, Status, IntoRequest) plus
native Pure-Rust primitives: `StatusCode` (all 17 codes), `Metadata` (ascii +
binary `-bin`/base64), `grpc-timeout` parse/format, and `CompressionEncoding`
(Identity/Gzip/Zstd) with an `Encoding` trait and compress/decompress dispatch
backed by OxiARC (oxiarc-deflate for gzip, oxiarc-zstd for zstd). `OxiRpcError`
is `#[non_exhaustive]` with Status/Transport/Build/Tls/Compression/Proto/Timeout/
Cancelled variants and an `OxiRpcResult<T>` alias. TLS module provides Pure Rust
client/server config via OxiTLS (rustls-rustcrypto). ~890 SLOC production code.

## Core Implementation
- [x] Implement native `StatusCode` enum (all 17 gRPC status codes) independent of tonic
- [x] Implement native `Metadata` type: typed headers (ascii + binary) with insertion/lookup
- [x] Native message/RPC primitives: `Status`, `Request<T>`, `Response<T>`, `Interceptor`, `MessageCodec`, `CancellationToken`, `Streaming<T>` (completed 2026-05-25)
  - **Goal:** A Pure-Rust native surface in `oxirpc-core` living in dedicated submodules to avoid the crate-root `pub use tonic::{Code, IntoRequest, Request, Response, Status}` collision.
  - **Design:** `oxirpc_core::rpc::Status { code: StatusCode, message: String, details: Vec<u8>, metadata: Metadata }` with ok/new/with_details/with_metadata ctors and tonic bridges. `oxirpc_core::message::{Request<T>, Response<T>}` mirroring tonic API using `http::Extensions`. `oxirpc_core::interceptor::Interceptor` — sync object-safe trait (`&self`) + `AsyncInterceptor` reserved stub. `oxirpc_core::codec::MessageCodec<T>` (name avoids `encoding::Encoding` clash) with `IdentityCodec`. `oxirpc_core::cancel::CancellationToken { flag: Arc<AtomicBool> }` sync by default, async wait gated behind `tokio` feature. `oxirpc_core::stream::Streaming<T>` wraps `Pin<Box<dyn Stream<Item=Result<T,Status>>+Send>>`, requires `futures-core` dep.
  - **Files:** new `src/rpc.rs`, `src/message.rs`, `src/interceptor.rs`, `src/codec.rs`, `src/cancel.rs`, `src/stream.rs`; extend `src/lib.rs` with `mod`/`pub use` (submodule paths only, NOT crate root); add `futures-core` to `Cargo.toml` (workspace = true).
  - **Prerequisites:** add `futures-core` to root `[workspace.dependencies]` if absent; confirm `http::Extensions` is accessible.
  - **Tests:** `tests/core_types.rs` — Status tonic round-trip; Request/Response map+metadata; MessageCodec identity; CancellationToken parent/child propagation; Streaming collects + errors; Interceptor chain-of-one.
  - **Risk:** crate-root name collision mitigated by submodules. `futures-core` Pure Rust. Object-safety via `&self`. Each new file < 2000 lines.
- [x] Implement native gRPC wire framing: `encode_grpc_frame` / `decode_grpc_frame` (1-byte flag + 4-byte BE length + payload), `encode_message` / `decode_message` with prost + optional compression, `FrameIterator` for multi-frame slices, and `ProstCodec<T>` implementing `MessageCodec<T>` for any `prost::Message + Default`. (grpc.rs, completed 2026-05-26)
- [x] Implement `Encoding` trait for message serialization (proto, JSON, custom)
- [x] Implement gRPC compression types: `CompressionEncoding` enum (Identity, Gzip, Zstd)
- [x] Implement gRPC compression via OxiARC: gzip using oxiarc-deflate, zstd using oxiarc-zstd
- [x] Implement deadline/timeout propagation: `grpc-timeout` header parsing and format
- [x] Add `OxiRpcError::Compression(String)` variant for compression failures
- [x] Add `OxiRpcError::Timeout` variant for deadline exceeded
- [x] Add `OxiRpcError::Cancelled` variant for client cancellation

## API Improvements
- [x] Make `OxiRpcError` implement `From<OxiProtoError>` for proto decode failures (added `Proto` variant + `From<TimeoutError>`/`From<MetadataError>`)
- [x] Add `#[non_exhaustive]` to `OxiRpcError`
- [x] Add `OxiRpcResult<T>` type alias
- [x] Add `Metadata::get_bin(key)` for binary metadata values (base64-encoded)
- [x] Implement `Display` for `StatusCode` with canonical names

## Testing
- [x] Test all 17 gRPC status codes: OK, Cancelled, Unknown, InvalidArgument, DeadlineExceeded, NotFound, AlreadyExists, PermissionDenied, ResourceExhausted, FailedPrecondition, Aborted, OutOfRange, Unimplemented, Internal, Unavailable, DataLoss, Unauthenticated
- [x] Test metadata insertion/lookup with ASCII and binary keys (core_types.rs: metadata_ascii_insert_get, metadata_binary_insert_get, metadata_key_kind_mismatch, etc.)
- [x] Test gRPC timeout header parsing ("5S", "100m", "1000000u", "1H") (timeout.rs inline tests: parse_units, parse_rejects_bad_input, format_round_trips)
- [x] Test TLS client_config and server_config with self-signed certs (core_types.rs tls_tests module, 2026-05-26)
- [x] Test compression round-trip: compress -> decompress preserves message (core_types.rs: encoding_gzip_round_trip, encoding_zstd_round_trip, encoding_identity_is_passthrough)
- [x] Test error conversions: tonic::Status -> OxiRpcError, TimeoutError -> OxiRpcError, all display variants (core_types.rs, 2026-05-26)

## Performance
- [x] Criterion-based benchmarks for codec / compression / metadata / channel pool / retry (planned 2026-05-26)
  - **Goal:** Stand up criterion benchmark harnesses. OxiArc throughput baseline (flate2 banned by COOLJAPAN policy).
  - **Design (core part):**
    - new `crates/oxirpc-core/benches/frame_codec.rs`: bench `encode_grpc_frame`/`decode_grpc_frame` for 64B/1KiB/64KiB/1MiB payloads.
    - new `crates/oxirpc-core/benches/compression_throughput.rs`: bench `encoding::compress(Gzip, ...)`/`decompress` for same sizes; report MB/s.
    - new `crates/oxirpc-core/benches/metadata_lookup.rs`: bench `Metadata::get` over N=8/64/512 entries.
    - new `crates/oxirpc-core/benches/tls_config_construction.rs`: bench `tls::server_config(...)` cold construction; gated on `tls` feature.
    - extend `crates/oxirpc-core/Cargo.toml`: criterion = { workspace = true, features = ["html_reports"] } dev-dep + [[bench]] harness=false sections.
  - **Files:** new benches/*.rs (4 files), extend Cargo.toml
  - **Tests:** N/A — benchmarks. Verify `cargo bench -p oxirpc-core --no-run` compiles.

## Integration
- [x] Type exclusivity audit + conversion helpers: `From<OxiRpcError> for tonic::Status`, `GrpcStatusCode ↔ StatusCode`, `StatusCode::from_i32`/`as_i32` (done 2026-05-27 — From<OxiRpcError> for tonic::Status, GrpcStatusCode↔StatusCode, StatusCode::as_i32, from_status_code)
  - **Goal:** Bidirectional conversion helpers making native `StatusCode`/`OxiRpcError` a first-class alternative to tonic types. Additive — does NOT break existing APIs.
  - **Files:** extend `src/lib.rs` (`From<OxiRpcError> for tonic::Status`, `from_status_code` helper), extend `src/status.rs` (`from_i32`, `as_i32`), extend `src/h2.rs` (`GrpcStatusCode ↔ StatusCode`), new `tests/type_conversions.rs`
  - **Tests:** 12 tests covering all OxiRpcError→Status mappings, wire encoding roundtrips, GrpcStatusCode↔StatusCode conversions, from_status_code constructor
- [x] Ensure TLS config works with tonic transport — RESOLVED: Pure-Rust TLS works on both tonic-transport (via `PureRustTlsConnector`) and native channel; tonic's own FFI-gated TLS intentionally not wired per Pure-Rust Policy (completed 2026-05-30, R14)
  - **Goal:** Close this item. The `oxirpc-core::tls::{client_config,server_config}` (rustls via `oxitls::pure_provider()`) pair works on both paths. No tonic 1.0 required.
  - **Design:** No new code in oxirpc-core. Evidence: `PureRustTlsConnector` (oxirpc-client) bridges tonic transport; `native_channel::connection` has AnyIo::Tls; server has `tls_acceptor`/`incoming_tls`. R13 added a native-channel TLS roundtrip test; R14 adds the tonic-transport connector test.
  - **Files:** See oxirpc-client and oxirpc-server for the implementation.
  - **Tests:** Covered by oxirpc-client R13 native TLS test + R14 tonic-transport connector test.
  - **Risk:** None — this is a decision closure, not new code.
- [x] Coordinate compression with OxiARC (oxiarc-deflate for gzip, oxiarc-zstd for zstd)
