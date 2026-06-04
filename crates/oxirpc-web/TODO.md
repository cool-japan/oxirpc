# oxirpc-web TODO

## Status
Wraps tonic-web (`GrpcWebLayer`, `GrpcWebService`, `grpc_web_layer()`) and adds native Pure-Rust primitives: a `codec` module (5-byte gRPC-Web frame encode/decode, trailer frames, base64 text mode) and a `cors` module (`CorsPolicy` builder producing preflight + response header sets). ~326 SLOC production code.

## Core Implementation
- [x] gRPC-Web spec completion: negotiation, header xlate, stream sequencer, `GrpcWebConfig`, **FLAG_COMPRESSED wire-level handling (Phase 1 closure)** (done 2026-05-25)
  - **Goal:** Make the gRPC-Web codec spec-complete. Critically, close root TODO Phase 1 by actually wiring compress/decompress when FLAG_COMPRESSED is set (currently parsed but ignored).
  - **Design:** `negotiate(content_type: &str) -> Option<WebMode { Binary, Text }>`. Header translation: `grpc_to_http_headers(Metadata) -> Vec<(String,String)>` and reverse, reusing `metadata::base64_*` for `-bin` keys. `StreamSequencer` — stateful incremental decoder with `push(&mut self, chunk) -> Vec<Frame>` (handles split 5-byte length-prefix). `GrpcWebConfig { mode: WebMode, compression: CompressionEncoding, max_message_size }`. Phase 1 closure: in `codec.rs`, `encode_frame`/`encode_body` call `oxirpc_core::encoding::compress(enc,payload)` + set FLAG_COMPRESSED; `decode_body` checks FLAG_COMPRESSED and calls `decompress`. Wire encode/decode to accept `CompressionEncoding` param. Enable `oxirpc-core` compression/gzip/zstd features (OxiARC — Pure Rust).
  - **Files:** `src/codec.rs` (extend for compression); new `src/negotiate.rs` or extend `src/lib.rs`; `Cargo.toml` — enable `oxirpc-core` gzip/zstd features.
  - **Prerequisites:** `oxirpc_core::encoding::{compress,decompress}` exist (verified); enable feature.
  - **Tests:** `tests/codec_cors.rs` extensions — negotiate binary/text; header round-trip incl -bin keys; sequencer reassembles frames split at byte boundaries; compressed frame round-trip (gzip encode→FLAG set→decode restores payload); oversized message rejected.
  - **Risk:** OxiARC stays Pure Rust. Sequencer split-chunk edge cases covered by tests. This closes Phase 1 in root TODO.md.
- [x] Implement gRPC-Web base64 message framing: 1-byte frame type + 4-byte length + payload (codec module)
- [x] Implement gRPC-Web text mode: base64-encode entire response body for `application/grpc-web-text`
- [x] Implement Envoy-compatible framing: trailers in response body (length-prefixed trailer frame)
- [x] Implement CORS handling: configurable allowed origins, methods, headers, max-age (cors module)
- [x] Implement preflight OPTIONS request handler header set for CORS (`CorsPolicy::preflight_headers`)
- [x] Add configurable CORS policy builder: `CorsPolicy::new().allow_origin(...).allow_methods(...)`
- [x] Native gRPC-Web ↔ gRPC translation layer replacing tonic-web::GrpcWebLayer internals (done 2026-05-27)
  - **Goal:** `NativeGrpcWebLayer`/`NativeGrpcWebService<S>` translating inbound gRPC-Web HTTP/1.1 to gRPC HTTP/2, and packaging response+trailers as gRPC-Web body. Replaces tonic-web runtime dep; compatibility re-export stays.
  - **Design:**
    - new `crates/oxirpc-web/src/translate.rs`: `translate_request(req) -> Result<Request<Body>, FrameError>` (base64-decode for text mode, strip headers, set te: trailers + grpc content-type), `translate_response(resp, content_type) -> Response<BoxBody>` (buffer body, append trailer frame via codec.rs FLAG_TRAILER, base64-encode if text mode), `GrpcWebContentType` enum (Binary/Text/BinaryProto/TextProto).
    - new `crates/oxirpc-web/src/native.rs`: `NativeGrpcWebLayer { config }`, `NativeGrpcWebService<S> { inner, config }`, impl `tower::Layer` + `tower::Service<Request<Body>>` — pass through non-gRPC-Web and OPTIONS, otherwise translate.
    - Extend `crates/oxirpc-web/src/lib.rs`: re-export `NativeGrpcWebLayer`, `NativeGrpcWebService`, `native_grpc_web_layer()`, `native_grpc_web()`.
    - Extend tests (codec_cors.rs or new tests/native.rs): ~8 tests.
  - **Files:** new src/translate.rs, new src/native.rs, extend src/lib.rs, extend tests
  - **Tests:** native_translates_binary_request_to_grpc, native_translates_text_request_to_grpc, native_translates_response_appends_trailers_frame, native_text_response_base64_encodes_whole_body, native_passes_through_non_grpc_web_requests, native_passes_through_options_preflight, native_handles_grpc_status_2_error_in_trailer, native_layer_composes_with_cors_layer

## API Improvements
- [x] Add `grpc_web_layer_with_cors(cors_policy)` for custom CORS configuration (done 2026-05-25)
- [x] Add `grpc_web_layer_with_config(config)` for full configuration control (done 2026-05-25)
- [x] Add middleware for exposing gRPC-Web on a sub-path (e.g., `/grpc-web/` prefix) (done 2026-05-26)
- [x] Document HTTP/1.1 requirement: `accept_http1(true)` must be set on the server (done 2026-05-26)

## Testing
- [x] Test base64 message framing round-trip: encode -> decode preserves message (done 2026-05-26)
- [x] Test text mode encoding: binary -> base64 -> binary round-trip (done 2026-05-26)
- [x] Test CORS headers are present in preflight response (done 2026-05-25)
- [x] Test content-type negotiation: correct handling of all 4 content types (covered by plan block above)
- [x] Test trailer frame encoding: trailers appear in response body (done 2026-05-26)
- [x] Test server-streaming: multiple data frames followed by trailer frame (covered by plan block above)
- [ ] Integration test with a browser-based gRPC-Web client (via headless browser)
  - **BLOCKED: requires headless browser environment**
- [x] Test error responses: gRPC error status codes translated to HTTP status + trailers (done 2026-05-26 — `all_grpc_status_codes_in_trailers` covers codes 0-16; `error_trailer_with_unicode_message` covers UTF-8 grpc-message; `two_data_frames_then_trailer_roundtrip` covers server-streaming shape)

## Performance
- [x] Benchmark base64 encoding/decoding throughput (done 2026-05-27 — benches/web_bench.rs: bench_base64_encode_decode with 64/1024/65536 byte payloads)
- [x] Benchmark gRPC-Web overhead vs native gRPC (done 2026-05-27 — bench_binary_frame_encode + bench_binary_frame_decode vs bench_base64_encode_decode shows overhead)
- [x] Profile memory allocation for frame encoding — `benches/frame_alloc_memory.rs` with CountingAllocator; 1000 frames × sizes 64/1024/65536 (done 2026-05-29)

## Integration
- [x] Ensure grpc_web_layer works with oxirpc-server's ServerBuilder (documented in lib.rs crate-level doc: `accept_http1(true)` + `grpc_web_layer()` pattern for ServerBuilder integration, done 2026-05-26)
- [x] Test with oxirpc-health (done 2026-05-27 — health_request_roundtrips_through_grpc_web_codec, health_request_empty_service_roundtrip, health_response_not_serving_roundtrip, health_error_response_text_mode_roundtrip in tests/codec_cors.rs; native_layer_with_health_content_type in tests/native.rs)
- [x] Cross-crate integration test: NativeGrpcWebLayer wrapping NativeReflectionService — gRPC-Web-framed ListServices roundtrip (done 2026-05-27)
  - **Goal:** Prove gRPC-Web translation works end-to-end with a real native reflection service. Synthesize gRPC-Web-framed `ServerReflectionRequest` (ListServices), call through `NativeGrpcWebService<NativeReflectionServiceV1>`, decode response, assert service list.
  - **Files:** new `tests/reflect_integration.rs`, extend `Cargo.toml` (dev-deps: oxirpc-reflect, prost-types)
  - **Tests:** `reflect_list_services_via_grpc_web_binary`, `reflect_via_grpc_web_text_mode`, `reflect_file_by_name_via_grpc_web`, `reflect_unknown_service_returns_error_via_grpc_web`, `grpc_web_layer_preserves_reflection_service_name`
- [ ] Verify compatibility with grpc-web JavaScript/TypeScript client libraries
  - **BLOCKED: requires headless browser environment**
