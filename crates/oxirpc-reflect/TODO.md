# oxirpc-reflect TODO

## Status
Functional gRPC server reflection: v1 and v1alpha protocols via tonic-reflection wrapper. `reflection_service_from_static` / `reflection_service_owned` / `reflection_service_v1alpha_from_static` build mountable reflection services from encoded FileDescriptorSet bytes. ~60 SLOC production code.

## Core Implementation
- [x] Implement native ServerReflection service replacing tonic-reflection internals (done 2026-05-27)
  - **Goal:** A native `grpc.reflection.v1.ServerReflection` service (and `v1alpha` shim) implementing bidi-streaming `ServerReflectionInfo` RPC, backed by the existing `DescriptorPool`.
  - **Design:**
    - new `crates/oxirpc-reflect/src/proto.rs`: prost-derived `ServerReflectionRequest`, `ServerReflectionResponse`, `FileDescriptorResponse`, `ExtensionRequest`, `ExtensionNumberResponse`, `ListServiceResponse`, `ServiceResponse`, `ErrorResponse` with `prost(oneof)` tags.
    - new `crates/oxirpc-reflect/src/service.rs`: `NativeReflectionService { pool: Arc<DescriptorPool>, version: ReflectVersion }`, impl `NamedService` (v1 or v1alpha NAME), impl `tower::Service<Request<Body>>` dispatching bidi `ServerReflectionInfo`; per-request dispatch: ListServices, FileByFilename, FileContainingSymbol, FileContainingExtension, AllExtensionNumbers → ErrorResponse on miss.
    - Extend `crates/oxirpc-reflect/src/lib.rs`: `find_file_by_name`, `find_file_containing_symbol`, `find_file_containing_extension` on `DescriptorPool`; `ReflectionBuilder::build_native() -> (v1, v1alpha)`, `build_native_v1()`, deprecate old `build()`.
    - Extend `crates/oxirpc-reflect/Cargo.toml`: add tokio-stream if missing.
  - **Files:** new proto.rs, new service.rs, extend lib.rs, extend Cargo.toml, extend tests/reflect.rs (~10 new tests)
  - **Tests:** proto_request_oneof_roundtrips_bytes, list_services_returns_registered_names, file_by_filename_returns_correct_descriptor, file_containing_symbol_resolves_dotted_fqn, file_containing_symbol_strips_leading_dot, file_containing_extension_finds_right_file, all_extension_numbers_returns_expected, unknown_filename_returns_not_found_error_response, v1alpha_service_has_v1alpha_name, bidi_handler_responds_to_multiple_requests_in_order
- [x] Implement v1 ServerReflection service: ServerReflectionInfo bidirectional streaming RPC (done 2026-05-27 — NativeReflectionServiceV1 in service.rs)
- [x] Implement request handling: ListServices, GetFileByName, GetFileContainingSymbol, GetFileContainingExtension, GetAllExtensionNumbers (done 2026-05-27 — full handler in service.rs)
- [x] Implement v1alpha ServerReflection service (done 2026-05-27 — NativeReflectionServiceV1Alpha in service.rs)
- [x] Implement `DescriptorPoolBuilder` for programmatic descriptor registration without raw bytes (60-80 SLOC)
- [x] Add `reflection_service_from_pool(pool: DescriptorPool)` accepting pre-built pool (20-30 SLOC)
- [x] Add dynamic service registration: register/unregister services at runtime (60-80 SLOC)
- [x] Implement file descriptor serialization for on-the-wire transfer (40-50 SLOC)
- [x] Implement extension number listing from registered descriptors (40-50 SLOC)

## API Improvements
- [x] Reflect: replace `Vec::leak` with `Arc<[u8]>`, combined `ReflectionBuilder`, `ReflectError::Decode` (done 2026-05-25)
  - **Goal:** Remove the permanent memory leak (`Vec::leak`) and provide a single builder for v1 + v1alpha reflection.
  - **Design:** `reflection_service_owned` stores `Arc<[u8]>` and passes `&*arc` to `tonic_reflection::server::Builder::register_encoded_file_descriptor_set` (verified: builder is `Builder<'b>` over `&'b [u8]`, decodes eagerly on `build_v1()` into `Arc<FileDescriptorProto>` — borrow need not be `'static`; Arc stays alive in returned wrapper). `ReflectionBuilder { fds: Vec<Arc<[u8]>>, include_v1alpha: bool }` → `build()` returns v1 (+ optionally v1alpha) services. `ReflectError::Decode(prost::DecodeError)` new variant with Display + `source()` (current enum only has `Build`).
  - **Files:** `src/lib.rs`.
  - **Prerequisites:** verified Arc<[u8]> lifetime satisfies builder borrow within build() scope.
  - **Tests:** `tests/` — owned builder from real FDS registers cleanly; combined builder produces v1+v1alpha; Decode error surfaces on malformed FDS.
  - **Risk:** if builder retained `&'b` beyond build() we'd need `'static` (verified it does NOT). Keep Arc alive in returned service wrapper.
- [x] Add convenience: `register_service_named<S: NamedService>()` auto-registering by service name (done 2026-05-26)
- [x] Implement `Display` for `ReflectError` with more context

## Testing
- [x] Test ListServices returns all registered service names
- [x] Test GetFileByName returns correct FileDescriptorProto
- [x] Test GetFileContainingSymbol finds the right file for a message/service name
- [x] Test v1alpha compatibility with older grpcurl versions
- [x] Test dynamic registration: add a service, verify it appears in ListServices
- [x] Test register_service_named filter: matching, non-matching, partial, multi-filter (done 2026-05-26)
- [x] Test DescriptorPool encode/decode round-trip preserves service count (done 2026-05-26)
- [x] Test list_services empty on empty pool (done 2026-05-26)
- [x] Test error handling: unregistered service name returns NOT_FOUND
- [ ] Integration test with grpcurl/grpc_cli against running reflection service

## Performance
- [x] Benchmark descriptor lookup time for large service registries (100+ services)
- [x] Profile memory usage of leaked FDS bytes in reflection_service_owned — `benches/leaked_fds_memory.rs` with CountingAllocator; N=1/10/100 services (done 2026-05-29)
- [x] Extended descriptor pool benchmarks: `bench_find_file_containing_extension`, `bench_extension_numbers_of_type`, `bench_build_native_pair` + `pool_with_100_descriptors_handles_all_lookups` scaling test (done 2026-05-27 — bench_find_file_containing_extension, bench_extension_numbers_of_type, bench_build_native_pair in reflect_bench.rs; pool_with_100_descriptors_handles_all_lookups in tests/reflect.rs)

## Integration
- [x] Ensure oxirpc-server can auto-register reflection service — `ServerBuilder::reflection_service(fds)` under `reflect` feature (done 2026-05-29)
- [x] Ensure oxirpc-build generates FDS bytes compatible with reflection registration — `tests/reflect_compat.rs` in oxirpc-build (done 2026-05-29)
- [x] Use oxiproto-reflect DescriptorPool once native implementation exists (done 2026-05-30 — `PoolBackend::Oxiproto` + `NativeReflectionService::with_oxiproto_pool` + `ReflectionBuilder::register_oxiproto_pool` under `oxiproto` feature)
- [ ] Test interop with standard gRPC reflection clients (grpcurl, grpcui, Postman)
