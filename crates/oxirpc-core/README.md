# oxirpc-core — Core gRPC types, status, metadata, and wire codec for OxiRPC

[![Crates.io](https://img.shields.io/crates/v/oxirpc-core.svg)](https://crates.io/crates/oxirpc-core)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`oxirpc-core` is the runtime foundation of **OxiRPC**, the COOLJAPAN Pure-Rust gRPC stack. It provides the shared error type, native gRPC status codes, typed metadata, deadline/timeout parsing, message compression encodings, and the byte-level gRPC-over-HTTP/2 wire format that the rest of the ecosystem (`oxirpc-build`, `oxirpc-client`, `oxirpc-server`, `oxirpc-health`) builds on.

The crate re-exports tonic's request/response/status types for the current facade, while also exposing **native, dependency-light primitives** (`StatusCode`, `Metadata`, `CompressionEncoding`, the `wire` module) that downstream code can use without binding to tonic. Compression is Pure Rust via OxiARC (`oxiarc-deflate` for gzip, `oxiarc-zstd` for zstd) — no `flate2`, no C/FFI codec. TLS, when enabled, is Pure Rust via OxiTLS + `rustls-rustcrypto` (no `ring`, no `aws-lc`). The crate is `#![forbid(unsafe_code)]`.

## Installation

```toml
[dependencies]
oxirpc-core = "0.2.0"

# With Pure-Rust TLS config helpers:
oxirpc-core = { version = "0.2.0", features = ["tls"] }

# With gRPC message compression:
oxirpc-core = { version = "0.2.0", features = ["gzip", "zstd"] }
```

## Quick Start

```rust
use oxirpc_core::status::StatusCode;
use oxirpc_core::metadata::Metadata;

// Native gRPC status codes — wire-accurate, no tonic dependency required.
assert_eq!(StatusCode::Ok as i32, 0);
assert_eq!(StatusCode::from_i32(5), Some(StatusCode::NotFound));
assert_eq!(StatusCode::NotFound.as_str(), "NOT_FOUND");
assert!(StatusCode::Unavailable.is_retryable());

// Typed metadata: ASCII keys plus binary (`-bin`) keys.
let mut md = Metadata::new();
md.insert("x-trace-id", "abc123")?;
md.insert_bin("token-bin", b"\x00\x01\x02")?;
assert_eq!(md.get("x-trace-id"), Some("abc123"));
assert_eq!(md.get_bin("token-bin").unwrap(), vec![0, 1, 2]);
# Ok::<(), oxirpc_core::metadata::MetadataError>(())
```

### gRPC frame codec (wire-level)

```rust
use oxirpc_core::grpc::{encode_grpc_frame, decode_grpc_frame};

// Length-prefixed gRPC frame: 1 compression byte + 4 length bytes + payload.
let framed = encode_grpc_frame(b"hello", false);
let (compressed, payload) = decode_grpc_frame(&framed)?;
assert!(!compressed);
assert_eq!(payload, b"hello");
# Ok::<(), oxirpc_core::codec::CodecError>(())
```

### Message compression (requires `gzip`)

```rust
# #[cfg(feature = "gzip")]
# {
use oxirpc_core::encoding::{CompressionEncoding, compress, decompress};

let original = b"hello gRPC compression";
let packed = compress(CompressionEncoding::Gzip, original)?;
let unpacked = decompress(CompressionEncoding::Gzip, &packed)?;
assert_eq!(unpacked, original);
# }
# Ok::<(), oxirpc_core::encoding::EncodingError>(())
```

## API Overview

### Crate root re-exports

| Item | Origin | Notes |
|------|--------|-------|
| `Code`, `Request`, `Response`, `Status`, `IntoRequest` | re-exported from `tonic` | Facade types for the current tonic-backed path |
| `Metadata` | `metadata` module | Native typed-header multimap |
| `StatusCode` | `status` module | Native gRPC status enum |
| `ServerCompressionPrefs` | `encoding` module | Server-operator compression preferences |
| `OxiRpcError`, `OxiRpcResult<T>` | this crate | The shared error type and result alias |

### `OxiRpcError` (and `OxiRpcResult<T>`)

`#[non_exhaustive]` error enum. Converts to/from `tonic::Status` and from several lower-level error types.

| Variant | Meaning |
|---------|---------|
| `Status(tonic::Status)` | A gRPC status error |
| `Transport(String)` | Transport-layer failure (maps to `Status::unavailable`) |
| `Build(String)` | Build-time error (maps to `Status::internal`) |
| `Tls(String)` | TLS configuration or handshake error |
| `Compression(String)` | Compression/decompression failure |
| `Proto(String)` | Protobuf encode/decode failure |
| `Timeout` | Deadline expired (maps to `Status::deadline_exceeded`) |
| `Cancelled` | Operation cancelled (maps to `Status::cancelled`) |

| Method / impl | Description |
|---------------|-------------|
| `OxiRpcError::from_status_code(code, msg)` | Build a `Status` variant from a native `StatusCode` + message |
| `From<tonic::Status>` / `From<OxiRpcError> for tonic::Status` | Bidirectional bridge to tonic |
| `From<tonic::transport::Error>` | Wraps transport errors as `Transport` |
| `From<timeout::TimeoutError>` / `From<metadata::MetadataError>` | Lower-level error conversions |
| `From<oxiproto::OxiProtoError>` | Only with the `oxiproto` feature |

### `status::StatusCode` — all 17 gRPC codes

`#[repr(i32)]` enum whose discriminants equal the wire values (`StatusCode::NotFound as i32 == 5`). Derives `Copy`, `Eq`, `Hash`, `Ord`.

| Item | Description |
|------|-------------|
| Variants | `Ok`, `Cancelled`, `Unknown`, `InvalidArgument`, `DeadlineExceeded`, `NotFound`, `AlreadyExists`, `PermissionDenied`, `ResourceExhausted`, `FailedPrecondition`, `Aborted`, `OutOfRange`, `Unimplemented`, `Internal`, `Unavailable`, `DataLoss`, `Unauthenticated` |
| `StatusCode::ALL` | `[StatusCode; 17]` in numeric order |
| `from_i32(i32) -> Option<StatusCode>` | Parse a wire value (`None` if out of range) |
| `from_i32_lossy(i32) -> StatusCode` | Out-of-range maps to `Unknown` (gRPC convention) |
| `as_str()` | Canonical screaming-snake name (e.g. `"NOT_FOUND"`) |
| `is_retryable()` | Whether the code is conventionally retryable |
| `From<StatusCode> for tonic::Code` / `From<tonic::Code>` | Lossless interconversion |

### `metadata::Metadata`

A multimap of gRPC key/value pairs. Keys are normalised to lowercase ASCII; binary keys end in `-bin` and carry raw bytes (base64-encoded on the wire).

| Method | Description |
|--------|-------------|
| `new()` / `is_empty()` / `len()` | Construction and size |
| `is_binary_key(key)` | Whether `key` ends with `-bin` |
| `insert(key, value)` | Set an ASCII value (replacing prior) |
| `append(key, value)` | Append an ASCII value (multimap semantics) |
| `insert_bin(key, value)` | Set a binary value on a `-bin` key |
| `get(key)` / `get_all(key)` | First / all ASCII values |
| `get_bin(key)` | Decoded binary value for a `-bin` key |
| `contains_key(key)` / `remove(key)` | Membership and removal |
| `iter()` | Iterate `(&str, &[u8])` pairs |
| `to_wire()` | Serialise to `Vec<(String, String)>` (base64 for `-bin`) |
| `decode_wire_bin(value)` | Decode a base64 wire value to bytes |

`MetadataError` variants: `InvalidKey(String)`, `InvalidAsciiValue`, `InvalidBase64`, `KeyKindMismatch`. The constant `metadata::BINARY_SUFFIX` is `"-bin"`.

### `encoding` — message compression (OxiARC-backed)

| Item | Description |
|------|-------------|
| `CompressionEncoding` | `Identity` (default) / `Gzip` / `Zstd` |
| `CompressionEncoding::as_str()` | Wire token (`"identity"` / `"gzip"` / `"zstd"`) |
| `from_str_opt(token)` | Case-insensitive parse (`None` if unknown) |
| `is_compressing()` | `true` for everything except `Identity` |
| `negotiate(accept_header, prefs)` | Pick the first preference the peer accepts |
| `Encoding` trait | Pluggable compress/decompress backend (`Send + Sync`) |
| `compress(enc, data)` / `decompress(enc, data)` | Free-function dispatch to the backend |
| `decodable_encodings()` | Encodings the build can decode |
| `is_request_encoding_acceptable(...)` | Validate an inbound `grpc-encoding` |
| `accept_encoding_header_value(accept)` | Build a `grpc-accept-encoding` value |
| `EncodingError` | `Unsupported(CompressionEncoding)` / `Codec(String)`; converts into `OxiRpcError` |
| `ServerCompressionPrefs` | `{ send, accept }` server-operator preferences (re-exported at the crate root) |

### `codec::MessageCodec<T>`

| Item | Description |
|------|-------------|
| `MessageCodec<T>` trait | `encode(&self, msg, buf)` / `decode(&self, buf) -> T` (`Send + Sync`) |
| `IdentityCodec` | Passthrough codec treating `Vec<u8>` as the message |
| `CodecError` | `Encode(String)` / `Decode(String)` |

### `grpc` — wire framing + prost codec

| Item | Description |
|------|-------------|
| `encode_grpc_frame(payload, compressed)` | Build a length-prefixed gRPC frame |
| `decode_grpc_frame(data)` | Decode one frame → `(compressed, &[u8])` |
| `encode_message<T: Message>(...)` | Full message-encode pipeline |
| `decode_message<T: Message + Default>(...)` | Full message-decode pipeline |
| `FrameIterator<'a>` | Iterate multiple frames in a buffer |
| `ProstCodec<T>` | Adapts any `prost::Message + Default` to `MessageCodec` |

### `timeout` — gRPC deadline header

| Item | Description |
|------|-------------|
| `parse_grpc_timeout(value)` | Parse a `grpc-timeout` header into `Duration` |
| `format_grpc_timeout(dur)` | Format a `Duration` as a `grpc-timeout` value |
| `TimeoutError` | Parse error (converts into `OxiRpcError::Timeout`) |

### `rpc` — native request/response/status

| Item | Description |
|------|-------------|
| `rpc::Status` | Native status: `ok()`, `new(code, msg)`, `with_details`, `with_metadata`, `is_ok`; bridges to/from `tonic::Status` |
| `message::Request<T>` | Native request envelope: `new`, `into_inner`, `get_ref`/`get_mut`, `metadata`/`metadata_mut`, `extensions`/`extensions_mut`, `map` |
| `message::Response<T>` | Native response envelope (same accessor surface as `Request<T>`) |
| `stream::Streaming<T>` | Type-erased message stream wrapper (`new`) |

### `interceptor` and `cancel`

| Item | Description |
|------|-------------|
| `interceptor::Interceptor` | Synchronous request interceptor trait (`Send + Sync`) |
| `interceptor::AsyncInterceptor` | Async request interceptor trait (`Send + Sync`) |
| `cancel::CancellationToken` | Cooperative cancellation: `new`, `cancel`, `is_cancelled`, `child` |

### `h2` — gRPC-over-HTTP/2 constants and header types

Phase 2 foundation: type-safe constants and header structs for the wire format. Does **not** implement transport.

| Item | Description |
|------|-------------|
| Constants | `GRPC_METHOD`, `GRPC_CONTENT_TYPE`, `GRPC_CONTENT_TYPE_PREFIX`, `GRPC_ENCODING`, `GRPC_ACCEPT_ENCODING`, `GRPC_STATUS`, `GRPC_MESSAGE`, `GRPC_TIMEOUT`, `GRPC_USER_AGENT_PREFIX` |
| `GrpcRequestHeaders` / `GrpcResponseHeaders` | Strongly-typed header builders |
| `GrpcStatusCode` | Wire status code enum for the H2 layer |
| `is_grpc_content_type(ct)` | Content-type predicate |

### `wire` — native gRPC-over-HTTP/2 byte format

The authoritative byte-level implementation: framing, headers, trailers, compression pipeline, deadlines, and body channels. Re-exported at the crate root.

| Item | Description |
|------|-------------|
| `Frame`, `FrameEncoder`, `FrameDecoder`, `FrameOptions` | Length-prefixed frame primitives |
| `MessagePipeline` | Encode/decode pipeline over frames |
| `NativeBody`, `NativeBodySender`, `body_channel()` | `http_body`-compatible streaming body + sender |
| `Deadline` | gRPC deadline tracking |
| `GrpcResponseStatus`, `WireError` | Response status + wire error type |
| `encode_grpc_message` / `decode_grpc_message` | Single-message encode/decode |
| `encode_grpc_message_with_encoding` / `decode_grpc_message_with_encoding` | …with compression negotiation |
| `read_unary_request` / `read_unary_request_with_encoding` | Read a unary request body |
| `unary_response_body` / `unary_response_body_compressed` | Build a unary response body |
| `streaming_response_body` / `bidi_sequential_response` | Build streaming response bodies |
| `bidi_sequential_response_with_encoding` | …with compression (requires `gzip`/`zstd`) |
| `grpc_response_headers` | Build response headers |
| `ok_grpc_trailers` / `error_grpc_trailers` / `error_response_body` | Trailer + error-body helpers |

### `tls` — Pure-Rust TLS config (feature `tls`)

Constructs `rustls` configs backed by `rustls-rustcrypto` via OxiTLS. The crypto provider is injected per-config — `CryptoProvider::install_default()` is never called.

| Function | Description |
|----------|-------------|
| `client_config(roots: RootCertStore)` | Build a `rustls::ClientConfig` trusting `roots` |
| `client_config_arc(roots)` | …returning `Arc<ClientConfig>` |
| `server_config(cert_pem, key_pem)` | Build a `rustls::ServerConfig` from PEM bytes |
| `server_config_arc(...)` | …returning `Arc<ServerConfig>` |

### `compression` — legacy gzip helper (feature `compression`)

Legacy module retained for back-compat: `OxiArcGzip` (compress/decompress via `oxiarc-deflate`) and `CompressionError`. New code should prefer the `encoding` module.

## Feature Flags

| Feature | Enables | Pure Rust |
|---------|---------|-----------|
| `tls` | `tls` module (`rustls` + OxiTLS RustCrypto provider) | Yes |
| `gzip` | gzip encoding in `encoding`/`wire` via `oxiarc-deflate` | Yes |
| `zstd` | zstd encoding in `encoding`/`wire` via `oxiarc-zstd` | Yes |
| `compression` | legacy `compression::OxiArcGzip` helper via `oxiarc-deflate` | Yes |
| `oxiproto` | `From<oxiproto::OxiProtoError> for OxiRpcError` | Yes |

`default = []` — the crate is dependency-light out of the box.

## Error-variant reference

See the **`OxiRpcError`** table above for the crate-wide error. Module-local errors that convert into it: `metadata::MetadataError`, `timeout::TimeoutError`, `encoding::EncodingError`, `codec::CodecError`, and `wire::WireError`.

## Cross-references

`oxirpc-core` is consumed by every other crate in the workspace:

- [`oxirpc`](../oxirpc) — top-level facade re-exporting the stack.
- [`oxirpc-build`](../oxirpc-build) — `.proto` → service-stub code generation.
- [`oxirpc-client`](../oxirpc-client) — channel/client builders (uses `tls`, `encoding`, `wire`).
- [`oxirpc-server`](../oxirpc-server) — server/router (uses `ServerCompressionPrefs`, `wire`, `tls`).
- [`oxirpc-health`](../oxirpc-health) — gRPC health-checking service.
- `oxirpc-reflect`, `oxirpc-web`, `oxirpc-adapter-aws-lc` — reflection, grpc-web, and an opt-in aws-lc provider adapter.

## License

Apache-2.0 — COOLJAPAN OU (Team Kitasan)
