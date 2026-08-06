#![no_main]
//! Fuzz the gRPC status-trailer parser (percent-decoded `grpc-message`,
//! `grpc-status` code parsing) and the `-bin`/base64 metadata decoder --
//! both parse attacker-controlled bytes arriving in HTTP/2 and HTTP/3
//! trailer frames.
use http::HeaderMap;
use libfuzzer_sys::fuzz_target;
use oxirpc_core::wire::trailer::{parse_trailers, parse_trailers_only};
use oxirpc_core::Metadata;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let (head, rest) = data.split_at(1);
    let split_at = if rest.is_empty() {
        0
    } else {
        (head[0] as usize) % rest.len()
    };
    let (status_bytes, message_bytes) = rest.split_at(split_at);

    let mut headers = HeaderMap::new();
    if let Ok(v) = http::HeaderValue::from_bytes(status_bytes) {
        headers.insert("grpc-status", v);
    }
    if let Ok(v) = http::HeaderValue::from_bytes(message_bytes) {
        headers.insert("grpc-message", v);
    }

    // Percent-decoding + status-code parsing must never panic on arbitrary
    // trailer bytes -- only ever return `Ok(..)` or a typed `Err(..)`.
    let _ = parse_trailers(&headers);
    let _ = parse_trailers_only(&headers);

    // The `-bin` metadata decoder (base64) must likewise never panic.
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = Metadata::decode_wire_bin(s);
    }
});
