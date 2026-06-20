//! Tests for the native core types: StatusCode, Metadata, CompressionEncoding,
//! and gRPC wire framing / ProstCodec.

use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_core::metadata::{base64_decode, base64_encode, Metadata, MetadataError};
use oxirpc_core::status::StatusCode;

// ─── StatusCode ───────────────────────────────────────────────────────────────

#[test]
fn status_code_numeric_values_match_spec() {
    assert_eq!(StatusCode::Ok as i32, 0);
    assert_eq!(StatusCode::Cancelled as i32, 1);
    assert_eq!(StatusCode::Unknown as i32, 2);
    assert_eq!(StatusCode::InvalidArgument as i32, 3);
    assert_eq!(StatusCode::DeadlineExceeded as i32, 4);
    assert_eq!(StatusCode::NotFound as i32, 5);
    assert_eq!(StatusCode::AlreadyExists as i32, 6);
    assert_eq!(StatusCode::PermissionDenied as i32, 7);
    assert_eq!(StatusCode::ResourceExhausted as i32, 8);
    assert_eq!(StatusCode::FailedPrecondition as i32, 9);
    assert_eq!(StatusCode::Aborted as i32, 10);
    assert_eq!(StatusCode::OutOfRange as i32, 11);
    assert_eq!(StatusCode::Unimplemented as i32, 12);
    assert_eq!(StatusCode::Internal as i32, 13);
    assert_eq!(StatusCode::Unavailable as i32, 14);
    assert_eq!(StatusCode::DataLoss as i32, 15);
    assert_eq!(StatusCode::Unauthenticated as i32, 16);
}

#[test]
fn status_code_all_round_trip_i32() {
    for code in StatusCode::ALL {
        let n = code as i32;
        assert_eq!(StatusCode::from_i32(n), Some(code));
    }
    assert_eq!(StatusCode::ALL.len(), 17);
}

#[test]
fn status_code_from_i32_out_of_range() {
    assert_eq!(StatusCode::from_i32(-1), None);
    assert_eq!(StatusCode::from_i32(17), None);
    assert_eq!(StatusCode::from_i32_lossy(99), StatusCode::Unknown);
}

#[test]
fn status_code_display_canonical_names() {
    assert_eq!(StatusCode::Ok.to_string(), "OK");
    assert_eq!(StatusCode::NotFound.to_string(), "NOT_FOUND");
    assert_eq!(
        StatusCode::DeadlineExceeded.to_string(),
        "DEADLINE_EXCEEDED"
    );
    assert_eq!(StatusCode::Unauthenticated.to_string(), "UNAUTHENTICATED");
}

#[test]
fn status_code_retryable() {
    assert!(StatusCode::Unavailable.is_retryable());
    assert!(StatusCode::ResourceExhausted.is_retryable());
    assert!(!StatusCode::NotFound.is_retryable());
    assert!(!StatusCode::Ok.is_retryable());
}

#[test]
fn status_code_tonic_interconvert() {
    for code in StatusCode::ALL {
        let tonic_code: tonic::Code = code.into();
        let back: StatusCode = tonic_code.into();
        assert_eq!(code, back, "round-trip via tonic::Code failed for {code}");
    }
}

// ─── Per-variant StatusCode tests ────────────────────────────────────────────

#[test]
fn test_status_code_ok() {
    let code = StatusCode::Ok;
    assert_eq!(code as i32, 0);
    assert_eq!(StatusCode::from_i32(0), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_cancelled() {
    let code = StatusCode::Cancelled;
    assert_eq!(code as i32, 1);
    assert_eq!(StatusCode::from_i32(1), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_unknown() {
    let code = StatusCode::Unknown;
    assert_eq!(code as i32, 2);
    assert_eq!(StatusCode::from_i32(2), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_invalid_argument() {
    let code = StatusCode::InvalidArgument;
    assert_eq!(code as i32, 3);
    assert_eq!(StatusCode::from_i32(3), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_deadline_exceeded() {
    let code = StatusCode::DeadlineExceeded;
    assert_eq!(code as i32, 4);
    assert_eq!(StatusCode::from_i32(4), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_not_found() {
    let code = StatusCode::NotFound;
    assert_eq!(code as i32, 5);
    assert_eq!(StatusCode::from_i32(5), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_already_exists() {
    let code = StatusCode::AlreadyExists;
    assert_eq!(code as i32, 6);
    assert_eq!(StatusCode::from_i32(6), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_permission_denied() {
    let code = StatusCode::PermissionDenied;
    assert_eq!(code as i32, 7);
    assert_eq!(StatusCode::from_i32(7), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_resource_exhausted() {
    let code = StatusCode::ResourceExhausted;
    assert_eq!(code as i32, 8);
    assert_eq!(StatusCode::from_i32(8), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_failed_precondition() {
    let code = StatusCode::FailedPrecondition;
    assert_eq!(code as i32, 9);
    assert_eq!(StatusCode::from_i32(9), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_aborted() {
    let code = StatusCode::Aborted;
    assert_eq!(code as i32, 10);
    assert_eq!(StatusCode::from_i32(10), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_out_of_range() {
    let code = StatusCode::OutOfRange;
    assert_eq!(code as i32, 11);
    assert_eq!(StatusCode::from_i32(11), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_unimplemented() {
    let code = StatusCode::Unimplemented;
    assert_eq!(code as i32, 12);
    assert_eq!(StatusCode::from_i32(12), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_internal() {
    let code = StatusCode::Internal;
    assert_eq!(code as i32, 13);
    assert_eq!(StatusCode::from_i32(13), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_unavailable() {
    let code = StatusCode::Unavailable;
    assert_eq!(code as i32, 14);
    assert_eq!(StatusCode::from_i32(14), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_data_loss() {
    let code = StatusCode::DataLoss;
    assert_eq!(code as i32, 15);
    assert_eq!(StatusCode::from_i32(15), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

#[test]
fn test_status_code_unauthenticated() {
    let code = StatusCode::Unauthenticated;
    assert_eq!(code as i32, 16);
    assert_eq!(StatusCode::from_i32(16), Some(code));
    assert!(!code.as_str().is_empty());
    assert!(code.to_string().contains(code.as_str()));
    assert!(!code.is_retryable());
    let tonic_code: tonic::Code = code.into();
    let back: StatusCode = tonic_code.into();
    assert_eq!(code, back);
}

// ─── Metadata ───────────────────────────────────────────────────────────────

#[test]
fn metadata_ascii_insert_get() {
    let mut md = Metadata::new();
    md.insert("x-trace-id", "abc123").unwrap();
    assert_eq!(md.get("x-trace-id"), Some("abc123"));
    // Case-insensitive lookup.
    assert_eq!(md.get("X-Trace-Id"), Some("abc123"));
    assert_eq!(md.len(), 1);
    assert!(!md.is_empty());
}

#[test]
fn metadata_binary_insert_get() {
    let mut md = Metadata::new();
    md.insert_bin("token-bin", &[0u8, 1, 2, 255]).unwrap();
    assert_eq!(md.get_bin("token-bin"), Some(vec![0, 1, 2, 255]));
    // Plain getter on a -bin key returns None.
    assert_eq!(md.get("token-bin"), None);
}

#[test]
fn metadata_key_kind_mismatch() {
    let mut md = Metadata::new();
    // ASCII insert on -bin key fails.
    assert_eq!(
        md.insert("foo-bin", "x"),
        Err(MetadataError::KeyKindMismatch)
    );
    // Binary insert on non -bin key fails.
    assert_eq!(
        md.insert_bin("foo", &[1]),
        Err(MetadataError::KeyKindMismatch)
    );
}

#[test]
fn metadata_invalid_key_rejected() {
    let mut md = Metadata::new();
    assert!(matches!(
        md.insert("bad key!", "v"),
        Err(MetadataError::InvalidKey(_))
    ));
    assert!(matches!(
        md.insert("", "v"),
        Err(MetadataError::InvalidKey(_))
    ));
}

#[test]
fn metadata_invalid_ascii_value_rejected() {
    let mut md = Metadata::new();
    // Newline is not a printable ASCII value char.
    assert_eq!(
        md.insert("k", "line1\nline2"),
        Err(MetadataError::InvalidAsciiValue)
    );
}

#[test]
fn metadata_append_multivalue() {
    let mut md = Metadata::new();
    md.append("k", "a").unwrap();
    md.append("k", "b").unwrap();
    let all = md.get_all("k");
    assert_eq!(all, vec!["a", "b"]);
}

#[test]
fn metadata_remove_and_contains() {
    let mut md = Metadata::new();
    md.insert("k", "v").unwrap();
    assert!(md.contains_key("K"));
    assert!(md.remove("k"));
    assert!(!md.contains_key("k"));
    assert!(!md.remove("k"));
}

#[test]
fn metadata_to_wire_encodes_binary() {
    let mut md = Metadata::new();
    md.insert_bin("data-bin", &[0xde, 0xad, 0xbe, 0xef])
        .unwrap();
    let wire = md.to_wire();
    assert_eq!(wire.len(), 1);
    let (k, v) = &wire[0];
    assert_eq!(k, "data-bin");
    // Decoding the wire value yields the original bytes.
    let decoded = Metadata::decode_wire_bin(v).unwrap();
    assert_eq!(decoded, vec![0xde, 0xad, 0xbe, 0xef]);
}

#[test]
fn metadata_is_binary_key() {
    assert!(Metadata::is_binary_key("trace-bin"));
    assert!(!Metadata::is_binary_key("trace"));
    // "-bin" alone (no name part) is not considered a binary key.
    assert!(!Metadata::is_binary_key("-bin"));
}

// ─── base64 ───────────────────────────────────────────────────────────────────

#[test]
fn base64_round_trip_all_lengths() {
    for len in 0..=32usize {
        let data: Vec<u8> = (0..len).map(|i| (i * 7 % 256) as u8).collect();
        let encoded = base64_encode(&data);
        let decoded = base64_decode(&encoded).expect("decode");
        assert_eq!(decoded, data, "round-trip failed at len {len}");
    }
}

#[test]
fn base64_decode_tolerates_padding() {
    // "Zm9v" = "foo"; with padding it should also decode.
    assert_eq!(base64_decode("Zm9v").unwrap(), b"foo");
    assert_eq!(base64_decode("Zm8=").unwrap(), b"fo");
    assert_eq!(base64_decode("Zg==").unwrap(), b"f");
}

#[test]
fn base64_decode_rejects_invalid() {
    assert!(base64_decode("@@@@").is_none());
    // A lone trailing symbol is invalid.
    assert!(base64_decode("A").is_none());
}

// ─── CompressionEncoding ────────────────────────────────────────────────────

#[test]
fn encoding_token_round_trip() {
    for enc in [
        CompressionEncoding::Identity,
        CompressionEncoding::Gzip,
        CompressionEncoding::Zstd,
    ] {
        let token = enc.as_str();
        assert_eq!(CompressionEncoding::from_str_opt(token), Some(enc));
    }
    assert_eq!(
        CompressionEncoding::from_str_opt("GZIP"),
        Some(CompressionEncoding::Gzip)
    );
    assert_eq!(CompressionEncoding::from_str_opt("br"), None);
}

#[test]
fn encoding_negotiate() {
    // Peer accepts gzip + identity; we prefer zstd then gzip.
    let chosen = CompressionEncoding::negotiate(
        "identity, gzip",
        &[CompressionEncoding::Zstd, CompressionEncoding::Gzip],
    );
    assert_eq!(chosen, CompressionEncoding::Gzip);

    // No overlap → identity.
    let none = CompressionEncoding::negotiate("identity", &[CompressionEncoding::Zstd]);
    assert_eq!(none, CompressionEncoding::Identity);
}

#[cfg(feature = "gzip")]
#[test]
fn encoding_gzip_round_trip() {
    use oxirpc_core::encoding::{compress, decompress};
    let original = b"hello gRPC compression via gzip backend";
    let packed = compress(CompressionEncoding::Gzip, original).expect("compress");
    assert_eq!(packed[0], 0x1f, "gzip magic byte 0");
    let unpacked = decompress(CompressionEncoding::Gzip, &packed).expect("decompress");
    assert_eq!(unpacked.as_slice(), original.as_slice());
}

#[cfg(feature = "zstd")]
#[test]
fn encoding_zstd_round_trip() {
    use oxirpc_core::encoding::{compress, decompress};
    let original = b"hello gRPC compression via zstd backend -- AAAAAAAAAAAAAAAAAA";
    let packed = compress(CompressionEncoding::Zstd, original).expect("compress");
    let unpacked = decompress(CompressionEncoding::Zstd, &packed).expect("decompress");
    assert_eq!(unpacked.as_slice(), original.as_slice());
}

#[test]
fn encoding_identity_is_passthrough() {
    use oxirpc_core::encoding::{compress, decompress};
    let original = b"unchanged";
    let packed = compress(CompressionEncoding::Identity, original).unwrap();
    assert_eq!(packed.as_slice(), original.as_slice());
    let unpacked = decompress(CompressionEncoding::Identity, &packed).unwrap();
    assert_eq!(unpacked.as_slice(), original.as_slice());
}

// ─── Encoding negotiation edge cases ─────────────────────────────────────────

#[test]
fn encoding_negotiate_empty_header_yields_identity() {
    let result = CompressionEncoding::negotiate("", &[CompressionEncoding::Gzip]);
    assert_eq!(result, CompressionEncoding::Identity);
}

#[test]
fn encoding_negotiate_unknown_tokens_yields_identity() {
    let result = CompressionEncoding::negotiate(
        "br, snappy",
        &[CompressionEncoding::Gzip, CompressionEncoding::Zstd],
    );
    assert_eq!(result, CompressionEncoding::Identity);
}

#[test]
fn encoding_negotiate_mixed_case_with_whitespace_yields_gzip() {
    // "  GZIP  " — whitespace trimmed and case-folded by from_str_opt
    let result = CompressionEncoding::negotiate("  GZIP  ", &[CompressionEncoding::Gzip]);
    assert_eq!(result, CompressionEncoding::Gzip);
}

#[test]
fn encoding_negotiate_comma_only_yields_identity() {
    let result = CompressionEncoding::negotiate(",", &[CompressionEncoding::Gzip]);
    assert_eq!(result, CompressionEncoding::Identity);
}

// ─── EncodingError ───────────────────────────────────────────────────────────

#[test]
fn encoding_error_unsupported_display_contains_not_enabled() {
    use oxirpc_core::encoding::EncodingError;
    let msg = EncodingError::Unsupported(CompressionEncoding::Gzip).to_string();
    // The display says "is not enabled in this build"
    assert!(
        msg.contains("not enabled"),
        "expected 'not enabled' in: {msg}"
    );
}

#[test]
fn encoding_error_codec_display_contains_message() {
    use oxirpc_core::encoding::EncodingError;
    let msg = EncodingError::Codec("boom".to_string()).to_string();
    assert!(msg.contains("boom"), "expected 'boom' in: {msg}");
}

#[test]
fn encoding_error_converts_to_oxirpcerror_compression() {
    use oxirpc_core::encoding::EncodingError;
    use oxirpc_core::OxiRpcError;
    let err: OxiRpcError = EncodingError::Codec("bad compress".to_string()).into();
    assert!(
        matches!(err, OxiRpcError::Compression(_)),
        "expected Compression variant, got: {err:?}"
    );
}

// ─── rpc::Status ─────────────────────────────────────────────────────────────

#[test]
fn rpc_status_ok_is_ok() {
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;
    let s = Status::ok();
    assert!(s.is_ok());
    assert_eq!(s.code, StatusCode::Ok);
}

#[test]
fn rpc_status_new_carries_message() {
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;
    let s = Status::new(StatusCode::NotFound, "entity missing");
    assert_eq!(s.code, StatusCode::NotFound);
    assert_eq!(s.message, "entity missing");
    assert!(!s.is_ok());
}

#[test]
fn rpc_status_with_details_and_metadata() {
    use oxirpc_core::metadata::Metadata;
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;

    let mut md = Metadata::new();
    md.insert("x-req-id", "abc").unwrap();

    let s = Status::new(StatusCode::Internal, "boom")
        .with_details(vec![1, 2, 3])
        .with_metadata(md);

    assert_eq!(s.details, vec![1, 2, 3]);
    assert_eq!(s.metadata.get("x-req-id"), Some("abc"));
}

#[test]
fn rpc_status_tonic_round_trip() {
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;

    let native = Status::new(StatusCode::Unavailable, "retry later");
    let tonic_s: tonic::Status = native.clone().into();
    let back: Status = tonic_s.into();

    assert_eq!(back.code, StatusCode::Unavailable);
    assert_eq!(back.message, "retry later");
}

#[test]
fn rpc_status_display_contains_code() {
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;
    let s = Status::new(StatusCode::PermissionDenied, "no access");
    let text = s.to_string();
    assert!(text.contains("PERMISSION_DENIED"));
    assert!(text.contains("no access"));
}

// ─── message::Request and message::Response ──────────────────────────────────

#[test]
fn request_new_into_inner() {
    use oxirpc_core::message::Request;
    let req = Request::new(42u32);
    assert_eq!(*req.get_ref(), 42u32);
    assert_eq!(req.into_inner(), 42u32);
}

#[test]
fn request_map_transforms_message() {
    use oxirpc_core::message::Request;
    let req: Request<u64> = Request::new(7u32).map(|x| x as u64);
    assert_eq!(req.into_inner(), 7u64);
}

#[test]
fn request_metadata_round_trip() {
    use oxirpc_core::message::Request;
    let mut req = Request::new(());
    req.metadata_mut().insert("x-id", "xyz").unwrap();
    assert_eq!(req.metadata().get("x-id"), Some("xyz"));
}

#[test]
fn response_new_into_inner() {
    use oxirpc_core::message::Response;
    let resp = Response::new("hello");
    assert_eq!(*resp.get_ref(), "hello");
    assert_eq!(resp.into_inner(), "hello");
}

#[test]
fn response_map_transforms_message() {
    use oxirpc_core::message::Response;
    let resp: oxirpc_core::message::Response<usize> =
        Response::new("hello world").map(|s: &str| s.len());
    assert_eq!(resp.into_inner(), 11);
}

#[test]
fn response_extensions_are_mutable() {
    use oxirpc_core::message::Response;
    let mut resp = Response::new(0u8);
    resp.extensions_mut().insert(42u32);
    assert_eq!(resp.extensions().get::<u32>().copied(), Some(42u32));
}

// ─── codec::MessageCodec / IdentityCodec ─────────────────────────────────────

#[test]
fn identity_codec_round_trip() {
    use oxirpc_core::codec::{IdentityCodec, MessageCodec};
    let codec = IdentityCodec;
    let msg = vec![1u8, 2, 3];
    let mut buf = Vec::new();
    codec.encode(&msg, &mut buf).unwrap();
    let decoded = codec.decode(&buf).unwrap();
    assert_eq!(decoded, msg);
}

#[test]
fn identity_codec_appends_to_existing_buf() {
    use oxirpc_core::codec::{IdentityCodec, MessageCodec};
    let codec = IdentityCodec;
    let mut buf = vec![0u8, 0];
    codec.encode(&vec![1, 2], &mut buf).unwrap();
    assert_eq!(buf, vec![0, 0, 1, 2]);
}

// ─── cancel::CancellationToken ────────────────────────────────────────────────

#[test]
fn cancellation_token_starts_uncancelled() {
    use oxirpc_core::cancel::CancellationToken;
    let tok = CancellationToken::new();
    assert!(!tok.is_cancelled());
}

#[test]
fn cancellation_token_cancel_is_visible() {
    use oxirpc_core::cancel::CancellationToken;
    let tok = CancellationToken::new();
    tok.cancel();
    assert!(tok.is_cancelled());
}

#[test]
fn cancellation_token_child_observes_parent_cancel() {
    use oxirpc_core::cancel::CancellationToken;
    let parent = CancellationToken::new();
    let child = parent.child();
    parent.cancel();
    assert!(child.is_cancelled());
}

#[test]
fn cancellation_token_parent_observes_child_cancel() {
    use oxirpc_core::cancel::CancellationToken;
    let parent = CancellationToken::new();
    let child = parent.child();
    child.cancel();
    assert!(parent.is_cancelled());
}

// ─── stream::Streaming ───────────────────────────────────────────────────────

#[test]
fn streaming_collects_items() {
    use futures_core::Stream;
    use oxirpc_core::rpc::Status;
    use oxirpc_core::stream::Streaming;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct VecStream {
        items: Vec<Result<u32, Status>>,
    }

    impl Stream for VecStream {
        type Item = Result<u32, Status>;
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.items.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.items.remove(0)))
            }
        }
    }

    let source = VecStream {
        items: vec![Ok(1), Ok(2), Ok(3)],
    };

    let mut streaming = Streaming::new(source);

    let waker = futures_task_noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut collected = Vec::new();
    loop {
        match Pin::new(&mut streaming).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(v))) => collected.push(v),
            Poll::Ready(Some(Err(_))) => panic!("unexpected error"),
            Poll::Ready(None) => break,
            Poll::Pending => panic!("should not pend"),
        }
    }
    assert_eq!(collected, vec![1, 2, 3]);
}

/// Minimal no-op waker for polling futures/streams in tests without a runtime.
fn futures_task_noop_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};

    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(|ptr| RawWaker::new(ptr, &VTABLE), |_| {}, |_| {}, |_| {});

    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

// ─── interceptor::Interceptor ────────────────────────────────────────────────

#[test]
fn interceptor_closure_passes_through() {
    use oxirpc_core::interceptor::Interceptor;
    use oxirpc_core::message::Request;
    use oxirpc_core::rpc::Status;

    let interceptor = |req: Request<()>| -> Result<Request<()>, Status> { Ok(req) };

    let req = Request::new(());
    let result = interceptor.intercept(req);
    assert!(result.is_ok());
}

#[test]
fn interceptor_closure_can_abort() {
    use oxirpc_core::interceptor::Interceptor;
    use oxirpc_core::message::Request;
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;

    let interceptor = |_req: Request<()>| -> Result<Request<()>, Status> {
        Err(Status::new(StatusCode::PermissionDenied, "denied"))
    };

    let req = Request::new(());
    let result = interceptor.intercept(req);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, StatusCode::PermissionDenied);
}

#[tokio::test]
async fn async_interceptor_closure_passes_through() {
    use oxirpc_core::interceptor::AsyncInterceptor;
    use oxirpc_core::message::Request;
    use oxirpc_core::rpc::Status;

    let interceptor = |mut req: Request<()>| async move {
        req.metadata_mut().insert("x-added", "yes").unwrap();
        Ok::<_, Status>(req)
    };

    let req = Request::new(());
    let out = interceptor.intercept_async(req).await;
    let req = out.expect("interceptor should pass through");
    assert_eq!(req.metadata().get("x-added"), Some("yes"));
}

#[tokio::test]
async fn async_interceptor_closure_can_abort() {
    use oxirpc_core::interceptor::AsyncInterceptor;
    use oxirpc_core::message::Request;
    use oxirpc_core::rpc::Status;
    use oxirpc_core::status::StatusCode;

    let interceptor = |_req: Request<()>| async move {
        Err::<Request<()>, Status>(Status::new(StatusCode::PermissionDenied, "denied"))
    };

    let req = Request::new(());
    let result = interceptor.intercept_async(req).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code, StatusCode::PermissionDenied);
}

// ─── OxiProto error bridge ────────────────────────────────────────────────────

#[cfg(feature = "oxiproto")]
mod oxiproto_bridge {
    use oxiproto::OxiProtoError;
    use oxirpc_core::OxiRpcError;

    #[test]
    fn oxiproto_parse_error_maps_to_proto_variant() {
        let err: OxiRpcError = OxiProtoError::ParseError("bad proto".into()).into();
        assert!(
            matches!(err, OxiRpcError::Proto(_)),
            "expected Proto variant, got: {err:?}"
        );
        let display = err.to_string();
        assert!(
            display.contains("bad proto"),
            "expected message in display: {display}"
        );
    }

    #[test]
    fn oxiproto_codegen_error_maps_to_build_variant() {
        let err: OxiRpcError = OxiProtoError::CodegenError("gen failed".into()).into();
        assert!(
            matches!(err, OxiRpcError::Build(_)),
            "expected Build variant, got: {err:?}"
        );
        let display = err.to_string();
        assert!(
            display.contains("gen failed"),
            "expected message in display: {display}"
        );
    }
}

// ─── gRPC wire framing ───────────────────────────────────────────────────────

#[allow(deprecated)]
#[test]
fn grpc_frame_encode_decode_round_trip() {
    use oxirpc_core::grpc::{decode_grpc_frame, encode_grpc_frame};

    let payload = b"hello grpc framing";
    let frame = encode_grpc_frame(payload, false);
    let (compressed, decoded) = decode_grpc_frame(&frame).expect("decode should succeed");
    assert!(!compressed);
    assert_eq!(decoded, payload);
}

#[allow(deprecated)]
#[test]
fn grpc_frame_uncompressed_flag() {
    use oxirpc_core::grpc::encode_grpc_frame;

    let frame = encode_grpc_frame(b"data", false);
    assert_eq!(frame[0], 0x00, "uncompressed flag must be 0x00");
    // Length field should match payload length.
    let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
    assert_eq!(len, 4u32);
}

#[allow(deprecated)]
#[test]
fn grpc_frame_compressed_flag() {
    use oxirpc_core::grpc::encode_grpc_frame;

    let frame = encode_grpc_frame(b"compressed-data", true);
    assert_eq!(frame[0], 0x01, "compressed flag must be 0x01");
}

#[allow(deprecated)]
#[test]
fn grpc_frame_truncated_errors() {
    use oxirpc_core::grpc::decode_grpc_frame;

    // Only 3 bytes — too short for a 5-byte header.
    let result = decode_grpc_frame(&[0x00, 0x00, 0x00]);
    assert!(result.is_err(), "truncated header should fail");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("too short") || msg.contains("truncated"),
        "error message was: {msg}"
    );
}

#[allow(deprecated)]
#[test]
fn grpc_frame_length_overflow() {
    use oxirpc_core::grpc::decode_grpc_frame;

    // Header claims 100 bytes of payload but only 10 bytes follow.
    let mut frame = vec![0x00u8]; // uncompressed
    frame.extend_from_slice(&100u32.to_be_bytes()); // claims 100 bytes
    frame.extend_from_slice(&[0u8; 10]); // only 10 bytes present
    let result = decode_grpc_frame(&frame);
    assert!(result.is_err(), "length overflow should fail");
}

#[allow(deprecated)]
#[test]
fn frame_iterator_multi_message() {
    use oxirpc_core::grpc::{encode_grpc_frame, FrameIterator};

    let payloads: &[&[u8]] = &[b"alpha", b"beta", b"gamma"];
    let mut concatenated = Vec::new();
    for p in payloads {
        concatenated.extend_from_slice(&encode_grpc_frame(p, false));
    }

    let items: Vec<_> = FrameIterator::new(&concatenated).collect();
    assert_eq!(items.len(), 3, "should have iterated 3 frames");
    for (i, result) in items.into_iter().enumerate() {
        let (compressed, payload) = result.expect("frame should be valid");
        assert!(!compressed);
        assert_eq!(payload, payloads[i]);
    }
}

// ─── ProstCodec ──────────────────────────────────────────────────────────────

#[test]
fn prost_codec_encode_decode_round_trip() {
    use oxirpc_core::codec::MessageCodec;
    use oxirpc_core::grpc::ProstCodec;
    use prost_types::Timestamp;

    let codec: ProstCodec<Timestamp> = ProstCodec::new();
    let ts = Timestamp {
        seconds: 1_700_000_000,
        nanos: 42,
    };

    let mut buf = Vec::new();
    codec.encode(&ts, &mut buf).expect("encode should succeed");
    let decoded: Timestamp = codec.decode(&buf).expect("decode should succeed");
    assert_eq!(decoded.seconds, ts.seconds);
    assert_eq!(decoded.nanos, ts.nanos);
}

// ─── encode_message / decode_message ─────────────────────────────────────────

#[allow(deprecated)]
#[test]
fn encode_message_identity_round_trip() {
    use oxirpc_core::encoding::CompressionEncoding;
    use oxirpc_core::grpc::{decode_message, encode_message};
    use prost_types::Timestamp;

    let ts = Timestamp {
        seconds: 1_234_567_890,
        nanos: 999,
    };
    let frame = encode_message(&ts, CompressionEncoding::Identity).expect("encode");
    let decoded: Timestamp = decode_message(&frame, CompressionEncoding::Identity).expect("decode");
    assert_eq!(decoded.seconds, ts.seconds);
    assert_eq!(decoded.nanos, ts.nanos);
}

#[allow(deprecated)]
#[cfg(feature = "gzip")]
#[test]
fn encode_message_gzip_round_trip() {
    use oxirpc_core::encoding::CompressionEncoding;
    use oxirpc_core::grpc::{decode_message, encode_message};
    use prost_types::Timestamp;

    let ts = Timestamp {
        seconds: 9_876_543_210,
        nanos: 111,
    };
    let frame = encode_message(&ts, CompressionEncoding::Gzip).expect("encode with gzip");
    // Frame flag byte should be 0x01 (compressed).
    assert_eq!(
        frame[0], 0x01,
        "gzip-encoded frame should have compressed flag"
    );
    let decoded: Timestamp =
        decode_message(&frame, CompressionEncoding::Gzip).expect("decode with gzip");
    assert_eq!(decoded.seconds, ts.seconds);
    assert_eq!(decoded.nanos, ts.nanos);
}

// ─── OxiRpcError conversions ─────────────────────────────────────────────────

#[test]
fn oxi_rpc_error_from_tonic_status_internal() {
    use oxirpc_core::OxiRpcError;
    use tonic::Status;

    let status = Status::internal("test error");
    let err: OxiRpcError = status.into();
    assert!(
        matches!(err, OxiRpcError::Status(_)),
        "expected Status variant, got: {err:?}"
    );
    let display = err.to_string();
    assert!(
        display.contains("gRPC status"),
        "display should mention gRPC status: {display}"
    );
}

#[test]
fn oxi_rpc_error_from_tonic_status_not_found() {
    use oxirpc_core::OxiRpcError;
    use tonic::Status;

    let status = Status::not_found("missing entity");
    let err: OxiRpcError = status.into();
    assert!(
        matches!(err, OxiRpcError::Status(_)),
        "expected Status variant, got: {err:?}"
    );
}

#[test]
fn oxi_rpc_error_from_timeout_error() {
    use oxirpc_core::timeout::TimeoutError;
    use oxirpc_core::OxiRpcError;

    let err: OxiRpcError = TimeoutError::Empty.into();
    assert!(
        matches!(err, OxiRpcError::Timeout),
        "expected Timeout variant, got: {err:?}"
    );
    // Verify display
    assert_eq!(err.to_string(), "deadline exceeded");
}

#[test]
fn oxi_rpc_error_timeout_all_variants_convert() {
    use oxirpc_core::timeout::TimeoutError;
    use oxirpc_core::OxiRpcError;

    // All TimeoutError variants should map to OxiRpcError::Timeout.
    let variants: Vec<TimeoutError> = vec![
        TimeoutError::Empty,
        TimeoutError::InvalidValue,
        TimeoutError::InvalidUnit('z'),
        TimeoutError::Overflow,
    ];
    for v in variants {
        let err: OxiRpcError = v.into();
        assert!(
            matches!(err, OxiRpcError::Timeout),
            "expected Timeout variant for TimeoutError variant"
        );
    }
}

#[test]
fn oxi_rpc_error_display_variants() {
    use oxirpc_core::OxiRpcError;

    let cases = [
        (
            OxiRpcError::Transport("net error".into()),
            "transport error",
        ),
        (OxiRpcError::Build("build failed".into()), "build error"),
        (OxiRpcError::Tls("tls error".into()), "TLS error"),
        (
            OxiRpcError::Compression("bad compress".into()),
            "compression error",
        ),
        (OxiRpcError::Proto("proto failed".into()), "proto error"),
        (OxiRpcError::Timeout, "deadline exceeded"),
        (OxiRpcError::Cancelled, "operation cancelled"),
    ];

    for (err, expected_fragment) in cases {
        let display = err.to_string();
        assert!(
            display.contains(expected_fragment),
            "expected '{expected_fragment}' in display: {display}"
        );
    }
}

// ─── TLS config tests ─────────────────────────────────────────────────────────

#[cfg(feature = "tls")]
mod tls_tests {
    use oxirpc_core::tls;
    use rustls::RootCertStore;
    use rustls_pemfile::certs;
    use std::io::BufReader;

    /// Generate a localhost self-signed cert using rcgen.
    /// Returns `(cert_pem, key_pem)`.
    fn localhost_cert() -> (String, String) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("rcgen::generate_simple_self_signed");
        (cert.cert.pem(), cert.signing_key.serialize_pem())
    }

    #[test]
    fn tls_client_config_empty_roots_succeeds() {
        let roots = RootCertStore::empty();
        let cfg = tls::client_config(roots).expect("client_config with empty roots");
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"h2".to_vec()],
            "h2 ALPN must be set"
        );
    }

    #[test]
    fn tls_client_config_has_h2_alpn() {
        let roots = RootCertStore::empty();
        let cfg = tls::client_config(roots).expect("client_config");
        assert!(
            cfg.alpn_protocols.contains(&b"h2".to_vec()),
            "expected h2 ALPN in {:?}",
            cfg.alpn_protocols
        );
    }

    #[test]
    fn tls_client_config_with_self_signed_root() {
        let (cert_pem, _key_pem) = localhost_cert();
        let mut roots = RootCertStore::empty();
        let ders: Vec<_> = certs(&mut BufReader::new(cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .expect("parse cert DERs");
        for der in ders {
            roots.add(der).expect("add root");
        }
        let cfg = tls::client_config(roots).expect("client_config with self-signed root");
        assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn tls_server_config_with_rcgen_self_signed_cert() {
        let (cert_pem, key_pem) = localhost_cert();
        let cfg = tls::server_config(cert_pem.as_bytes(), key_pem.as_bytes())
            .expect("server_config with rcgen self-signed cert");
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"h2".to_vec()],
            "h2 ALPN must be set on server config"
        );
    }

    #[test]
    fn tls_server_config_rejects_empty_pem() {
        let err = tls::server_config(b"", b"");
        assert!(err.is_err(), "empty cert/key must return an error");
    }

    #[test]
    fn tls_server_config_rejects_cert_as_key() {
        let (cert_pem, _key_pem) = localhost_cert();
        // Pass cert PEM as both cert and key — no private key present in the key slot.
        let err = tls::server_config(cert_pem.as_bytes(), cert_pem.as_bytes());
        assert!(
            err.is_err(),
            "server_config with cert PEM in key slot must return an error"
        );
    }

    #[test]
    fn tls_client_config_arc_wraps_config() {
        let roots = RootCertStore::empty();
        let arc_cfg = tls::client_config_arc(roots).expect("client_config_arc");
        assert_eq!(arc_cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn tls_server_config_arc_wraps_config() {
        let (cert_pem, key_pem) = localhost_cert();
        let arc_cfg = tls::server_config_arc(cert_pem.as_bytes(), key_pem.as_bytes())
            .expect("server_config_arc");
        assert_eq!(arc_cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn tls_roundtrip_config_construction_both_have_h2_alpn() {
        let (cert_pem, key_pem) = localhost_cert();

        let server_cfg =
            tls::server_config(cert_pem.as_bytes(), key_pem.as_bytes()).expect("server_config");

        let mut roots = RootCertStore::empty();
        for der in certs(&mut BufReader::new(cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .expect("cert DERs")
        {
            roots.add(der).expect("add root");
        }
        let client_cfg = tls::client_config(roots).expect("client_config");

        assert_eq!(server_cfg.alpn_protocols, vec![b"h2".to_vec()]);
        assert_eq!(client_cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }
}
