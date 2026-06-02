//! Tests for ServerBuilder compression advertisement knobs:
//! `accept_compressed`, `send_compressed`, and `accepted_encodings`.

use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_server::ServerBuilder;

// ─── accept_compressed ────────────────────────────────────────────────────────

#[test]
fn accept_compressed_adds_gzip_encoding() {
    let builder = ServerBuilder::new().accept_compressed(CompressionEncoding::Gzip);
    assert!(
        builder
            .accepted_encodings()
            .contains(&CompressionEncoding::Gzip),
        "Gzip should be present after accept_compressed(Gzip)"
    );
}

#[test]
fn accept_compressed_adds_zstd_encoding() {
    let builder = ServerBuilder::new().accept_compressed(CompressionEncoding::Zstd);
    assert!(
        builder
            .accepted_encodings()
            .contains(&CompressionEncoding::Zstd),
        "Zstd should be present after accept_compressed(Zstd)"
    );
}

#[test]
fn accept_compressed_deduplicates_gzip() {
    let builder = ServerBuilder::new()
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    assert_eq!(
        builder.accepted_encodings().len(),
        1,
        "Duplicate Gzip entries should be collapsed to one"
    );
}

#[test]
fn accept_compressed_multiple_distinct_encodings() {
    let builder = ServerBuilder::new()
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd);
    let enc = builder.accepted_encodings();
    assert_eq!(enc.len(), 2, "Both Gzip and Zstd should be present");
    assert!(enc.contains(&CompressionEncoding::Gzip));
    assert!(enc.contains(&CompressionEncoding::Zstd));
}

#[test]
fn accepted_encodings_empty_by_default() {
    let builder = ServerBuilder::new();
    assert!(
        builder.accepted_encodings().is_empty(),
        "No encodings should be registered on a fresh ServerBuilder"
    );
}

// ─── send_compressed ──────────────────────────────────────────────────────────

#[test]
fn send_compressed_sets_gzip() {
    let builder = ServerBuilder::new().send_compressed(CompressionEncoding::Gzip);
    assert_eq!(
        builder.send_encoding(),
        Some(CompressionEncoding::Gzip),
        "send_encoding should be Gzip after send_compressed(Gzip)"
    );
}

#[test]
fn send_compressed_sets_zstd() {
    let builder = ServerBuilder::new().send_compressed(CompressionEncoding::Zstd);
    assert_eq!(
        builder.send_encoding(),
        Some(CompressionEncoding::Zstd),
        "send_encoding should be Zstd after send_compressed(Zstd)"
    );
}

#[test]
fn send_compressed_last_call_wins() {
    // Calling send_compressed multiple times: the last call should override.
    let builder = ServerBuilder::new()
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd);
    assert_eq!(
        builder.send_encoding(),
        Some(CompressionEncoding::Zstd),
        "Last send_compressed call should win"
    );
}

#[test]
fn send_encoding_none_by_default() {
    let builder = ServerBuilder::new();
    assert!(
        builder.send_encoding().is_none(),
        "send_encoding should be None on a fresh ServerBuilder"
    );
}

// ─── Display includes compression ─────────────────────────────────────────────

#[test]
fn display_includes_accept_encoding_when_none() {
    let s = ServerBuilder::new().to_string();
    assert!(
        s.contains("accept_encoding=[]"),
        "Display should show accept_encoding=[] when empty, got: {s}"
    );
}

#[test]
fn display_includes_gzip_when_registered() {
    let s = ServerBuilder::new()
        .accept_compressed(CompressionEncoding::Gzip)
        .to_string();
    assert!(
        s.contains("gzip"),
        "Display should mention 'gzip' when Gzip is registered, got: {s}"
    );
    assert!(
        s.contains("accept_encoding="),
        "Display should include accept_encoding= field, got: {s}"
    );
}

#[test]
fn display_includes_multiple_encodings() {
    let s = ServerBuilder::new()
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .to_string();
    assert!(s.contains("gzip"), "Display should contain 'gzip': {s}");
    assert!(s.contains("zstd"), "Display should contain 'zstd': {s}");
}

// ─── Chaining with other builder methods ──────────────────────────────────────

#[test]
fn accept_compressed_chains_with_other_settings() {
    use std::time::Duration;
    let builder = ServerBuilder::new()
        .accept_http1(false)
        .timeout(Duration::from_millis(1000))
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);
    assert!(builder
        .accepted_encodings()
        .contains(&CompressionEncoding::Gzip));
    assert_eq!(builder.send_encoding(), Some(CompressionEncoding::Gzip));
}
