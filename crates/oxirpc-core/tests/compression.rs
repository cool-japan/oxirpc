//! Integration tests for OxiArcGzip compression via oxiarc-deflate.
//!
//! These tests run only when the `compression` feature is enabled:
//!   cargo nextest run -p oxirpc-core --features compression

#![cfg(feature = "compression")]

use oxirpc_core::compression::{CompressionError, OxiArcGzip};

// ── round-trip ────────────────────────────────────────────────────────────────

#[test]
fn round_trip_small() {
    let original = b"hello gRPC compression!";
    let compressed = OxiArcGzip::compress(original).expect("compress failed");
    let decompressed = OxiArcGzip::decompress(&compressed).expect("decompress failed");
    assert_eq!(decompressed.as_slice(), original.as_slice());
}

#[test]
fn round_trip_empty() {
    let original: &[u8] = b"";
    let compressed = OxiArcGzip::compress(original).expect("compress empty failed");
    let decompressed = OxiArcGzip::decompress(&compressed).expect("decompress empty failed");
    assert_eq!(decompressed.as_slice(), original);
}

#[test]
fn round_trip_64kb_text() {
    // 64 KiB of repetitive ASCII — exercises real DEFLATE back-references.
    let chunk = b"oxirpc gzip compression test payload -- repetitive text content.\n";
    let mut original = Vec::with_capacity(64 * 1024);
    while original.len() < 64 * 1024 {
        original.extend_from_slice(chunk);
    }
    original.truncate(64 * 1024);

    let compressed = OxiArcGzip::compress(&original).expect("compress 64KB failed");
    let decompressed = OxiArcGzip::decompress(&compressed).expect("decompress 64KB failed");
    assert_eq!(decompressed, original, "64 KiB round-trip mismatch");
}

// ── size check ────────────────────────────────────────────────────────────────

#[test]
fn compressed_smaller_than_input_for_repetitive_data() {
    // 4 KiB of repeating 'A' bytes — highly compressible.
    let data = vec![b'A'; 4096];
    let compressed = OxiArcGzip::compress(&data).expect("compress failed");
    assert!(
        compressed.len() < data.len(),
        "expected compressed ({} bytes) < original ({} bytes)",
        compressed.len(),
        data.len()
    );
}

// ── gzip magic header ─────────────────────────────────────────────────────────

#[test]
fn output_has_gzip_magic_bytes() {
    let compressed = OxiArcGzip::compress(b"check gzip magic").expect("compress failed");
    assert!(
        compressed.len() >= 2,
        "compressed output too short to contain gzip magic"
    );
    assert_eq!(
        compressed[0], 0x1f,
        "expected gzip ID1 byte 0x1f, got 0x{:02x}",
        compressed[0]
    );
    assert_eq!(
        compressed[1], 0x8b,
        "expected gzip ID2 byte 0x8b, got 0x{:02x}",
        compressed[1]
    );
}

// ── level variants ────────────────────────────────────────────────────────────

#[test]
fn all_levels_round_trip() {
    let original = b"level round-trip: AAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBB";
    for level in 0u8..=9 {
        let compressed = OxiArcGzip::compress_with_level(original, level).expect("compress failed");
        let decompressed = OxiArcGzip::decompress(&compressed).expect("decompress failed");
        assert_eq!(
            decompressed.as_slice(),
            original.as_slice(),
            "round-trip failed at level {level}"
        );
    }
}

#[test]
fn level_clamped_above_9() {
    // Levels above 9 should be accepted (clamped to 9) without panic.
    let original = b"clamped level test";
    let compressed =
        OxiArcGzip::compress_with_level(original, 255).expect("compress_with_level(255) failed");
    let decompressed = OxiArcGzip::decompress(&compressed).expect("decompress failed");
    assert_eq!(decompressed.as_slice(), original.as_slice());
}

// ── error cases ───────────────────────────────────────────────────────────────

#[test]
fn decompress_garbage_returns_err() {
    let garbage = b"this is definitely not a gzip stream!!!";
    let result = OxiArcGzip::decompress(garbage);
    assert!(
        result.is_err(),
        "expected Err for garbage input, got Ok({:?})",
        result
    );
}

#[test]
fn decompress_truncated_returns_err() {
    // A valid gzip header (2 magic bytes) but truncated — should fail.
    let truncated = b"\x1f\x8b";
    let result = OxiArcGzip::decompress(truncated);
    assert!(result.is_err(), "expected Err for truncated gzip, got Ok");
}

#[test]
fn compression_error_is_display() {
    // Ensure CompressionError implements Display (needed for OxiRpcError::Compression).
    let e = CompressionError::InvalidData("test".to_owned());
    let s = e.to_string();
    assert!(s.contains("test"), "Display output missing message: {s}");

    let e2 = CompressionError::Failed("oops".to_owned());
    let s2 = e2.to_string();
    assert!(s2.contains("oops"), "Display output missing message: {s2}");
}

#[test]
fn compression_error_converts_to_oxirpc_error() {
    use oxirpc_core::OxiRpcError;
    let ce = CompressionError::Failed("test failure".to_owned());
    let rpc_err: OxiRpcError = ce.into();
    let msg = rpc_err.to_string();
    assert!(
        msg.contains("compression"),
        "expected 'compression' in error string, got: {msg}"
    );
}
