//! Integration tests for `oxirpc_client::load_reporting`.

use std::time::{Duration, Instant};

use oxirpc_client::load_reporting::{CallTelemetry, LoadReporter};
use oxirpc_core::StatusCode;

// ─── CallTelemetry tests ─────────────────────────────────────────────────────

#[test]
fn call_telemetry_new_computes_latency() {
    let start = Instant::now();
    // Inject a small artificial delay so the elapsed Duration is non-zero.
    std::thread::sleep(Duration::from_millis(1));
    let tel = CallTelemetry::new(None, start, 0, 0, StatusCode::Ok);
    assert!(
        tel.latency > Duration::ZERO,
        "latency should be > 0 after sleeping 1 ms, got {:?}",
        tel.latency
    );
}

#[test]
fn call_telemetry_zero_latency_on_same_instant() {
    // Without any sleep the latency may be exactly zero on some platforms.
    // We only assert it is non-negative (i.e. the Duration is valid).
    let start = Instant::now();
    let tel = CallTelemetry::new(None, start, 0, 0, StatusCode::Ok);
    // Duration can never be negative — the type enforces this.
    // A panicking subtraction would signal a bug, so this test acts as a compile-time
    // and runtime guard.
    let _ = tel.latency;
}

#[test]
fn call_telemetry_records_bytes() {
    let start = Instant::now();
    let tel = CallTelemetry::new(None, start, 256, 1024, StatusCode::Ok);
    assert_eq!(tel.bytes_sent, 256, "bytes_sent mismatch");
    assert_eq!(tel.bytes_received, 1024, "bytes_received mismatch");
}

#[test]
fn call_telemetry_status_code() {
    let start = Instant::now();
    let tel = CallTelemetry::new(None, start, 0, 0, StatusCode::NotFound);
    assert_eq!(
        tel.status,
        StatusCode::NotFound,
        "status code should be NotFound"
    );
}

#[test]
fn call_telemetry_no_endpoint() {
    let start = Instant::now();
    let tel = CallTelemetry::new(None, start, 0, 0, StatusCode::Ok);
    assert!(
        tel.endpoint.is_none(),
        "endpoint should be None when passed None"
    );
}

// ─── LoadReporter tests ──────────────────────────────────────────────────────

#[test]
fn load_reporter_sends_and_receives() {
    let (reporter, mut rx) = LoadReporter::channel(8);

    let start = Instant::now();
    let tel = CallTelemetry::new(None, start, 128, 256, StatusCode::Ok);
    reporter
        .send(tel)
        .expect("send should succeed on empty channel (buffer=8)");

    let received = rx.try_recv().expect("record should be in the channel");
    assert_eq!(received.bytes_sent, 128, "bytes_sent should round-trip");
    assert_eq!(
        received.bytes_received, 256,
        "bytes_received should round-trip"
    );
    assert_eq!(received.status, StatusCode::Ok, "status should round-trip");
}
