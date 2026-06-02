//! Per-call telemetry and load reporting for oxirpc-client.
//!
//! [`CallTelemetry`] captures the key metrics of a single completed RPC:
//! which endpoint was targeted, how long the call took, how many bytes were
//! transferred, and the final gRPC status code.
//!
//! [`LoadReporter`] is a lightweight sender side of an
//! [`tokio::sync::mpsc`] channel that emits [`CallTelemetry`] records after
//! each RPC completes.  The receiving side (held by the caller) can aggregate
//! counters, feed a telemetry sink, or compute p99 latencies.
//!
//! # Example
//!
//! ```rust
//! use std::time::Instant;
//! use oxirpc_core::StatusCode;
//! use oxirpc_client::load_reporting::{CallTelemetry, LoadReporter};
//!
//! let (reporter, mut rx) = LoadReporter::channel(64);
//! let start = Instant::now();
//! let tel = CallTelemetry::new(None, start, 128, 512, StatusCode::Ok);
//! reporter.send(tel).ok();
//! let received = rx.try_recv().expect("should have record");
//! assert_eq!(received.bytes_sent, 128);
//! ```

use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::balance::Endpoint;
use oxirpc_core::StatusCode;

// ─── CallTelemetry ───────────────────────────────────────────────────────────

/// Per-call telemetry record emitted after each RPC completes.
///
/// Contains the target endpoint (if any), the observed end-to-end latency,
/// payload byte counts, and the final gRPC [`StatusCode`].
#[derive(Debug, Clone)]
pub struct CallTelemetry {
    /// The backend endpoint that handled this call, if known.
    pub endpoint: Option<Endpoint>,
    /// Elapsed time from call initiation to response receipt.
    pub latency: Duration,
    /// Number of bytes sent (request payload, not including framing).
    pub bytes_sent: usize,
    /// Number of bytes received (response payload, not including framing).
    pub bytes_received: usize,
    /// The final gRPC status code for this call.
    pub status: StatusCode,
}

impl CallTelemetry {
    /// Create a new [`CallTelemetry`] record.
    ///
    /// `latency` is computed as `start.elapsed()` at the time of construction,
    /// so call this immediately after the RPC completes.
    ///
    /// # Parameters
    ///
    /// - `endpoint` — the backend that handled the call; `None` if the channel
    ///   is not endpoint-aware (e.g. a plain tonic `Channel`).
    /// - `start`    — the [`Instant`] captured immediately before the call.
    /// - `bytes_sent`     — request payload bytes (excluding gRPC/HTTP framing).
    /// - `bytes_received` — response payload bytes (excluding gRPC/HTTP framing).
    /// - `status`   — the gRPC status code returned by the server (or inferred
    ///   from a transport error).
    pub fn new(
        endpoint: Option<Endpoint>,
        start: Instant,
        bytes_sent: usize,
        bytes_received: usize,
        status: StatusCode,
    ) -> Self {
        Self {
            endpoint,
            latency: start.elapsed(),
            bytes_sent,
            bytes_received,
            status,
        }
    }
}

// ─── LoadReporter ────────────────────────────────────────────────────────────

/// Sender side of a per-call telemetry channel.
///
/// Wrap an `mpsc::Sender<CallTelemetry>` and call [`LoadReporter::send`] after
/// each RPC to emit a [`CallTelemetry`] record to the receiving side.
///
/// Clone freely — all clones share the same logical channel and will deliver
/// records to the same receiver.
///
/// # Example
///
/// ```rust
/// use std::time::Instant;
/// use oxirpc_core::StatusCode;
/// use oxirpc_client::load_reporting::{CallTelemetry, LoadReporter};
///
/// let (reporter, mut rx) = LoadReporter::channel(32);
/// let tel = CallTelemetry::new(None, Instant::now(), 0, 0, StatusCode::Ok);
/// reporter.send(tel).ok();
/// assert!(rx.try_recv().is_ok());
/// ```
#[derive(Clone, Debug)]
pub struct LoadReporter {
    tx: mpsc::Sender<CallTelemetry>,
}

impl LoadReporter {
    /// Create a bounded `(LoadReporter, mpsc::Receiver<CallTelemetry>)` pair.
    ///
    /// `buffer` is the number of [`CallTelemetry`] records that can be queued
    /// before [`LoadReporter::send`] starts returning backpressure errors.
    pub fn channel(buffer: usize) -> (Self, mpsc::Receiver<CallTelemetry>) {
        let (tx, rx) = mpsc::channel(buffer);
        (Self { tx }, rx)
    }

    /// Attempt to emit a [`CallTelemetry`] record.
    ///
    /// Returns `Ok(())` if the record was queued, or
    /// `Err(Box<CallTelemetry>)` if the channel is full or closed (the caller
    /// may choose to drop or log the rejected record).
    ///
    /// This method does **not** block or `await`.  It uses
    /// [`mpsc::Sender::try_send`] so it is safe to call from synchronous
    /// contexts and will never introduce latency into the RPC hot path.
    pub fn send(&self, telemetry: CallTelemetry) -> Result<(), Box<CallTelemetry>> {
        self.tx
            .try_send(telemetry)
            .map_err(|e| Box::new(e.into_inner()))
    }
}
