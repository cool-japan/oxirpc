//! Exponential backoff with deterministic jitter for ADS reconnects.
//!
//! This module provides [`Backoff`] — an iterator-style helper that returns
//! increasing wait durations after each attempt.  Jitter is derived from a
//! cheap bit-mixing hash of the attempt counter so no external `rand` crate
//! is needed.

use std::time::Duration;

// ── Backoff ──────────────────────────────────────────────────────────────────

/// Exponential backoff with bounded jitter.
///
/// # Example
/// ```rust
/// use oxirpc_client::xds::backoff::Backoff;
///
/// let mut b = Backoff::default_grpc();
/// let d0 = b.next(); // ≈ 100 ms ± 20%
/// let d1 = b.next(); // ≈ 160 ms ± 20%
/// b.reset();
/// let d2 = b.next(); // back to ≈ 100 ms
/// ```
pub struct Backoff {
    /// Base delay (attempt 0).
    base: Duration,
    /// Maximum delay (ceiling after factor multiplication).
    max: Duration,
    /// Multiplicative growth factor per attempt.
    factor: f64,
    /// Jitter fraction in `[0.0, 1.0)`.  A value of `0.2` gives ±20 % jitter.
    jitter_frac: f64,
    /// Number of attempts so far (incremented by [`Backoff::next`]).
    attempt: u32,
}

impl Backoff {
    /// Create a new backoff with explicit parameters.
    ///
    /// * `base`        — initial delay.
    /// * `max`         — upper bound on the delay (after jitter).
    /// * `factor`      — exponential growth per attempt (e.g. `1.6`).
    /// * `jitter_frac` — fractional jitter width, e.g. `0.2` for ±20 %.
    pub fn new(base: Duration, max: Duration, factor: f64, jitter_frac: f64) -> Self {
        Self {
            base,
            max,
            factor,
            jitter_frac,
            attempt: 0,
        }
    }

    /// Sensible defaults for a gRPC ADS reconnect loop.
    ///
    /// `base = 100 ms`, `max = 10 s`, `factor = 1.6`, `jitter_frac = 0.2`.
    pub fn default_grpc() -> Self {
        Self::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            1.6,
            0.2,
        )
    }

    /// Return the delay for the current attempt, then advance the counter.
    ///
    /// Delay formula:
    /// ```text
    /// raw   = base * factor^attempt
    /// hash  = (attempt × 2654435761) & 0xFFFF / 65536.0   (∈ [0, 1))
    /// delay = raw × (1 − jitter_frac + hash × 2 × jitter_frac)
    /// delay = clamp(delay, 0, max)
    /// ```
    ///
    /// Named `next_duration` (not `next`) to avoid confusion with
    /// [`std::iter::Iterator::next`].
    pub fn next_duration(&mut self) -> Duration {
        let attempt = self.attempt;
        self.attempt = self.attempt.saturating_add(1);

        // Raw exponential delay.
        let raw_secs = self.base.as_secs_f64() * self.factor.powi(attempt as i32);

        // Deterministic jitter in [0, 1).
        let hash = f64::from((attempt.wrapping_mul(2_654_435_761_u32)) & 0xFFFF) / 65536.0_f64;
        let jitter_multiplier = 1.0 - self.jitter_frac + hash * 2.0 * self.jitter_frac;

        let delay_secs = raw_secs * jitter_multiplier;
        // Clamp to max (do this in f64 to avoid overflow converting a huge
        // float to Duration).
        let clamped_secs = delay_secs.min(self.max.as_secs_f64());
        // Guard against NaN / negative (shouldn't happen with valid inputs).
        let clamped_secs = clamped_secs.max(0.0);

        Duration::from_secs_f64(clamped_secs)
    }

    /// Reset the attempt counter after a successful server acknowledgement.
    ///
    /// Call this each time the ADS session delivers at least one successful
    /// `DiscoveryResponse` so that the next reconnect starts from the base
    /// delay again.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}
