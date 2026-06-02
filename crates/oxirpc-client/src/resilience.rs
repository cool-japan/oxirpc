//! Client-side resilience policies: retry, circuit-breaking, and hedging.
//!
//! # Overview
//!
//! - [`Backoff`] — exponential backoff with optional LCG-based jitter.
//! - [`RetryPolicy`] — wraps [`Backoff`] with a per-attempt retryability check.
//! - [`CircuitBreaker`] — three-state (Closed / Open / HalfOpen) fault isolator.
//! - [`Hedging`] — emits a schedule for speculative parallel requests.
//!
//! All types are transport-agnostic policy objects; the caller is responsible
//! for executing the actual RPC and feeding outcomes back to the policies.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use oxirpc_core::status::StatusCode;

// ── Backoff ───────────────────────────────────────────────────────────────────

/// Exponential back-off with optional LCG-based jitter.
///
/// Jitter uses a 64-bit linear congruential generator seeded from the
/// sub-second nanoseconds of the current monotonic clock — no `rand` crate
/// dependency required.
#[derive(Clone, Debug)]
pub struct Backoff {
    /// Initial delay (attempt 0).
    pub base: Duration,
    /// Multiplicative factor applied on each successive attempt.
    pub factor: f64,
    /// Upper cap on the computed delay.
    pub max: Duration,
    /// When `true`, apply ±50 % uniform jitter around the capped delay.
    pub jitter: bool,
}

impl Backoff {
    /// Create a sensible exponential backoff (factor 2, max 30 s, jitter on).
    pub fn exponential(base: Duration) -> Self {
        Self {
            base,
            factor: 2.0,
            max: Duration::from_secs(30),
            jitter: true,
        }
    }

    /// Compute the delay before the given `attempt` (0-indexed).
    ///
    /// The formula is `min(base * factor^attempt, max)`, optionally jittered to
    /// a random value in `[0.5·capped, capped]` using a fast LCG.
    pub fn next_delay(&self, attempt: u32) -> Duration {
        let secs = self.base.as_secs_f64() * self.factor.powi(attempt as i32);
        let capped = secs.min(self.max.as_secs_f64());

        let delay_secs = if self.jitter {
            // Seed the LCG with sub-second nanos of the monotonic clock.
            // This is not cryptographically random, but is sufficient to
            // spread retries across clients without any external dep.
            let seed = Instant::now().elapsed().subsec_nanos() as u64;
            let lcg = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            // Map the upper 31 bits to [0.0, 1.0)
            let jitter_frac = (lcg >> 33) as f64 / u32::MAX as f64;
            // Delay in [0.5·capped, capped]
            capped * (0.5 + jitter_frac * 0.5)
        } else {
            capped
        };

        Duration::from_secs_f64(delay_secs)
    }
}

// ── RetryPolicy ───────────────────────────────────────────────────────────────

/// Policy that decides whether a failed attempt should be retried.
///
/// The caller drives the retry loop:
///
/// ```no_run
/// # use std::time::Duration;
/// # use oxirpc_client::resilience::RetryPolicy;
/// # use oxirpc_core::status::StatusCode;
/// let policy = RetryPolicy::new(3);
/// let mut attempt = 0u32;
/// loop {
///     // … perform RPC, get status_code …
///     # let status_code = StatusCode::Unavailable;
///     match policy.should_retry(attempt, status_code) {
///         Some(delay) => {
///             // sleep(delay) then retry
///             attempt += 1;
///         }
///         None => break, // give up or succeeded
///     }
///     # break;
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (not counting the original).
    pub max_attempts: u32,
    /// Back-off configuration.
    pub backoff: Backoff,
    /// Function returning `true` if a given status code is safe to retry.
    pub retryable: fn(StatusCode) -> bool,
}

impl RetryPolicy {
    /// Create a policy with `max_attempts` retries, default exponential
    /// backoff (100 ms base), and the standard gRPC retryability predicate.
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            backoff: Backoff::exponential(Duration::from_millis(100)),
            retryable: StatusCode::is_retryable,
        }
    }

    /// Override the back-off configuration (builder-style).
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Override the retryability predicate (builder-style).
    pub fn with_retryable(mut self, f: fn(StatusCode) -> bool) -> Self {
        self.retryable = f;
        self
    }

    /// Determine whether `attempt` (0-indexed) should be retried for `code`.
    ///
    /// Returns the delay to wait before retrying, or `None` if the attempt
    /// ceiling has been reached or the code is not retryable.
    pub fn should_retry(&self, attempt: u32, code: StatusCode) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        if !(self.retryable)(code) {
            return None;
        }
        Some(self.backoff.next_delay(attempt))
    }
}

// ── CircuitBreaker internal state ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    /// Normal operation.  `failures` counts consecutive failures since last reset.
    Closed { failures: u32 },
    /// Tripped.  No requests allowed until `since + open_cooldown`.
    Open { since: Instant },
    /// Probe state entered after the cooldown expires.  One request is let
    /// through; additional successes close the circuit.
    HalfOpen { successes: u32 },
}

// ── CircuitBreaker ─────────────────────────────────────────────────────────────

/// Three-state circuit breaker (Closed → Open → HalfOpen → Closed).
///
/// After `failure_threshold` consecutive failures the breaker *opens*, blocking
/// all requests.  Once `open_cooldown` has elapsed it moves to *half-open*,
/// allowing probes through.  After `success_threshold` consecutive successes in
/// the half-open state it closes again.
pub struct CircuitBreaker {
    state: Mutex<CircuitState>,
    failure_threshold: u32,
    success_threshold: u32,
    open_cooldown: Duration,
}

impl CircuitBreaker {
    /// Create a circuit breaker with the given thresholds and cooldown.
    pub fn new(failure_threshold: u32, success_threshold: u32, open_cooldown: Duration) -> Self {
        Self {
            state: Mutex::new(CircuitState::Closed { failures: 0 }),
            failure_threshold,
            success_threshold,
            open_cooldown,
        }
    }

    /// Returns `true` if a new request should be dispatched.
    ///
    /// An open circuit returns `false` until the cooldown expires, at which
    /// point it transitions to half-open and returns `true` for probe requests.
    pub fn allow_request(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match *state {
            CircuitState::Closed { .. } => true,
            CircuitState::Open { since } => {
                if since.elapsed() >= self.open_cooldown {
                    *state = CircuitState::HalfOpen { successes: 0 };
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen { .. } => true,
        }
    }

    /// Record a successful RPC outcome.
    ///
    /// In `HalfOpen` state this counts towards re-closing the circuit.
    pub fn on_success(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match *state {
            CircuitState::Closed { .. } => {
                *state = CircuitState::Closed { failures: 0 };
            }
            CircuitState::HalfOpen { successes } => {
                if successes + 1 >= self.success_threshold {
                    *state = CircuitState::Closed { failures: 0 };
                } else {
                    *state = CircuitState::HalfOpen {
                        successes: successes + 1,
                    };
                }
            }
            CircuitState::Open { .. } => {
                // Successes while fully open are ignored; the circuit must
                // move through HalfOpen before it can close.
            }
        }
    }

    /// Record a failed RPC outcome.
    ///
    /// In `Closed` state this increments the failure counter; once it reaches
    /// `failure_threshold` the circuit opens.  In `HalfOpen` a single failure
    /// immediately re-opens the circuit.
    pub fn on_failure(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match *state {
            CircuitState::Closed { failures } => {
                if failures + 1 >= self.failure_threshold {
                    *state = CircuitState::Open {
                        since: Instant::now(),
                    };
                } else {
                    *state = CircuitState::Closed {
                        failures: failures + 1,
                    };
                }
            }
            CircuitState::HalfOpen { .. } => {
                *state = CircuitState::Open {
                    since: Instant::now(),
                };
            }
            CircuitState::Open { .. } => {
                // Already open; update timestamp so the cooldown restarts.
                *state = CircuitState::Open {
                    since: Instant::now(),
                };
            }
        }
    }

    /// Returns `true` if the circuit is currently in the `Open` state.
    pub fn is_open(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        matches!(*state, CircuitState::Open { .. })
    }
}

// ── Hedging ────────────────────────────────────────────────────────────────────

/// Policy for hedged requests.
///
/// Hedging dispatches additional copies of a request after fixed delays
/// without waiting for the original to fail.  The first response wins;
/// all other in-flight requests are cancelled by the caller.
///
/// This type only *describes* the schedule; the actual fan-out is the
/// caller's responsibility (e.g. via `tokio::select!` or channel tasks).
#[derive(Clone, Debug)]
pub struct Hedging {
    /// Maximum number of additional requests to dispatch (beyond the first).
    pub max_hedges: u32,
    /// Base delay between hedge dispatches.
    pub delay: Duration,
}

impl Hedging {
    /// Create a hedging policy.
    pub fn new(max_hedges: u32, delay: Duration) -> Self {
        Self { max_hedges, delay }
    }

    /// Return the delay before dispatching hedge number `hedge_idx` (1-indexed).
    ///
    /// Returns `None` if `hedge_idx` is `0` or exceeds `max_hedges`.
    pub fn delay_for(&self, hedge_idx: u32) -> Option<Duration> {
        if hedge_idx == 0 || hedge_idx > self.max_hedges {
            None
        } else {
            Some(self.delay * hedge_idx)
        }
    }

    /// Return the full schedule of hedge dispatch times as a `Vec`.
    ///
    /// The `i`-th element (0-indexed) is the delay before the `(i+1)`-th
    /// additional request, i.e. `delay * (i + 1)`.
    pub fn schedule(&self) -> Vec<Duration> {
        (1..=self.max_hedges).map(|i| self.delay * i).collect()
    }
}
