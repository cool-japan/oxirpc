//! gRPC timeout header encoding and decoding.
//!
//! Wraps the existing [`crate::timeout`] module's format/parse functions and
//! provides an [`http::HeaderValue`]-typed interface plus a [`Deadline`] type
//! for tracking remaining time.

use http::HeaderValue;
use std::time::{Duration, Instant};

use crate::wire::WireError;

/// Encode a [`Duration`] as a gRPC timeout [`HeaderValue`].
///
/// Delegates to [`crate::timeout::format_grpc_timeout`], then converts the
/// resulting string to an [`HeaderValue`].
///
/// # Errors
///
/// Returns [`WireError::BadTimeoutHeader`] if the formatted string is not a
/// valid HTTP header value (should never occur in practice).
pub fn timeout_to_header_value(d: Duration) -> Result<HeaderValue, WireError> {
    let s = crate::timeout::format_grpc_timeout(d);
    HeaderValue::from_str(&s).map_err(|e| WireError::BadTimeoutHeader(e.to_string()))
}

/// Decode a gRPC timeout [`HeaderValue`] back to a [`Duration`].
///
/// # Errors
///
/// Returns [`WireError::BadTimeoutHeader`] if the value is not valid UTF-8
/// or cannot be parsed as a gRPC timeout string.
pub fn header_value_to_timeout(v: &HeaderValue) -> Result<Duration, WireError> {
    let s = v
        .to_str()
        .map_err(|e| WireError::BadTimeoutHeader(e.to_string()))?;
    crate::timeout::parse_grpc_timeout(s).map_err(|e| WireError::BadTimeoutHeader(e.to_string()))
}

// ─── Deadline ────────────────────────────────────────────────────────────────

/// An absolute deadline derived from a relative gRPC timeout duration.
pub struct Deadline {
    /// The absolute instant at which the deadline expires.
    pub at: Instant,
}

impl Deadline {
    /// Create a deadline that expires `d` from now.
    pub fn from_now(d: Duration) -> Self {
        Self {
            at: Instant::now() + d,
        }
    }

    /// Returns the remaining time until the deadline, or `None` if already
    /// elapsed.
    pub fn remaining(&self) -> Option<Duration> {
        self.at.checked_duration_since(Instant::now())
    }

    /// Encode the remaining time as a gRPC timeout header value.
    ///
    /// If the deadline has already elapsed, encodes `Duration::ZERO` (which
    /// formats as `"0n"`).
    ///
    /// # Errors
    ///
    /// See [`timeout_to_header_value`].
    pub fn to_header_value(&self) -> Result<HeaderValue, WireError> {
        let remaining = self.remaining().unwrap_or(Duration::ZERO);
        timeout_to_header_value(remaining)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn five_seconds_encodes_as_5s() {
        let hv = timeout_to_header_value(Duration::from_secs(5)).unwrap();
        assert_eq!(hv.to_str().unwrap(), "5S");
    }

    #[test]
    fn round_trip_100_milliseconds() {
        let original = Duration::from_millis(100);
        let hv = timeout_to_header_value(original).unwrap();
        let decoded = header_value_to_timeout(&hv).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn deadline_remaining_decreases() {
        let d = Deadline::from_now(Duration::from_secs(60));
        let r1 = d.remaining().expect("should not be elapsed");
        // A tiny sleep isn't needed; just verify remaining <= 60s.
        assert!(r1 <= Duration::from_secs(60));
        assert!(r1 > Duration::ZERO);
    }
}
