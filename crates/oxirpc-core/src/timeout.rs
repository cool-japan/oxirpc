//! gRPC deadline / timeout propagation via the `grpc-timeout` header.
//!
//! The gRPC wire format encodes a timeout as an ASCII string `<value><unit>`
//! where `value` is at most 8 decimal digits and `unit` is one of:
//!
//! | unit | meaning      |
//! |------|--------------|
//! | `H`  | hours        |
//! | `M`  | minutes      |
//! | `S`  | seconds      |
//! | `m`  | milliseconds |
//! | `u`  | microseconds |
//! | `n`  | nanoseconds  |
//!
//! # Example
//!
//! ```rust
//! use std::time::Duration;
//! use oxirpc_core::timeout::{parse_grpc_timeout, format_grpc_timeout};
//!
//! assert_eq!(parse_grpc_timeout("5S").unwrap(), Duration::from_secs(5));
//! assert_eq!(parse_grpc_timeout("100m").unwrap(), Duration::from_millis(100));
//! // The coarsest exact unit is chosen: 2 seconds → "2S".
//! assert_eq!(format_grpc_timeout(Duration::from_secs(2)), "2S");
//! assert_eq!(format_grpc_timeout(Duration::from_millis(100)), "100m");
//! ```

use std::time::Duration;

/// Errors produced while parsing a `grpc-timeout` header.
#[derive(Debug, PartialEq, Eq)]
pub enum TimeoutError {
    /// The header was empty or missing its numeric component.
    Empty,
    /// The numeric component was not a valid non-negative integer, or exceeded
    /// 8 digits as mandated by the spec.
    InvalidValue,
    /// The unit suffix was not one of `H M S m u n`.
    InvalidUnit(char),
    /// The resulting duration overflowed [`Duration`].
    Overflow,
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutError::Empty => write!(f, "empty grpc-timeout header"),
            TimeoutError::InvalidValue => write!(f, "invalid grpc-timeout value"),
            TimeoutError::InvalidUnit(c) => write!(f, "invalid grpc-timeout unit: {c:?}"),
            TimeoutError::Overflow => write!(f, "grpc-timeout value overflowed Duration"),
        }
    }
}

impl std::error::Error for TimeoutError {}

/// Parse a `grpc-timeout` header value into a [`Duration`].
///
/// # Errors
///
/// See [`TimeoutError`].
pub fn parse_grpc_timeout(value: &str) -> Result<Duration, TimeoutError> {
    if value.is_empty() {
        return Err(TimeoutError::Empty);
    }
    let bytes = value.as_bytes();
    let unit = bytes[bytes.len() - 1];
    let digits = &value[..value.len() - 1];

    if digits.is_empty() || digits.len() > 8 {
        return Err(TimeoutError::InvalidValue);
    }
    let n: u64 = digits.parse().map_err(|_| TimeoutError::InvalidValue)?;

    let dur = match unit {
        b'H' => Duration::from_secs(n.checked_mul(3600).ok_or(TimeoutError::Overflow)?),
        b'M' => Duration::from_secs(n.checked_mul(60).ok_or(TimeoutError::Overflow)?),
        b'S' => Duration::from_secs(n),
        b'm' => Duration::from_millis(n),
        b'u' => Duration::from_micros(n),
        b'n' => Duration::from_nanos(n),
        other => return Err(TimeoutError::InvalidUnit(other as char)),
    };
    Ok(dur)
}

/// Format a [`Duration`] as a `grpc-timeout` header value.
///
/// The chosen unit is the **coarsest** one that represents `dur` *exactly*
/// (no precision loss) while keeping the numeric value within the spec's
/// 8-digit limit. For example, two seconds formats as `"2S"`, 100 milliseconds
/// as `"100m"`, and 250 microseconds as `"250u"`. Durations that are not an
/// exact multiple of any coarse unit fall through to nanoseconds; values too
/// large for 8 digits in a fine unit are promoted to a coarser one.
pub fn format_grpc_timeout(dur: Duration) -> String {
    const MAX: u128 = 99_999_999;
    let nanos = dur.as_nanos();

    // Candidate units from coarsest to finest: (divisor-in-nanos, suffix).
    const UNITS: [(u128, char); 6] = [
        (3_600_000_000_000, 'H'),
        (60_000_000_000, 'M'),
        (1_000_000_000, 'S'),
        (1_000_000, 'm'),
        (1_000, 'u'),
        (1, 'n'),
    ];

    // Prefer the coarsest unit that divides exactly and fits in 8 digits.
    for (div, suffix) in UNITS {
        if nanos.is_multiple_of(div) {
            let value = nanos / div;
            if value <= MAX {
                return format!("{value}{suffix}");
            }
        }
    }

    // Not exact in any unit (only possible for sub-nanosecond, which cannot
    // occur) or every exact unit overflowed 8 digits — fall back to the finest
    // unit whose value still fits, clamping at the maximum as a last resort.
    for (div, suffix) in UNITS.into_iter().rev() {
        let value = nanos / div;
        if value <= MAX {
            return format!("{value}{suffix}");
        }
    }
    format!("{MAX}H")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_units() {
        assert_eq!(parse_grpc_timeout("1H").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_grpc_timeout("2M").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_grpc_timeout("5S").unwrap(), Duration::from_secs(5));
        assert_eq!(
            parse_grpc_timeout("100m").unwrap(),
            Duration::from_millis(100)
        );
        assert_eq!(
            parse_grpc_timeout("1000000u").unwrap(),
            Duration::from_secs(1)
        );
        assert_eq!(
            parse_grpc_timeout("500n").unwrap(),
            Duration::from_nanos(500)
        );
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert_eq!(parse_grpc_timeout(""), Err(TimeoutError::Empty));
        assert_eq!(parse_grpc_timeout("S"), Err(TimeoutError::InvalidValue));
        assert_eq!(
            parse_grpc_timeout("123456789S"),
            Err(TimeoutError::InvalidValue)
        );
        assert_eq!(
            parse_grpc_timeout("5Z"),
            Err(TimeoutError::InvalidUnit('Z'))
        );
        assert_eq!(parse_grpc_timeout("abS"), Err(TimeoutError::InvalidValue));
    }

    #[test]
    fn format_round_trips() {
        for d in [
            Duration::from_secs(5),
            Duration::from_millis(100),
            Duration::from_micros(250),
            Duration::from_nanos(750),
            // Not an exact multiple of ms/s — must still round-trip via finer units.
            Duration::from_nanos(1_500_500),
            Duration::from_secs(7200),
        ] {
            let s = format_grpc_timeout(d);
            let back = parse_grpc_timeout(&s).unwrap();
            assert_eq!(back, d, "round-trip failed for {d:?} -> {s}");
        }
    }

    #[test]
    fn format_prefers_coarsest_exact_unit() {
        assert_eq!(format_grpc_timeout(Duration::from_secs(2)), "2S");
        assert_eq!(format_grpc_timeout(Duration::from_millis(100)), "100m");
        assert_eq!(format_grpc_timeout(Duration::from_micros(250)), "250u");
        assert_eq!(format_grpc_timeout(Duration::from_nanos(750)), "750n");
        assert_eq!(format_grpc_timeout(Duration::from_secs(3600)), "1H");
    }
}
