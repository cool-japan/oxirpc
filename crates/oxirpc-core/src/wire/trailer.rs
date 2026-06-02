//! gRPC status trailers.
//!
//! gRPC carries RPC result information in HTTP/2 trailers:
//! - `grpc-status`: integer status code (0 = OK).
//! - `grpc-message`: percent-encoded human-readable error description.
//! - `grpc-status-details-bin`: base64-encoded serialised `google.rpc.Status` proto.
//!
//! This module provides build/parse for those trailers plus helpers for the
//! percent-encoding scheme defined in the gRPC over HTTP/2 spec.

use http::{HeaderMap, HeaderValue};

use crate::{
    metadata::{base64_decode, base64_encode, Metadata},
    status::StatusCode,
    wire::WireError,
};

// ─── GrpcResponseStatus ───────────────────────────────────────────────────────

/// A decoded gRPC response status (trailers).
#[derive(Debug, Clone)]
pub struct GrpcResponseStatus {
    /// The gRPC status code.
    pub code: StatusCode,
    /// The percent-decoded status message.
    pub message: String,
    /// The raw (binary) status details, if present.
    pub details_bin: Option<bytes::Bytes>,
    /// Any other trailer headers as metadata.
    pub metadata: Metadata,
}

impl Default for GrpcResponseStatus {
    fn default() -> Self {
        Self {
            code: StatusCode::Ok,
            message: String::new(),
            details_bin: None,
            metadata: Metadata::new(),
        }
    }
}

// ─── build_trailers ──────────────────────────────────────────────────────────

/// Build HTTP trailer headers from a [`GrpcResponseStatus`].
///
/// - `grpc-status`: decimal status code.
/// - `grpc-message`: percent-encoded message (empty message omitted).
/// - `grpc-status-details-bin`: standard base64 of the details bytes (no padding).
/// - Trailing metadata is included as additional headers.
pub fn build_trailers(status: &GrpcResponseStatus) -> Result<HeaderMap, WireError> {
    let mut map = HeaderMap::new();

    // grpc-status
    let code_str = (status.code as i32).to_string();
    map.insert("grpc-status", hv(&code_str)?);

    // grpc-message (percent-encode)
    if !status.message.is_empty() {
        let encoded = percent_encode_message(&status.message);
        map.insert("grpc-message", hv(&encoded)?);
    }

    // grpc-status-details-bin (base64)
    if let Some(details) = &status.details_bin {
        let b64 = base64_encode(details);
        map.insert("grpc-status-details-bin", hv(&b64)?);
    }

    // Trailing metadata
    for (key, value_bytes) in status.metadata.iter() {
        let hn = hn(key)?;
        let hv = if Metadata::is_binary_key(key) {
            let encoded = base64_encode(value_bytes);
            HeaderValue::from_str(&encoded).map_err(|e| WireError::BadHeaderValue(e.to_string()))?
        } else {
            let s = std::str::from_utf8(value_bytes)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            hv(s)?
        };
        map.append(hn, hv);
    }

    Ok(map)
}

// ─── parse_trailers ──────────────────────────────────────────────────────────

/// Parse HTTP trailer headers into a [`GrpcResponseStatus`].
///
/// # Errors
///
/// - [`WireError::MissingStatusTrailer`] — no `grpc-status` key.
/// - [`WireError::BadStatusValue`] — `grpc-status` is not a valid integer or out
///   of range.
/// - [`WireError::BadStatusDetailsBin`] — `grpc-status-details-bin` base64 is
///   malformed.
pub fn parse_trailers(trailers: &HeaderMap) -> Result<GrpcResponseStatus, WireError> {
    // grpc-status — required
    let code_hv = trailers
        .get("grpc-status")
        .ok_or(WireError::MissingStatusTrailer)?;
    let code_str = code_hv
        .to_str()
        .map_err(|e| WireError::BadStatusValue(e.to_string()))?;
    let code_i32: i32 = code_str
        .parse()
        .map_err(|_| WireError::BadStatusValue(code_str.to_owned()))?;
    let code = StatusCode::from_i32_lossy(code_i32);

    // grpc-message (optional, percent-decoded)
    let message = if let Some(msg_hv) = trailers.get("grpc-message") {
        let msg_raw = msg_hv
            .to_str()
            .map_err(|e| WireError::PercentDecode(e.to_string()))?;
        percent_decode_message(msg_raw)?
    } else {
        String::new()
    };

    // grpc-status-details-bin (optional, base64-decoded)
    let details_bin = if let Some(det_hv) = trailers.get("grpc-status-details-bin") {
        let b64 = det_hv
            .to_str()
            .map_err(|_| WireError::BadStatusDetailsBin)?;
        let bytes = base64_decode(b64).ok_or(WireError::BadStatusDetailsBin)?;
        Some(bytes::Bytes::from(bytes))
    } else {
        None
    };

    // Trailing metadata (skip the known grpc-* fields)
    let reserved: std::collections::HashSet<&str> =
        ["grpc-status", "grpc-message", "grpc-status-details-bin"]
            .iter()
            .copied()
            .collect();

    let mut metadata = Metadata::new();
    for (name, value) in trailers.iter() {
        let key = name.as_str();
        if reserved.contains(key) {
            continue;
        }
        if Metadata::is_binary_key(key) {
            let b64 = value
                .to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            let bytes = base64_decode(b64)
                .ok_or_else(|| WireError::BadHeaderValue(format!("bad base64 in {key}")))?;
            metadata
                .insert_bin(key, &bytes)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        } else {
            let s = value
                .to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            metadata
                .insert(key, s)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        }
    }

    Ok(GrpcResponseStatus {
        code,
        message,
        details_bin,
        metadata,
    })
}

// ─── parse_trailers_only ─────────────────────────────────────────────────────

/// Parse a "trailers-only" response: a single `HEADERS+END_STREAM` frame that
/// carries both initial metadata and trailing status.
///
/// In addition to everything [`parse_trailers`] handles, this function also
/// accepts an HTTP `:status: 200` pseudo-header (typically stored as a regular
/// `":status"` or `"status"` entry by the h2 layer).
pub fn parse_trailers_only(headers: &HeaderMap) -> Result<GrpcResponseStatus, WireError> {
    // Accept :status 200 — the h2 layer may place this in the map.
    // We just delegate to parse_trailers which already handles the grpc-status key.
    parse_trailers(headers)
}

// ─── Percent-encoding helpers ────────────────────────────────────────────────

/// Percent-encode a `grpc-message` string.
///
/// Any byte outside the printable ASCII range (0x20..=0x7E) is encoded as
/// `%XX` with uppercase hex digits. Printable ASCII bytes other than `%`
/// are left as-is. The `%` character itself is encoded as `%25`.
pub(crate) fn percent_encode_message(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if (0x20..=0x7e).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_upper(b >> 4));
            out.push(hex_upper(b & 0xf));
        }
    }
    out
}

/// Percent-decode a `grpc-message` string.
///
/// # Errors
///
/// Returns [`WireError::PercentDecode`] for malformed `%XX` sequences.
pub(crate) fn percent_decode_message(s: &str) -> Result<String, WireError> {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(WireError::PercentDecode(format!(
                    "truncated percent-sequence at position {i}"
                )));
            }
            let hi = hex_val(bytes[i + 1]).ok_or_else(|| {
                WireError::PercentDecode(format!(
                    "invalid hex digit '{}' at position {}",
                    bytes[i + 1] as char,
                    i + 1
                ))
            })?;
            let lo = hex_val(bytes[i + 2]).ok_or_else(|| {
                WireError::PercentDecode(format!(
                    "invalid hex digit '{}' at position {}",
                    bytes[i + 2] as char,
                    i + 2
                ))
            })?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| WireError::PercentDecode(e.to_string()))
}

#[inline]
fn hex_upper(n: u8) -> char {
    b"0123456789ABCDEF"[n as usize] as char
}

#[inline]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

fn hv(s: &str) -> Result<HeaderValue, WireError> {
    HeaderValue::from_str(s).map_err(|e| WireError::BadHeaderValue(e.to_string()))
}

fn hn(s: &str) -> Result<http::HeaderName, WireError> {
    http::HeaderName::from_bytes(s.as_bytes()).map_err(|e| WireError::BadHeaderValue(e.to_string()))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::StatusCode;

    fn ok_status() -> GrpcResponseStatus {
        GrpcResponseStatus {
            code: StatusCode::Ok,
            message: String::new(),
            details_bin: None,
            metadata: Metadata::new(),
        }
    }

    #[test]
    fn ok_status_round_trip() {
        let status = ok_status();
        let trailers = build_trailers(&status).unwrap();
        let parsed = parse_trailers(&trailers).unwrap();
        assert_eq!(parsed.code, StatusCode::Ok);
        assert!(parsed.message.is_empty());
        assert!(parsed.details_bin.is_none());
    }

    #[test]
    fn non_ok_status_with_message_round_trip() {
        let status = GrpcResponseStatus {
            code: StatusCode::NotFound,
            message: "resource not found".to_owned(),
            details_bin: None,
            metadata: Metadata::new(),
        };
        let trailers = build_trailers(&status).unwrap();
        let parsed = parse_trailers(&trailers).unwrap();
        assert_eq!(parsed.code, StatusCode::NotFound);
        assert_eq!(parsed.message, "resource not found");
    }

    #[test]
    fn grpc_status_zero_with_message_preserved() {
        let status = GrpcResponseStatus {
            code: StatusCode::Ok,
            message: "all good".to_owned(),
            details_bin: None,
            metadata: Metadata::new(),
        };
        let trailers = build_trailers(&status).unwrap();
        let parsed = parse_trailers(&trailers).unwrap();
        assert_eq!(parsed.code, StatusCode::Ok);
        assert_eq!(parsed.message, "all good");
    }

    #[test]
    fn missing_grpc_status_returns_error() {
        let map = HeaderMap::new();
        let err = parse_trailers(&map).unwrap_err();
        assert!(matches!(err, WireError::MissingStatusTrailer));
    }

    #[test]
    fn status_details_bin_base64_round_trip() {
        let details = bytes::Bytes::from_static(b"\x08\x05\x12\x0bnot found");
        let status = GrpcResponseStatus {
            code: StatusCode::NotFound,
            message: String::new(),
            details_bin: Some(details.clone()),
            metadata: Metadata::new(),
        };
        let trailers = build_trailers(&status).unwrap();
        let parsed = parse_trailers(&trailers).unwrap();
        assert_eq!(parsed.details_bin.unwrap(), details);
    }

    #[test]
    fn bad_base64_in_details_bin_returns_error() {
        let mut map = HeaderMap::new();
        map.insert("grpc-status", HeaderValue::from_static("0"));
        // deliberately malformed base64
        map.insert("grpc-status-details-bin", HeaderValue::from_static("!!!"));
        let err = parse_trailers(&map).unwrap_err();
        assert!(matches!(err, WireError::BadStatusDetailsBin));
    }

    #[test]
    fn utf8_message_percent_encoded_and_decoded() {
        // Non-ASCII bytes must be percent-encoded on the wire.
        let original = "error: café";
        let encoded = percent_encode_message(original);
        // Must not contain raw non-ASCII
        assert!(encoded.bytes().all(|b| b.is_ascii()));
        let decoded = percent_decode_message(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn trailers_only_response_parsed_correctly() {
        let mut map = HeaderMap::new();
        map.insert("grpc-status", HeaderValue::from_static("5"));
        map.insert("grpc-message", HeaderValue::from_static("not%20found"));
        let parsed = parse_trailers_only(&map).unwrap();
        assert_eq!(parsed.code, StatusCode::NotFound);
        assert_eq!(parsed.message, "not found");
    }
}
