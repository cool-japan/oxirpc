//! Native gRPC metadata — typed headers independent of tonic.
//!
//! gRPC metadata is a multimap of key/value pairs sent as HTTP/2 headers.
//! Keys come in two flavours:
//!
//! - **ASCII keys** carry a plain UTF-8/ASCII string value.
//! - **Binary keys** end in the suffix `-bin` and carry arbitrary bytes; on the
//!   wire the value is base64-encoded (RFC 4648, standard alphabet).
//!
//! Keys are always lowercase ASCII. This module enforces that invariant on
//! insertion and performs case-insensitive lookup.
//!
//! # Example
//!
//! ```rust
//! use oxirpc_core::metadata::Metadata;
//!
//! let mut md = Metadata::new();
//! md.insert("x-trace-id", "abc123").unwrap();
//! md.insert_bin("token-bin", b"\x00\x01\x02").unwrap();
//!
//! assert_eq!(md.get("x-trace-id"), Some("abc123"));
//! assert_eq!(md.get_bin("token-bin").unwrap(), vec![0, 1, 2]);
//! ```

use std::collections::HashMap;

/// Suffix that marks a metadata key as carrying binary (base64) data.
pub const BINARY_SUFFIX: &str = "-bin";

/// Errors produced while manipulating [`Metadata`].
#[derive(Debug, PartialEq, Eq)]
pub enum MetadataError {
    /// The key contained characters outside the legal HTTP/2 header-name set.
    InvalidKey(String),
    /// An ASCII value contained bytes outside the printable ASCII range.
    InvalidAsciiValue,
    /// A binary value failed base64 decoding.
    InvalidBase64,
    /// `get_bin` / `insert_bin` was called on a key lacking the `-bin` suffix,
    /// or a plain accessor was used on a `-bin` key.
    KeyKindMismatch,
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataError::InvalidKey(k) => write!(f, "invalid metadata key: {k:?}"),
            MetadataError::InvalidAsciiValue => write!(f, "invalid ASCII metadata value"),
            MetadataError::InvalidBase64 => write!(f, "invalid base64 in binary metadata value"),
            MetadataError::KeyKindMismatch => {
                write!(f, "metadata key kind mismatch (ascii vs -bin)")
            }
        }
    }
}

impl std::error::Error for MetadataError {}

/// A multimap of gRPC metadata key/value pairs.
///
/// Internally values are stored as raw bytes. For ASCII keys the bytes are the
/// value verbatim; for `-bin` keys the bytes are the *decoded* binary value
/// (base64 decoding/encoding happens at the API boundary).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Metadata {
    map: HashMap<String, Vec<Vec<u8>>>,
}

impl Metadata {
    /// Create an empty metadata map.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Whether the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// The number of distinct keys.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether `key` ends with the binary [`BINARY_SUFFIX`].
    pub fn is_binary_key(key: &str) -> bool {
        key.len() > BINARY_SUFFIX.len() && key.ends_with(BINARY_SUFFIX)
    }

    /// Insert an ASCII metadata value, replacing any prior values for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidKey`] for an illegal key,
    /// [`MetadataError::KeyKindMismatch`] if `key` ends in `-bin`, or
    /// [`MetadataError::InvalidAsciiValue`] if `value` is not printable ASCII.
    pub fn insert(&mut self, key: &str, value: &str) -> Result<(), MetadataError> {
        let key = normalize_key(key)?;
        if Metadata::is_binary_key(&key) {
            return Err(MetadataError::KeyKindMismatch);
        }
        if !is_valid_ascii_value(value) {
            return Err(MetadataError::InvalidAsciiValue);
        }
        self.map.insert(key, vec![value.as_bytes().to_vec()]);
        Ok(())
    }

    /// Append an ASCII value to `key`, preserving existing values.
    ///
    /// See [`Metadata::insert`] for error semantics.
    pub fn append(&mut self, key: &str, value: &str) -> Result<(), MetadataError> {
        let key = normalize_key(key)?;
        if Metadata::is_binary_key(&key) {
            return Err(MetadataError::KeyKindMismatch);
        }
        if !is_valid_ascii_value(value) {
            return Err(MetadataError::InvalidAsciiValue);
        }
        self.map
            .entry(key)
            .or_default()
            .push(value.as_bytes().to_vec());
        Ok(())
    }

    /// Insert a binary metadata value, replacing any prior values for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::KeyKindMismatch`] if `key` does not end in
    /// `-bin`, or [`MetadataError::InvalidKey`] for an illegal key.
    pub fn insert_bin(&mut self, key: &str, value: &[u8]) -> Result<(), MetadataError> {
        let key = normalize_key(key)?;
        if !Metadata::is_binary_key(&key) {
            return Err(MetadataError::KeyKindMismatch);
        }
        self.map.insert(key, vec![value.to_vec()]);
        Ok(())
    }

    /// Get the first ASCII value for `key`, if present.
    ///
    /// Returns [`None`] if the key is absent, is a `-bin` key, or the stored
    /// value is not valid UTF-8.
    pub fn get(&self, key: &str) -> Option<&str> {
        if Metadata::is_binary_key(key) {
            return None;
        }
        let lower = key.to_ascii_lowercase();
        self.map
            .get(&lower)
            .and_then(|v| v.first())
            .and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Get all ASCII values for `key`.
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        if Metadata::is_binary_key(key) {
            return Vec::new();
        }
        let lower = key.to_ascii_lowercase();
        self.map
            .get(&lower)
            .map(|vals| {
                vals.iter()
                    .filter_map(|b| std::str::from_utf8(b).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the first binary value for `key`, if present.
    ///
    /// Returns [`None`] if the key is absent or is not a `-bin` key.
    pub fn get_bin(&self, key: &str) -> Option<Vec<u8>> {
        if !Metadata::is_binary_key(key) {
            return None;
        }
        let lower = key.to_ascii_lowercase();
        self.map.get(&lower).and_then(|v| v.first()).cloned()
    }

    /// Whether `key` is present (case-insensitive).
    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(&key.to_ascii_lowercase())
    }

    /// Remove `key`, returning whether it was present.
    pub fn remove(&mut self, key: &str) -> bool {
        self.map.remove(&key.to_ascii_lowercase()).is_some()
    }

    /// Iterate over `(key, value-bytes)` pairs. Binary keys yield their decoded
    /// (raw) bytes; ASCII keys yield the value bytes.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.map
            .iter()
            .flat_map(|(k, vals)| vals.iter().map(move |v| (k.as_str(), v.as_slice())))
    }

    /// Produce the on-the-wire header pairs. Binary keys have their values
    /// base64-encoded (no padding, per the gRPC convention).
    pub fn to_wire(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (k, vals) in &self.map {
            let binary = Metadata::is_binary_key(k);
            for v in vals {
                let value = if binary {
                    base64_encode(v)
                } else {
                    String::from_utf8_lossy(v).into_owned()
                };
                out.push((k.clone(), value));
            }
        }
        out
    }

    /// Decode the binary value carried by `wire_value` for a `-bin` key.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidBase64`] if decoding fails.
    pub fn decode_wire_bin(wire_value: &str) -> Result<Vec<u8>, MetadataError> {
        base64_decode(wire_value).ok_or(MetadataError::InvalidBase64)
    }
}

/// Validate and lower-case a metadata key.
fn normalize_key(key: &str) -> Result<String, MetadataError> {
    if key.is_empty() {
        return Err(MetadataError::InvalidKey(key.to_owned()));
    }
    // HTTP/2 header names: lowercase token chars. We accept the common subset:
    // a-z, 0-9, and the symbols - _ . that appear in gRPC metadata keys.
    for b in key.bytes() {
        let ok = b.is_ascii_lowercase()
            || b.is_ascii_uppercase()
            || b.is_ascii_digit()
            || matches!(b, b'-' | b'_' | b'.');
        if !ok {
            return Err(MetadataError::InvalidKey(key.to_owned()));
        }
    }
    Ok(key.to_ascii_lowercase())
}

/// gRPC ASCII metadata values must be printable ASCII (0x20..=0x7E), plus tab.
fn is_valid_ascii_value(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b == b'\t' || (0x20..=0x7e).contains(&b))
}

// ---------------------------------------------------------------------------
// Minimal RFC 4648 base64 (standard alphabet) — pure Rust, no dependency.
// ---------------------------------------------------------------------------

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode bytes as standard base64 **without** padding (gRPC convention).
pub fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(B64_ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(B64_ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_ALPHABET[((triple >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64_ALPHABET[(triple & 0x3f) as usize] as char);
        }
    }
    out
}

/// Decode standard base64, tolerating optional `=` padding (gRPC convention).
///
/// Returns [`None`] for invalid input.
pub fn base64_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    // Strip padding; we reconstruct length from the symbol count.
    let symbols: Vec<u8> = input.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(symbols.len() * 3 / 4);

    for chunk in symbols.chunks(4) {
        if chunk.len() == 1 {
            // A single trailing symbol is never valid base64.
            return None;
        }
        let mut acc: u32 = 0;
        for &c in chunk {
            acc = (acc << 6) | val(c)?;
        }
        // Left-justify the accumulated bits within 24.
        acc <<= 6 * (4 - chunk.len() as u32);
        out.push(((acc >> 16) & 0xff) as u8);
        if chunk.len() >= 3 {
            out.push(((acc >> 8) & 0xff) as u8);
        }
        if chunk.len() >= 4 {
            out.push((acc & 0xff) as u8);
        }
    }
    Some(out)
}
