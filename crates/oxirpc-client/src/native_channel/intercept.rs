//! Header ↔ `Request<()>` marshalling for the optional async interceptor.

use http::HeaderMap;
use oxirpc_core::message::Request;
use oxirpc_core::metadata::Metadata;

/// Build a metadata-only `Request<()>` from outgoing request headers.
///
/// gRPC metadata keys are the lowercase HTTP/2 header names. Binary (`-bin`)
/// headers carry base64 on the wire and are decoded into raw bytes; ASCII
/// headers are taken verbatim. Pseudo-headers never appear in a `HeaderMap`,
/// so no filtering is required here.
pub(crate) fn request_from_headers(headers: &HeaderMap) -> Request<()> {
    let mut req = Request::new(());
    let md = req.metadata_mut();
    for (name, value) in headers.iter() {
        let key = name.as_str();
        if Metadata::is_binary_key(key) {
            if let Ok(s) = value.to_str() {
                if let Ok(raw) = Metadata::decode_wire_bin(s) {
                    let _ = md.insert_bin(key, &raw);
                }
            }
        } else if let Ok(s) = value.to_str() {
            let _ = md.insert(key, s);
        }
    }
    req
}

/// Merge the (possibly mutated) metadata of `req` back into `headers`.
///
/// Every key present in the request metadata is written into `headers`,
/// replacing any prior value for that key. Binary keys are base64-encoded
/// (via `Metadata::to_wire`). Keys absent from the metadata are left
/// untouched so unrelated headers survive.
pub(crate) fn apply_metadata_to_headers(req: &Request<()>, headers: &mut HeaderMap) {
    for (key, value) in req.metadata().to_wire() {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(key.as_bytes()),
            http::HeaderValue::from_str(&value),
        ) {
            headers.remove(&name);
            headers.append(name, val);
        }
    }
}
