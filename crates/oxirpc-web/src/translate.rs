//! Native gRPC-Web ↔ gRPC HTTP/2 request/response translation (Pure Rust).
//!
//! This module translates between the gRPC-Web wire format (as sent by browsers
//! over HTTP/1.1) and the native gRPC HTTP/2 format that tonic services expect.
//!
//! # Request translation ([`translate_request`])
//!
//! A gRPC-Web request carries `Content-Type: application/grpc-web[+proto]` or
//! `application/grpc-web-text[+proto]`.  In text mode the body is base64-encoded.
//! After detection and optional base64-decode the function rewrites `content-type`
//! to `application/grpc+proto` and sets `te: trailers`, returning the modified
//! request for the inner tonic service.
//!
//! # Response translation ([`translate_response`])
//!
//! A gRPC response body consists of 5-byte-prefixed data frames.  HTTP/2 delivers
//! trailing metadata out-of-band; gRPC-Web requires those trailers to be appended
//! to the body as a trailer frame (`FLAG_TRAILER = 0x80`).  This function:
//!
//! 1. Collects all body bytes via `http_body_util::BodyExt::collect`.
//! 2. Reads the HTTP trailers from the collected body.
//! 3. Appends a [`crate::codec::Frame::trailers`] frame.
//! 4. Optionally base64-encodes the result (text mode).
//! 5. Returns a `200 OK` response with the encoded body.

use bytes::Bytes;
use http::{HeaderValue, Request, Response};
use http_body_util::BodyExt as _;
use tonic::body::Body as TonicBody;

use crate::codec::{encode_body, Frame, FrameError};
use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_core::metadata::base64_encode;

// ─── Content-type detection ───────────────────────────────────────────────────

/// Content-type variants for gRPC-Web.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcWebContentType {
    /// `application/grpc-web` or `application/grpc-web+proto`.
    Binary,
    /// `application/grpc-web-text` or `application/grpc-web-text+proto`.
    Text,
}

impl GrpcWebContentType {
    /// Parse the gRPC-Web content-type from a `Content-Type` header value.
    ///
    /// Returns [`None`] if the value does not correspond to a gRPC-Web type.
    pub fn from_header(value: &str) -> Option<Self> {
        // Strip parameters (e.g. `; charset=utf-8`).
        let ct = value.split(';').next().unwrap_or("").trim();
        match ct {
            "application/grpc-web" | "application/grpc-web+proto" => Some(Self::Binary),
            "application/grpc-web-text" | "application/grpc-web-text+proto" => Some(Self::Text),
            _ => None,
        }
    }

    /// The `Content-Type` to set on the translated gRPC request sent to the
    /// inner tonic service.
    #[allow(clippy::unused_self)]
    pub fn grpc_content_type(self) -> &'static str {
        "application/grpc+proto"
    }

    /// The `Content-Type` to set on the gRPC-Web response returned to the
    /// browser.
    pub fn response_content_type(self) -> &'static str {
        match self {
            Self::Binary => "application/grpc-web+proto",
            Self::Text => "application/grpc-web-text+proto",
        }
    }
}

// ─── Request translation ──────────────────────────────────────────────────────

/// Headers stripped from gRPC-Web requests before forwarding to the inner
/// gRPC service.  These are CORS-unsafe headers that browsers add.
const STRIP_REQUEST_HEADERS: &[&str] = &["origin", "referer", "x-user-agent"];

/// Translate an inbound gRPC-Web [`Request`] into a native gRPC [`Request`].
///
/// Steps:
/// 1. Collect the request body.
/// 2. If `content_type` is [`GrpcWebContentType::Text`], base64-decode the body.
/// 3. Strip CORS-unsafe headers (`origin`, `referer`, `x-user-agent`).
/// 4. Set `content-type: application/grpc+proto`.
/// 5. Set `te: trailers`.
/// 6. Remove `content-length` (length changes after base64-decode).
///
/// # Errors
///
/// Returns [`FrameError::InvalidBase64`] if the text-mode body is not valid
/// base64, or [`FrameError::Codec`] on a body-collection failure.
pub async fn translate_request<B>(
    req: Request<B>,
    content_type: GrpcWebContentType,
) -> Result<Request<TonicBody>, FrameError>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display,
{
    let (mut parts, body) = req.into_parts();

    // Collect the entire request body.
    let raw_bytes: Bytes = body
        .collect()
        .await
        .map_err(|e| FrameError::Codec(e.to_string()))?
        .to_bytes();

    // For text mode, base64-decode the body.
    let frame_bytes: Bytes = if content_type == GrpcWebContentType::Text {
        let text = std::str::from_utf8(&raw_bytes)
            .map_err(|e| FrameError::Codec(format!("grpc-web-text body is not UTF-8: {e}")))?;
        let decoded =
            oxirpc_core::metadata::base64_decode(text).ok_or(FrameError::InvalidBase64)?;
        Bytes::from(decoded)
    } else {
        raw_bytes
    };

    // Strip CORS-unsafe headers.
    for name in STRIP_REQUEST_HEADERS {
        if let Ok(header_name) = http::header::HeaderName::from_bytes(name.as_bytes()) {
            parts.headers.remove(&header_name);
        }
    }

    // Rewrite Content-Type to native gRPC.
    parts.headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc+proto"),
    );

    // Require trailers per the gRPC spec.
    parts
        .headers
        .insert(http::header::TE, HeaderValue::from_static("trailers"));

    // Remove content-length — the body length may have changed after base64 decode.
    parts.headers.remove(http::header::CONTENT_LENGTH);

    let new_body = TonicBody::new(http_body_util::Full::new(frame_bytes));
    Ok(Request::from_parts(parts, new_body))
}

// ─── Response translation ─────────────────────────────────────────────────────

/// Translate a gRPC [`Response`] into a gRPC-Web response.
///
/// gRPC delivers trailing metadata (e.g. `grpc-status`, `grpc-message`) as
/// HTTP/2 trailers — a mechanism unavailable over HTTP/1.1.  The gRPC-Web
/// protocol embeds trailers in the body as a trailer frame
/// (`FLAG_TRAILER = 0x80`).
///
/// Steps:
/// 1. Collect the entire gRPC response body (data frames + HTTP trailers).
/// 2. Convert the HTTP trailers into a gRPC-Web [`Frame::trailers`] frame.
/// 3. Concatenate the raw data bytes with the encoded trailer frame.
/// 4. If `content_type` is [`GrpcWebContentType::Text`], base64-encode the
///    entire binary buffer.
/// 5. Return a `200 OK` response with the encoded body and the appropriate
///    `Content-Type` header.
pub async fn translate_response<B>(
    resp: Response<B>,
    content_type: GrpcWebContentType,
) -> Response<TonicBody>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display,
{
    let (resp_parts, body) = resp.into_parts();

    // Collect body data and HTTP/2 trailers together.
    let collected = match body.collect().await {
        Ok(c) => c,
        Err(e) => {
            return error_body_response(
                format!("body collection failed: {e}"),
                content_type.response_content_type(),
            );
        }
    };

    // Extract HTTP trailers BEFORE consuming `collected` with `to_bytes()`.
    let trailer_pairs: Vec<(String, String)> = collected
        .trailers()
        .map(|map| {
            map.iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|v| (name.as_str().to_owned(), v.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default();

    // Consume `collected` to get the data bytes.
    let data_bytes = collected.to_bytes();

    // Build the gRPC-Web trailer frame from the HTTP trailers.
    let trailer_pairs_ref: Vec<(&str, &str)> = trailer_pairs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let trailer_frame = Frame::trailers(&trailer_pairs_ref);

    // Encode the trailer frame and append it to the raw gRPC data bytes.
    let trailer_encoded = match encode_body(&[trailer_frame], CompressionEncoding::Identity) {
        Ok(b) => b,
        Err(e) => {
            return error_body_response(
                format!("trailer frame encode failed: {e}"),
                content_type.response_content_type(),
            );
        }
    };

    let mut output = data_bytes.to_vec();
    output.extend_from_slice(&trailer_encoded);

    // Text mode: base64-encode the entire binary output.
    let final_bytes: Bytes = if content_type == GrpcWebContentType::Text {
        Bytes::from(base64_encode(&output).into_bytes())
    } else {
        Bytes::from(output)
    };

    // Build the 200 OK response with the gRPC-Web Content-Type.
    let new_body = TonicBody::new(http_body_util::Full::new(final_bytes));
    let mut builder = Response::builder().status(200).header(
        http::header::CONTENT_TYPE,
        content_type.response_content_type(),
    );

    // Propagate response headers, excluding headers that become stale after body
    // transformation (content-type is already set above; content-length changes
    // after trailer-frame append and optional base64 encoding;
    // transfer-encoding is invalid for HTTP/1.1 responses with a framed body).
    for (name, value) in &resp_parts.headers {
        if name == http::header::CONTENT_TYPE
            || name == http::header::CONTENT_LENGTH
            || name == http::header::TRANSFER_ENCODING
        {
            continue;
        }
        builder = builder.header(name, value);
    }

    builder
        .body(new_body)
        .unwrap_or_else(|_| Response::new(TonicBody::new(http_body_util::Full::new(Bytes::new()))))
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Build a gRPC-Web error response with `grpc-status: 13` (Internal) embedded
/// in the body as a trailer frame.  Used when body collection or encoding fails.
fn error_body_response(msg: String, response_content_type: &'static str) -> Response<TonicBody> {
    let trailer_frame = Frame::trailers(&[("grpc-status", "13"), ("grpc-message", &msg)]);
    let encoded = encode_body(&[trailer_frame], CompressionEncoding::Identity).unwrap_or_default();
    let body = TonicBody::new(http_body_util::Full::new(Bytes::from(encoded)));
    Response::builder()
        .status(200)
        .header(http::header::CONTENT_TYPE, response_content_type)
        .body(body)
        .unwrap_or_else(|_| Response::new(TonicBody::new(http_body_util::Full::new(Bytes::new()))))
}
