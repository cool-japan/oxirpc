//! Integration tests for the native server wire helpers.

use bytes::{BufMut, Bytes, BytesMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use oxirpc_core::wire::server::{
    bidi_sequential_response, decode_grpc_message, encode_grpc_message, error_grpc_trailers,
    error_response_body, grpc_response_headers, ok_grpc_trailers, read_unary_request,
    streaming_response_body, unary_response_body,
};
use oxirpc_core::{NativeBody, OxiRpcError};

// ─── Test proto message ───────────────────────────────────────────────────────
// We use a simple prost message for testing.

/// A minimal test proto message for encode/decode roundtrip.
#[derive(Clone, PartialEq, prost::Message)]
struct TestMsg {
    #[prost(string, tag = "1")]
    value: String,
    #[prost(int32, tag = "2")]
    count: i32,
}

/// Helper: build a simple gRPC-framed bytes buffer from a slice of proto messages.
fn build_grpc_body(messages: &[TestMsg]) -> Bytes {
    let mut buf = BytesMut::new();
    for msg in messages {
        let encoded = prost::Message::encode_to_vec(msg);
        buf.put_u8(0u8); // uncompressed
        buf.put_u32(encoded.len() as u32);
        buf.put_slice(&encoded);
    }
    buf.freeze()
}

/// A simple test body that returns a fixed Bytes value once.
struct OnceBytesBody {
    bytes: Option<Bytes>,
}

impl OnceBytesBody {
    fn new(bytes: Bytes) -> Self {
        Self { bytes: Some(bytes) }
    }
}

impl http_body::Body for OnceBytesBody {
    type Data = Bytes;
    type Error = OxiRpcError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.bytes.take() {
            Some(b) if !b.is_empty() => Poll::Ready(Some(Ok(http_body::Frame::data(b)))),
            _ => Poll::Ready(None),
        }
    }
}

/// Helper to poll body one frame at a time.
fn poll_one_frame(
    body: &mut std::pin::Pin<Box<NativeBody>>,
) -> Poll<Option<Result<http_body::Frame<Bytes>, OxiRpcError>>> {
    use http_body::Body;
    use std::task::{RawWaker, RawWakerVTable, Waker};
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let raw_waker = RawWaker::new(std::ptr::null(), &VTABLE);
    let waker = unsafe { Waker::from_raw(raw_waker) };
    let mut cx = Context::from_waker(&waker);
    body.as_mut().poll_frame(&mut cx)
}

// ─── Test 1: encode then decode roundtrip ────────────────────────────────────

#[test]
fn encode_then_decode_roundtrip() {
    let msg = TestMsg {
        value: "hello".to_owned(),
        count: 42,
    };
    let encoded = encode_grpc_message(&msg).expect("encode");
    assert!(
        encoded.len() > 5,
        "encoded should be at least header + payload"
    );
    let decoded: TestMsg = decode_grpc_message(encoded).expect("decode");
    assert_eq!(decoded.value, "hello");
    assert_eq!(decoded.count, 42);
}

// ─── Test 2: decode rejects empty body ───────────────────────────────────────

#[test]
fn decode_rejects_empty_body() {
    let result: Result<TestMsg, OxiRpcError> = decode_grpc_message(Bytes::new());
    assert!(result.is_err(), "empty bytes should fail");
}

// ─── Test 3: grpc_response_headers contains required fields ──────────────────

#[test]
fn grpc_response_headers_has_content_type() {
    let headers = grpc_response_headers();
    let ct = headers.get("content-type").expect("content-type missing");
    assert_eq!(ct.to_str().unwrap(), "application/grpc");
    let te = headers.get("te").expect("te missing");
    assert_eq!(te.to_str().unwrap(), "trailers");
}

// ─── Test 4: ok_grpc_trailers has grpc-status 0 ──────────────────────────────

#[test]
fn ok_grpc_trailers_has_status_zero() {
    let t = ok_grpc_trailers();
    let s = t.get("grpc-status").expect("grpc-status missing");
    assert_eq!(s.to_str().unwrap(), "0");
}

// ─── Test 5: error_grpc_trailers has correct code and message ────────────────

#[test]
fn error_grpc_trailers_has_code_and_message() {
    let t = error_grpc_trailers(5, "not found");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "5");
    // grpc-message is percent-encoded
    let msg_hv = t.get("grpc-message").expect("grpc-message missing");
    let msg = msg_hv.to_str().unwrap();
    assert!(msg.contains("not"), "message should contain 'not'");
}

// ─── Test 6: unary_response_body emits data then ok trailer ──────────────────

#[tokio::test]
async fn unary_response_body_emits_data_then_ok_trailer() {
    let msg = TestMsg {
        value: "resp".to_owned(),
        count: 1,
    };
    let body = unary_response_body(&msg).expect("unary_response_body");
    let mut body = Box::pin(body);

    // First poll: data frame
    let frame1 = match poll_one_frame(&mut body) {
        Poll::Ready(Some(Ok(f))) => f,
        other => panic!("expected data frame, got {other:?}"),
    };
    assert!(frame1.is_data(), "first frame should be data");
    let data = frame1.into_data().unwrap();
    assert!(data.len() > 5, "data should contain gRPC header + payload");

    // Second poll: trailers
    let frame2 = match poll_one_frame(&mut body) {
        Poll::Ready(Some(Ok(f))) => f,
        other => panic!("expected trailers frame, got {other:?}"),
    };
    assert!(frame2.is_trailers(), "second frame should be trailers");
    let trailers = frame2.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "0");

    // Third poll: None
    let frame3 = poll_one_frame(&mut body);
    assert!(
        matches!(frame3, Poll::Ready(None)),
        "third poll should be None"
    );
}

// ─── Test 7: error_response_body emits only error trailer ────────────────────

#[tokio::test]
async fn error_response_body_emits_only_error_trailer() {
    let body = error_response_body(5, "test error");
    let mut body = Box::pin(body);

    // First poll: error trailers
    let frame1 = match poll_one_frame(&mut body) {
        Poll::Ready(Some(Ok(f))) => f,
        other => panic!("expected trailer frame, got {other:?}"),
    };
    assert!(frame1.is_trailers(), "should be trailers");
    let trailers = frame1.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "5");

    // Second poll: None
    assert!(matches!(poll_one_frame(&mut body), Poll::Ready(None)));
}

// ─── Test 8: streaming_response_body data then trailer ───────────────────────

#[tokio::test]
async fn streaming_response_body_data_then_trailer() {
    let (sender, body) = streaming_response_body();

    // Send 3 data frames then OK trailers in a separate task.
    let send_handle = tokio::spawn(async move {
        for i in 0u8..3 {
            sender
                .send_data(Bytes::from(vec![i]))
                .await
                .expect("send_data");
        }
        sender
            .send_trailers(ok_grpc_trailers())
            .await
            .expect("send_trailers");
    });

    // Collect all frames.
    use http_body_util::BodyExt;
    let collected = body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();
    let all_bytes = collected.to_bytes();

    send_handle.await.expect("task");

    // 3 bytes of data total (one byte per frame merged by collect)
    assert_eq!(all_bytes.len(), 3);
    // OK trailers present
    let t = trailers.expect("trailers should be present");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "0");
}

// ─── Test 9: streaming_response_body error trailer ───────────────────────────

#[tokio::test]
async fn streaming_response_body_error_trailer() {
    let (sender, body) = streaming_response_body();

    let send_handle = tokio::spawn(async move {
        sender
            .send_data(Bytes::from(b"x".to_vec()))
            .await
            .expect("send_data");
        sender
            .send_trailers(error_grpc_trailers(13, "internal"))
            .await
            .expect("send_trailers");
    });

    use http_body_util::BodyExt;
    let collected = body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();
    let all_bytes = collected.to_bytes();

    send_handle.await.expect("task");

    assert_eq!(all_bytes.len(), 1);
    let t = trailers.expect("trailers should be present");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "13");
}

// ─── Test 10: bidi_sequential 3 in 3 out ─────────────────────────────────────

#[tokio::test]
async fn bidi_sequential_3_in_3_out() {
    let msgs = vec![
        TestMsg {
            value: "a".into(),
            count: 1,
        },
        TestMsg {
            value: "b".into(),
            count: 2,
        },
        TestMsg {
            value: "c".into(),
            count: 3,
        },
    ];
    let body_bytes = build_grpc_body(&msgs);
    let req_body = OnceBytesBody::new(body_bytes);

    let resp_body = bidi_sequential_response(req_body, move |req: TestMsg| {
        Ok(TestMsg {
            value: req.value.to_uppercase(),
            count: req.count * 10,
        })
    });

    // Collect the response body.
    use http_body_util::BodyExt;
    let collected = resp_body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();
    let all_bytes = collected.to_bytes();

    // 3 responses encoded as gRPC frames
    let mut pos = 0;
    let mut decoded_resps: Vec<TestMsg> = Vec::new();
    while pos < all_bytes.len() {
        if pos + 5 > all_bytes.len() {
            break;
        }
        let len = u32::from_be_bytes([
            all_bytes[pos + 1],
            all_bytes[pos + 2],
            all_bytes[pos + 3],
            all_bytes[pos + 4],
        ]) as usize;
        let payload = &all_bytes[pos + 5..pos + 5 + len];
        decoded_resps.push(prost::Message::decode(payload).expect("decode resp"));
        pos += 5 + len;
    }

    assert_eq!(decoded_resps.len(), 3);
    assert_eq!(decoded_resps[0].value, "A");
    assert_eq!(decoded_resps[0].count, 10);
    assert_eq!(decoded_resps[1].value, "B");
    assert_eq!(decoded_resps[2].value, "C");

    // OK trailers
    let t = trailers.expect("trailers");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "0");
}

// ─── Test 11: bidi handler error emits error trailer ─────────────────────────

#[tokio::test]
async fn bidi_handler_error_emits_error_trailer() {
    let msgs = vec![
        TestMsg {
            value: "ok".into(),
            count: 1,
        },
        TestMsg {
            value: "fail".into(),
            count: 2,
        },
    ];
    let body_bytes = build_grpc_body(&msgs);
    let req_body = OnceBytesBody::new(body_bytes);

    let resp_body = bidi_sequential_response(req_body, move |req: TestMsg| {
        if req.value == "fail" {
            Err(OxiRpcError::Transport("deliberate failure".into()))
        } else {
            Ok(TestMsg {
                value: "handled".into(),
                count: 0,
            })
        }
    });

    use http_body_util::BodyExt;
    let collected = resp_body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();

    // trailers should be error
    let t = trailers.expect("trailers must be present even on error");
    let code: u32 = t
        .get("grpc-status")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_ne!(code, 0, "error trailer should not be OK");
}

// ─── Test 12: send_trailers is last item ─────────────────────────────────────

#[tokio::test]
async fn send_trailers_is_last_item() {
    let (sender, body) = streaming_response_body();

    let send_handle = tokio::spawn(async move {
        sender
            .send_trailers(ok_grpc_trailers())
            .await
            .expect("send_trailers");
        // Subsequent sends should fail (receiver done after trailers)
        // But we can't test that easily without races; just verify ordering.
    });

    use http_body_util::BodyExt;
    let collected = body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();
    let data = collected.to_bytes();

    send_handle.await.expect("task");

    assert!(data.is_empty(), "no data should be present");
    let t = trailers.expect("trailers must be present");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "0");
}

// ─── Test 13: read_unary_request works with simple body ──────────────────────

#[tokio::test]
async fn read_unary_request_roundtrip() {
    let msg = TestMsg {
        value: "test".into(),
        count: 99,
    };
    let encoded = encode_grpc_message(&msg).expect("encode");
    let req_body = OnceBytesBody::new(encoded);

    let decoded: TestMsg = read_unary_request(req_body)
        .await
        .expect("read_unary_request");
    assert_eq!(decoded.value, "test");
    assert_eq!(decoded.count, 99);
}

// ─── Test 14: decode_grpc_message rejects compressed flag (identity server) ──

#[test]
fn decode_rejects_compressed_flag_without_encoding() {
    use bytes::BufMut as _;
    use oxirpc_core::OxiRpcError;
    // Build a gRPC frame with the compressed flag set (byte 0 = 0x01)
    // payload can be anything since the flag check fires first.
    let payload = b"fake compressed bytes";
    let mut buf = BytesMut::new();
    buf.put_u8(0x01u8); // FLAG_COMPRESSED
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    let bytes = buf.freeze();

    let result: Result<TestMsg, OxiRpcError> = decode_grpc_message(bytes);
    assert!(
        result.is_err(),
        "compressed flag on identity server should return an error"
    );
    match result.unwrap_err() {
        OxiRpcError::Compression(_) => {} // expected
        other => panic!("expected OxiRpcError::Compression, got {other:?}"),
    }
}

// ─── Test 15: gzip encode → decode roundtrip using _with_encoding ─────────────

#[cfg(feature = "gzip")]
#[test]
fn decode_with_encoding_decompresses_gzip_frame() {
    use oxirpc_core::{
        encoding::CompressionEncoding,
        wire::server::{decode_grpc_message_with_encoding, encode_grpc_message_with_encoding},
    };
    let original = TestMsg {
        value: "gzip-roundtrip".into(),
        count: 7,
    };
    let framed =
        encode_grpc_message_with_encoding(&original, CompressionEncoding::Gzip).expect("encode");
    // Byte 0 should be FLAG_COMPRESSED (0x01)
    assert_eq!(framed[0], 0x01u8, "compressed flag must be set");
    let decoded: TestMsg =
        decode_grpc_message_with_encoding(framed, CompressionEncoding::Gzip).expect("decode");
    assert_eq!(decoded.value, "gzip-roundtrip");
    assert_eq!(decoded.count, 7);
}

// ─── Test 16: encode_grpc_message_with_encoding produces a compressed frame ──

#[cfg(feature = "gzip")]
#[test]
fn encode_with_encoding_compresses_gzip_frame() {
    use oxirpc_core::{
        encoding::CompressionEncoding, wire::server::encode_grpc_message_with_encoding,
    };
    let msg = TestMsg {
        value: "compress-me".into(),
        count: 3,
    };
    let uncompressed = encode_grpc_message(&msg).expect("uncompressed encode");
    let compressed =
        encode_grpc_message_with_encoding(&msg, CompressionEncoding::Gzip).expect("gzip encode");
    // Compressed flag byte must be 1
    assert_eq!(compressed[0], 0x01u8, "compressed flag must be 0x01");
    // The gzip-compressed payload header is set in bytes 1-4
    let compressed_payload_len =
        u32::from_be_bytes([compressed[1], compressed[2], compressed[3], compressed[4]]) as usize;
    assert!(
        compressed_payload_len > 0,
        "compressed payload must not be empty"
    );
    // Compressed size is likely smaller (or at least different) for non-trivial data;
    // just confirm the frame is structurally valid.
    assert_eq!(compressed.len(), 5 + compressed_payload_len);
    // Identity path for completeness — must produce uncompressed flag
    assert_eq!(uncompressed[0], 0x00u8, "uncompressed flag must be 0x00");
}

// ─── Test 17: read_unary_request_with_encoding roundtrip ─────────────────────

#[cfg(feature = "gzip")]
#[tokio::test]
async fn read_unary_request_with_encoding_roundtrip() {
    use oxirpc_core::{
        encoding::CompressionEncoding,
        wire::server::{encode_grpc_message_with_encoding, read_unary_request_with_encoding},
    };
    let original = TestMsg {
        value: "body-roundtrip".into(),
        count: 55,
    };
    let compressed_body =
        encode_grpc_message_with_encoding(&original, CompressionEncoding::Gzip).expect("encode");
    let req_body = OnceBytesBody::new(compressed_body);

    let decoded: TestMsg = read_unary_request_with_encoding(req_body, CompressionEncoding::Gzip)
        .await
        .expect("read_unary_request_with_encoding");
    assert_eq!(decoded.value, "body-roundtrip");
    assert_eq!(decoded.count, 55);
}

// ─── Tests 19-21: bidi_sequential_response_with_encoding ─────────────────────

#[cfg(feature = "gzip")]
mod bidi_encoding_tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use http_body_util::BodyExt;
    use oxirpc_core::{
        encoding::CompressionEncoding,
        wire::server::{bidi_sequential_response_with_encoding, encode_grpc_message_with_encoding},
        OxiRpcError,
    };
    use prost::Message;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMsg {
        #[prost(string, tag = "1")]
        value: String,
        #[prost(int32, tag = "2")]
        count: i32,
    }

    struct OnceBytesBody {
        bytes: Option<Bytes>,
    }

    impl OnceBytesBody {
        fn new(bytes: Bytes) -> Self {
            Self { bytes: Some(bytes) }
        }
    }

    impl http_body::Body for OnceBytesBody {
        type Data = Bytes;
        type Error = OxiRpcError;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            match self.bytes.take() {
                Some(b) if !b.is_empty() => Poll::Ready(Some(Ok(http_body::Frame::data(b)))),
                _ => Poll::Ready(None),
            }
        }
    }

    /// Build an uncompressed multi-message gRPC body.
    fn build_identity_grpc_body(messages: &[TestMsg]) -> Bytes {
        let mut buf = BytesMut::new();
        for msg in messages {
            let encoded = msg.encode_to_vec();
            buf.put_u8(0x00u8); // FLAG_UNCOMPRESSED
            buf.put_u32(encoded.len() as u32);
            buf.put_slice(&encoded);
        }
        buf.freeze()
    }

    /// Build a compressed multi-message gRPC body using gzip encoding.
    fn build_gzip_grpc_body(messages: &[TestMsg]) -> Bytes {
        let mut buf = BytesMut::new();
        for msg in messages {
            let frame = encode_grpc_message_with_encoding(msg, CompressionEncoding::Gzip)
                .expect("gzip encode");
            buf.put_slice(&frame);
        }
        buf.freeze()
    }

    /// Decode multiple response frames from a flat bytes buffer.
    fn decode_response_frames(bytes: &Bytes) -> Vec<TestMsg> {
        let mut pos = 0;
        let mut results = Vec::new();
        while pos + 5 <= bytes.len() {
            let flag = bytes[pos];
            let payload_len = u32::from_be_bytes([
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
                bytes[pos + 4],
            ]) as usize;
            let total = 5 + payload_len;
            if pos + total > bytes.len() {
                break;
            }
            let payload_slice = &bytes[pos + 5..pos + total];
            if flag == 0x01 {
                // compressed — use decode_grpc_message_with_encoding via the full frame
                let frame_bytes = bytes.slice(pos..pos + total);
                let msg: TestMsg = oxirpc_core::wire::server::decode_grpc_message_with_encoding(
                    frame_bytes,
                    CompressionEncoding::Gzip,
                )
                .expect("decode compressed response frame");
                results.push(msg);
            } else {
                let msg: TestMsg =
                    prost::Message::decode(payload_slice).expect("decode identity response frame");
                results.push(msg);
            }
            pos += total;
        }
        results
    }

    // ── Test 19: Identity encoding produces the same output as the original ────

    #[tokio::test]
    async fn bidi_with_encoding_identity_matches_original() {
        let msgs = vec![
            TestMsg {
                value: "x".into(),
                count: 1,
            },
            TestMsg {
                value: "y".into(),
                count: 2,
            },
        ];
        let body_bytes = build_identity_grpc_body(&msgs);

        // Collect reference output from the original (identity) helper.
        let ref_bytes = {
            use oxirpc_core::wire::server::bidi_sequential_response;
            let ref_body =
                bidi_sequential_response(OnceBytesBody::new(body_bytes.clone()), |req: TestMsg| {
                    Ok(TestMsg {
                        value: req.value.to_uppercase(),
                        count: req.count * 2,
                    })
                });
            let collected = ref_body.collect().await.expect("collect ref");
            collected.to_bytes()
        };

        // Collect output from the encoding-aware helper with Identity.
        let enc_bytes = {
            let enc_body = bidi_sequential_response_with_encoding(
                OnceBytesBody::new(body_bytes),
                CompressionEncoding::Identity,
                |req: TestMsg| {
                    Ok(TestMsg {
                        value: req.value.to_uppercase(),
                        count: req.count * 2,
                    })
                },
            );
            let collected = enc_body.collect().await.expect("collect enc");
            collected.to_bytes()
        };

        assert_eq!(
            ref_bytes, enc_bytes,
            "Identity encoding must produce identical bytes to bidi_sequential_response"
        );
    }

    // ── Test 20: Gzip compresses response frames ───────────────────────────────

    #[tokio::test]
    async fn bidi_with_encoding_gzip_compresses_responses() {
        let msgs = vec![
            TestMsg {
                value: "compress-me".into(),
                count: 10,
            },
            TestMsg {
                value: "also-compress".into(),
                count: 20,
            },
        ];
        let body_bytes = build_identity_grpc_body(&msgs);

        let resp_body = bidi_sequential_response_with_encoding(
            OnceBytesBody::new(body_bytes),
            CompressionEncoding::Gzip,
            |req: TestMsg| {
                Ok(TestMsg {
                    value: req.value.to_uppercase(),
                    count: req.count + 1,
                })
            },
        );

        let collected = resp_body.collect().await.expect("collect");
        let trailers = collected.trailers().cloned();
        let all_bytes = collected.to_bytes();

        // Every response frame must have the compressed flag set (byte 0 == 0x01).
        let mut pos = 0;
        let mut frame_count = 0usize;
        while pos + 5 <= all_bytes.len() {
            let flag = all_bytes[pos];
            assert_eq!(
                flag, 0x01u8,
                "response frame at offset {pos} should have compressed flag"
            );
            let payload_len = u32::from_be_bytes([
                all_bytes[pos + 1],
                all_bytes[pos + 2],
                all_bytes[pos + 3],
                all_bytes[pos + 4],
            ]) as usize;
            pos += 5 + payload_len;
            frame_count += 1;
        }
        assert_eq!(frame_count, 2, "should have exactly 2 response frames");

        // Trailers must be OK.
        let t = trailers.expect("trailers must be present");
        assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "0");

        // Decode and verify content.
        let decoded = decode_response_frames(&all_bytes);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].value, "COMPRESS-ME");
        assert_eq!(decoded[0].count, 11);
        assert_eq!(decoded[1].value, "ALSO-COMPRESS");
        assert_eq!(decoded[1].count, 21);
    }

    // ── Test 21: Gzip decompresses incoming request frames ────────────────────

    #[tokio::test]
    async fn bidi_with_encoding_gzip_decompresses_requests() {
        let msgs = vec![
            TestMsg {
                value: "incoming-compressed".into(),
                count: 5,
            },
            TestMsg {
                value: "second-compressed".into(),
                count: 7,
            },
        ];
        let body_bytes = build_gzip_grpc_body(&msgs);

        // Verify the input frames actually have the compressed flag set.
        assert_eq!(body_bytes[0], 0x01u8, "input frame must be compressed");

        let resp_body = bidi_sequential_response_with_encoding(
            OnceBytesBody::new(body_bytes),
            CompressionEncoding::Gzip,
            |req: TestMsg| {
                // The handler receives the *decompressed* proto type.
                Ok(TestMsg {
                    value: format!("echo:{}", req.value),
                    count: req.count * 3,
                })
            },
        );

        let collected = resp_body.collect().await.expect("collect");
        let trailers = collected.trailers().cloned();
        let all_bytes = collected.to_bytes();

        // Trailers must be OK (no decompression error).
        let t = trailers.expect("trailers must be present");
        assert_eq!(
            t.get("grpc-status").unwrap().to_str().unwrap(),
            "0",
            "decompression of request frames must not produce an error"
        );

        // Decode and verify the handler received decompressed data correctly.
        let decoded = decode_response_frames(&all_bytes);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].value, "echo:incoming-compressed");
        assert_eq!(decoded[0].count, 15);
        assert_eq!(decoded[1].value, "echo:second-compressed");
        assert_eq!(decoded[1].count, 21);
    }
}

// ─── Test 18: bidi_sequential_response rejects compressed input frame ─────────

#[tokio::test]
async fn bidi_response_rejects_compressed_input() {
    use bytes::BufMut as _;
    // Build a bidi request with the compressed flag set.
    let payload = b"fake compressed request";
    let mut buf = BytesMut::new();
    buf.put_u8(0x01u8); // FLAG_COMPRESSED
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    let body_bytes = buf.freeze();
    let req_body = OnceBytesBody::new(body_bytes);

    let resp_body = bidi_sequential_response(req_body, |_req: TestMsg| {
        // handler should never be called
        Ok(TestMsg {
            value: "unreachable".into(),
            count: 0,
        })
    });

    use http_body_util::BodyExt;
    let collected = resp_body.collect().await.expect("collect");
    let trailers = collected
        .trailers()
        .cloned()
        .expect("trailers must be present");
    let status_str = trailers
        .get("grpc-status")
        .expect("grpc-status")
        .to_str()
        .unwrap();
    let status_code: u32 = status_str.parse().expect("status is a number");
    assert_ne!(
        status_code, 0,
        "compressed bidi frame must result in error trailer"
    );
}

// ─── Test 22: encode_grpc_message produces byte-identical output ──────────────

#[test]
fn encode_grpc_message_byte_identical() {
    use prost::Message;
    // Use a populated message so encoded_len > 0 and the payload path is exercised.
    let msg = TestMsg {
        value: "byte-identical".to_owned(),
        count: 12345,
    };

    // Reference: manual encoding via encode_to_vec
    let raw = msg.encode_to_vec();
    let payload_len = raw.len() as u32;
    let mut expected = BytesMut::with_capacity(5 + raw.len());
    expected.extend_from_slice(&[0x00u8]); // FLAG_UNCOMPRESSED
    expected.extend_from_slice(&payload_len.to_be_bytes());
    expected.extend_from_slice(&raw);

    let actual = encode_grpc_message(&msg).expect("encode should succeed");
    assert_eq!(
        actual,
        expected.freeze(),
        "optimized encoder must produce byte-identical output to the reference implementation"
    );
}

// ─── Test 23: encode_grpc_message empty roundtrip ─────────────────────────────

#[test]
fn encode_grpc_message_empty_roundtrip() {
    let msg = TestMsg::default();
    let frame = encode_grpc_message(&msg).expect("encode should succeed");
    let decoded: TestMsg = decode_grpc_message(frame).expect("decode should succeed");
    assert_eq!(decoded, msg);
}

// ─── Tests 24-27: streaming bidi driver (incremental frame-by-frame) ─────────

/// A scripted body that yields data frames or transport errors from a queue.
/// Used to control exactly what the bidi driver sees step by step.
struct ScriptedBody {
    steps: std::collections::VecDeque<Result<Bytes, OxiRpcError>>,
}

impl ScriptedBody {
    fn new(steps: impl IntoIterator<Item = Result<Bytes, OxiRpcError>>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
        }
    }
}

impl http_body::Body for ScriptedBody {
    type Data = Bytes;
    type Error = OxiRpcError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.steps.pop_front() {
            None => Poll::Ready(None),
            Some(Ok(b)) => Poll::Ready(Some(Ok(http_body::Frame::data(b)))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
        }
    }
}

/// A channel-backed body for streaming tests. The body yields DATA frames as
/// they are sent through the `tx` channel and terminates when `tx` is dropped.
struct ChunkedBody {
    rx: tokio::sync::mpsc::UnboundedReceiver<Bytes>,
}

impl http_body::Body for ChunkedBody {
    type Data = Bytes;
    type Error = OxiRpcError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(http_body::Frame::data(data)))),
        }
    }
}

// ─── Test 24: Multi-chunk bidi (gRPC frame split across two DATA chunks) ──────

#[tokio::test]
async fn drive_bidi_multi_chunk_request() {
    // Build one gRPC frame that we'll send in two HTTP DATA chunks.
    let msg = TestMsg {
        value: "split".into(),
        count: 42,
    };
    let framed = encode_grpc_message(&msg).expect("encode");

    // Split the framed bytes roughly in half.
    let split = framed.len() / 2;
    let chunk1 = framed.slice(..split);
    let chunk2 = framed.slice(split..);

    // Also add a second complete gRPC frame in one chunk.
    let msg2 = TestMsg {
        value: "second".into(),
        count: 7,
    };
    let framed2 = encode_grpc_message(&msg2).expect("encode");

    let req_body = ScriptedBody::new([Ok(chunk1), Ok(chunk2), Ok(framed2)]);

    let resp_body = bidi_sequential_response(req_body, |req: TestMsg| {
        Ok(TestMsg {
            value: format!("echo:{}", req.value),
            count: req.count,
        })
    });

    use http_body_util::BodyExt;
    let collected = resp_body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();
    let all_bytes = collected.to_bytes();

    // Decode two response frames.
    let mut pos = 0usize;
    let mut responses: Vec<TestMsg> = Vec::new();
    while pos + 5 <= all_bytes.len() {
        let payload_len = u32::from_be_bytes([
            all_bytes[pos + 1],
            all_bytes[pos + 2],
            all_bytes[pos + 3],
            all_bytes[pos + 4],
        ]) as usize;
        let payload = &all_bytes[pos + 5..pos + 5 + payload_len];
        responses.push(prost::Message::decode(payload).expect("decode"));
        pos += 5 + payload_len;
    }

    assert_eq!(responses.len(), 2, "expected 2 responses");
    assert_eq!(responses[0].value, "echo:split");
    assert_eq!(responses[0].count, 42);
    assert_eq!(responses[1].value, "echo:second");
    assert_eq!(responses[1].count, 7);

    let t = trailers.expect("trailers");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "0");
}

// ─── Test 25: Streaming proof — response arrives before request body EOF ──────

#[tokio::test]
async fn drive_bidi_response_before_request_eof() {
    // This test proves the streaming rewrite works. The buffered (old) driver
    // would deadlock here because it waits for EOF before sending any response.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let msg1 = TestMsg {
        value: "first".into(),
        count: 1,
    };
    let framed1 = encode_grpc_message(&msg1).expect("encode");

    let msg2 = TestMsg {
        value: "second".into(),
        count: 2,
    };
    let framed2 = encode_grpc_message(&msg2).expect("encode");

    let req_body = ChunkedBody { rx };

    let resp_body = bidi_sequential_response(req_body, |req: TestMsg| {
        Ok(TestMsg {
            value: format!("resp:{}", req.value),
            count: req.count * 10,
        })
    });

    // Pin the response body so we can read it incrementally.
    let mut resp_body = Box::pin(resp_body);

    // Send frame 1 then read response 1 before closing the channel.
    tx.send(framed1).expect("send frame1");

    // Poll for the first DATA frame from the response body.
    // The streaming driver should emit it immediately after processing frame 1,
    // without waiting for the body to close.
    let resp1_data = loop {
        use http_body_util::BodyExt as _;
        let http_frame = resp_body
            .as_mut()
            .frame()
            .await
            .expect("frame1 must be present")
            .expect("frame1 must not be an error");
        if let Ok(data) = http_frame.into_data() {
            break data;
        }
        // If we got a non-data frame (e.g. trailers), keep polling.
    };

    // Decode the first response.
    let resp1_payload_len =
        u32::from_be_bytes([resp1_data[1], resp1_data[2], resp1_data[3], resp1_data[4]]) as usize;
    let resp1: TestMsg =
        prost::Message::decode(&resp1_data[5..5 + resp1_payload_len]).expect("decode resp1");
    assert_eq!(resp1.value, "resp:first");
    assert_eq!(resp1.count, 10);

    // Now send frame 2 and close the channel.
    tx.send(framed2).expect("send frame2");
    drop(tx);

    // Collect the rest of the response (response 2 + trailers).
    use http_body_util::BodyExt;
    let remaining = resp_body.collect().await.expect("collect remaining");
    let trailers = remaining.trailers().cloned();
    let remaining_bytes = remaining.to_bytes();

    // Decode the second response.
    let resp2_payload_len = u32::from_be_bytes([
        remaining_bytes[1],
        remaining_bytes[2],
        remaining_bytes[3],
        remaining_bytes[4],
    ]) as usize;
    let resp2: TestMsg =
        prost::Message::decode(&remaining_bytes[5..5 + resp2_payload_len]).expect("decode resp2");
    assert_eq!(resp2.value, "resp:second");
    assert_eq!(resp2.count, 20);

    let t = trailers.expect("trailers");
    assert_eq!(t.get("grpc-status").unwrap().to_str().unwrap(), "0");
}

// ─── Test 26: Transport error after first response ────────────────────────────

#[tokio::test]
async fn drive_bidi_transport_error_after_first_response() {
    // Feed a valid first gRPC frame, then a transport error.
    // With the streaming driver: response 1 is produced, then error trailers arrive.
    let msg1 = TestMsg {
        value: "ok-first".into(),
        count: 1,
    };
    let framed1 = encode_grpc_message(&msg1).expect("encode");

    let req_body = ScriptedBody::new([
        Ok(framed1),
        Err(OxiRpcError::Transport("simulated transport failure".into())),
    ]);

    let resp_body = bidi_sequential_response(req_body, |req: TestMsg| {
        Ok(TestMsg {
            value: format!("echo:{}", req.value),
            count: req.count,
        })
    });

    use http_body_util::BodyExt;
    let collected = resp_body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();
    let all_bytes = collected.to_bytes();

    // Response 1 must have been emitted (data bytes present).
    assert!(
        !all_bytes.is_empty(),
        "response 1 data must be present before the error"
    );

    // Trailers must be error (non-zero status).
    let t = trailers.expect("error trailers must be present");
    let code: u32 = t
        .get("grpc-status")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_ne!(
        code, 0,
        "error trailer should not be OK after transport failure"
    );
}

// ─── Test 27: Truncated gRPC frame at EOF ────────────────────────────────────

#[tokio::test]
async fn drive_bidi_truncated_frame_at_eof() {
    // Feed a partial gRPC frame header (3 bytes of a 5-byte header) then EOF.
    // The driver should return a transport error, not panic.
    let partial_header = Bytes::from_static(&[0x00u8, 0x00, 0x00]); // only 3 of 5 header bytes

    let req_body = ScriptedBody::new([Ok(partial_header)]);

    let resp_body = bidi_sequential_response(req_body, |_req: TestMsg| {
        Ok(TestMsg {
            value: "unreachable".into(),
            count: 0,
        })
    });

    use http_body_util::BodyExt;
    let collected = resp_body.collect().await.expect("collect");
    let trailers = collected.trailers().cloned();

    // Must have error trailers (non-zero grpc-status).
    let t = trailers.expect("error trailers must be present");
    let code: u32 = t
        .get("grpc-status")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_ne!(
        code, 0,
        "truncated frame must result in error trailer, not OK"
    );
}
