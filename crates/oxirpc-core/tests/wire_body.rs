//! Tests for `NativeBody::pinned` — the dynamic body wrapping variant.

use bytes::Bytes;
use http::HeaderMap;
use http_body_util::BodyExt;
use oxirpc_core::wire::{body_channel, NativeBody};
use std::pin::Pin;
use std::task::{Context, Poll};

// ─── Test 1: Unpin invariant ──────────────────────────────────────────────────

#[test]
fn native_body_is_unpin() {
    fn assert_unpin<T: Unpin>() {}
    assert_unpin::<NativeBody>();
}

// ─── Test 2: pinned wraps a once-body roundtrip ───────────────────────────────

#[tokio::test]
async fn pinned_wraps_once_body_roundtrip() {
    let data = Bytes::from_static(b"hello from pinned");
    let inner = NativeBody::once(data.clone());
    let pinned = NativeBody::pinned(inner);
    let collected = pinned.collect().await.expect("collect");
    assert_eq!(collected.to_bytes(), data);
}

// ─── Test 3: pinned passes through trailers ───────────────────────────────────

#[tokio::test]
async fn pinned_passes_through_trailers() {
    let (tx, rx) = body_channel(4);
    let pinned = NativeBody::pinned(rx);

    let mut want_trailers = HeaderMap::new();
    want_trailers.insert("grpc-status", http::HeaderValue::from_static("0"));

    let tx_clone = tx.clone();
    let want_clone = want_trailers.clone();
    tokio::spawn(async move {
        tx_clone.send_data(Bytes::from_static(b"frame1")).await.ok();
        tx_clone.send_trailers(want_clone).await.ok();
    });

    let collected = pinned.collect().await.expect("collect");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let body_bytes = collected.to_bytes();
    assert_eq!(body_bytes, Bytes::from_static(b"frame1"));
    assert_eq!(
        trailers.get("grpc-status").map(|v| v.as_bytes()),
        Some(b"0".as_slice())
    );
}

// ─── Test 4: pinned maps foreign error ───────────────────────────────────────

/// A minimal body that immediately yields an `std::io::Error`.
struct ErrBody;

impl http_body::Body for ErrBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(Some(Err(std::io::Error::other("injected test error"))))
    }

    fn is_end_stream(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn pinned_maps_foreign_error() {
    let pinned = NativeBody::pinned(ErrBody);
    let result = pinned.collect().await;
    match result {
        Err(oxirpc_core::OxiRpcError::Transport(msg)) => {
            assert!(msg.contains("injected test error"), "got: {msg}");
        }
        other => panic!("expected OxiRpcError::Transport, got: {other:?}"),
    }
}
