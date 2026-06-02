//! Streaming body type for native gRPC-over-HTTP/2 calls.
//!
//! [`NativeBody`] implements [`http_body::Body`] and is used for both the request
//! and response body in native channel calls. The body can be empty, a single
//! chunk, or a multi-frame channel-backed stream.

use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

use crate::OxiRpcError;

// ── BodyItem ──────────────────────────────────────────────────────────────────

/// Items that can be sent through the body channel.
pub(crate) enum BodyItem {
    /// A data chunk.
    Data(Bytes),
    /// Trailing headers to be emitted after all data frames.
    Trailers(HeaderMap),
    /// A terminal error.
    Error(OxiRpcError),
}

// ── NativeBodyKind ────────────────────────────────────────────────────────────

/// Internal representation of a [`NativeBody`] stream.
enum NativeBodyKind {
    /// Fully consumed or always-empty body.
    Empty,
    /// A single in-memory chunk (not yet consumed).
    Once(Option<Bytes>),
    /// A multi-frame streaming body backed by an mpsc channel.
    Channel(mpsc::Receiver<BodyItem>),
    /// A dynamically-typed body wrapping an arbitrary `http_body::Body`
    /// with errors mapped to [`OxiRpcError`].
    Pinned(std::pin::Pin<Box<dyn http_body::Body<Data = Bytes, Error = OxiRpcError> + Send>>),
}

// ── NativeBody ────────────────────────────────────────────────────────────────

/// A streaming body for use with native gRPC-over-HTTP/2 channels.
///
/// Supports three modes:
/// - [`NativeBody::empty`] — no data.
/// - [`NativeBody::once`] — one in-memory chunk.
/// - [`body_channel`] — multi-frame async stream via mpsc.
pub struct NativeBody {
    inner: NativeBodyKind,
    /// Trailers supplied at EOF via `with_trailers()`.
    /// Used by the once/empty paths; the channel path uses in-band BodyItem::Trailers.
    trailers: Option<HeaderMap>,
}

impl NativeBody {
    /// Create an empty body that immediately signals end-of-stream.
    pub fn empty() -> Self {
        Self {
            inner: NativeBodyKind::Empty,
            trailers: None,
        }
    }

    /// Create a body with a single chunk of bytes.
    pub fn once(b: Bytes) -> Self {
        if b.is_empty() {
            return Self::empty();
        }
        Self {
            inner: NativeBodyKind::Once(Some(b)),
            trailers: None,
        }
    }

    /// Create a streaming body backed by an mpsc receiver.
    ///
    /// The sender half is [`NativeBodySender`]; use [`body_channel`] to create
    /// a matched pair.
    pub(crate) fn from_channel(rx: mpsc::Receiver<BodyItem>) -> Self {
        Self {
            inner: NativeBodyKind::Channel(rx),
            trailers: None,
        }
    }

    /// Attach trailers to be emitted after the last data frame.
    ///
    /// Used by the response pump to inject trailing metadata (e.g., OK-status
    /// with custom trailers) into the body stream after all data frames.
    pub fn with_trailers(mut self, trailers: HeaderMap) -> Self {
        self.trailers = Some(trailers);
        self
    }

    /// Wrap an arbitrary `http_body::Body` (e.g. `hyper::body::Incoming`) inside a
    /// `NativeBody`, mapping any foreign error into [`OxiRpcError::Transport`].
    ///
    /// This constructor accepts any body whose error type implements [`std::error::Error`]
    /// (and is `Send + 'static`), making it suitable for bridging hyper bodies and other
    /// foreign streaming bodies into the native gRPC pipeline without requiring a direct
    /// dependency on hyper in this crate.
    pub fn pinned<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + 'static,
    {
        use http_body_util::BodyExt;
        let mapped = body.map_err(|e| OxiRpcError::Transport(e.to_string()));
        Self {
            inner: NativeBodyKind::Pinned(Box::pin(mapped)),
            trailers: None,
        }
    }
}

impl Body for NativeBody {
    type Data = Bytes;
    type Error = OxiRpcError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.inner {
            NativeBodyKind::Empty => {
                // Emit trailers if any, then signal end.
                if let Some(trailers) = self.trailers.take() {
                    Poll::Ready(Some(Ok(Frame::trailers(trailers))))
                } else {
                    Poll::Ready(None)
                }
            }
            NativeBodyKind::Once(slot) => {
                if let Some(bytes) = slot.take() {
                    // After emitting the single chunk, mark as empty.
                    self.inner = NativeBodyKind::Empty;
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                } else if let Some(trailers) = self.trailers.take() {
                    Poll::Ready(Some(Ok(Frame::trailers(trailers))))
                } else {
                    Poll::Ready(None)
                }
            }
            NativeBodyKind::Channel(rx) => {
                match rx.poll_recv(cx) {
                    Poll::Ready(Some(BodyItem::Data(bytes))) => {
                        Poll::Ready(Some(Ok(Frame::data(bytes))))
                    }
                    Poll::Ready(Some(BodyItem::Trailers(h))) => {
                        // Trailer item consumed — transition to empty.
                        self.inner = NativeBodyKind::Empty;
                        Poll::Ready(Some(Ok(Frame::trailers(h))))
                    }
                    Poll::Ready(Some(BodyItem::Error(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        // Channel closed with no Trailers item — fall through to
                        // static trailers set via with_trailers() (once/empty path).
                        self.inner = NativeBodyKind::Empty;
                        if let Some(trailers) = self.trailers.take() {
                            Poll::Ready(Some(Ok(Frame::trailers(trailers))))
                        } else {
                            Poll::Ready(None)
                        }
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            NativeBodyKind::Pinned(inner) => {
                // inner: &mut Pin<Box<dyn Body<...>>>
                // Get a Pin<&mut dyn Body<...>> and delegate.
                inner.as_mut().poll_frame(cx)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            NativeBodyKind::Empty => self.trailers.is_none(),
            NativeBodyKind::Once(slot) => slot.is_none() && self.trailers.is_none(),
            NativeBodyKind::Channel(_) => false,
            NativeBodyKind::Pinned(inner) => inner.is_end_stream(),
        }
    }
}

// ── NativeBodySender ──────────────────────────────────────────────────────────

/// The sender half of a [`NativeBody`] channel.
///
/// Obtain via [`body_channel`]. Send data frames with [`send_data`](Self::send_data),
/// trailers with [`send_trailers`](Self::send_trailers), and propagate transport
/// errors with [`send_error`](Self::send_error).
#[derive(Clone)]
pub struct NativeBodySender(mpsc::Sender<BodyItem>);

impl NativeBodySender {
    /// Send a data chunk to the body stream.
    ///
    /// Returns an error if the receiver has been dropped (body was abandoned).
    pub async fn send_data(&self, data: Bytes) -> Result<(), OxiRpcError> {
        self.0
            .send(BodyItem::Data(data))
            .await
            .map_err(|_| OxiRpcError::Transport("body channel receiver dropped".to_owned()))
    }

    /// Send trailing headers to the body stream.
    ///
    /// After this call the receiver will emit the trailer frame and then close.
    pub async fn send_trailers(&self, headers: HeaderMap) -> Result<(), OxiRpcError> {
        self.0
            .send(BodyItem::Trailers(headers))
            .await
            .map_err(|_| OxiRpcError::Transport("body channel receiver dropped".to_owned()))
    }

    /// Send a terminal error to the body stream.
    ///
    /// After this call the receiver will yield the error and then close.
    pub async fn send_error(&self, e: OxiRpcError) {
        // Ignore send errors — receiver may already be dropped.
        let _ = self.0.send(BodyItem::Error(e)).await;
    }
}

// ── body_channel ──────────────────────────────────────────────────────────────

/// Create a matched sender/receiver pair for a streaming [`NativeBody`].
///
/// `buffer` is the number of frames to buffer before the sender blocks.
pub fn body_channel(buffer: usize) -> (NativeBodySender, NativeBody) {
    let (tx, rx) = mpsc::channel(buffer);
    (NativeBodySender(tx), NativeBody::from_channel(rx))
}

// ── std::fmt::Debug impls ─────────────────────────────────────────────────────

impl std::fmt::Debug for NativeBodyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NativeBodyKind::Empty => write!(f, "Empty"),
            NativeBodyKind::Once(_) => write!(f, "Once(..)"),
            NativeBodyKind::Channel(_) => write!(f, "Channel(..)"),
            NativeBodyKind::Pinned(_) => write!(f, "Pinned(..)"),
        }
    }
}

impl std::fmt::Debug for NativeBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBody")
            .field("inner", &self.inner)
            .field("has_trailers", &self.trailers.is_some())
            .finish()
    }
}

impl std::fmt::Debug for NativeBodySender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NativeBodySender(..)")
    }
}
