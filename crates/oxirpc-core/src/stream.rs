//! Native gRPC streaming wrapper.
//!
//! [`Streaming<T>`] is an owned, type-erased `Stream` of `Result<T, Status>`
//! items. It hides the concrete stream type behind a `Pin<Box<dyn Stream>>`
//! so that callers do not need to parameterise on the underlying stream
//! implementation.

use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::rpc::Status;

/// An owned stream of fallible items of type `T`.
///
/// Yields `Result<T, Status>` values; the stream ends when the inner stream
/// returns `Poll::Ready(None)`.
pub struct Streaming<T> {
    inner: Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>,
}

impl<T> Streaming<T> {
    /// Construct a [`Streaming<T>`] from any `Send + 'static` stream.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<T, Status>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> std::fmt::Debug for Streaming<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streaming").finish_non_exhaustive()
    }
}

impl<T> Stream for Streaming<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
