//! Native gRPC `Request<T>` and `Response<T>` types — independent of tonic.
//!
//! Both types carry a message payload, gRPC metadata, and an `http::Extensions`
//! bag for middleware to stash per-request values without polluting the public API.
//!
//! These live in `oxirpc_core::message` (not at the crate root) so they do not
//! collide with the `pub use tonic::{Request, Response}` re-exports in `lib.rs`.

use crate::metadata::Metadata;

/// A native gRPC request wrapping a message of type `T`.
#[derive(Debug)]
pub struct Request<T> {
    /// The request payload.
    pub message: T,
    /// gRPC metadata (headers sent with the request).
    pub metadata: Metadata,
    /// Opaque extension bag for middleware.
    pub extensions: http::Extensions,
}

impl<T> Request<T> {
    /// Wrap a message in a new request with empty metadata and extensions.
    pub fn new(message: T) -> Self {
        Self {
            message,
            metadata: Metadata::new(),
            extensions: http::Extensions::new(),
        }
    }

    /// Consume the request, returning the inner message.
    pub fn into_inner(self) -> T {
        self.message
    }

    /// Return a reference to the inner message.
    pub fn get_ref(&self) -> &T {
        &self.message
    }

    /// Return a mutable reference to the inner message.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.message
    }

    /// Return a reference to the request metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Return a mutable reference to the request metadata.
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Return a reference to the extension bag.
    pub fn extensions(&self) -> &http::Extensions {
        &self.extensions
    }

    /// Return a mutable reference to the extension bag.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        &mut self.extensions
    }

    /// Apply `f` to the message, producing a `Request<U>` with the same
    /// metadata and extensions.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Request<U> {
        Request {
            message: f(self.message),
            metadata: self.metadata,
            extensions: self.extensions,
        }
    }
}

/// A native gRPC response wrapping a message of type `T`.
#[derive(Debug)]
pub struct Response<T> {
    /// The response payload.
    pub message: T,
    /// gRPC trailing metadata (headers returned with the response).
    pub metadata: Metadata,
    /// Opaque extension bag for middleware.
    pub extensions: http::Extensions,
}

impl<T> Response<T> {
    /// Wrap a message in a new response with empty metadata and extensions.
    pub fn new(message: T) -> Self {
        Self {
            message,
            metadata: Metadata::new(),
            extensions: http::Extensions::new(),
        }
    }

    /// Consume the response, returning the inner message.
    pub fn into_inner(self) -> T {
        self.message
    }

    /// Return a reference to the inner message.
    pub fn get_ref(&self) -> &T {
        &self.message
    }

    /// Return a mutable reference to the inner message.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.message
    }

    /// Return a reference to the response metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Return a mutable reference to the response metadata.
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Return a reference to the extension bag.
    pub fn extensions(&self) -> &http::Extensions {
        &self.extensions
    }

    /// Return a mutable reference to the extension bag.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        &mut self.extensions
    }

    /// Apply `f` to the message, producing a `Response<U>` with the same
    /// metadata and extensions.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Response<U> {
        Response {
            message: f(self.message),
            metadata: self.metadata,
            extensions: self.extensions,
        }
    }
}
