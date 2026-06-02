//! Type-erased service wrapper for the native service registry.
//!
//! [`BoxedNativeService`] is the concrete type stored inside
//! [`super::registry::NativeServiceRegistry`].  Using
//! [`tower::util::BoxCloneService`] means each entry is `Clone`, which is
//! required to hand out per-request copies during dispatch without holding a
//! lock across the async boundary.

use std::convert::Infallible;

use http::{Request, Response};
use oxirpc_core::wire::NativeBody;
use tower::util::BoxCloneService;

/// A type-erased, cloneable, `Send` gRPC service.
///
/// The request body is generic over `B` so the registry can be constructed
/// with any body type that satisfies `http_body::Body<Data = Bytes>`.
/// Defaults to [`NativeBody`] (Phase 4.2).
///
/// The response body is always [`NativeBody`] (Phase 4.1 — the unnecessary
/// `tonic::body::Body::new(native_body)` wrapping has been removed).
pub type BoxedNativeService<B = NativeBody> =
    BoxCloneService<Request<B>, Response<NativeBody>, Infallible>;
