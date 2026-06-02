//! Native gRPC interceptor traits — sync and async.
//!
//! Interceptors allow middleware to inspect or mutate `Request<()>` values
//! before they are dispatched. The sync [`Interceptor`] trait is object-safe
//! and usable today; [`AsyncInterceptor`] is reserved for the future native
//! async channel and is not yet wired into any dispatch path.

use crate::message::Request;
use crate::rpc::Status;

/// A synchronous, object-safe request interceptor.
///
/// Implementations receive a `Request<()>` and either return a (possibly
/// mutated) request to continue the chain, or a [`Status`] error to abort.
pub trait Interceptor: Send + Sync {
    /// Intercept a request, returning it (possibly modified) or an error.
    fn intercept(&self, req: Request<()>) -> Result<Request<()>, Status>;
}

/// Blanket impl so that closures can be used directly as interceptors.
impl<F> Interceptor for F
where
    F: Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync,
{
    fn intercept(&self, req: Request<()>) -> Result<Request<()>, Status> {
        (self)(req)
    }
}

/// Reserved stub for a future native async interceptor.
///
/// Not yet usable — no async dispatch path exists. Callers must poll
/// [`Interceptor::intercept`] for now.
pub trait AsyncInterceptor: Send + Sync {
    /// Intercept a request asynchronously.
    fn intercept_async<'a>(
        &'a self,
        req: Request<()>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Request<()>, Status>> + Send + 'a>>;
}
