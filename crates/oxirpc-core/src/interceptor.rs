//! Native gRPC interceptor traits — sync and async.
//!
//! Interceptors allow middleware to inspect or mutate `Request<()>` values
//! before they are dispatched. The sync [`Interceptor`] trait is object-safe
//! and usable today; [`AsyncInterceptor`] is an opt-in async interceptor wired
//! into the native client channel and the native server registry.

use crate::message::Request;
use crate::rpc::Status;
use std::future::Future;
use std::pin::Pin;

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

/// An opt-in asynchronous request interceptor.
///
/// Wired into the native client channel
/// (`NativeChannelBuilder::with_async_interceptor`) and the native server
/// registry (`NativeServiceRegistry::with_async_interceptor`). Implementations
/// receive a metadata-only `Request<()>` built from the request headers and
/// either return a (possibly mutated) request to continue — the mutated
/// metadata is merged back into the outgoing/incoming headers — or a
/// [`Status`] error to abort the call.
pub trait AsyncInterceptor: Send + Sync {
    /// Intercept a request asynchronously, returning it (possibly modified) or
    /// an error.
    fn intercept_async<'a>(
        &'a self,
        req: Request<()>,
    ) -> Pin<Box<dyn Future<Output = Result<Request<()>, Status>> + Send + 'a>>;
}

/// Blanket impl so that closures returning a future can be used as async
/// interceptors.
impl<F, Fut> AsyncInterceptor for F
where
    F: Fn(Request<()>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Request<()>, Status>> + Send + 'static,
{
    fn intercept_async<'a>(
        &'a self,
        req: Request<()>,
    ) -> Pin<Box<dyn Future<Output = Result<Request<()>, Status>> + Send + 'a>> {
        Box::pin((self)(req))
    }
}
