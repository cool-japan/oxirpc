//! [`NativeServiceRegistry`] — builder that accumulates named services.
//!
//! Each service is erased into a [`BoxedNativeService<B>`] keyed by its
//! `NamedService::NAME`.  Duplicate names overwrite the previous entry.
//!
//! Call [`NativeServiceRegistry::into_service`] once all services have been
//! registered to get a dispatchable [`RegistryService<B>`].

use std::collections::HashMap;
use std::convert::Infallible;

use crate::OxiNamedService;
use bytes::Bytes;
use http::{Request, Response};
use oxirpc_core::wire::NativeBody;
use tower::Service;

use super::dispatch::RegistryService;
use super::service_set::BoxedNativeService;

/// A builder that accumulates gRPC services by name.
///
/// # Type parameter
///
/// `B` is the request body type.  Defaults to [`NativeBody`] so most
/// callers never need to spell it out.
pub struct NativeServiceRegistry<B = NativeBody> {
    services: HashMap<&'static str, BoxedNativeService<B>>,
    fallback: Option<BoxedNativeService<B>>,
    async_interceptor: Option<std::sync::Arc<dyn oxirpc_core::interceptor::AsyncInterceptor>>,
}

impl<B> NativeServiceRegistry<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// Create an empty registry with no services and no fallback.
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
            fallback: None,
            async_interceptor: None,
        }
    }

    /// Register a service.
    ///
    /// The service's [`OxiNamedService::NAME`] is used as the lookup key.
    /// Registering a second service with the same name silently replaces the
    /// first.
    pub fn add_service<S>(mut self, svc: S) -> Self
    where
        S: OxiNamedService
            + Service<Request<B>, Response = Response<NativeBody>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.services.insert(S::NAME, BoxedNativeService::new(svc));
        self
    }

    /// Register a fallback service that handles all unrecognised service names.
    ///
    /// When set, any request whose service name is not found in the registry is
    /// forwarded to this service instead of returning UNIMPLEMENTED.
    pub fn with_fallback<S>(mut self, svc: S) -> Self
    where
        S: Service<Request<B>, Response = Response<NativeBody>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.fallback = Some(BoxedNativeService::new(svc));
        self
    }

    /// Set an opt-in asynchronous request interceptor.
    ///
    /// The interceptor runs on every dispatched request, before the matched
    /// service handler. It receives a metadata-only
    /// [`oxirpc_core::message::Request`] built from the request headers and may
    /// mutate that metadata (merged back into the request headers) or return a
    /// [`oxirpc_core::rpc::Status`] to short-circuit the call with a gRPC error
    /// response. When unset, dispatch is unchanged.
    pub fn with_async_interceptor(
        mut self,
        interceptor: std::sync::Arc<dyn oxirpc_core::interceptor::AsyncInterceptor>,
    ) -> Self {
        self.async_interceptor = Some(interceptor);
        self
    }

    /// Iterate over every registered service name.
    ///
    /// Order is unspecified (determined by the underlying `HashMap`).
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.services.keys().copied()
    }

    /// Consume the registry and produce a dispatchable [`RegistryService<B>`].
    pub fn into_service(self) -> RegistryService<B> {
        RegistryService::new(self.services, self.fallback, self.async_interceptor)
    }
}

impl<B> Default for NativeServiceRegistry<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn default() -> Self {
        Self::new()
    }
}
