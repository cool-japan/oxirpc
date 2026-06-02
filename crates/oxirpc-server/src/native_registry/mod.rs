//! Service registry for the native HTTP/2 transport.
//!
//! [`NativeServiceRegistry`] collects a set of named gRPC services and
//! type-erases them behind [`BoxedNativeService`].  After all services have
//! been added, call [`NativeServiceRegistry::into_service`] to obtain a
//! [`RegistryService`] that implements `tower::Service` and can be driven
//! by the native hyper transport.
//!
//! Dispatch is by *service name* prefix: the path `/pkg.Svc/Method` routes to
//! the service whose `NamedService::NAME == "pkg.Svc"`.

pub mod dispatch;
pub mod registry;
pub mod service_set;

pub use dispatch::RegistryService;
pub use registry::NativeServiceRegistry;
