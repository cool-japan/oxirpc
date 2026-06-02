/// Service name constant for gRPC dispatch.
///
/// Implement this trait on any type that can be dispatched by service name.
/// The blanket impl covers all [`tonic::server::NamedService`] types, so
/// existing tonic services automatically satisfy this bound.
///
/// # Direct implementation
///
/// You can implement this trait directly on any type that does **not** already
/// implement [`tonic::server::NamedService`]:
///
/// ```rust
/// use oxirpc_server::OxiNamedService;
///
/// struct MyCustomService;
///
/// impl OxiNamedService for MyCustomService {
///     const NAME: &'static str = "my.package.MyService";
/// }
///
/// assert_eq!(MyCustomService::NAME, "my.package.MyService");
/// ```
///
/// Types that **do** implement [`tonic::server::NamedService`] automatically
/// satisfy this bound through the blanket impl below — no manual impl needed.
pub trait OxiNamedService {
    /// The fully-qualified service name, e.g. `"grpc.health.v1.Health"`.
    const NAME: &'static str;
}

impl<S: tonic::server::NamedService> OxiNamedService for S {
    const NAME: &'static str = S::NAME;
}
