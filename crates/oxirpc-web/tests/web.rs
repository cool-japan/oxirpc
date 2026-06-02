use tower::Layer as _;

#[test]
fn grpc_web_layer_type_checks() {
    // Verify the layer compiles and is the correct type.
    let layer = oxirpc_web::grpc_web_layer();
    // GrpcWebLayer implements tower::Layer; confirm it is accessible
    let _: &dyn std::any::Any = &layer;
    // Verify the layer produces GrpcWebService<S> when applied.
    // We use a mock service to confirm the layer call doesn't panic.
    let mock = MockService;
    let _wrapped: oxirpc_web::GrpcWebService<MockService> = layer.layer(mock);
}

// ── minimal mock service to satisfy tower::Layer::layer(S) ────────────────────

#[derive(Clone, Debug)]
struct MockService;

impl<R> tower::Service<R> for MockService {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: R) -> Self::Future {
        std::future::ready(Ok(http::Response::new(tonic::body::Body::default())))
    }
}
