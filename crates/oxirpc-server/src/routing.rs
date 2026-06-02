//! Method-level routing for gRPC services.
//! Full implementation in this file is added by the MethodRouter slice.

use http::{Request, Response};
use std::collections::HashMap;
use tower::util::BoxCloneService;

/// Routes incoming gRPC requests to different services by URI path.
///
/// Routes are matched by exact URI path string (e.g. `"/pkg.Service/Method"`).
/// If no route matches and no fallback is set, returns `UNIMPLEMENTED`.
pub struct MethodRouter {
    routes: HashMap<
        String,
        BoxCloneService<
            Request<tonic::body::Body>,
            Response<tonic::body::Body>,
            std::convert::Infallible,
        >,
    >,
    fallback: Option<
        BoxCloneService<
            Request<tonic::body::Body>,
            Response<tonic::body::Body>,
            std::convert::Infallible,
        >,
    >,
}

impl MethodRouter {
    /// Create an empty router with no routes and no fallback.
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            fallback: None,
        }
    }

    /// Register a handler for an exact URI path.
    ///
    /// If a handler for `path` already exists it is silently replaced.
    pub fn route<S>(mut self, path: impl Into<String>, service: S) -> Self
    where
        S: tower::Service<
                Request<tonic::body::Body>,
                Response = Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.routes
            .insert(path.into(), BoxCloneService::new(service));
        self
    }

    /// Register a fallback handler used when no exact route matches.
    pub fn fallback<S>(mut self, service: S) -> Self
    where
        S: tower::Service<
                Request<tonic::body::Body>,
                Response = Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.fallback = Some(BoxCloneService::new(service));
        self
    }
}

impl Default for MethodRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MethodRouter {
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
            fallback: self.fallback.clone(),
        }
    }
}

impl tower::Service<Request<tonic::body::Body>> for MethodRouter {
    type Response = Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<tonic::body::Body>) -> Self::Future {
        let path = req.uri().path().to_owned();
        if let Some(svc) = self.routes.get_mut(&path) {
            svc.call(req)
        } else if let Some(fb) = self.fallback.as_mut() {
            fb.call(req)
        } else {
            Box::pin(async move {
                let status = tonic::Status::unimplemented(format!("no handler for path: {path}"));
                Ok(status.into_http::<tonic::body::Body>())
            })
        }
    }
}
