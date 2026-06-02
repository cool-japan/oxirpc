//! Native gRPC-Web HTTP/1.1 + HTTP/2 server transport ([`GrpcWebServer`]).
//!
//! This module provides a zero-tonic TCP listener that serves gRPC-Web requests
//! over **both** HTTP/1.1 (browser clients) and HTTP/2 (native gRPC clients)
//! via protocol auto-negotiation from
//! [`hyper_util::server::conn::auto::Builder`].
//!
//! The inner [`tower::Service`] is wrapped with [`crate::native::NativeGrpcWebLayer`]
//! (gRPC-Web framing translation) and an optional [`crate::cors::CorsLayer`]
//! (CORS preflight + header injection), so a single port handles all client
//! types.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::net::SocketAddr;
//! use oxirpc_web::transport::GrpcWebServer;
//! use oxirpc_web::cors::CorsPolicy;
//!
//! async fn run(my_service: impl tower::Service<
//!     http::Request<tonic::body::Body>,
//!     Response = http::Response<tonic::body::Body>,
//!     Error = std::convert::Infallible,
//!     Future: Send + 'static,
//! > + Clone + Send + 'static) -> Result<(), oxirpc_core::OxiRpcError> {
//!     let addr: SocketAddr = "0.0.0.0:50051".parse().unwrap();
//!     GrpcWebServer::new(my_service)
//!         .cors(CorsPolicy::new().allow_any_origin())
//!         .serve(addr)
//!         .await
//! }
//! ```

use std::net::SocketAddr;

use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tower::Layer as _;

use crate::cors::{CorsLayer, CorsPolicy};
use crate::native::NativeGrpcWebLayer;
use oxirpc_core::OxiRpcError;

// ─── GrpcWebServer ────────────────────────────────────────────────────────────

/// A native TCP server that serves gRPC-Web over HTTP/1.1 **and** HTTP/2.
///
/// Protocol version is auto-negotiated per-connection by
/// [`hyper_util::server::conn::auto::Builder`] — HTTP/1.1 for browsers using
/// the gRPC-Web protocol and HTTP/2 for native gRPC clients — all on the same
/// port.
///
/// The inner `S` is wrapped (outermost-to-innermost) as:
///
/// ```text
/// CorsLayer (optional)  →  NativeGrpcWebLayer  →  S
/// ```
///
/// # Generic parameters
///
/// `S` must implement `tower::Service<Request<tonic::body::Body>, …>` and be
/// `Clone + Send + 'static`.  Any `S::Error` that is convertible to
/// `Box<dyn Error + Send + Sync>` is accepted; in practice this means
/// `Infallible` or any boxed error.
pub struct GrpcWebServer<S> {
    service: S,
    cors: Option<CorsPolicy>,
    config: crate::negotiate::GrpcWebConfig,
}

impl<S> GrpcWebServer<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    /// Create a new [`GrpcWebServer`] wrapping `service`.
    ///
    /// No CORS policy is attached by default — call [`Self::cors`] to enable
    /// CORS header injection and preflight handling.
    pub fn new(service: S) -> Self {
        Self {
            service,
            cors: None,
            config: crate::negotiate::GrpcWebConfig::new(crate::negotiate::WebMode::Binary),
        }
    }

    /// Attach a [`CorsPolicy`] to the server.
    ///
    /// When set, `OPTIONS` preflight requests are answered with the appropriate
    /// CORS headers **before** the gRPC-Web translation layer sees the request.
    pub fn cors(mut self, cors: CorsPolicy) -> Self {
        self.cors = Some(cors);
        self
    }

    /// Override the [`crate::negotiate::GrpcWebConfig`] used for negotiation.
    pub fn config(mut self, config: crate::negotiate::GrpcWebConfig) -> Self {
        self.config = config;
        self
    }

    /// Bind `addr` and serve connections until the process is killed.
    ///
    /// Equivalent to `serve_with_shutdown(addr, std::future::pending())`.
    ///
    /// # Errors
    ///
    /// Returns [`OxiRpcError::Transport`] if the TCP listener cannot be bound or
    /// if the accept loop fails with a non-transient error.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), OxiRpcError> {
        self.serve_with_shutdown(addr, std::future::pending()).await
    }

    /// Bind `addr` and serve connections until `shutdown` resolves.
    ///
    /// In-flight connections continue until they finish naturally; new
    /// connections are not accepted after the shutdown future resolves.
    ///
    /// # Errors
    ///
    /// Returns [`OxiRpcError::Transport`] if the TCP listener cannot be bound.
    pub async fn serve_with_shutdown(
        self,
        addr: SocketAddr,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), OxiRpcError> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Drive the caller's shutdown future and notify the accept loop.
        tokio::spawn(async move {
            shutdown.await;
            // Ignore send errors — receiver may already be gone if serve returns.
            let _ = shutdown_tx.send(true);
        });

        match self.cors {
            Some(policy) => {
                let wrapped =
                    CorsLayer::new(policy).layer(NativeGrpcWebLayer::new().layer(self.service));
                run_accept_loop(listener, wrapped, &mut shutdown_rx).await
            }
            None => {
                let wrapped = NativeGrpcWebLayer::new().layer(self.service);
                run_accept_loop(listener, wrapped, &mut shutdown_rx).await
            }
        }
    }
}

// ─── Accept loop ─────────────────────────────────────────────────────────────

/// Core accept loop: accepts TCP connections and spawns a hyper auto-builder
/// task per connection.
///
/// The loop exits when `shutdown_rx` signals `true` or when there are no more
/// connections to accept.
async fn run_accept_loop<Svc>(
    listener: TcpListener,
    service: Svc,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), OxiRpcError>
where
    Svc: tower::Service<
            http::Request<hyper::body::Incoming>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());

    loop {
        // Race accept against the shutdown signal.
        let (stream, _peer_addr) = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                tracing::debug!(
                    target: "oxirpc::grpc_web_transport",
                    "shutdown signal received, stopping accept loop"
                );
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(
                            target: "oxirpc::grpc_web_transport",
                            "accept error: {e}"
                        );
                        continue;
                    }
                }
            }
        };

        let svc_clone = service.clone();
        let builder_clone = builder.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let hyper_svc = make_hyper_service(svc_clone);
            // serve_connection_with_upgrades enables HTTP/1.1 upgrade (e.g. WebSocket),
            // required for some gRPC-Web clients that negotiate via Upgrade headers.
            let _ = builder_clone
                .serve_connection_with_upgrades(io, hyper_svc)
                .await;
        });
    }

    Ok(())
}

// ─── Hyper service adapter ────────────────────────────────────────────────────

/// Wrap a `tower::Service<Request<hyper::body::Incoming>>` in a
/// `hyper::service::Service` backed by `service_fn`.
///
/// This mirrors the `make_hyper_service` helper in
/// `oxirpc-server/src/native_transport.rs`, adapted for the gRPC-Web case
/// where the wrapped tower service is already generic over the request body
/// and expects `Incoming` directly.
fn make_hyper_service<S>(
    svc: S,
) -> impl hyper::service::Service<
    http::Request<hyper::body::Incoming>,
    Response = http::Response<tonic::body::Body>,
    Error = Box<dyn std::error::Error + Send + Sync>,
    Future = impl std::future::Future<
        Output = Result<
            http::Response<tonic::body::Body>,
            Box<dyn std::error::Error + Send + Sync>,
        >,
    >,
>
where
    S: tower::Service<
            http::Request<hyper::body::Incoming>,
            Response = http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
        let mut svc_clone = svc.clone();
        async move {
            svc_clone
                .call(req)
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
        }
    })
}
