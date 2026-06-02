//! Native HTTP/2 server transport using `hyper::server::conn::http2::Builder`.
//!
//! This module provides a raw hyper-H2 accept loop that bypasses tonic's
//! transport layer entirely in the hot path. The accept loop:
//!
//! 1. Binds / reuses a [`TcpListener`].
//! 2. Optionally wraps each accepted stream with TLS via `tokio-rustls`.
//! 3. Hands the I/O to `hyper::server::conn::http2::Builder::serve_connection`.
//! 4. Converts `hyper::body::Incoming` → [`oxirpc_core::wire::NativeBody`] before
//!    calling the provided service.
//!
//! # Two entry points
//!
//! - [`serve_native`] — accepts a [`tonic::service::Routes`] (the original API,
//!   kept for backwards compatibility with [`crate::ServeReady`]).
//! - [`serve_native_with_service`] — accepts any `tower::Service` that handles
//!   `Request<NativeBody>` and returns `Response<RespB>` (generic over
//!   the response body type). Used by [`crate::ServeReady::serve_native_registry`].
//!
//! # Graceful shutdown
//!
//! Pass a `tokio::sync::watch::Receiver<()>` as `shutdown`.  When the sender
//! fires, the accept loop exits.  In-flight connections complete naturally
//! because each connection is handled in a spawned task.
//!
//! # Body conversion
//!
//! `tonic::service::Routes` implements
//! `tower::Service<Request<B>>` for any `B: http_body::Body<Data=Bytes>`.
//! `hyper::body::Incoming` satisfies those bounds, so we wrap it with
//! `NativeBody::pinned(incoming)` and pass `Request<NativeBody>` to the service.
//!
//! The response body satisfies
//! `http_body::Body<Data=Bytes, Error: Into<Box<dyn Error+Send+Sync>>>`,
//! which is all hyper requires of the response body.

use std::convert::Infallible;

use http::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use oxirpc_core::wire::NativeBody;
use tokio::net::TcpListener;
use tower::Service;

use oxirpc_core::OxiRpcError;

// ─── Public generic core ──────────────────────────────────────────────────────

/// Serve gRPC requests from `listener` using any compatible `tower::Service`.
///
/// This is the generic core used by both the `Routes`-based API and the
/// [`crate::RegistryService`]-based API.
///
/// # Parameters
///
/// - `listener` — a pre-bound [`TcpListener`].
/// - `service`  — a `tower::Service<Request<NativeBody>>` that is
///   `Clone + Send + 'static`.
/// - `tls_config` — when present, each TCP stream is wrapped with TLS before
///   the H2 handshake. Requires the `tls` feature.
/// - `shutdown` — optional watch-channel receiver; when a value is sent the
///   accept loop exits after the current iteration.
///
/// # Errors
///
/// Returns [`OxiRpcError::Transport`] if the listener fails in a non-transient
/// way.  Transient `accept` errors (e.g. `EMFILE`) are logged and skipped.
pub async fn serve_native_with_service<S, RespB>(
    listener: TcpListener,
    service: S,
    #[cfg(feature = "tls")] tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
    #[cfg(not(feature = "tls"))] _tls_config: Option<std::convert::Infallible>,
    shutdown: Option<tokio::sync::watch::Receiver<()>>,
) -> Result<(), OxiRpcError>
where
    S: Service<Request<NativeBody>, Response = http::Response<RespB>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    RespB: http_body::Body + Send + 'static,
    RespB::Data: Send,
    RespB::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());

    loop {
        // Check shutdown signal before accepting.
        if let Some(ref rx) = shutdown {
            if rx.has_changed().unwrap_or(false) {
                tracing::debug!(
                    target: "oxirpc::native_transport",
                    "shutdown signal received, stopping accept loop"
                );
                break;
            }
        }

        // Wait for an incoming connection, racing against the shutdown signal.
        let accept_result = if let Some(ref mut rx) = shutdown.clone() {
            tokio::select! {
                biased;
                _ = rx.changed() => {
                    tracing::debug!(
                        target: "oxirpc::native_transport",
                        "shutdown during accept, stopping"
                    );
                    break;
                }
                result = listener.accept() => result,
            }
        } else {
            listener.accept().await
        };

        let (stream, _peer_addr) = match accept_result {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(target: "oxirpc::native_transport", "accept error: {e}");
                continue;
            }
        };

        let svc_clone = service.clone();
        let builder_clone = builder.clone();

        #[cfg(feature = "tls")]
        if let Some(ref tls_cfg) = tls_config {
            let tls_cfg = std::sync::Arc::clone(tls_cfg);
            tokio::spawn(async move {
                let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
                let tls_stream = match acceptor.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            target: "oxirpc::native_transport",
                            "TLS handshake error: {e}"
                        );
                        return;
                    }
                };
                let io = TokioIo::new(tls_stream);
                let hyper_svc = make_hyper_service(svc_clone);
                let _ = builder_clone.serve_connection(io, hyper_svc).await;
            });
            continue;
        }

        // Plaintext path.
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let hyper_svc = make_hyper_service(svc_clone);
            let _ = builder_clone.serve_connection(io, hyper_svc).await;
        });
    }

    Ok(())
}

// ─── Routes-based entry point (original API) ─────────────────────────────────

/// Serve gRPC requests from `listener` using the hyper HTTP/2 builder.
///
/// This is the original `Routes`-based entry point kept for backwards
/// compatibility.  It wraps [`serve_native_with_service`].
///
/// # Parameters
///
/// - `listener` — a pre-bound [`TcpListener`] (may be port `0` for OS-assigned).
/// - `routes` — a [`tonic::service::Routes`] that handles each request.
/// - `tls_config` — when present, each TCP stream is wrapped with TLS before H2.
///   Requires the `tls` feature.
/// - `shutdown` — optional watch-channel receiver; when a value is sent the
///   accept loop exits after the current iteration.
///
/// # Errors
///
/// Returns [`OxiRpcError::Transport`] if the listener fails to accept a
/// connection after a transient error threshold is exceeded.
pub async fn serve_native(
    listener: TcpListener,
    mut routes: tonic::service::Routes,
    #[cfg(feature = "tls")] tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
    #[cfg(not(feature = "tls"))] _tls_config: Option<std::convert::Infallible>,
    shutdown: Option<tokio::sync::watch::Receiver<()>>,
) -> Result<(), OxiRpcError> {
    // Prepare the axum router for improved routing performance.
    routes = routes.prepare();

    serve_native_with_service(
        listener,
        routes,
        #[cfg(feature = "tls")]
        tls_config,
        #[cfg(not(feature = "tls"))]
        None::<std::convert::Infallible>,
        shutdown,
    )
    .await
}

// ─── Hyper service adapter ────────────────────────────────────────────────────

/// Wrap a `tower::Service<Request<NativeBody>>` in a `hyper` service
/// that converts `Request<hyper::body::Incoming>` → `Request<NativeBody>`
/// before dispatching.
///
/// Generic over `RespB` so it works with both `tonic::body::Body` responses
/// (tonic Routes path) and `NativeBody` responses (native registry path).
fn make_hyper_service<S, RespB>(
    svc: S,
) -> impl hyper::service::Service<
    Request<hyper::body::Incoming>,
    Response = http::Response<RespB>,
    Error = Infallible,
    Future = impl std::future::Future<Output = Result<http::Response<RespB>, Infallible>>,
>
where
    S: Service<Request<NativeBody>, Response = http::Response<RespB>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    RespB: http_body::Body + Send + 'static,
    RespB::Data: Send,
    RespB::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
        let mut svc_req = svc.clone();
        async move {
            let (parts, incoming) = req.into_parts();
            let native_body = NativeBody::pinned(incoming);
            let native_req = Request::from_parts(parts, native_body);
            svc_req.call(native_req).await
        }
    })
}
