//! TLS acceptor helpers for oxirpc-server.
//!
//! Provides a stream of TLS-wrapped connections suitable for use with
//! `tonic::transport::Router::serve_with_incoming`.
//!
//! All crypto is pure Rust via OxiTLS / rustls-rustcrypto. No `ring` or
//! `aws-lc-rs` on normal dependency edges.

use std::sync::Arc;

use futures_core::Stream;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{server::TlsStream, TlsAcceptor};

pub use rustls::ServerConfig;

/// Wrap a `rustls::ServerConfig` in a `TlsAcceptor`.
///
/// The `Arc` is required by `tokio-rustls`; this helper handles the wrapping.
pub fn tls_acceptor(config: Arc<ServerConfig>) -> TlsAcceptor {
    TlsAcceptor::from(config)
}

/// Perform a single TLS handshake on `stream`.
///
/// Returns a `TlsStream` ready for HTTP/2 I/O, wrapped in a Tokio-compatible
/// adapter. Propagates I/O errors from the handshake.
pub async fn accept_one(
    acceptor: &TlsAcceptor,
    stream: TcpStream,
) -> std::io::Result<TlsStream<TcpStream>> {
    acceptor.accept(stream).await
}

/// Produce a `Stream` of TLS-wrapped connections from a bound `TcpListener`.
///
/// Each accepted TCP stream undergoes a TLS handshake. If the handshake fails,
/// the error is logged via `tracing::warn` and the stream is skipped — the
/// server continues accepting new connections rather than dying on a bad client.
///
/// The yielded items are `TlsStream<TcpStream>` (from `tokio-rustls`). They
/// implement `AsyncRead + AsyncWrite + Unpin` and can be passed to
/// `tonic::transport::Router::serve_with_incoming` when wrapped in a
/// newtype that implements `Connected` (see `crate::serve::TlsConnectedStream`).
pub fn incoming_tls(
    listener: TcpListener,
    acceptor: TlsAcceptor,
) -> impl Stream<Item = Result<TlsStream<TcpStream>, std::io::Error>> {
    async_stream::try_stream! {
        loop {
            let (stream, _addr) = listener.accept().await?;
            let acceptor = acceptor.clone();
            match acceptor.accept(stream).await {
                Ok(tls_stream) => yield tls_stream,
                Err(e) => {
                    tracing::warn!("TLS handshake error: {e}");
                    // skip — don't kill the server
                }
            }
        }
    }
}
