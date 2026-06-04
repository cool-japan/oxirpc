//! Pure-Rust TLS connector for oxirpc-client.
//!
//! Implements a [`tower::Service<http::Uri>`] backed by `tokio-rustls` +
//! `rustls-rustcrypto` (no ring, no aws-lc-rs). The connector dials TCP,
//! then performs a TLS handshake and returns a
//! `hyper_util::rt::TokioIo`-wrapped stream that satisfies tonic 0.14's
//! `hyper::rt::Read + hyper::rt::Write` bounds for
//! `Endpoint::connect_with_connector`.
//!
//! # Audit: OxiTLS compliance
//!
//! This module uses OxiTLS (oxirpc-core::tls / rustls 0.23 no-default-features).
//! It MUST NOT depend on ring or aws-lc-rs in default features.
//!
//! Verified: the rustls `ClientConfig` is built from `oxirpc_core::tls::client_config()`
//! which uses the RustCrypto provider (`rustls-rustcrypto`) — a Pure-Rust path
//! with no C FFI on the normal dependency edge.
//!
//! # Example
//!
//! ```rust,ignore
//! use rustls::RootCertStore;
//! use rustls_pki_types::ServerName;
//! use oxirpc_client::ClientBuilder;
//! use oxirpc_client::tls_connector::PureRustTlsConnector;
//! use oxirpc_core::tls::client_config;
//!
//! async fn example() -> Result<(), oxirpc_core::OxiRpcError> {
//!     let roots = RootCertStore::empty();
//!     let config = client_config(roots)?;
//!     let server_name = ServerName::try_from("example.com")
//!         .expect("valid DNS name");
//!     let connector = PureRustTlsConnector::new(config, server_name);
//!     let _channel = ClientBuilder::new("https://example.com:443")
//!         .connect_with_connector(connector)
//!         .await?;
//!     Ok(())
//! }
//! ```

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use http::Uri;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tower::Service;

/// The IO type returned by [`PureRustTlsConnector`].
///
/// Wraps `tokio_rustls::client::TlsStream<TcpStream>` in
/// `hyper_util::rt::TokioIo` so it satisfies tonic 0.14's
/// `hyper::rt::Read + hyper::rt::Write` bounds.
pub type TlsIo = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;

/// A Pure-Rust TLS connector backed by `tokio-rustls`.
///
/// Implements `tower::Service<Uri>` and produces a [`TlsIo`] ready for use
/// with `ClientBuilder::connect_with_connector`.
///
/// The connector dials a TCP connection to the host/port from the URI,
/// performs a TLS handshake using SNI from the stored `server_name`, and
/// wraps the result in [`TokioIo`] for hyper compatibility.
///
/// ## Clone semantics
///
/// `PureRustTlsConnector` is `Clone`. Cloning is cheap because the
/// [`TlsConnector`] wraps an `Arc<ClientConfig>`.
#[derive(Clone)]
pub struct PureRustTlsConnector {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl PureRustTlsConnector {
    /// Create a new connector from a [`rustls::ClientConfig`] and SNI name.
    ///
    /// # Arguments
    ///
    /// * `config` — a `ClientConfig` built via [`oxirpc_core::tls::client_config`]
    ///   (or manually). The config must have `h2` ALPN if connecting to a gRPC server.
    /// * `server_name` — the SNI hostname presented during the TLS handshake.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rustls::RootCertStore;
    /// use rustls_pki_types::ServerName;
    /// use oxirpc_client::tls_connector::PureRustTlsConnector;
    /// use oxirpc_core::tls::client_config;
    ///
    /// let config = client_config(RootCertStore::empty()).unwrap();
    /// let name = ServerName::try_from("example.com").unwrap();
    /// let connector = PureRustTlsConnector::new(config, name);
    /// ```
    pub fn new(config: ClientConfig, server_name: ServerName<'static>) -> Self {
        Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        }
    }
}

impl std::fmt::Debug for PureRustTlsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PureRustTlsConnector")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl Service<Uri> for PureRustTlsConnector {
    /// The wrapped TLS stream type; satisfies `hyper::rt::Read + hyper::rt::Write`.
    type Response = TlsIo;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connector = self.connector.clone();
        let server_name = self.server_name.clone();

        Box::pin(async move {
            let host = uri
                .host()
                .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                    "URI missing host".into()
                })?;

            let port = uri.port_u16().unwrap_or(443);
            let addr = format!("{host}:{port}");

            let tcp = TcpStream::connect(&addr)
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

            let tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

            Ok(TokioIo::new(tls_stream))
        })
    }
}
