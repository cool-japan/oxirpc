//! Pure-native HTTP/2 channel for oxirpc-client.
//!
//! [`NativeChannel`] is a zero-tonic, pure-`h2` gRPC channel that manages a
//! pool of connections per endpoint, driven by a [`crate::balance::DynResolver`].
//! It implements [`tower::Service`] so it can be used anywhere a tonic stub
//! accepts a `Service<http::Request<B>>`.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use oxirpc_client::native_channel::NativeChannelBuilder;
//! use oxirpc_client::balance::StaticResolver;
//!
//! # async fn example() -> Result<(), oxirpc_core::OxiRpcError> {
//! let resolver = StaticResolver::new(vec![
//!     oxirpc_client::balance::Endpoint::new("http://127.0.0.1:50051".parse().unwrap()),
//! ]);
//! let channel = NativeChannelBuilder::new()
//!     .resolver(resolver)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod body;
pub mod builder;
pub mod call;
pub mod channel;
pub mod connection;

pub use body::{body_channel, NativeBody, NativeBodySender};
pub use builder::NativeChannelBuilder;
pub use channel::NativeChannel;
#[cfg(feature = "tls")]
pub use connection::TlsConfig;
