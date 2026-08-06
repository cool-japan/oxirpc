//! Pure-native HTTP/3 (gRPC-over-QUIC) client channel for oxirpc-client.
//!
//! This module mirrors the HTTP/2 [`super`] channel but rides on the OxiQUIC
//! transport and the hyperium `h3` crate instead of `h2`:
//!
//! - [`H3Connection`] owns one HTTP/3 connection ( a `h3::client::SendRequest`
//!   plus the background connection-driver task) over an OxiQUIC
//!   `DrivenConnection`.
//! - [`execute_h3`] runs one gRPC call over that connection: it opens a request
//!   stream, `split()`s it for full-duplex operation, pumps the request body,
//!   and decodes the response into a [`NativeBody`](super::NativeBody) with gRPC status-trailer
//!   handling identical to the h2 path.
//! - [`H3Channel`] / [`H3ChannelBuilder`] provide an ergonomic front-end that
//!   dials (and lazily re-dials) a single endpoint and exposes `call` /
//!   `call_with_deadline`, matching [`super::channel::NativeChannel`]'s surface.
//!
//! # QUIC / ALPN / provider constraints
//!
//! HTTP/3 runs only over QUIC, which is TLS-1.3-only (RFC 9001) and negotiates
//! the `"h3"` ALPN token (RFC 9114 §3.3). The [`rustls::ClientConfig`] handed to
//! [`H3Connection::connect`] **must** be built from
//! [`oxirpc_core::tls::client_config_h3`] — its provider is the OxiQUIC crypto
//! provider whose cipher suites carry the `quic: Some(..)` key schedule. A config
//! built from the generic pure provider (e.g. [`oxirpc_core::tls::client_config`])
//! has `quic: None` and will fail the QUIC handshake.
//!
//! This whole module is gated behind the `http3` cargo feature.

pub mod call;
pub mod channel;
pub mod connection;

pub use call::execute_h3;
pub use channel::{H3Channel, H3ChannelBuilder};
pub use connection::{h3_conn_error_to_oxirpc, h3_stream_error_to_oxirpc, H3Connection};
