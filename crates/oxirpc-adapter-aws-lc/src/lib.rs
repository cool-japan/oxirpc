#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! `oxirpc-adapter-aws-lc` — aws-lc-rs backed rustls `CryptoProvider` for OxiRPC.
//!
//! # Feature flags
//! | Feature   | Effect |
//! |-----------|--------|
//! | `aws-lc`  | Enables the aws-lc-rs provider via `rustls/aws_lc_rs`. Brings in C/FFI code. |
//!
//! The **default** set of features is intentionally empty so that taking a
//! dependency on this crate (without opting in) does **not** pull any C code
//! into the build closure.  The purity tripwire in `tests/purity.rs` enforces
//! this automatically.
//!
//! # Usage
//! ```no_run
//! # #[cfg(feature = "aws-lc")]
//! # {
//! use oxirpc_adapter_aws_lc::aws_lc_provider;
//! use rustls::ServerConfig;
//!
//! let provider = aws_lc_provider();
//! let _builder = ServerConfig::builder_with_provider(provider)
//!     .with_safe_default_protocol_versions()
//!     .unwrap();
//! # }
//! ```
//!
//! # Note on TLS helpers
//!
//! This crate provides the `CryptoProvider` only.  To wire it into gRPC
//! server/client configs, pair it with `oxirpc`'s `tls` feature:
//!
//! ```toml
//! [dependencies]
//! oxirpc = { version = "0.1", features = ["tls", "aws-lc"] }
//! ```

mod provider;

#[cfg(feature = "aws-lc")]
pub use provider::aws_lc_provider;
