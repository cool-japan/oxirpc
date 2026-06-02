//! Minimal xDS endpoint types, push-based resolver, and ADS streaming client.
//!
//! This module provides the data types and resolver implementation needed to
//! integrate with an xDS control plane (e.g. Envoy's EDS).  The ADS streaming
//! client that connects to a real xDS server lives in [`ads`].
//!
//! # Modules
//!
//! - [`resolver`] — [`XdsResolver`] (implements [`Resolver`]) and [`XdsWatcher`]
//!   (the write side).
//! - [`types`] — [`ClusterLoadAssignment`], [`LocalityLbEndpoints`],
//!   [`LbEndpoint`], [`SocketAddress`], and [`HealthStatus`].
//! - [`proto`] — hand-authored prost types for the Envoy xDS API v3.
//! - [`backoff`] — exponential backoff with deterministic jitter.
//! - [`ads`] — [`AdsClient`] and [`AdsConfig`] for ADS streaming.
//!
//! [`Resolver`]: crate::balance::Resolver

pub mod ads;
pub mod backoff;
pub mod proto;
pub mod resolver;
pub mod types;

pub use ads::{AdsClient, AdsConfig};
pub use resolver::{xds_resolver, XdsResolver, XdsWatcher};
pub use types::{
    ClusterLoadAssignment, HealthStatus, LbEndpoint, LocalityLbEndpoints, SocketAddress,
};
