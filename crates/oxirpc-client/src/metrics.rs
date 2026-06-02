//! Basic RPC metrics counters for oxirpc-client.
//!
//! [`RpcMetrics`] provides lightweight, `Arc`-shared atomic counters that track
//! how many RPCs have been started, completed successfully, and failed. The
//! struct is `Clone` — all clones share the same underlying counters.
//!
//! # Example
//!
//! ```rust
//! use oxirpc_client::RpcMetrics;
//!
//! let metrics = RpcMetrics::new();
//! metrics.record_started();
//! metrics.record_completed();
//! assert_eq!(metrics.started(), 1);
//! assert_eq!(metrics.completed(), 1);
//! assert_eq!(metrics.failed(), 0);
//! ```

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Shared atomic counters tracking RPC lifecycle events.
///
/// All [`RpcMetrics`] clones share the same [`Arc`]-backed counters, so
/// incrementing one is immediately visible to all others.
#[derive(Debug, Default, Clone)]
pub struct RpcMetrics {
    inner: Arc<RpcMetricsInner>,
}

#[derive(Debug, Default)]
struct RpcMetricsInner {
    rpcs_started: AtomicU64,
    rpcs_completed: AtomicU64,
    rpcs_failed: AtomicU64,
}

impl RpcMetrics {
    /// Create a new zeroed metrics instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the started-RPCs counter by 1.
    ///
    /// Call this immediately before dispatching each RPC.
    #[inline]
    pub fn record_started(&self) {
        self.inner.rpcs_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the completed-RPCs counter by 1.
    ///
    /// Call this on every successful RPC response.
    #[inline]
    pub fn record_completed(&self) {
        self.inner.rpcs_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the failed-RPCs counter by 1.
    ///
    /// Call this on every RPC that returns an error or non-OK status.
    #[inline]
    pub fn record_failed(&self) {
        self.inner.rpcs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Return the total number of RPCs started.
    #[inline]
    pub fn started(&self) -> u64 {
        self.inner.rpcs_started.load(Ordering::Relaxed)
    }

    /// Return the total number of RPCs completed successfully.
    #[inline]
    pub fn completed(&self) -> u64 {
        self.inner.rpcs_completed.load(Ordering::Relaxed)
    }

    /// Return the total number of RPCs that ended in failure.
    #[inline]
    pub fn failed(&self) -> u64 {
        self.inner.rpcs_failed.load(Ordering::Relaxed)
    }
}
