//! Shared health state store for the native `grpc.health.v1.Health` service.
//!
//! [`HealthState`] holds the per-service [`ServingStatus`] map and per-service
//! [`tokio::sync::watch`] channels so that both the unary `Check` RPC and the
//! server-streaming `Watch` RPC can share a single source of truth.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{watch, Mutex, RwLock};
use tonic_health::ServingStatus;

use crate::proto::ServingStatusProto;

// ─────────────────────────────────────────────────────────────────────────────

/// The numeric sentinel used for a service that has never been registered.
const SERVICE_UNKNOWN: i32 = ServingStatusProto::ServiceUnknown as i32;

/// The numeric value for NOT_SERVING, used during shutdown.
const NOT_SERVING: i32 = ServingStatusProto::NotServing as i32;

// ─────────────────────────────────────────────────────────────────────────────

/// Shared health-state store.
///
/// Wraps two `Arc`-guarded maps:
/// - `statuses`: the authoritative per-service status (behind a `RwLock`).
/// - `watchers`: per-service `watch::Sender<i32>` channels (behind a `Mutex`);
///   created on demand when a client calls `Watch`.
///
/// All writes go through [`HealthState::set`], which atomically updates the
/// status map and notifies watchers (using `send_if_modified` so consecutive
/// identical updates produce only one notification).
pub struct HealthState {
    statuses: Arc<RwLock<HashMap<String, ServingStatus>>>,
    watchers: Arc<Mutex<HashMap<String, watch::Sender<i32>>>>,
}

impl std::fmt::Debug for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthState").finish_non_exhaustive()
    }
}

impl HealthState {
    /// Create a new, empty [`HealthState`] wrapped in an `Arc`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            statuses: Arc::new(RwLock::new(HashMap::new())),
            watchers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Set the [`ServingStatus`] for `name`.
    ///
    /// Consecutive calls with the same value for the same service produce
    /// only one watch notification (dedup at write-site via
    /// `watch::Sender::send_if_modified`).
    pub async fn set(&self, name: impl Into<String>, status: ServingStatus) {
        let name: String = name.into();
        let proto_val = serving_status_to_proto_i32(status);

        // Update the authoritative status map.
        {
            let mut map = self.statuses.write().await;
            map.insert(name.clone(), status);
        }

        // Notify any existing watcher without sending duplicate values.
        let mut watchers = self.watchers.lock().await;
        if let Some(tx) = watchers.get(&name) {
            // send_if_modified only marks changed and notifies if the
            // predicate returns true; this is our dedup mechanism.
            tx.send_if_modified(|cur| {
                if *cur == proto_val {
                    false
                } else {
                    *cur = proto_val;
                    true
                }
            });
        } else {
            // Pre-create the channel so a subsequent Watch call for this
            // name can read the current value immediately.
            let (tx, _rx) = watch::channel(proto_val);
            watchers.insert(name, tx);
        }
    }

    /// Return the current [`ServingStatus`] for `name`, or `None` if not registered.
    pub async fn get_status(&self, name: &str) -> Option<ServingStatus> {
        let map = self.statuses.read().await;
        map.get(name).copied()
    }

    /// Return a `watch::Receiver<i32>` for `name`.
    ///
    /// The receiver is seeded with the current proto status value (or
    /// `SERVICE_UNKNOWN` if the service has never been registered).  Cloning
    /// the receiver here is cheap and gives each caller an independent
    /// notification cursor.
    pub async fn watcher(&self, name: &str) -> watch::Receiver<i32> {
        let mut watchers = self.watchers.lock().await;

        if let Some(tx) = watchers.get(name) {
            return tx.subscribe();
        }

        // No sender yet — look up current status for the seed value.
        let initial = {
            let map = self.statuses.read().await;
            map.get(name)
                .map(|&s| serving_status_to_proto_i32(s))
                .unwrap_or(SERVICE_UNKNOWN)
        };

        let (tx, rx) = watch::channel(initial);
        watchers.insert(name.to_owned(), tx);
        rx
    }

    /// Flip every registered service to `NOT_SERVING` and notify all watchers.
    ///
    /// Idempotent: calling `shutdown` multiple times is safe (subsequent calls
    /// are no-ops for already-drained services).
    pub async fn shutdown(&self) {
        let names: Vec<String> = {
            let map = self.statuses.read().await;
            map.keys().cloned().collect()
        };
        for name in names {
            self.set(name, ServingStatus::NotServing).await;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Map a `tonic_health::ServingStatus` to its proto-wire `i32` value.
fn serving_status_to_proto_i32(status: ServingStatus) -> i32 {
    match status {
        ServingStatus::Unknown => ServingStatusProto::Unknown as i32,
        ServingStatus::Serving => ServingStatusProto::Serving as i32,
        ServingStatus::NotServing => NOT_SERVING,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_and_get_status() {
        let state = HealthState::new();
        assert!(state.get_status("svc").await.is_none());
        state.set("svc", ServingStatus::Serving).await;
        assert_eq!(state.get_status("svc").await, Some(ServingStatus::Serving));
    }

    #[tokio::test]
    async fn watcher_receives_current_value_immediately() {
        let state = HealthState::new();
        state.set("svc", ServingStatus::Serving).await;
        let rx = state.watcher("svc").await;
        assert_eq!(*rx.borrow(), ServingStatusProto::Serving as i32);
    }

    #[tokio::test]
    async fn watcher_unknown_service_gets_service_unknown() {
        let state = HealthState::new();
        let rx = state.watcher("nonexistent").await;
        assert_eq!(*rx.borrow(), SERVICE_UNKNOWN);
    }

    #[tokio::test]
    async fn shutdown_flips_to_not_serving() {
        let state = HealthState::new();
        state.set("a", ServingStatus::Serving).await;
        state.set("b", ServingStatus::Serving).await;
        state.shutdown().await;
        assert_eq!(state.get_status("a").await, Some(ServingStatus::NotServing));
        assert_eq!(state.get_status("b").await, Some(ServingStatus::NotServing));
    }
}
