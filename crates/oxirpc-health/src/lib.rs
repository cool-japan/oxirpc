#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! `oxirpc-health` — gRPC Health Checking (v1) service for OxiRPC.
//!
//! Wraps [`tonic_health`]'s `health_reporter` function and the [`HealthReporter`]
//! / `HealthServer` pair into an ergonomic API that integrates with the OxiRPC
//! ecosystem conventions.
//!
//! # Example
//!
//! ```rust,no_run
//! use oxirpc_health::{health_service, ServingStatus};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (health_svc, mut handle) = health_service();
//! handle.set_serving("my.Service").await;
//! handle.set_not_serving("my.LegacyService").await;
//!
//! // Mount `health_svc` on your tonic Server via `add_service`:
//! //   tonic::transport::Server::builder()
//! //       .add_service(health_svc)
//! //       .add_service(my_svc)
//! //       .serve(addr).await?;
//! # }
//! ```
//!
//! ## Kubernetes integration
//!
//! Configure liveness and readiness probes in your Kubernetes deployment:
//!
//! ```yaml
//! livenessProbe:
//!   grpc:
//!     port: 50051
//!   initialDelaySeconds: 5
//! readinessProbe:
//!   grpc:
//!     port: 50051
//!     service: my.package.MyService
//!   initialDelaySeconds: 5
//! ```
//!
//! Use [`ProbeType::Liveness`] and [`ProbeType::Readiness`] with
//! [`HealthHandle::register_k8s_probe`] to register async probe functions.
//!
//! ```rust,no_run
//! use oxirpc_health::{health_service, ProbeFn, ProbeType};
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (_health_svc, mut handle) = health_service();
//!
//! // Readiness probe: checks that your database connection is healthy.
//! let readiness: ProbeFn = Arc::new(|| {
//!     Box::pin(async {
//!         // Replace with a real check, e.g. a DB ping.
//!         true
//!     })
//! });
//! handle.register_k8s_probe(ProbeType::Readiness, "my.Service", readiness);
//!
//! // Liveness probe: indicates the process is alive.
//! let liveness: ProbeFn = Arc::new(|| Box::pin(async { true }));
//! handle.register_k8s_probe(ProbeType::Liveness, "my.Service", liveness);
//!
//! // Optionally start a background loop that fires probes every second.
//! let _jh = handle.start_probe_loop(tokio::time::Duration::from_secs(1));
//! # }
//! ```

pub use tonic_health::server::HealthReporter;
pub use tonic_health::ServingStatus;

pub mod proto;
pub mod service;
pub mod state;

pub use service::NativeHealthService;
pub use state::HealthState;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::server::health_reporter;

/// Async probe function type: an `Arc`-wrapped factory that produces a boxed future returning `bool`.
///
/// `true` → service is healthy (Serving); `false` → service is unhealthy (NotServing).
pub type ProbeFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// Classifies a health probe in the Kubernetes probe model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeType {
    /// Signals when the process has started successfully (gate for liveness/readiness).
    Startup,
    /// Signals that the process is alive and should not be killed.
    Liveness,
    /// Signals that the process can accept traffic.
    Readiness,
}

struct ProbeEntry {
    service: String,
    probe_fn: ProbeFn,
    probe_type: Option<ProbeType>,
}

/// Type alias for the status-change callback stored in [`HealthHandle`].
///
/// The callback receives the service name and its new [`ServingStatus`] after
/// every [`HealthHandle::set_status`] call.
///
/// **Bypass warning:** callers that hold the raw re-exported [`HealthReporter`]
/// can update the tonic-health state directly, bypassing this callback. Always
/// use [`HealthHandle::set_status`] (and its convenience wrappers) to ensure
/// the callback fires.
type OnChangeCb = Arc<dyn Fn(&str, ServingStatus) + Send + Sync>;

/// A handle to the health registry.
///
/// Use this to set or clear service statuses. The corresponding [`HealthServer`]
/// will reflect these changes to any connected health-check clients.
///
/// The handle keeps a local mirror of the per-service status so that callers can
/// query the current status ([`HealthHandle::get_status`]), list registered
/// services ([`HealthHandle::list_services`]), and perform bulk updates
/// ([`HealthHandle::set_all_serving`] / [`HealthHandle::set_all_not_serving`])
/// without round-tripping through the wire protocol.
///
/// An optional `on_change` callback is fired after every [`HealthHandle::set_status`]
/// call (and the convenience wrappers that delegate to it). Register it via
/// [`HealthHandle::on_change`] or via [`HealthBuilder::on_change`].
pub struct HealthHandle {
    reporter: HealthReporter,
    /// Local mirror of the authoritative status held by the reporter.
    statuses: BTreeMap<String, ServingStatus>,
    /// Optional callback fired after every `set_status` invocation.
    on_change_cb: Option<OnChangeCb>,
    /// Registered async health probes.
    probes: Vec<ProbeEntry>,
    /// Shared native health state (present only for handles created via
    /// `HealthBuilder::build_native`). Used by `shutdown()` and `Drop`.
    native_state: Option<Arc<HealthState>>,
}

impl std::fmt::Debug for HealthHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthHandle")
            .field("statuses", &self.statuses)
            .field(
                "on_change_cb",
                &self.on_change_cb.as_ref().map(|_| "<callback>"),
            )
            .field("probes_count", &self.probes.len())
            .field("native", &self.native_state.is_some())
            .finish_non_exhaustive()
    }
}

impl HealthHandle {
    /// Mark `service` as [`ServingStatus::Serving`].
    pub async fn set_serving(&mut self, service: &str) {
        self.set_status(service, ServingStatus::Serving).await;
    }

    /// Mark `service` as [`ServingStatus::NotServing`].
    pub async fn set_not_serving(&mut self, service: &str) {
        self.set_status(service, ServingStatus::NotServing).await;
    }

    /// Remove `service` from the registry entirely.
    ///
    /// After clearing, `Check` for that service name will return `NOT_FOUND`.
    pub async fn clear(&mut self, service: &str) {
        self.reporter.clear_service_status(service).await;
        self.statuses.remove(service);
    }

    /// Set an arbitrary [`ServingStatus`] for `service`.
    ///
    /// This is the single choke point for status updates. The optional
    /// `on_change` callback (set via [`HealthHandle::on_change`] or
    /// [`HealthBuilder::on_change`]) is fired *after* the reporter and local
    /// mirror have been updated.
    ///
    /// **Bypass warning:** callers holding the raw re-exported [`HealthReporter`]
    /// can update tonic-health state without going through this method, which
    /// means the callback will **not** fire. Always use this method (or its
    /// convenience wrappers) to ensure observability.
    pub async fn set_status(&mut self, service: &str, status: ServingStatus) {
        self.reporter.set_service_status(service, status).await;
        self.statuses.insert(service.to_owned(), status);
        if let Some(cb) = &self.on_change_cb {
            cb(service, status);
        }
        // Propagate to the native health state when using build_native().
        if let Some(s) = &self.native_state {
            s.set(service, status).await;
        }
    }

    /// Register a callback that is invoked after every [`HealthHandle::set_status`] call.
    ///
    /// The callback receives the service name and new [`ServingStatus`]. Only one
    /// callback can be registered at a time; calling this method again replaces
    /// the previous callback.
    ///
    /// See also [`HealthBuilder::on_change`] for setting the callback at build time.
    pub fn on_change<F>(&mut self, cb: F)
    where
        F: Fn(&str, ServingStatus) + Send + Sync + 'static,
    {
        self.on_change_cb = Some(Arc::new(cb));
    }

    /// Get the last-known [`ServingStatus`] for `service`, if registered.
    ///
    /// Reflects the local mirror maintained by this handle. Returns [`None`] if
    /// the service was never registered or has been cleared.
    pub fn get_status(&self, service: &str) -> Option<ServingStatus> {
        self.statuses.get(service).copied()
    }

    /// List the names of all currently-registered services, sorted.
    pub fn list_services(&self) -> Vec<String> {
        self.statuses.keys().cloned().collect()
    }

    /// Return a reference to the full status map (service name → [`ServingStatus`]).
    ///
    /// Reflects the local mirror maintained by this handle. Useful for inspecting
    /// all registered services at once.
    pub fn statuses(&self) -> &BTreeMap<String, ServingStatus> {
        &self.statuses
    }

    /// Set every currently-registered service to the given [`ServingStatus`].
    ///
    /// Iterates over all services that have been registered with this handle and
    /// calls [`HealthHandle::set_status`] for each, which means the `on_change`
    /// callback (if set) fires for every service.
    ///
    /// This is the primary graceful-drain helper: call
    /// `handle.set_all(ServingStatus::NotServing).await` during shutdown to
    /// immediately fail readiness probes while in-flight RPCs drain.
    pub async fn set_all(&mut self, status: ServingStatus) {
        let services: Vec<String> = self.statuses.keys().cloned().collect();
        for svc in services {
            self.set_status(&svc, status).await;
        }
    }

    /// Set every currently-registered service to [`ServingStatus::Serving`].
    pub async fn set_all_serving(&mut self) {
        let names: Vec<String> = self.statuses.keys().cloned().collect();
        for name in names {
            self.set_status(&name, ServingStatus::Serving).await;
        }
    }

    /// Set every currently-registered service to [`ServingStatus::NotServing`].
    ///
    /// Useful during graceful shutdown to fail readiness probes while in-flight
    /// RPCs drain.
    pub async fn set_all_not_serving(&mut self) {
        let names: Vec<String> = self.statuses.keys().cloned().collect();
        for name in names {
            self.set_status(&name, ServingStatus::NotServing).await;
        }
    }

    /// Register an async health probe for `service`.
    ///
    /// `probe_fn` is called periodically by [`start_probe_loop`](Self::start_probe_loop).
    /// Returns `true` → Serving; `false` → NotServing.
    pub fn register_probe(&mut self, service: &str, probe_fn: ProbeFn) {
        self.probes.push(ProbeEntry {
            service: service.to_owned(),
            probe_fn,
            probe_type: None,
        });
    }

    /// Register an async health probe associated with a Kubernetes probe type.
    pub fn register_k8s_probe(&mut self, probe_type: ProbeType, service: &str, probe_fn: ProbeFn) {
        self.probes.push(ProbeEntry {
            service: service.to_owned(),
            probe_fn,
            probe_type: Some(probe_type),
        });
    }

    /// Spawn a background task that invokes all registered probes every `interval`.
    ///
    /// Each probe's result updates the service status directly via the `HealthReporter`.
    /// Note: probe-driven updates do NOT update the local `statuses` mirror or fire the
    /// `on_change_cb` — those are only updated through [`set_status`](Self::set_status).
    ///
    /// Returns the `JoinHandle` for the background task (cancel it to stop probing).
    pub fn start_probe_loop(&self, interval: tokio::time::Duration) -> tokio::task::JoinHandle<()> {
        let reporter = self.reporter.clone();
        // Collect probe info as (service_name, probe_fn) pairs
        let probes: Vec<(String, ProbeFn)> = self
            .probes
            .iter()
            .map(|e| (e.service.clone(), e.probe_fn.clone()))
            .collect();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                for (service, probe_fn) in &probes {
                    let healthy = probe_fn().await;
                    let status = if healthy {
                        ServingStatus::Serving
                    } else {
                        ServingStatus::NotServing
                    };
                    reporter.set_service_status(service, status).await;
                }
            }
        })
    }

    /// Returns the aggregate health status across all registered services.
    ///
    /// - All services Serving → Serving.
    /// - Any service NotServing → NotServing.
    /// - No services registered → NotServing (conservative default).
    pub fn aggregate_status(&self) -> ServingStatus {
        if self.statuses.is_empty() {
            return ServingStatus::NotServing;
        }
        for status in self.statuses.values() {
            if *status != ServingStatus::Serving {
                return ServingStatus::NotServing;
            }
        }
        ServingStatus::Serving
    }

    /// Set health status for a service identified by its [`tonic::server::NamedService::NAME`].
    ///
    /// Delegates to [`HealthHandle::set_status`]; the `on_change` callback fires if set.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use oxirpc_health::{health_service, ServingStatus};
    /// # struct MySvc;
    /// # impl tonic::server::NamedService for MySvc { const NAME: &'static str = "my.MySvc"; }
    /// # #[tokio::main] async fn main() {
    /// let (_svc, mut handle) = health_service();
    /// handle.register_named::<MySvc>(ServingStatus::Serving).await;
    /// # }
    /// ```
    pub async fn register_named<S: tonic::server::NamedService>(&mut self, status: ServingStatus) {
        self.set_status(S::NAME, status).await;
    }

    /// Get the health status for a service identified by its [`tonic::server::NamedService::NAME`].
    ///
    /// Returns [`None`] if the service was never registered or has been cleared.
    pub fn status_for<S: tonic::server::NamedService>(&self) -> Option<ServingStatus> {
        self.get_status(S::NAME)
    }

    /// Shut down the native health service by flipping all services to NOT_SERVING.
    ///
    /// Only has an effect for handles created via [`HealthBuilder::build_native`].
    /// Calling `shutdown()` multiple times is safe (idempotent).
    pub fn shutdown(&self) {
        if let Some(state) = &self.native_state {
            let state = Arc::clone(state);
            tokio::spawn(async move {
                state.shutdown().await;
            });
        }
    }

    /// Returns the aggregate status for probes of a specific Kubernetes probe type.
    ///
    /// Only considers services registered via [`register_k8s_probe`](Self::register_k8s_probe)
    /// with the matching type. Returns Serving if all matching services are Serving,
    /// NotServing otherwise. Returns Serving if no probes of that type are registered
    /// (opt-in model).
    pub fn check_probe(&self, probe_type: ProbeType) -> ServingStatus {
        let relevant: Vec<&str> = self
            .probes
            .iter()
            .filter(|e| e.probe_type == Some(probe_type))
            .map(|e| e.service.as_str())
            .collect();
        if relevant.is_empty() {
            return ServingStatus::Serving; // opt-in: no probes registered = pass
        }
        for service in relevant {
            match self.statuses.get(service) {
                Some(ServingStatus::Serving) | None => {}
                _ => return ServingStatus::NotServing,
            }
        }
        ServingStatus::Serving
    }
}

impl Drop for HealthHandle {
    /// Trigger a graceful shutdown when the handle is dropped.
    ///
    /// Only has an effect for handles created via [`HealthBuilder::build_native`].
    /// A tokio task is spawned to perform the async shutdown; if no Tokio
    /// runtime is running at the time of drop the notification is silently skipped.
    fn drop(&mut self) {
        if let Some(state) = self.native_state.take() {
            let _ = tokio::runtime::Handle::try_current().map(|h| {
                h.spawn(async move {
                    state.shutdown().await;
                });
            });
        }
    }
}

/// Create a health service + handle pair.
///
/// The returned [`HealthServer`] is mountable on a tonic `Server` via
/// `add_service`. The [`HealthHandle`] is used to update per-service statuses.
///
/// For richer construction (initial statuses, `on_change` callback) consider
/// using [`HealthBuilder`] instead.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_health::health_service;
///
/// # #[tokio::main]
/// # async fn main() {
/// let (health_svc, mut handle) = health_service();
/// handle.set_serving("my.Service").await;
/// // then: server.add_service(health_svc)
/// # }
/// ```
#[deprecated(note = "use HealthBuilder::build_native() for the native service")]
pub fn health_service() -> (HealthServer<impl Health>, HealthHandle) {
    let (reporter, server) = health_reporter();
    let handle = HealthHandle {
        reporter,
        statuses: BTreeMap::new(),
        on_change_cb: None,
        probes: Vec::new(),
        native_state: None,
    };
    (server, handle)
}

/// Builder for a health service + handle pair.
///
/// Provides an ergonomic way to register initial service statuses and set an
/// `on_change` callback before constructing the [`HealthHandle`].
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_health::{HealthBuilder, ServingStatus};
///
/// # #[tokio::main]
/// # async fn main() {
/// let (health_svc, mut handle) = HealthBuilder::new()
///     .register("my.Service", ServingStatus::Serving)
///     .on_change(|svc, status| {
///         eprintln!("Health status changed: {} → {:?}", svc, status);
///     })
///     .build()
///     .await;
/// // then: server.add_service(health_svc)
/// # }
/// ```
pub struct HealthBuilder {
    initial: BTreeMap<String, ServingStatus>,
    on_change_cb: Option<OnChangeCb>,
}

impl std::fmt::Debug for HealthBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthBuilder")
            .field("initial", &self.initial)
            .field(
                "on_change_cb",
                &self.on_change_cb.as_ref().map(|_| "<callback>"),
            )
            .finish()
    }
}

impl HealthBuilder {
    /// Create a new [`HealthBuilder`] with no initial statuses and no callback.
    pub fn new() -> Self {
        Self {
            initial: BTreeMap::new(),
            on_change_cb: None,
        }
    }

    /// Register an initial [`ServingStatus`] for `service`.
    ///
    /// The reporter will be seeded with this status when [`HealthBuilder::build`]
    /// is called.
    pub fn register(mut self, service: impl Into<String>, status: ServingStatus) -> Self {
        self.initial.insert(service.into(), status);
        self
    }

    /// Register an initial [`ServingStatus`] for a service identified by its
    /// [`tonic::server::NamedService::NAME`].
    ///
    /// Equivalent to `.register(S::NAME, status)`.
    pub fn register_named<S: tonic::server::NamedService>(self, status: ServingStatus) -> Self {
        self.register(S::NAME, status)
    }

    /// Set a callback that is invoked whenever [`HealthHandle::set_status`] is
    /// called on the resulting [`HealthHandle`].
    ///
    /// The callback receives the service name and the new [`ServingStatus`].
    ///
    /// **Bypass warning:** callers using the raw re-exported [`HealthReporter`]
    /// bypass this callback. Use [`HealthHandle::set_status`] to ensure it fires.
    pub fn on_change<F>(mut self, cb: F) -> Self
    where
        F: Fn(&str, ServingStatus) + Send + Sync + 'static,
    {
        self.on_change_cb = Some(Arc::new(cb));
        self
    }

    /// Build the health service and handle.
    ///
    /// Seeds the reporter with all registered initial statuses (which also fires
    /// the `on_change` callback for each, if set). Returns the [`HealthServer`]
    /// for mounting on a tonic `Server` and the [`HealthHandle`] for ongoing
    /// status management.
    pub async fn build(self) -> (HealthServer<impl Health>, HealthHandle) {
        let (reporter, server) = health_reporter();
        let mut handle = HealthHandle {
            reporter,
            statuses: BTreeMap::new(),
            on_change_cb: self.on_change_cb,
            probes: Vec::new(),
            native_state: None,
        };
        for (svc, status) in self.initial {
            handle.set_status(&svc, status).await;
        }
        (server, handle)
    }

    /// Build a **native** health service and handle backed by [`HealthState`].
    ///
    /// Unlike [`HealthBuilder::build`], this returns a [`NativeHealthService`]
    /// instead of a `HealthServer<impl Health>`.  The returned service can be
    /// used with any router that accepts a `tower::Service<Request<Body>>`.
    ///
    /// The resulting [`HealthHandle`] supports a `shutdown()` method that flips
    /// all registered services to NOT_SERVING and notifies active `Watch` streams.
    /// `shutdown()` is also called automatically when the handle is dropped.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use oxirpc_health::{HealthBuilder, ServingStatus};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (native_svc, mut handle) = HealthBuilder::new()
    ///     .register("my.Service", ServingStatus::Serving)
    ///     .build_native()
    ///     .await;
    /// // then: add native_svc to your router
    /// # }
    /// ```
    pub async fn build_native(self) -> (NativeHealthService, HealthHandle) {
        let state = HealthState::new();

        // Seed the native state with the registered initial statuses.
        for (svc, status) in &self.initial {
            state.set(svc.clone(), *status).await;
        }

        let native_svc = NativeHealthService::new(Arc::clone(&state));

        // Build a tonic-health pair for the HealthHandle (the reporter and
        // local mirror); we don't expose the HealthServer to the caller, but
        // the HealthHandle methods continue to work via the reporter.
        let (reporter, _unused_server) = health_reporter();
        let mut handle = HealthHandle {
            reporter,
            statuses: BTreeMap::new(),
            on_change_cb: self.on_change_cb,
            probes: Vec::new(),
            native_state: Some(Arc::clone(&state)),
        };

        // Mirror initial statuses into the handle so get_status / list_services work.
        for (svc, status) in self.initial {
            handle.set_status(&svc, status).await;
        }

        (native_svc, handle)
    }
}

impl Default for HealthBuilder {
    fn default() -> Self {
        Self::new()
    }
}
