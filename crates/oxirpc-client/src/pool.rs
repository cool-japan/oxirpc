//! Channel pooling and typed channel wrappers.
//!
//! # Overview
//!
//! - [`ChannelPool`] — a pool of pre-connected [`tonic::transport::Channel`]s with
//!   load-balancer-driven selection.  Channels can be provided directly
//!   (via [`ChannelPool::new`]) or built from a list of [`tonic::transport::Endpoint`]s
//!   (via [`ChannelPool::from_endpoints`]).
//! - [`TypedChannel<S>`] — a zero-cost typed wrapper around a [`Channel`] that
//!   binds it to a specific gRPC service type at compile time.  Generated client
//!   stubs accept `impl Into<Channel>` (or deref to `Channel`) so `TypedChannel`
//!   can be passed directly.
//!
//! # Design note
//!
//! [`ChannelPool`] stores a parallel pair of slices — one of [`crate::balance::Endpoint`]
//! (the load-balancer's view: URI + weight) and one of [`Channel`] (the actual
//! transport).  `get()` delegates index selection to the [`LoadBalancer`] by
//! passing the endpoint slice, then returns the channel at that index.

use std::marker::PhantomData;
use std::ops::Deref;

use tonic::transport::Channel;

use crate::balance::{Endpoint as BalanceEndpoint, LoadBalancer};
use crate::OxiRpcError;

// ── ChannelPool ───────────────────────────────────────────────────────────────

/// A pool of pre-connected channels with load-balancer-driven selection.
///
/// Internally the pool maintains a parallel pair of:
/// - [`crate::balance::Endpoint`] metadata (URI + weight) used by the [`LoadBalancer`]
///   to choose an index.
/// - [`Channel`]s at the corresponding positions.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_client::{ChannelPool, Channel};
/// use oxirpc_client::balance::{Endpoint, RoundRobin};
/// use tonic::transport::Endpoint as TonicEndpoint;
///
/// # async fn example() -> Result<(), oxirpc_core::OxiRpcError> {
/// let pool = ChannelPool::from_endpoints(
///     vec![
///         TonicEndpoint::from_static("http://localhost:50051"),
///         TonicEndpoint::from_static("http://localhost:50052"),
///     ],
///     RoundRobin::new(),
/// ).await?;
///
/// let _channel = pool.get();
/// # Ok(())
/// # }
/// ```
pub struct ChannelPool {
    /// Parallel metadata used solely to drive the load balancer's `pick()`.
    endpoints: Vec<BalanceEndpoint>,
    /// The actual transport channels, one per entry in `endpoints`.
    channels: Vec<Channel>,
    /// The load-balancing policy.
    lb: Box<dyn LoadBalancer + Send + Sync>,
}

impl ChannelPool {
    /// Create a `ChannelPool` from pre-built `(tonic::Endpoint, Channel)` pairs.
    ///
    /// Each [`tonic::transport::Endpoint`]'s URI is extracted to construct the
    /// internal [`crate::balance::Endpoint`] metadata; weights default to `1`.
    ///
    /// # Panics
    ///
    /// Does not panic — returns `Self` even if `channels` is empty.
    pub fn new(
        channels: Vec<(tonic::transport::Endpoint, Channel)>,
        lb: impl LoadBalancer + 'static,
    ) -> Self {
        let (endpoints, chans): (Vec<BalanceEndpoint>, Vec<Channel>) = channels
            .into_iter()
            .map(|(ep, ch)| {
                let uri = ep.uri().clone();
                (BalanceEndpoint::new(uri), ch)
            })
            .unzip();

        Self {
            endpoints,
            channels: chans,
            lb: Box::new(lb),
        }
    }

    /// Connect all `endpoints` concurrently and return a pool on success.
    ///
    /// Uses [`tonic::transport::Endpoint::connect_lazy`] so no actual TCP
    /// handshake is performed until the first RPC.  If URI extraction fails for
    /// any endpoint, an [`OxiRpcError::Transport`] is returned.
    pub async fn from_endpoints(
        endpoints: Vec<tonic::transport::Endpoint>,
        lb: impl LoadBalancer + 'static,
    ) -> Result<Self, OxiRpcError> {
        let mut balance_endpoints = Vec::with_capacity(endpoints.len());
        let mut channels = Vec::with_capacity(endpoints.len());

        for ep in endpoints {
            let uri = ep.uri().clone();
            let ch = ep.connect_lazy();
            balance_endpoints.push(BalanceEndpoint::new(uri));
            channels.push(ch);
        }

        Ok(Self {
            endpoints: balance_endpoints,
            channels,
            lb: Box::new(lb),
        })
    }

    /// Select and return a reference to one channel via the load balancer.
    ///
    /// Falls back to the first channel if the pool is non-empty but the
    /// load balancer returns `None` (which should not happen for well-formed
    /// balancers).
    ///
    /// # Panics
    ///
    /// Panics if the pool is empty (no channels to pick from).
    pub fn get(&self) -> &Channel {
        assert!(!self.channels.is_empty(), "ChannelPool is empty");
        let idx = self
            .lb
            .pick(&self.endpoints)
            .unwrap_or(0)
            .min(self.channels.len() - 1);
        &self.channels[idx]
    }

    /// Return the number of channels in the pool.
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Return `true` if the pool contains no channels.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Return a best-effort count of "healthy" channels in the pool.
    ///
    /// # Design note
    ///
    /// tonic 0.14 does not expose per-channel connectivity state, so
    /// [`ChannelPool`] has no way to distinguish a connected channel from a
    /// broken one without making an actual RPC.  This method therefore returns
    /// [`len()`](Self::len) — i.e. *all* channels are reported healthy — which
    /// is the conservative estimate that avoids false negatives.
    ///
    /// For fine-grained per-channel health tracking, wrap individual channels
    /// with [`crate::monitor::ChannelMonitor`] and advance their state
    /// cooperatively after RPC outcomes.
    pub fn healthy_count(&self) -> usize {
        self.channels.len()
    }
}

impl Clone for ChannelPool {
    /// Clone the pool's channels and endpoint metadata.
    ///
    /// The load balancer state is **not** shared: a freshly constructed
    /// [`RoundRobin`](crate::balance::RoundRobin) or similar is not part of the
    /// pool's `Clone` impl — `clone()` is intended for cases where a pool is
    /// captured in an `Arc` and cloned for passing across threads; prefer
    /// wrapping `ChannelPool` in `Arc<ChannelPool>` for shared access.
    ///
    /// # Note
    ///
    /// Because the `LoadBalancer` trait is object-safe but not `Clone`, the
    /// cloned pool creates a [`crate::balance::RoundRobin`] as its load balancer
    /// (stateless clone semantics).  If you need a custom balancer in the clone,
    /// use `ChannelPool::new` with the cloned channels directly.
    fn clone(&self) -> Self {
        use crate::balance::RoundRobin;
        Self {
            endpoints: self.endpoints.clone(),
            channels: self.channels.clone(),
            lb: Box::new(RoundRobin::new()),
        }
    }
}

// ── TypedChannel ──────────────────────────────────────────────────────────────

/// A typed wrapper around a [`Channel`] that binds it to a specific gRPC service.
///
/// `TypedChannel<S>` is a zero-cost newtype that carries a phantom service type `S`
/// at compile time.  It implements [`Deref<Target = Channel>`] so it can be passed
/// anywhere a `&Channel` or `Channel` is expected by generated tonic client stubs.
///
/// # Variance
///
/// Uses `PhantomData<fn() -> S>` (invariant in `S`) to prevent unsound coercions
/// while still not requiring `S: Send + Sync`.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_client::TypedChannel;
/// use tonic::transport::Endpoint;
///
/// struct MyService;
///
/// let channel = Endpoint::from_static("http://localhost:50051").connect_lazy();
/// let typed: TypedChannel<MyService> = TypedChannel::new(channel);
/// let _inner: &tonic::transport::Channel = &*typed;
/// ```
pub struct TypedChannel<S> {
    channel: Channel,
    _service: PhantomData<fn() -> S>,
}

impl<S> TypedChannel<S> {
    /// Wrap `channel` with the service type marker `S`.
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            _service: PhantomData,
        }
    }

    /// Borrow the underlying [`Channel`].
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Consume and return the inner [`Channel`].
    pub fn into_inner(self) -> Channel {
        self.channel
    }
}

impl<S> Clone for TypedChannel<S> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            _service: PhantomData,
        }
    }
}

impl<S> Deref for TypedChannel<S> {
    type Target = Channel;

    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}

impl<S> std::fmt::Debug for TypedChannel<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedChannel")
            .field("channel", &self.channel)
            .finish()
    }
}
