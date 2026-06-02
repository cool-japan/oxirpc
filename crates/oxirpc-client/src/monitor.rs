//! Connection state monitoring for gRPC channels.
//!
//! # Overview
//!
//! [`ChannelMonitor`] wraps a [`tonic::transport::Channel`] and exposes an
//! observable [`ConnectionState`] — a cooperative state machine that callers
//! advance by calling [`ChannelMonitor::set_state`].
//!
//! # Design: cooperative state model
//!
//! tonic 0.14's `tonic::transport::Channel` does **not** expose a built-in
//! connectivity-state API (unlike gRPC-Java or gRPC-Go).  `ChannelMonitor`
//! therefore implements a *cooperative* model:
//!
//! - State starts as [`ConnectionState::Idle`].
//! - Call [`set_state`](ChannelMonitor::set_state) from application code to
//!   advance the state (e.g., to [`ConnectionState::Connecting`] before
//!   dispatching a dial, [`ConnectionState::Ready`] after a successful RPC).
//! - Registered callbacks are fired on every state *change* (same-state
//!   transitions are no-ops).
//! - On explicit drop, the state is automatically advanced to
//!   [`ConnectionState::Shutdown`] and all callbacks are invoked.
//!
//! This model is intentional: it keeps the implementation 100 % Pure Rust, adds
//! no background tasks, and composes cleanly with existing tower/tonic service
//! layers.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

// ── ConnectionState ────────────────────────────────────────────────────────────

/// Observable connection state for a gRPC channel.
///
/// Models the same five states used by the gRPC connectivity-state specification
/// (see <https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md>).
///
/// Note: because tonic 0.14 does not surface connectivity state automatically,
/// this enum must be advanced *cooperatively* via
/// [`ChannelMonitor::set_state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectionState {
    /// No active call; no attempt to connect.
    Idle = 0,
    /// A connection attempt is in progress.
    Connecting = 1,
    /// A connection is established and the channel is ready for RPCs.
    Ready = 2,
    /// A transient failure occurred; the channel may retry.
    TransientFailure = 3,
    /// The channel has been shut down.  Terminal state.
    Shutdown = 4,
}

impl ConnectionState {
    /// Infallible conversion from a `u8` stored in an [`AtomicU8`].
    ///
    /// Unknown discriminants map to [`ConnectionState::Idle`].
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Idle,
            1 => Self::Connecting,
            2 => Self::Ready,
            3 => Self::TransientFailure,
            4 => Self::Shutdown,
            _ => Self::Idle,
        }
    }
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Idle => "Idle",
            Self::Connecting => "Connecting",
            Self::Ready => "Ready",
            Self::TransientFailure => "TransientFailure",
            Self::Shutdown => "Shutdown",
        };
        f.write_str(s)
    }
}

// ── ChannelMonitor ─────────────────────────────────────────────────────────────

type Callback = Box<dyn Fn(ConnectionState) + Send + Sync + 'static>;

/// A connection monitor that tracks channel connectivity state.
///
/// Wraps a [`tonic::transport::Channel`] and provides state observation via a
/// *cooperative* state machine.  Callers advance the state by calling
/// [`set_state`](ChannelMonitor::set_state); registered callbacks are invoked
/// on every state change.
///
/// # Cooperative model
///
/// tonic 0.14 does not expose an intrinsic connectivity-state API.  State
/// changes must therefore be driven by the surrounding application layer:
///
/// ```rust,no_run
/// use oxirpc_client::monitor::{ChannelMonitor, ConnectionState};
/// use tonic::transport::Endpoint;
///
/// let ch = Endpoint::from_static("http://localhost:50051").connect_lazy();
/// let monitor = ChannelMonitor::new(ch);
///
/// // Before dialling:
/// monitor.set_state(ConnectionState::Connecting);
/// // After first successful RPC:
/// monitor.set_state(ConnectionState::Ready);
/// ```
///
/// # Thread safety
///
/// All shared state is protected by `Arc<AtomicU8>` (state) and
/// `Arc<Mutex<Vec<Callback>>>` (callbacks).  `ChannelMonitor` is `Clone`;
/// clones share the same underlying state and callback list.
#[derive(Clone)]
pub struct ChannelMonitor {
    channel: tonic::transport::Channel,
    state: Arc<AtomicU8>,
    callbacks: Arc<Mutex<Vec<Callback>>>,
}

impl std::fmt::Debug for ChannelMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelMonitor")
            .field("state", &self.state())
            .field("channel", &self.channel)
            .finish()
    }
}

impl ChannelMonitor {
    /// Wrap `channel` with connection monitoring.
    ///
    /// The initial state is [`ConnectionState::Idle`].
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            channel,
            state: Arc::new(AtomicU8::new(ConnectionState::Idle as u8)),
            callbacks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a callback invoked on every state *change*.
    ///
    /// The callback receives the **new** state.  Transitioning to the same
    /// state as the current state is a no-op and does **not** fire the callback.
    ///
    /// Callbacks are held inside an `Arc<Mutex<_>>` and therefore must be
    /// `Send + Sync + 'static`.  They are called synchronously on the thread
    /// that calls [`set_state`](ChannelMonitor::set_state).
    pub fn on_state_change(&self, callback: impl Fn(ConnectionState) + Send + Sync + 'static) {
        let guard = match self.callbacks.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        // Shadow `guard` with a mutable binding.
        let mut guard = guard;
        guard.push(Box::new(callback));
    }

    /// Return the current connection state.
    pub fn state(&self) -> ConnectionState {
        ConnectionState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Advance the connection state.
    ///
    /// If `new_state` equals the current state, this is a no-op and no
    /// callbacks are fired.  Otherwise, the state is updated atomically and all
    /// registered callbacks are invoked in registration order with `new_state`.
    ///
    /// This is the only mutating method: the cooperative model requires the
    /// caller to drive state transitions.
    pub fn set_state(&self, new_state: ConnectionState) {
        let prev = self.state.swap(new_state as u8, Ordering::AcqRel);
        if prev == new_state as u8 {
            // No change — do not fire callbacks.
            return;
        }
        let guard = match self.callbacks.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        for cb in guard.iter() {
            cb(new_state);
        }
    }

    /// Returns a clone of the underlying channel for making RPCs.
    ///
    /// `tonic::transport::Channel` is internally `Arc`-backed, so cloning is
    /// cheap and returns a handle to the same underlying connection.
    pub fn channel(&self) -> tonic::transport::Channel {
        self.channel.clone()
    }

    /// Returns `true` if the channel is currently in the [`ConnectionState::Ready`] state.
    pub fn is_ready(&self) -> bool {
        self.state() == ConnectionState::Ready
    }
}

impl Drop for ChannelMonitor {
    /// Advance to [`ConnectionState::Shutdown`] and invoke all callbacks.
    fn drop(&mut self) {
        // Only fire if this is the last Arc holder (no other clones alive).
        // We can't know for sure, but we use strong_count as a best-effort
        // guard so we don't spam callbacks when intermediate clones are dropped.
        if Arc::strong_count(&self.state) == 1 {
            self.set_state(ConnectionState::Shutdown);
        }
    }
}
