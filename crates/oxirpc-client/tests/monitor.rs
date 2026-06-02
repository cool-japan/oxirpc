//! Tests for ChannelMonitor and ConnectionState.

use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use oxirpc_client::monitor::{ChannelMonitor, ConnectionState};
use tonic::transport::Endpoint;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn lazy_monitor() -> ChannelMonitor {
    let ch = Endpoint::from_static("http://127.0.0.1:9999").connect_lazy();
    ChannelMonitor::new(ch)
}

// ── Initial state ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_monitor_starts_idle() {
    let monitor = lazy_monitor();
    assert_eq!(monitor.state(), ConnectionState::Idle);
}

#[tokio::test]
async fn channel_monitor_is_not_ready_initially() {
    let monitor = lazy_monitor();
    assert!(!monitor.is_ready());
}

// ── State transitions ─────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_monitor_set_state_changes_state() {
    let monitor = lazy_monitor();
    monitor.set_state(ConnectionState::Connecting);
    assert_eq!(monitor.state(), ConnectionState::Connecting);

    monitor.set_state(ConnectionState::Ready);
    assert_eq!(monitor.state(), ConnectionState::Ready);
    assert!(monitor.is_ready());
}

#[tokio::test]
async fn channel_monitor_transient_failure() {
    let monitor = lazy_monitor();
    monitor.set_state(ConnectionState::Ready);
    monitor.set_state(ConnectionState::TransientFailure);
    assert_eq!(monitor.state(), ConnectionState::TransientFailure);
    assert!(!monitor.is_ready());
}

#[tokio::test]
async fn channel_monitor_shutdown_state() {
    let monitor = lazy_monitor();
    monitor.set_state(ConnectionState::Shutdown);
    assert_eq!(monitor.state(), ConnectionState::Shutdown);
}

// ── Same-state no-op ──────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_monitor_same_state_does_not_fire_callback() {
    let monitor = lazy_monitor();
    let count = Arc::new(AtomicU32::new(0));
    let count2 = count.clone();

    monitor.on_state_change(move |_s| {
        count2.fetch_add(1, Ordering::SeqCst);
    });

    // Transitioning to the same state (Idle -> Idle) must not fire.
    monitor.set_state(ConnectionState::Idle);
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "same-state should not fire callback"
    );

    // A real change should fire exactly once.
    monitor.set_state(ConnectionState::Connecting);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ── Callback fires on state change ────────────────────────────────────────────

#[tokio::test]
async fn channel_monitor_callback_fires_on_state_change() {
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();

    let monitor = lazy_monitor();
    monitor.on_state_change(move |_state| {
        called2.store(true, Ordering::SeqCst);
    });

    monitor.set_state(ConnectionState::Connecting);
    assert!(
        called.load(Ordering::SeqCst),
        "callback must fire on state change"
    );
}

#[tokio::test]
async fn channel_monitor_callback_receives_new_state() {
    use std::sync::Mutex;

    let seen: Arc<Mutex<Vec<ConnectionState>>> = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();

    let monitor = lazy_monitor();
    monitor.on_state_change(move |s| {
        let mut g = match seen2.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.push(s);
    });

    monitor.set_state(ConnectionState::Connecting);
    monitor.set_state(ConnectionState::Ready);
    monitor.set_state(ConnectionState::TransientFailure);

    let states = match seen.lock() {
        Ok(g) => g.clone(),
        Err(p) => p.into_inner().clone(),
    };
    assert_eq!(
        states,
        vec![
            ConnectionState::Connecting,
            ConnectionState::Ready,
            ConnectionState::TransientFailure,
        ]
    );
}

#[tokio::test]
async fn channel_monitor_multiple_callbacks() {
    let count_a = Arc::new(AtomicU32::new(0));
    let count_b = Arc::new(AtomicU32::new(0));
    let a2 = count_a.clone();
    let b2 = count_b.clone();

    let monitor = lazy_monitor();
    monitor.on_state_change(move |_| {
        a2.fetch_add(1, Ordering::SeqCst);
    });
    monitor.on_state_change(move |_| {
        b2.fetch_add(1, Ordering::SeqCst);
    });

    monitor.set_state(ConnectionState::Ready);
    assert_eq!(count_a.load(Ordering::SeqCst), 1);
    assert_eq!(count_b.load(Ordering::SeqCst), 1);
}

// ── channel() accessor ────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_monitor_channel_returns_channel() {
    let monitor = lazy_monitor();
    // Calling channel() twice yields independent clones (Arc-backed, cheap).
    let _ch1 = monitor.channel();
    let _ch2 = monitor.channel();
}

// ── Clone shares state ────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_monitor_clone_shares_state() {
    let monitor = lazy_monitor();
    let clone = monitor.clone();

    monitor.set_state(ConnectionState::Ready);
    assert_eq!(
        clone.state(),
        ConnectionState::Ready,
        "clone must observe state change made via original"
    );
}

#[tokio::test]
async fn channel_monitor_clone_shares_callbacks() {
    let count = Arc::new(AtomicU32::new(0));
    let count2 = count.clone();

    let monitor = lazy_monitor();
    let clone = monitor.clone();

    // Register callback via the *original*.
    monitor.on_state_change(move |_| {
        count2.fetch_add(1, Ordering::SeqCst);
    });

    // Fire via the *clone*.
    clone.set_state(ConnectionState::Connecting);
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "callback registered on original must fire via clone"
    );
}

// ── ConnectionState display / debug ──────────────────────────────────────────

#[test]
fn connection_state_debug() {
    assert_eq!(format!("{:?}", ConnectionState::Idle), "Idle");
    assert_eq!(format!("{:?}", ConnectionState::Connecting), "Connecting");
    assert_eq!(format!("{:?}", ConnectionState::Ready), "Ready");
    assert_eq!(
        format!("{:?}", ConnectionState::TransientFailure),
        "TransientFailure"
    );
    assert_eq!(format!("{:?}", ConnectionState::Shutdown), "Shutdown");
}

#[test]
fn connection_state_display() {
    assert_eq!(ConnectionState::Idle.to_string(), "Idle");
    assert_eq!(ConnectionState::Ready.to_string(), "Ready");
    assert_eq!(ConnectionState::Shutdown.to_string(), "Shutdown");
}

#[test]
fn connection_state_equality() {
    assert_eq!(ConnectionState::Ready, ConnectionState::Ready);
    assert_ne!(ConnectionState::Ready, ConnectionState::Idle);
}

#[test]
fn connection_state_copy() {
    let s = ConnectionState::Connecting;
    let s2 = s; // Copy
    assert_eq!(s, s2);
}
