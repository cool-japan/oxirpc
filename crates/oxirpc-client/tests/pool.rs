//! Tests for ChannelPool and TypedChannel.

use oxirpc_client::balance::RoundRobin;
use oxirpc_client::{ChannelPool, TypedChannel};
use tonic::transport::{Channel, Endpoint};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn lazy_channel(addr: &'static str) -> Channel {
    Endpoint::from_static(addr).connect_lazy()
}

fn two_channel_pairs() -> Vec<(Endpoint, Channel)> {
    vec![
        (
            Endpoint::from_static("http://localhost:50051"),
            lazy_channel("http://localhost:50051"),
        ),
        (
            Endpoint::from_static("http://localhost:50052"),
            lazy_channel("http://localhost:50052"),
        ),
    ]
}

// ── ChannelPool::new + get ────────────────────────────────────────────────────

#[tokio::test]
async fn channel_pool_new_and_get() {
    let pool = ChannelPool::new(two_channel_pairs(), RoundRobin::new());
    // get() should return a channel reference without panicking.
    let _ch = pool.get();
    let _ch2 = pool.get();
    let _ch3 = pool.get();
}

// ── ChannelPool::len + is_empty ───────────────────────────────────────────────

#[tokio::test]
async fn channel_pool_len_empty() {
    let pool = ChannelPool::new(two_channel_pairs(), RoundRobin::new());
    assert_eq!(pool.len(), 2);
    assert!(!pool.is_empty());
}

#[test]
fn channel_pool_empty_has_correct_len() {
    // Empty pool: no channels — no runtime needed since no lazy connections.
    let pool = ChannelPool::new(vec![], RoundRobin::new());
    assert_eq!(pool.len(), 0);
    assert!(pool.is_empty());
}

// ── RoundRobin cycles ─────────────────────────────────────────────────────────

/// Verify that RoundRobin cycling with ChannelPool doesn't panic and
/// repeatedly calling get() returns valid channel references.
#[tokio::test]
async fn channel_pool_round_robin_picks_different() {
    let pool = ChannelPool::new(two_channel_pairs(), RoundRobin::new());
    // With RoundRobin, the first pick is index 0 and the second is index 1.
    // We verify by observing the raw pointer of each returned channel differs
    // across consecutive get() calls (or stays the same if it wraps — either
    // way, no panic and valid references).
    let ptr0 = pool.get() as *const Channel;
    let ptr1 = pool.get() as *const Channel;
    let ptr2 = pool.get() as *const Channel;

    // After two calls the balancer should have cycled: ptr0 != ptr1.
    assert_ne!(ptr0, ptr1, "RoundRobin should alternate between channels");
    // Third call wraps back to index 0.
    assert_eq!(ptr0, ptr2, "Third RoundRobin pick wraps back to index 0");
}

// ── ChannelPool::from_endpoints ───────────────────────────────────────────────

#[tokio::test]
async fn channel_pool_from_endpoints_valid() {
    let endpoints = vec![
        Endpoint::from_static("http://localhost:50051"),
        Endpoint::from_static("http://localhost:50052"),
    ];
    let pool = ChannelPool::from_endpoints(endpoints, RoundRobin::new())
        .await
        .expect("from_endpoints should succeed for valid endpoints");

    assert_eq!(pool.len(), 2);
    let _ch = pool.get();
}

/// from_endpoints with an empty list succeeds (empty pool).
#[tokio::test]
async fn channel_pool_from_endpoints_empty() {
    let pool = ChannelPool::from_endpoints(vec![], RoundRobin::new())
        .await
        .expect("empty endpoint list should succeed");
    assert!(pool.is_empty());
}

// ── ChannelPool::clone ────────────────────────────────────────────────────────

#[tokio::test]
async fn channel_pool_clone_has_same_len() {
    let pool = ChannelPool::new(two_channel_pairs(), RoundRobin::new());
    let cloned = pool.clone();
    assert_eq!(pool.len(), cloned.len());
}

// ── TypedChannel ──────────────────────────────────────────────────────────────

struct MyService;

#[tokio::test]
async fn typed_channel_deref() {
    let ch = lazy_channel("http://localhost:50051");
    let typed: TypedChannel<MyService> = TypedChannel::new(ch);

    // Deref should give &Channel.
    let _ref: &Channel = &typed;
    // channel() accessor should give &Channel.
    let _ref2: &Channel = typed.channel();
}

#[tokio::test]
async fn typed_channel_into_inner() {
    let ch = lazy_channel("http://localhost:50051");
    let typed: TypedChannel<MyService> = TypedChannel::new(ch);
    let _inner: Channel = typed.into_inner();
}

#[tokio::test]
async fn typed_channel_clone() {
    let ch = lazy_channel("http://localhost:50051");
    let typed: TypedChannel<MyService> = TypedChannel::new(ch);
    let _cloned = typed.clone();
}

#[tokio::test]
async fn typed_channel_debug() {
    let ch = lazy_channel("http://localhost:50051");
    let typed: TypedChannel<MyService> = TypedChannel::new(ch);
    let s = format!("{typed:?}");
    assert!(s.contains("TypedChannel"));
}

/// TypedChannel should not require S: Send + Sync (only the Channel need be Send).
#[tokio::test]
async fn typed_channel_non_send_service() {
    // std::rc::Rc is not Send, but TypedChannel<Rc<()>> should still compile
    // since PhantomData<fn() -> S> doesn't impose Send/Sync on S.
    use std::rc::Rc;
    let ch = lazy_channel("http://localhost:50051");
    let _typed: TypedChannel<Rc<()>> = TypedChannel::new(ch);
}
