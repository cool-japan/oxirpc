//! Integration tests for balance::* and resilience::* policy types.
//!
//! Tests that require network access (DnsResolver) are gated with `#[ignore]`.

use std::time::Duration;

use oxirpc_client::balance::{
    DnsResolver, Endpoint, LoadBalancer, PickFirst, Resolver, RoundRobin, StaticResolver, Weighted,
};
use oxirpc_client::resilience::{Backoff, CircuitBreaker, Hedging, RetryPolicy};
use oxirpc_core::status::StatusCode;

// ── Endpoint helpers ──────────────────────────────────────────────────────────

fn make_endpoint(addr: &str) -> Endpoint {
    let uri = format!("http://{addr}").parse().expect("valid URI");
    Endpoint::new(uri)
}

fn three_endpoints() -> Vec<Endpoint> {
    vec![
        make_endpoint("10.0.0.1:50051"),
        make_endpoint("10.0.0.2:50051"),
        make_endpoint("10.0.0.3:50051"),
    ]
}

// ── PickFirst ─────────────────────────────────────────────────────────────────

#[test]
fn pick_first_always_returns_zero() {
    let lb = PickFirst;
    let eps = three_endpoints();
    for _ in 0..10 {
        assert_eq!(lb.pick(&eps), Some(0));
    }
}

#[test]
fn pick_first_returns_none_on_empty() {
    let lb = PickFirst;
    assert_eq!(lb.pick(&[]), None);
}

// ── RoundRobin ────────────────────────────────────────────────────────────────

#[test]
fn round_robin_cycles_through_all_endpoints() {
    let lb = RoundRobin::new();
    let eps = three_endpoints();
    // First full cycle
    assert_eq!(lb.pick(&eps), Some(0));
    assert_eq!(lb.pick(&eps), Some(1));
    assert_eq!(lb.pick(&eps), Some(2));
    // Second full cycle
    assert_eq!(lb.pick(&eps), Some(0));
    assert_eq!(lb.pick(&eps), Some(1));
    assert_eq!(lb.pick(&eps), Some(2));
}

#[test]
fn round_robin_returns_none_on_empty() {
    let lb = RoundRobin::new();
    assert_eq!(lb.pick(&[]), None);
}

#[test]
fn round_robin_default_is_same_as_new() {
    let lb: RoundRobin = Default::default();
    let eps = three_endpoints();
    assert_eq!(lb.pick(&eps), Some(0));
}

// ── Weighted ──────────────────────────────────────────────────────────────────

/// Verify that over 6 consecutive picks with weights [1, 2, 3], the
/// distribution matches the expected allocation exactly.
#[test]
fn weighted_distribution_matches_weights() {
    let lb = Weighted::new();

    // weights: ep0=1, ep1=2, ep2=3  →  total=6
    let mut eps = three_endpoints();
    eps[0].weight = 1;
    eps[1].weight = 2;
    eps[2].weight = 3;

    let mut counts = [0usize; 3];
    for _ in 0..6 {
        let idx = lb.pick(&eps).expect("non-empty list");
        counts[idx] += 1;
    }
    assert_eq!(counts[0], 1, "ep0 should be picked once in 6");
    assert_eq!(counts[1], 2, "ep1 should be picked twice in 6");
    assert_eq!(counts[2], 3, "ep2 should be picked three times in 6");
}

#[test]
fn weighted_returns_none_on_empty() {
    let lb = Weighted::new();
    assert_eq!(lb.pick(&[]), None);
}

#[test]
fn weighted_all_zero_weights_returns_first() {
    let lb = Weighted::new();
    let mut eps = three_endpoints();
    for ep in eps.iter_mut() {
        ep.weight = 0;
    }
    // With all-zero weights the fallback is index 0.
    assert_eq!(lb.pick(&eps), Some(0));
}

#[test]
fn weighted_default_equals_new() {
    let lb: Weighted = Default::default();
    let eps = three_endpoints();
    assert!(lb.pick(&eps).is_some());
}

// ── StaticResolver ────────────────────────────────────────────────────────────

#[tokio::test]
async fn static_resolver_returns_configured_endpoints() {
    let expected = three_endpoints();
    let resolver = StaticResolver::new(expected.clone());
    let resolved = resolver.resolve().await.expect("should resolve cleanly");
    assert_eq!(resolved.len(), expected.len());
    for (got, want) in resolved.iter().zip(expected.iter()) {
        assert_eq!(got, want);
    }
}

#[tokio::test]
async fn static_resolver_empty_list() {
    let resolver = StaticResolver::new(vec![]);
    let resolved = resolver.resolve().await.expect("empty resolve is valid");
    assert!(resolved.is_empty());
}

// ── DnsResolver ───────────────────────────────────────────────────────────────

/// Requires a live DNS resolver.  Run manually with:
///   cargo test -p oxirpc-client dns_resolver -- --ignored
#[tokio::test]
#[ignore]
async fn dns_resolver_resolves_localhost() {
    let resolver = DnsResolver::new("localhost", 50051);
    let result = resolver.resolve().await;
    // On CI there may be no such host; just assert it doesn't panic.
    match result {
        Ok(eps) => {
            // localhost should resolve to at least one address.
            assert!(
                !eps.is_empty(),
                "expected at least one address for localhost"
            );
        }
        Err(e) => {
            // Acceptable in environments without DNS.
            println!("DNS lookup failed (expected in sandboxed env): {e}");
        }
    }
}

// ── RetryPolicy ───────────────────────────────────────────────────────────────

#[test]
fn retry_policy_should_retry_unavailable_at_attempt_zero() {
    let policy = RetryPolicy::new(3);
    let result = policy.should_retry(0, StatusCode::Unavailable);
    assert!(
        result.is_some(),
        "attempt 0 on Unavailable must be retryable"
    );
}

#[test]
fn retry_policy_should_retry_resource_exhausted() {
    let policy = RetryPolicy::new(3);
    let result = policy.should_retry(0, StatusCode::ResourceExhausted);
    assert!(
        result.is_some(),
        "ResourceExhausted is retryable by default"
    );
}

#[test]
fn retry_policy_returns_none_at_max_attempts() {
    let policy = RetryPolicy::new(3);
    // Attempt 3 means we have already retried 3 times → stop.
    assert_eq!(policy.should_retry(3, StatusCode::Unavailable), None);
    assert_eq!(policy.should_retry(4, StatusCode::Unavailable), None);
}

#[test]
fn retry_policy_returns_none_for_non_retryable_code() {
    let policy = RetryPolicy::new(5);
    // These codes are not retryable by default.
    for code in [
        StatusCode::NotFound,
        StatusCode::InvalidArgument,
        StatusCode::PermissionDenied,
        StatusCode::Unimplemented,
    ] {
        assert_eq!(
            policy.should_retry(0, code),
            None,
            "{code:?} should not be retryable"
        );
    }
}

#[test]
fn retry_policy_backoff_grows_with_attempt() {
    let policy = RetryPolicy::new(10).with_backoff(Backoff {
        base: Duration::from_millis(100),
        factor: 2.0,
        max: Duration::from_secs(10),
        jitter: false, // deterministic for testing
    });

    let d0 = policy
        .should_retry(0, StatusCode::Unavailable)
        .expect("attempt 0 retryable");
    let d1 = policy
        .should_retry(1, StatusCode::Unavailable)
        .expect("attempt 1 retryable");
    let d2 = policy
        .should_retry(2, StatusCode::Unavailable)
        .expect("attempt 2 retryable");

    assert!(
        d1 > d0,
        "backoff should grow: attempt1={d1:?} > attempt0={d0:?}"
    );
    assert!(
        d2 > d1,
        "backoff should grow: attempt2={d2:?} > attempt1={d1:?}"
    );
}

#[test]
fn retry_policy_custom_retryable_predicate() {
    let policy = RetryPolicy::new(5).with_retryable(|code| code == StatusCode::NotFound);
    assert!(policy.should_retry(0, StatusCode::NotFound).is_some());
    assert!(policy.should_retry(0, StatusCode::Unavailable).is_none());
}

// ── CircuitBreaker ────────────────────────────────────────────────────────────

#[test]
fn circuit_breaker_opens_after_failure_threshold() {
    let cb = CircuitBreaker::new(3, 2, Duration::from_secs(60));

    // Two failures: still closed.
    cb.on_failure();
    cb.on_failure();
    assert!(!cb.is_open());
    assert!(cb.allow_request());

    // Third failure: open.
    cb.on_failure();
    assert!(cb.is_open());
    assert!(!cb.allow_request());
}

#[test]
fn circuit_breaker_rejects_requests_when_open() {
    let cb = CircuitBreaker::new(1, 1, Duration::from_secs(60));
    cb.on_failure();
    assert!(cb.is_open());
    // All subsequent allow_request() calls must return false while open.
    for _ in 0..5 {
        assert!(!cb.allow_request());
    }
}

#[test]
fn circuit_breaker_transitions_to_half_open_after_cooldown() {
    // Use a zero cooldown so the test doesn't need to sleep.
    let cb = CircuitBreaker::new(1, 1, Duration::ZERO);
    cb.on_failure();
    assert!(cb.is_open());

    // With a zero cooldown the circuit should immediately transition to HalfOpen.
    assert!(
        cb.allow_request(),
        "should be HalfOpen (cooldown=0) and allow a probe"
    );
    assert!(
        !cb.is_open(),
        "should no longer be Open after transitioning"
    );
}

#[test]
fn circuit_breaker_closes_after_success_threshold_in_half_open() {
    let cb = CircuitBreaker::new(1, 2, Duration::ZERO);

    // Trip the breaker.
    cb.on_failure();
    assert!(cb.is_open());

    // Move to HalfOpen via allow_request (cooldown=0).
    assert!(cb.allow_request());

    // First success in HalfOpen: not yet closed.
    cb.on_success();
    assert!(!cb.is_open());

    // Second success: closed.
    cb.on_success();
    assert!(!cb.is_open());

    // Now requests should be allowed again.
    assert!(cb.allow_request());
}

#[test]
fn circuit_breaker_failure_in_half_open_reopens() {
    let cb = CircuitBreaker::new(1, 3, Duration::ZERO);
    cb.on_failure();
    assert!(cb.is_open());

    // Enter HalfOpen.
    assert!(cb.allow_request());

    // A failure in HalfOpen immediately re-opens the circuit.
    cb.on_failure();
    assert!(cb.is_open());
}

#[test]
fn circuit_breaker_starts_closed_and_allows_requests() {
    let cb = CircuitBreaker::new(3, 2, Duration::from_secs(30));
    assert!(!cb.is_open());
    assert!(cb.allow_request());
}

// ── Hedging ───────────────────────────────────────────────────────────────────

#[test]
fn hedging_schedule_has_correct_length() {
    let h = Hedging::new(3, Duration::from_millis(50));
    let sched = h.schedule();
    assert_eq!(sched.len(), 3);
}

#[test]
fn hedging_schedule_delays_are_correct_multiples() {
    let base = Duration::from_millis(50);
    let h = Hedging::new(4, base);
    let sched = h.schedule();
    assert_eq!(sched[0], base * 1);
    assert_eq!(sched[1], base * 2);
    assert_eq!(sched[2], base * 3);
    assert_eq!(sched[3], base * 4);
}

#[test]
fn hedging_delay_for_returns_correct_values() {
    let base = Duration::from_millis(100);
    let h = Hedging::new(3, base);
    assert_eq!(h.delay_for(1), Some(base * 1));
    assert_eq!(h.delay_for(2), Some(base * 2));
    assert_eq!(h.delay_for(3), Some(base * 3));
}

#[test]
fn hedging_delay_for_returns_none_out_of_range() {
    let h = Hedging::new(2, Duration::from_millis(100));
    assert_eq!(h.delay_for(0), None, "index 0 is invalid (1-indexed)");
    assert_eq!(h.delay_for(3), None, "exceeds max_hedges=2");
}

#[test]
fn hedging_zero_max_hedges_gives_empty_schedule() {
    let h = Hedging::new(0, Duration::from_millis(50));
    assert!(h.schedule().is_empty());
}
