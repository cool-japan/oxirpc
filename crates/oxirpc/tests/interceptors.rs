//! Unit tests for the built-in gRPC interceptors in `oxirpc::interceptors`.

use oxirpc::interceptors::{
    BearerAuthInterceptor, DeadlineInterceptor, InterceptorChain, LoggingInterceptor,
    MetricsInterceptor, RateLimitInterceptor, TimeoutInterceptor, TracingInterceptor,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::service::Interceptor;
use tonic::{Code, Request};

// ---------------------------------------------------------------------------
// BearerAuthInterceptor
// ---------------------------------------------------------------------------

#[test]
fn auth_interceptor_rejects_missing_header() {
    let mut interceptor = BearerAuthInterceptor::new("secret");
    let req: Request<()> = Request::new(());
    let err = interceptor.call(req).unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[test]
fn auth_interceptor_rejects_wrong_token() {
    let mut interceptor = BearerAuthInterceptor::new("correct");
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("authorization", "Bearer wrong".parse().unwrap());
    let err = interceptor.call(req).unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[test]
fn auth_interceptor_rejects_missing_bearer_prefix() {
    let mut interceptor = BearerAuthInterceptor::new("token");
    let mut req: Request<()> = Request::new(());
    // Token present but without "Bearer " prefix.
    req.metadata_mut()
        .insert("authorization", "token".parse().unwrap());
    let err = interceptor.call(req).unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[test]
fn auth_interceptor_accepts_correct_token() {
    let mut interceptor = BearerAuthInterceptor::new("mysecret");
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("authorization", "Bearer mysecret".parse().unwrap());
    assert!(interceptor.call(req).is_ok());
}

#[test]
fn auth_interceptor_is_clone() {
    let interceptor = BearerAuthInterceptor::new("t");
    let _clone = interceptor.clone();
}

// ---------------------------------------------------------------------------
// TracingInterceptor
// ---------------------------------------------------------------------------

#[test]
fn tracing_interceptor_injects_request_id() {
    let mut interceptor = TracingInterceptor;
    let req: Request<()> = Request::new(());
    let req = interceptor.call(req).expect("tracing must not fail");
    assert!(
        req.metadata().get("x-request-id").is_some(),
        "x-request-id must be present after tracing interceptor"
    );
}

#[test]
fn tracing_interceptor_ids_are_numeric() {
    let mut interceptor = TracingInterceptor;
    let req: Request<()> = Request::new(());
    let req = interceptor.call(req).unwrap();
    let id_str = req
        .metadata()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id should be ASCII");
    id_str.parse::<u64>().expect("x-request-id must be a u64");
}

#[test]
fn tracing_interceptor_ids_are_unique() {
    let mut interceptor = TracingInterceptor;

    let req1: Request<()> = Request::new(());
    let req1 = interceptor.call(req1).unwrap();
    let id1 = req1
        .metadata()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let req2: Request<()> = Request::new(());
    let req2 = interceptor.call(req2).unwrap();
    let id2 = req2
        .metadata()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .parse::<u64>()
        .unwrap();

    assert_ne!(id1, id2, "consecutive request IDs must differ");
}

#[test]
fn tracing_interceptor_is_clone() {
    let interceptor = TracingInterceptor;
    let _clone = interceptor.clone();
}

// ---------------------------------------------------------------------------
// DeadlineInterceptor
// ---------------------------------------------------------------------------

#[test]
fn deadline_interceptor_injects_timeout_when_absent() {
    let mut interceptor = DeadlineInterceptor::new(Duration::from_secs(3));
    let req: Request<()> = Request::new(());
    let req = interceptor.call(req).expect("deadline must not fail");
    let hdr = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .expect("grpc-timeout must be injected");
    // 3 s = 3000 ms → format is "3000m"
    assert_eq!(hdr, "3000m");
}

#[test]
fn deadline_interceptor_preserves_existing_timeout() {
    let mut interceptor = DeadlineInterceptor::new(Duration::from_secs(10));
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("grpc-timeout", "500m".parse().unwrap());
    let req = interceptor.call(req).unwrap();
    let hdr = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    // The caller's value must not be overwritten.
    assert_eq!(hdr, "500m");
}

#[test]
fn deadline_interceptor_is_clone() {
    let interceptor = DeadlineInterceptor::new(Duration::from_secs(1));
    let _clone = interceptor.clone();
}

// ---------------------------------------------------------------------------
// RateLimitInterceptor
// ---------------------------------------------------------------------------

#[test]
fn rate_limit_allows_up_to_capacity() {
    // capacity=3, zero refill so tokens never regenerate during the test.
    let mut limiter = RateLimitInterceptor::new(3, 0.0);
    assert!(limiter.call(Request::new(())).is_ok());
    assert!(limiter.call(Request::new(())).is_ok());
    assert!(limiter.call(Request::new(())).is_ok());
    let err = limiter.call(Request::new(())).unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
}

#[test]
fn rate_limit_returns_resource_exhausted_when_full() {
    let mut limiter = RateLimitInterceptor::new(0, 0.0);
    let err = limiter.call(Request::new(())).unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
}

#[test]
fn rate_limit_is_clone() {
    let limiter = RateLimitInterceptor::new(10, 1.0);
    let _clone = limiter.clone();
}

// ---------------------------------------------------------------------------
// LoggingInterceptor
// ---------------------------------------------------------------------------

#[test]
fn logging_interceptor_sink_is_called() {
    let count = Arc::new(Mutex::new(0u32));
    let count_clone = count.clone();
    let mut interceptor = LoggingInterceptor::new(move |_req| {
        *count_clone.lock().unwrap() += 1;
    });
    interceptor.call(Request::new(())).unwrap();
    interceptor.call(Request::new(())).unwrap();
    assert_eq!(*count.lock().unwrap(), 2);
}

#[test]
fn logging_interceptor_passes_request_through() {
    let mut interceptor = LoggingInterceptor::noop();
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("x-custom", "hello".parse().unwrap());
    let req = interceptor.call(req).unwrap();
    assert!(req.metadata().get("x-custom").is_some());
}

#[test]
fn logging_interceptor_noop_succeeds() {
    let mut interceptor = LoggingInterceptor::noop();
    assert!(interceptor.call(Request::new(())).is_ok());
}

#[test]
fn logging_interceptor_is_clone() {
    let interceptor = LoggingInterceptor::noop();
    let _clone = interceptor.clone();
}

// ---------------------------------------------------------------------------
// MetricsInterceptor
// ---------------------------------------------------------------------------

#[test]
fn metrics_interceptor_increments_total() {
    let mut interceptor = MetricsInterceptor::new();
    interceptor.call(Request::new(())).unwrap();
    interceptor.call(Request::new(())).unwrap();
    assert_eq!(interceptor.snapshot().total, 2);
}

#[test]
fn metrics_interceptor_tracks_by_path() {
    let mut interceptor = MetricsInterceptor::new();

    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("x-grpc-method", "/svc/Foo".parse().unwrap());
    interceptor.call(req).unwrap();

    let mut req2: Request<()> = Request::new(());
    req2.metadata_mut()
        .insert("x-grpc-method", "/svc/Foo".parse().unwrap());
    interceptor.call(req2).unwrap();

    let mut req3: Request<()> = Request::new(());
    req3.metadata_mut()
        .insert("x-grpc-method", "/svc/Bar".parse().unwrap());
    interceptor.call(req3).unwrap();

    let snap = interceptor.snapshot();
    assert_eq!(snap.total, 3);
    assert_eq!(snap.by_path["/svc/Foo"], 2);
    assert_eq!(snap.by_path["/svc/Bar"], 1);
}

#[test]
fn metrics_interceptor_unknown_path_when_no_header() {
    let mut interceptor = MetricsInterceptor::new();
    interceptor.call(Request::new(())).unwrap();
    let snap = interceptor.snapshot();
    assert_eq!(snap.by_path["unknown"], 1);
}

#[test]
fn metrics_interceptor_default_works() {
    let mut interceptor = MetricsInterceptor::default();
    assert!(interceptor.call(Request::new(())).is_ok());
}

#[test]
fn metrics_interceptor_is_clone() {
    let interceptor = MetricsInterceptor::new();
    let _clone = interceptor.clone();
}

// ---------------------------------------------------------------------------
// TimeoutInterceptor
// ---------------------------------------------------------------------------

#[test]
fn timeout_interceptor_injects_when_absent() {
    let mut interceptor = TimeoutInterceptor::new(Duration::from_secs(5));
    let req: Request<()> = Request::new(());
    let req = interceptor.call(req).unwrap();
    let hdr = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .expect("grpc-timeout must be injected");
    // 5 seconds → "5S" (coarsest exact unit).
    assert_eq!(hdr, "5S");
}

#[test]
fn timeout_interceptor_no_op_when_present() {
    let mut interceptor = TimeoutInterceptor::new(Duration::from_secs(10));
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("grpc-timeout", "200m".parse().unwrap());
    let req = interceptor.call(req).unwrap();
    let hdr = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    // Caller's value must not be overwritten.
    assert_eq!(hdr, "200m");
}

#[test]
fn timeout_interceptor_milliseconds_use_coarsest_unit() {
    let mut interceptor = TimeoutInterceptor::new(Duration::from_millis(100));
    let req = interceptor.call(Request::new(())).unwrap();
    let hdr = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(hdr, "100m");
}

#[test]
fn timeout_interceptor_is_clone() {
    let interceptor = TimeoutInterceptor::new(Duration::from_secs(1));
    let _clone = interceptor.clone();
}

// ---------------------------------------------------------------------------
// InterceptorChain
// ---------------------------------------------------------------------------

#[test]
fn interceptor_chain_runs_all_steps_in_order() {
    let mut chain = InterceptorChain::new()
        .push(TracingInterceptor)
        .push(TimeoutInterceptor::new(Duration::from_secs(3)));

    let req: Request<()> = Request::new(());
    let req = chain.call(req).unwrap();
    assert!(
        req.metadata().get("x-request-id").is_some(),
        "tracing step must run"
    );
    assert!(
        req.metadata().get("grpc-timeout").is_some(),
        "timeout step must run"
    );
}

#[test]
fn interceptor_chain_short_circuits_on_error() {
    // The auth interceptor will fail; the tracing interceptor must not run.
    let seen = Arc::new(Mutex::new(false));
    let seen_clone = seen.clone();
    let mut chain = InterceptorChain::new()
        .push(BearerAuthInterceptor::new("secret"))
        .push(LoggingInterceptor::new(move |_| {
            *seen_clone.lock().unwrap() = true;
        }));

    let req: Request<()> = Request::new(());
    let err = chain.call(req).unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    assert!(
        !*seen.lock().unwrap(),
        "steps after a failure must not execute"
    );
}

#[test]
fn interceptor_chain_empty_is_identity() {
    let mut chain = InterceptorChain::new();
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("x-test", "value".parse().unwrap());
    let req = chain.call(req).unwrap();
    assert!(req.metadata().get("x-test").is_some());
}

#[test]
fn interceptor_chain_default_is_empty() {
    let mut chain = InterceptorChain::default();
    assert!(chain.call(Request::new(())).is_ok());
}
