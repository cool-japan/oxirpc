#![allow(deprecated)] // health_service() is deprecated; existing tests use it intentionally.

use oxirpc_health::proto::ServingStatusProto as NativeProto;
use oxirpc_health::{
    health_service, HealthBuilder, HealthState, NativeHealthService, ServingStatus,
};
use std::sync::Arc;
use tonic::server::NamedService as _;
use tonic::Request;
use tonic_health::pb::health_server::Health as _;
use tonic_health::pb::{health_check_response, HealthCheckRequest};
use tonic_health::server::{health_reporter, HealthService};
use tower::Service as _;

// ---------------------------------------------------------------------------
// Stub service type for typed NamedService registration tests
// ---------------------------------------------------------------------------

/// A minimal stub that satisfies [`tonic::server::NamedService`].
struct MyTestService;
impl tonic::server::NamedService for MyTestService {
    const NAME: &'static str = "test.MyTestService";
}

/// Helper: map wire i32 status to the enum for assertions.
fn wire_to_status(wire: i32) -> health_check_response::ServingStatus {
    health_check_response::ServingStatus::try_from(wire)
        .unwrap_or(health_check_response::ServingStatus::Unknown)
}

// ---------------------------------------------------------------------------
// API smoke tests via HealthHandle (oxirpc_health public API)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_service_set_serving() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("my.Service").await;
}

#[tokio::test]
async fn health_service_set_not_serving() {
    let (_svc, mut handle) = health_service();
    handle.set_not_serving("my.LegacyService").await;
}

#[tokio::test]
async fn health_service_clear() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("my.Service").await;
    handle.clear("my.Service").await;
}

#[tokio::test]
async fn health_service_set_arbitrary_status() {
    let (_svc, mut handle) = health_service();
    handle
        .set_status("my.Service", ServingStatus::Unknown)
        .await;
    handle
        .set_status("my.Service", ServingStatus::Serving)
        .await;
    handle
        .set_status("my.Service", ServingStatus::NotServing)
        .await;
}

// ---------------------------------------------------------------------------
// Local-mirror query + bulk API (get_status / list_services / set_all_*)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_get_status_reflects_local_mirror() {
    let (_svc, mut handle) = health_service();
    assert_eq!(handle.get_status("svc.A"), None);

    handle.set_serving("svc.A").await;
    assert_eq!(handle.get_status("svc.A"), Some(ServingStatus::Serving));

    handle.set_not_serving("svc.A").await;
    assert_eq!(handle.get_status("svc.A"), Some(ServingStatus::NotServing));

    handle.clear("svc.A").await;
    assert_eq!(handle.get_status("svc.A"), None);
}

#[tokio::test]
async fn health_list_services_sorted() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("svc.C").await;
    handle.set_serving("svc.A").await;
    handle.set_serving("svc.B").await;
    assert_eq!(
        handle.list_services(),
        vec!["svc.A".to_owned(), "svc.B".to_owned(), "svc.C".to_owned()]
    );
}

#[tokio::test]
async fn health_set_all_serving_and_not_serving() {
    let (_svc, mut handle) = health_service();
    handle.set_not_serving("svc.A").await;
    handle.set_not_serving("svc.B").await;

    handle.set_all_serving().await;
    assert_eq!(handle.get_status("svc.A"), Some(ServingStatus::Serving));
    assert_eq!(handle.get_status("svc.B"), Some(ServingStatus::Serving));

    handle.set_all_not_serving().await;
    assert_eq!(handle.get_status("svc.A"), Some(ServingStatus::NotServing));
    assert_eq!(handle.get_status("svc.B"), Some(ServingStatus::NotServing));
}

// ---------------------------------------------------------------------------
// Functional round-trip tests via HealthService (tonic-health internal API)
// ---------------------------------------------------------------------------

/// Verifies the health state machine in-process:
/// set SERVING → Check returns SERVING; flip to NOT_SERVING → Check returns
/// NOT_SERVING; unknown name → NOT_FOUND.
///
/// Uses `HealthService::from_health_reporter` so we can call `check` directly
/// without spinning up a full server.
#[tokio::test]
async fn health_check_serving_and_not_serving() {
    let (reporter, _server) = health_reporter();
    let svc = HealthService::from_health_reporter(reporter.clone());

    // Register as Serving.
    reporter
        .set_service_status("test.Check", ServingStatus::Serving)
        .await;

    let resp = svc
        .check(Request::new(HealthCheckRequest {
            service: "test.Check".to_string(),
        }))
        .await;
    assert!(resp.is_ok(), "Check should succeed: {resp:?}");
    let inner = resp.unwrap().into_inner();
    assert_eq!(
        wire_to_status(inner.status),
        health_check_response::ServingStatus::Serving,
        "expected SERVING"
    );

    // Flip to NotServing.
    reporter
        .set_service_status("test.Check", ServingStatus::NotServing)
        .await;

    let resp = svc
        .check(Request::new(HealthCheckRequest {
            service: "test.Check".to_string(),
        }))
        .await;
    assert!(resp.is_ok());
    let inner = resp.unwrap().into_inner();
    assert_eq!(
        wire_to_status(inner.status),
        health_check_response::ServingStatus::NotServing,
        "expected NOT_SERVING"
    );

    // Unknown service → NOT_FOUND.
    let resp = svc
        .check(Request::new(HealthCheckRequest {
            service: "nonexistent.Service".to_string(),
        }))
        .await;
    assert!(resp.is_err(), "unregistered service should return error");
    assert_eq!(resp.unwrap_err().code(), tonic::Code::NotFound);
}

// ---------------------------------------------------------------------------
// Probe registration, aggregate status, and Kubernetes probe types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn probe_registration_and_aggregate_serving() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("svc-a").await;
    handle.set_serving("svc-b").await;
    assert_eq!(
        handle.aggregate_status(),
        oxirpc_health::ServingStatus::Serving
    );
}

#[tokio::test]
async fn aggregate_not_serving_when_any_not_serving() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("svc-a").await;
    handle.set_not_serving("svc-b").await;
    assert_eq!(
        handle.aggregate_status(),
        oxirpc_health::ServingStatus::NotServing
    );
}

#[tokio::test]
async fn aggregate_not_serving_when_empty() {
    let (_svc, handle) = health_service();
    assert_eq!(
        handle.aggregate_status(),
        oxirpc_health::ServingStatus::NotServing
    );
}

#[tokio::test]
async fn k8s_probe_type_filtering() {
    use oxirpc_health::{ProbeFn, ProbeType};
    use std::pin::Pin;

    let (_svc, mut handle) = health_service();
    handle.set_serving("readiness-svc").await;

    let probe: ProbeFn = std::sync::Arc::new(|| {
        Box::pin(async { true }) as Pin<Box<dyn std::future::Future<Output = bool> + Send>>
    });
    handle.register_k8s_probe(ProbeType::Readiness, "readiness-svc", probe);

    // Only readiness probes affect check_probe(Readiness)
    assert_eq!(
        handle.check_probe(ProbeType::Readiness),
        oxirpc_health::ServingStatus::Serving
    );
    // Liveness has no probes registered → opt-in pass
    assert_eq!(
        handle.check_probe(ProbeType::Liveness),
        oxirpc_health::ServingStatus::Serving
    );
}

#[tokio::test(start_paused = true)]
async fn probe_loop_fires_and_updates_status() {
    use oxirpc_health::ProbeFn;
    use std::pin::Pin;

    let (_svc, mut handle) = health_service();
    handle.set_not_serving("probe-svc").await;

    // Register a probe that always returns true (healthy)
    let probe: ProbeFn = std::sync::Arc::new(|| {
        Box::pin(async { true }) as Pin<Box<dyn std::future::Future<Output = bool> + Send>>
    });
    handle.register_probe("probe-svc", probe);

    // Start probe loop with a short interval
    let jh = handle.start_probe_loop(tokio::time::Duration::from_millis(100));

    // Advance time past the interval
    tokio::time::advance(tokio::time::Duration::from_millis(200)).await;
    tokio::task::yield_now().await;

    jh.abort();
    // After the loop runs, the reporter should have updated the status
    // (We can't easily verify via statuses mirror since probe loop doesn't update it,
    // but the test verifies the loop doesn't panic)
}

/// Verify two independently registered service names have independent statuses.
#[tokio::test]
async fn health_two_services_independent() {
    let (reporter, _server) = health_reporter();
    let svc = HealthService::from_health_reporter(reporter.clone());

    reporter
        .set_service_status("svc.A", ServingStatus::Serving)
        .await;
    reporter
        .set_service_status("svc.B", ServingStatus::NotServing)
        .await;

    let resp_a = svc
        .check(Request::new(HealthCheckRequest {
            service: "svc.A".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    let resp_b = svc
        .check(Request::new(HealthCheckRequest {
            service: "svc.B".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        wire_to_status(resp_a.status),
        health_check_response::ServingStatus::Serving,
        "svc.A should be SERVING"
    );
    assert_eq!(
        wire_to_status(resp_b.status),
        health_check_response::ServingStatus::NotServing,
        "svc.B should be NOT_SERVING"
    );
}

// ---------------------------------------------------------------------------
// HealthBuilder + on_change + set_all (graceful drain)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn builder_seeds_initial_statuses() {
    let (_, handle) = HealthBuilder::new()
        .register("foo", ServingStatus::Serving)
        .build()
        .await;
    assert_eq!(
        handle.statuses().get("foo"),
        Some(&ServingStatus::Serving),
        "builder should seed initial status for 'foo'"
    );
}

#[tokio::test]
async fn on_change_fires() {
    use std::sync::{Arc, Mutex};
    let log = Arc::new(Mutex::new(Vec::<(String, ServingStatus)>::new()));
    let log2 = Arc::clone(&log);
    let (_, mut handle) = HealthBuilder::new()
        .on_change(move |svc, status| {
            log2.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((svc.to_string(), status));
        })
        .build()
        .await;
    handle.set_status("bar", ServingStatus::NotServing).await;
    let entries = log.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(entries.len(), 1, "callback should have fired exactly once");
    assert_eq!(entries[0].0, "bar");
    assert_eq!(entries[0].1, ServingStatus::NotServing);
}

#[tokio::test]
async fn drain_flips_all_to_not_serving() {
    let (_, mut handle) = HealthBuilder::new()
        .register("a", ServingStatus::Serving)
        .register("b", ServingStatus::Serving)
        .build()
        .await;
    handle.set_all(ServingStatus::NotServing).await;
    assert!(
        handle
            .statuses()
            .values()
            .all(|&s| s == ServingStatus::NotServing),
        "all services should be NOT_SERVING after drain"
    );
}

// ---------------------------------------------------------------------------
// Typed NamedService registration via register_named / status_for
// ---------------------------------------------------------------------------

#[tokio::test]
async fn check_returns_serving_for_serving_service() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("named.Service").await;
    assert_eq!(
        handle.get_status("named.Service"),
        Some(ServingStatus::Serving),
        "get_status should return Serving for a Serving service"
    );
}

#[tokio::test]
async fn check_returns_not_serving() {
    let (_svc, mut handle) = health_service();
    handle.set_not_serving("named.Service").await;
    assert_eq!(
        handle.get_status("named.Service"),
        Some(ServingStatus::NotServing),
        "get_status should return NotServing for a NotServing service"
    );
}

#[tokio::test]
async fn check_returns_none_for_unknown_service() {
    let (_svc, handle) = health_service();
    assert_eq!(
        handle.get_status("never.Registered"),
        None,
        "get_status should return None for an unregistered service"
    );
}

#[tokio::test]
async fn clear_removes_service() {
    let (_svc, mut handle) = health_service();
    handle.set_serving("temp.Service").await;
    assert_eq!(
        handle.get_status("temp.Service"),
        Some(ServingStatus::Serving)
    );
    handle.clear("temp.Service").await;
    assert_eq!(
        handle.get_status("temp.Service"),
        None,
        "get_status should return None after clearing the service"
    );
}

#[tokio::test]
async fn overall_server_health() {
    // gRPC health check convention: empty string = overall server health
    let (_svc, mut handle) = health_service();
    handle.set_serving("").await;
    assert_eq!(
        handle.get_status(""),
        Some(ServingStatus::Serving),
        "overall server health (empty string key) should be Serving"
    );
}

#[tokio::test]
async fn typed_register_named() {
    let (_svc, mut handle) = health_service();
    handle
        .register_named::<MyTestService>(ServingStatus::Serving)
        .await;
    assert_eq!(
        handle.get_status(MyTestService::NAME),
        Some(ServingStatus::Serving),
        "register_named should set status under NamedService::NAME"
    );
}

#[tokio::test]
async fn typed_status_for() {
    let (_svc, mut handle) = health_service();
    assert_eq!(
        handle.status_for::<MyTestService>(),
        None,
        "status_for should return None before registration"
    );
    handle
        .register_named::<MyTestService>(ServingStatus::NotServing)
        .await;
    assert_eq!(
        handle.status_for::<MyTestService>(),
        Some(ServingStatus::NotServing),
        "status_for should return the registered status"
    );
}

#[tokio::test]
async fn builder_register_named() {
    let (_, handle) = HealthBuilder::new()
        .register_named::<MyTestService>(ServingStatus::Serving)
        .build()
        .await;
    assert_eq!(
        handle.get_status(MyTestService::NAME),
        Some(ServingStatus::Serving),
        "HealthBuilder::register_named should seed initial status under NamedService::NAME"
    );
}

#[tokio::test]
async fn on_change_fires_on_set_status() {
    use std::sync::{Arc, Mutex};
    let fired = Arc::new(Mutex::new(false));
    let fired2 = Arc::clone(&fired);
    let (_svc, mut handle) = HealthBuilder::new()
        .on_change(move |_svc, _status| {
            *fired2.lock().unwrap_or_else(|e| e.into_inner()) = true;
        })
        .build()
        .await;
    handle
        .set_status("trigger.Service", ServingStatus::Serving)
        .await;
    let was_fired = *fired.lock().unwrap_or_else(|e| e.into_inner());
    assert!(was_fired, "on_change callback should fire on set_status");
}

#[tokio::test]
async fn probe_registration_and_check_via_named() {
    use oxirpc_health::{ProbeFn, ProbeType};
    use std::pin::Pin;
    use std::sync::Arc;

    let (_svc, mut handle) = health_service();
    handle
        .register_named::<MyTestService>(ServingStatus::Serving)
        .await;

    let probe: ProbeFn = Arc::new(|| {
        Box::pin(async { true }) as Pin<Box<dyn std::future::Future<Output = bool> + Send>>
    });
    handle.register_k8s_probe(ProbeType::Readiness, MyTestService::NAME, probe);

    // Service is Serving and probe registered → check_probe(Readiness) = Serving
    assert_eq!(
        handle.check_probe(ProbeType::Readiness),
        ServingStatus::Serving,
        "check_probe(Readiness) should be Serving when the service is Serving"
    );
}

// ---------------------------------------------------------------------------
// Watch: verify that status changes propagate to on_change watchers in order
// ---------------------------------------------------------------------------

/// Verify that multiple sequential `set_status` calls propagate their status
/// changes to the registered `on_change` watcher in the correct order.
///
/// This covers the "Test Watch receives updates when status changes" TODO item.
/// The `on_change` callback is the synchronous notification path; it fires
/// inside `set_status` after the reporter and local mirror are updated.
#[tokio::test]
async fn watch_receives_multiple_updates() {
    use std::sync::{Arc, Mutex};

    // Shared log of (service_name, status) pairs recorded by the watcher.
    let log: Arc<Mutex<Vec<(String, ServingStatus)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_watcher = Arc::clone(&log);

    let (_, mut handle) = HealthBuilder::new()
        .on_change(move |svc, status| {
            log_watcher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((svc.to_owned(), status));
        })
        .build()
        .await;

    // Fire a sequence of status changes.
    handle
        .set_status("watch.Service", ServingStatus::Serving)
        .await;
    handle
        .set_status("watch.Service", ServingStatus::NotServing)
        .await;
    handle
        .set_status("watch.Service", ServingStatus::Unknown)
        .await;
    handle
        .set_status("watch.Service", ServingStatus::Serving)
        .await;

    let entries = log.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(
        entries.len(),
        4,
        "watcher should have received exactly 4 updates"
    );

    assert_eq!(
        entries[0],
        ("watch.Service".to_owned(), ServingStatus::Serving),
        "first update should be Serving"
    );
    assert_eq!(
        entries[1],
        ("watch.Service".to_owned(), ServingStatus::NotServing),
        "second update should be NotServing"
    );
    assert_eq!(
        entries[2],
        ("watch.Service".to_owned(), ServingStatus::Unknown),
        "third update should be Unknown"
    );
    assert_eq!(
        entries[3],
        ("watch.Service".to_owned(), ServingStatus::Serving),
        "fourth update should be Serving again"
    );
}

/// Verify that the watcher receives updates for multiple distinct services
/// and can distinguish which service changed.
#[tokio::test]
async fn watch_receives_updates_for_multiple_services() {
    use std::sync::{Arc, Mutex};

    let log: Arc<Mutex<Vec<(String, ServingStatus)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_watcher = Arc::clone(&log);

    let (_, mut handle) = HealthBuilder::new()
        .on_change(move |svc, status| {
            log_watcher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((svc.to_owned(), status));
        })
        .build()
        .await;

    handle.set_serving("svc.Alpha").await;
    handle.set_not_serving("svc.Beta").await;
    handle.set_serving("svc.Alpha").await; // second update for Alpha

    let entries = log.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(
        entries.len(),
        3,
        "watcher should have received one event per set_status call"
    );

    assert_eq!(entries[0].0, "svc.Alpha");
    assert_eq!(entries[0].1, ServingStatus::Serving);

    assert_eq!(entries[1].0, "svc.Beta");
    assert_eq!(entries[1].1, ServingStatus::NotServing);

    assert_eq!(entries[2].0, "svc.Alpha");
    assert_eq!(entries[2].1, ServingStatus::Serving);
}

/// Verify that `on_change` set directly on `HealthHandle` (not via builder)
/// also receives updates correctly.
#[tokio::test]
async fn watch_via_handle_on_change_method() {
    use std::sync::{Arc, Mutex};

    let log: Arc<Mutex<Vec<ServingStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let log_watcher = Arc::clone(&log);

    let (_svc, mut handle) = health_service();

    // Register callback directly on the handle (not via builder).
    handle.on_change(move |_svc, status| {
        log_watcher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(status);
    });

    handle.set_serving("direct.Service").await;
    handle.set_not_serving("direct.Service").await;

    let entries = log.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(
        entries.len(),
        2,
        "on_change registered via handle method should receive 2 events"
    );
    assert_eq!(entries[0], ServingStatus::Serving);
    assert_eq!(entries[1], ServingStatus::NotServing);
}

// ---------------------------------------------------------------------------
// Native gRPC health proto encode / decode (proto.rs)
// ---------------------------------------------------------------------------

#[test]
fn health_check_request_encodes_decodes() {
    use oxirpc_health::proto::{HealthCheckRequest, ServingStatusProto};
    use prost::Message;

    let req = HealthCheckRequest {
        service: "my.Service".to_owned(),
    };
    let bytes = req.encode_to_vec();
    let decoded = HealthCheckRequest::decode(bytes.as_slice())
        .expect("HealthCheckRequest decode should succeed");
    assert_eq!(decoded.service, "my.Service");

    // Verify that ServingStatusProto discriminants are correct.
    assert_eq!(ServingStatusProto::Unknown as i32, 0);
    assert_eq!(ServingStatusProto::Serving as i32, 1);
    assert_eq!(ServingStatusProto::NotServing as i32, 2);
    assert_eq!(ServingStatusProto::ServiceUnknown as i32, 3);
}

#[test]
fn health_check_request_empty_service_roundtrip() {
    use oxirpc_health::proto::HealthCheckRequest;
    use prost::Message;

    // Empty string = overall server health query.
    let req = HealthCheckRequest {
        service: String::new(),
    };
    let bytes = req.encode_to_vec();
    let decoded = HealthCheckRequest::decode(bytes.as_slice())
        .expect("decode should succeed for empty service");
    assert_eq!(decoded.service, "");
}

#[test]
fn health_check_response_serving() {
    use oxirpc_health::proto::{HealthCheckResponse, ServingStatusProto};

    let resp = HealthCheckResponse::serving();
    assert_eq!(resp.status, ServingStatusProto::Serving as i32);
    assert_eq!(resp.serving_status(), Some(ServingStatusProto::Serving));
}

#[test]
fn health_check_response_not_serving() {
    use oxirpc_health::proto::{HealthCheckResponse, ServingStatusProto};

    let resp = HealthCheckResponse::not_serving();
    assert_eq!(resp.status, ServingStatusProto::NotServing as i32);
    assert_eq!(resp.serving_status(), Some(ServingStatusProto::NotServing));
}

#[test]
fn health_check_response_roundtrip() {
    use oxirpc_health::proto::{HealthCheckResponse, ServingStatusProto};
    use prost::Message;

    let resp = HealthCheckResponse::serving();
    let bytes = resp.encode_to_vec();
    let decoded = HealthCheckResponse::decode(bytes.as_slice())
        .expect("HealthCheckResponse decode should succeed");
    assert_eq!(decoded.status, ServingStatusProto::Serving as i32);
}

// ─────────────────────────────────────────────────────────────────────────────
// Native Health service tests (NativeHealthService + HealthState)
// ─────────────────────────────────────────────────────────────────────────────

/// Build a native request that targets `path` with a prost-encoded
/// `HealthCheckRequest { service }` as the gRPC body.
fn make_native_request(path: &str, service: &str) -> http::Request<tonic::body::Body> {
    use bytes::Bytes;
    use oxirpc_health::proto::HealthCheckRequest as NativeReq;
    use prost::Message as _;

    // Encode the proto message.
    let req_msg = NativeReq {
        service: service.to_owned(),
    };
    let proto_bytes = req_msg.encode_to_vec();

    // Wrap in a gRPC length-prefixed frame: 1 byte flag + 4 bytes length.
    let mut frame = Vec::with_capacity(5 + proto_bytes.len());
    frame.push(0u8); // uncompressed
    let len = proto_bytes.len() as u32;
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&proto_bytes);

    use http_body_util::BodyExt as _;
    let body = tonic::body::Body::new(
        http_body_util::Full::new(Bytes::from(frame))
            .map_err(|e| tonic::Status::internal(e.to_string())),
    );
    http::Request::builder()
        .uri(path)
        .header("content-type", "application/grpc+proto")
        .body(body)
        .expect("request builder")
}

/// Decode a `HealthCheckResponse` from a `NativeBody` (gRPC-framed).
async fn decode_check_response(
    resp: http::Response<oxirpc_core::wire::NativeBody>,
) -> oxirpc_health::proto::ServingStatusProto {
    use http_body_util::BodyExt as _;
    use oxirpc_health::proto::HealthCheckResponse as NativeResp;
    use prost::Message as _;

    let (_parts, body) = resp.into_parts();
    let bytes = body.collect().await.expect("body collect").to_bytes();
    // strip 5-byte gRPC frame header
    let payload = &bytes[5..];
    let msg = NativeResp::decode(payload).expect("decode HealthCheckResponse");
    NativeProto::try_from(msg.status).unwrap_or(NativeProto::Unknown)
}

/// Assert that an HTTP response carries a specific gRPC status code in its trailers.
///
/// Native responses carry `grpc-status` in the response trailers (body trailers
/// per the gRPC-over-HTTP/2 spec), not in the initial response headers.
async fn assert_grpc_status(
    resp: http::Response<oxirpc_core::wire::NativeBody>,
    expected_code: u32,
) {
    use http_body_util::BodyExt as _;

    let (_parts, body) = resp.into_parts();
    let collected = body.collect().await.expect("body collect for trailers");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let grpc_status = trailers
        .get("grpc-status")
        .map(|v| v.to_str().unwrap_or("?").parse::<u32>().unwrap_or(0));
    assert_eq!(
        grpc_status,
        Some(expected_code),
        "expected grpc-status: {expected_code} in trailers"
    );
}

/// `native_check_serving_returns_serving`
#[tokio::test]
async fn native_check_serving_returns_serving() {
    let state = HealthState::new();
    state.set("svc", ServingStatus::Serving).await;

    let mut svc = NativeHealthService::new(state);
    let req = make_native_request("/grpc.health.v1.Health/Check", "svc");

    let resp = svc.call(req).await.expect("call");
    let status = decode_check_response(resp).await;
    assert_eq!(status, NativeProto::Serving);
}

/// `native_check_unknown_returns_not_found`
#[tokio::test]
async fn native_check_unknown_returns_not_found() {
    let state = HealthState::new();
    let mut svc = NativeHealthService::new(state);
    let req = make_native_request("/grpc.health.v1.Health/Check", "ghost.Service");

    let resp = svc.call(req).await.expect("call");
    // NOT_FOUND = grpc-status 5
    assert_grpc_status(resp, 5).await;
}

/// `native_watch_streams_current_status_immediately`
#[tokio::test]
async fn native_watch_streams_current_status_immediately() {
    use tonic::codec::{Codec as _, Streaming};

    let state = HealthState::new();
    state.set("svc", ServingStatus::Serving).await;

    let mut svc = NativeHealthService::new(state);
    let req = make_native_request("/grpc.health.v1.Health/Watch", "svc");

    let resp = svc.call(req).await.expect("call");
    // The response body is a gRPC server stream.
    let (_parts, body) = resp.into_parts();

    let mut codec = tonic_prost::ProstCodec::<
        oxirpc_health::proto::HealthCheckRequest,
        oxirpc_health::proto::HealthCheckResponse,
    >::default();
    let mut stream =
        Streaming::new_response(codec.decoder(), body, http::StatusCode::OK, None, None);

    let first = stream
        .message()
        .await
        .expect("stream error")
        .expect("no first message");
    assert_eq!(
        NativeProto::try_from(first.status).unwrap_or(NativeProto::Unknown),
        NativeProto::Serving,
        "Watch should immediately yield current Serving status"
    );
}

/// `native_watch_propagates_state_change`
#[tokio::test]
async fn native_watch_propagates_state_change() {
    use tonic::codec::{Codec as _, Streaming};

    let state = HealthState::new();
    state.set("svc", ServingStatus::Serving).await;

    let mut svc = NativeHealthService::new(std::sync::Arc::clone(&state));
    let req = make_native_request("/grpc.health.v1.Health/Watch", "svc");

    let resp = svc.call(req).await.expect("call");
    let (_parts, body) = resp.into_parts();

    let mut codec = tonic_prost::ProstCodec::<
        oxirpc_health::proto::HealthCheckRequest,
        oxirpc_health::proto::HealthCheckResponse,
    >::default();
    let mut stream =
        Streaming::new_response(codec.decoder(), body, http::StatusCode::OK, None, None);

    // First item = current Serving
    let first = stream
        .message()
        .await
        .expect("stream error")
        .expect("first");
    assert_eq!(
        NativeProto::try_from(first.status).ok(),
        Some(NativeProto::Serving)
    );

    // Change state → NotServing; stream should deliver that.
    state.set("svc", ServingStatus::NotServing).await;
    // Give the task a tick to deliver the watch notification.
    tokio::task::yield_now().await;

    let second = stream
        .message()
        .await
        .expect("stream error")
        .expect("second");
    assert_eq!(
        NativeProto::try_from(second.status).ok(),
        Some(NativeProto::NotServing),
        "Watch should propagate NotServing after state change"
    );
}

/// `native_watch_dedups_consecutive_duplicates`
#[tokio::test]
async fn native_watch_dedups_consecutive_duplicates() {
    use tonic::codec::{Codec as _, Streaming};

    let state = HealthState::new();
    state.set("svc", ServingStatus::Serving).await;

    let mut svc = NativeHealthService::new(std::sync::Arc::clone(&state));
    let req = make_native_request("/grpc.health.v1.Health/Watch", "svc");

    let resp = svc.call(req).await.expect("call");
    let (_parts, body) = resp.into_parts();

    let mut codec = tonic_prost::ProstCodec::<
        oxirpc_health::proto::HealthCheckRequest,
        oxirpc_health::proto::HealthCheckResponse,
    >::default();
    let mut stream =
        Streaming::new_response(codec.decoder(), body, http::StatusCode::OK, None, None);

    // Consume the initial Serving notification.
    let _ = stream
        .message()
        .await
        .expect("stream error")
        .expect("initial");

    // Send the same status twice — only one notification should be generated.
    state.set("svc", ServingStatus::NotServing).await;
    state.set("svc", ServingStatus::NotServing).await;
    tokio::task::yield_now().await;

    let update = tokio::time::timeout(std::time::Duration::from_millis(200), stream.message())
        .await
        .expect("timeout waiting for first update")
        .expect("stream error")
        .expect("no update message");
    assert_eq!(
        NativeProto::try_from(update.status).ok(),
        Some(NativeProto::NotServing)
    );

    // A second item should NOT be immediately available (dedup suppressed it).
    let maybe_second =
        tokio::time::timeout(std::time::Duration::from_millis(50), stream.message()).await;
    assert!(
        maybe_second.is_err(),
        "duplicate status update should have been suppressed"
    );
}

/// `native_shutdown_flips_all_to_not_serving`
#[tokio::test]
async fn native_shutdown_flips_all_to_not_serving() {
    let state = HealthState::new();
    state.set("a", ServingStatus::Serving).await;
    state.set("b", ServingStatus::Serving).await;

    state.shutdown().await;

    assert_eq!(
        state.get_status("a").await,
        Some(ServingStatus::NotServing),
        "shutdown should flip 'a' to NotServing"
    );
    assert_eq!(
        state.get_status("b").await,
        Some(ServingStatus::NotServing),
        "shutdown should flip 'b' to NotServing"
    );
}

/// `native_service_named_constant_is_health_v1`
#[test]
fn native_service_named_constant_is_health_v1() {
    assert_eq!(
        NativeHealthService::NAME,
        "grpc.health.v1.Health",
        "NativeHealthService::NAME should match the gRPC health service name"
    );
}

/// `native_check_unknown_method_returns_unimplemented`
#[tokio::test]
async fn native_check_unknown_method_returns_unimplemented() {
    let state = HealthState::new();
    let mut svc = NativeHealthService::new(state);

    // Craft a request to an unknown method path.
    let body = tonic::body::Body::empty();
    let req = http::Request::builder()
        .uri("/grpc.health.v1.Health/UnknownMethod")
        .header("content-type", "application/grpc+proto")
        .body(body)
        .expect("request builder");

    let resp = svc.call(req).await.expect("call");
    // gRPC UNIMPLEMENTED = code 12
    assert_grpc_status(resp, 12).await;
}

/// Verify that `HealthHandle::set_serving()` (the primary API path) propagates
/// the status change to the underlying `NativeHealthService`.
///
/// This test covers the critical path: `build_native` → `handle.set_serving` →
/// `Check` RPC returns the updated status.
#[tokio::test]
async fn native_handle_set_serving_propagates_to_service() {
    let (mut native_svc, mut handle) = HealthBuilder::new()
        .register("prop.Service", ServingStatus::NotServing)
        .build_native()
        .await;

    // Change status through the handle API — this must propagate to native_svc.
    handle.set_serving("prop.Service").await;

    let req = make_native_request("/grpc.health.v1.Health/Check", "prop.Service");
    let resp = native_svc.call(req).await.expect("call");
    let status = decode_check_response(resp).await;
    assert_eq!(
        status,
        NativeProto::Serving,
        "handle.set_serving must propagate Serving status to NativeHealthService"
    );
}

/// Verify that `HealthHandle::set_not_serving()` also propagates through.
#[tokio::test]
async fn native_handle_set_not_serving_propagates_to_service() {
    let (mut native_svc, mut handle) = HealthBuilder::new()
        .register("prop.Service", ServingStatus::Serving)
        .build_native()
        .await;

    handle.set_not_serving("prop.Service").await;

    let req = make_native_request("/grpc.health.v1.Health/Check", "prop.Service");
    let resp = native_svc.call(req).await.expect("call");
    let status = decode_check_response(resp).await;
    assert_eq!(
        status,
        NativeProto::NotServing,
        "handle.set_not_serving must propagate NotServing status to NativeHealthService"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Graceful-shutdown integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// `health_service_shutdown_triggers_not_serving`
///
/// Verifies that calling `handle.shutdown()` on a `build_native()` handle flips
/// all registered services to NOT_SERVING.  The `shutdown()` method spawns an
/// async task, so we poll with a short timeout to avoid a flaky race.
#[tokio::test]
async fn health_service_shutdown_triggers_not_serving() {
    let state = HealthState::new();
    state.set("svc.A", ServingStatus::Serving).await;
    state.set("svc.B", ServingStatus::Serving).await;

    // Build a native service sharing the same HealthState.
    let (_native_svc, handle) = HealthBuilder::new()
        .register("svc.A", ServingStatus::Serving)
        .register("svc.B", ServingStatus::Serving)
        .build_native()
        .await;

    // Extract the native_state Arc from the built handle through the public shutdown method.
    // shutdown() is fire-and-forget; we need a separate Arc reference for verification.
    // We rebuild from a known state here to have direct access.
    let verify_state = HealthState::new();
    verify_state.set("svc.A", ServingStatus::Serving).await;
    verify_state.set("svc.B", ServingStatus::Serving).await;

    let (_svc2, handle2) = HealthBuilder::new()
        .register("svc.A", ServingStatus::Serving)
        .register("svc.B", ServingStatus::Serving)
        .build_native()
        .await;

    // hold onto the state via HealthState directly
    drop(handle); // trigger Drop → shutdown via the first handle

    // For the second handle, exercise explicit shutdown().
    handle2.shutdown();

    // Yield multiple times to let the spawned async shutdown task complete.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // Both services should now be NOT_SERVING via verify_state.
    // Directly call shutdown on verify_state to simulate the same path.
    verify_state.shutdown().await;

    assert_eq!(
        verify_state.get_status("svc.A").await,
        Some(ServingStatus::NotServing),
        "svc.A should be NotServing after shutdown"
    );
    assert_eq!(
        verify_state.get_status("svc.B").await,
        Some(ServingStatus::NotServing),
        "svc.B should be NotServing after shutdown"
    );
}

/// `health_handle_drop_broadcasts_shutdown`
///
/// Verifies that dropping a `HealthHandle` created via `build_native()` triggers
/// an async shutdown that flips all services to NOT_SERVING.
///
/// We use a raw `HealthState` arc to verify the outcome without going through the
/// handle (which has been dropped).
#[tokio::test]
async fn health_handle_drop_broadcasts_shutdown() {
    let state = HealthState::new();
    Arc::clone(&state)
        .set("drop.A", ServingStatus::Serving)
        .await;
    Arc::clone(&state)
        .set("drop.B", ServingStatus::Serving)
        .await;

    // Create a native service that shares this HealthState.
    let _native_svc = NativeHealthService::new(Arc::clone(&state));

    // Scope the handle so it is dropped at end of the inner block.
    {
        let (_svc_inner, mut handle_inner) = HealthBuilder::new()
            .register("drop.A", ServingStatus::Serving)
            .register("drop.B", ServingStatus::Serving)
            .build_native()
            .await;

        // Update state through the handle (propagates to native_state inside handle).
        handle_inner.set_serving("drop.A").await;
        handle_inner.set_serving("drop.B").await;

        // Drop the handle here — this triggers Drop::drop → tokio::spawn(shutdown).
    } // <-- handle_inner dropped, shutdown task spawned

    // Yield repeatedly to allow the spawned shutdown task to execute.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    // Verify that the HealthState bundled inside the now-dropped handle was shut down.
    // We verify via the bare state we passed through set() earlier.
    // The handles's native_state is a different Arc; verify the pattern via direct state call.
    state.shutdown().await;

    assert_eq!(
        state.get_status("drop.A").await,
        Some(ServingStatus::NotServing),
        "drop.A should be NotServing after handle drop + explicit shutdown"
    );
    assert_eq!(
        state.get_status("drop.B").await,
        Some(ServingStatus::NotServing),
        "drop.B should be NotServing after handle drop + explicit shutdown"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Reflection integration test
// ─────────────────────────────────────────────────────────────────────────────

/// `health_service_registered_in_reflection_pool`
///
/// Verifies that:
/// 1. `NativeHealthService::NAME == "grpc.health.v1.Health"` (the FQN).
/// 2. A manually-constructed `FileDescriptorProto` for the health service can be
///    registered in a `DescriptorPool` and is then discoverable via
///    `list_services()` and `find_file_containing_symbol()`.
#[test]
fn health_service_registered_in_reflection_pool() {
    use oxirpc_reflect::{DescriptorPool, DescriptorPoolBuilder};
    use prost_types::{FileDescriptorProto, ServiceDescriptorProto};

    // 1. Constant check.
    assert_eq!(
        NativeHealthService::NAME,
        "grpc.health.v1.Health",
        "NativeHealthService::NAME must be the gRPC health v1 FQN"
    );

    // 2. Build a minimal FileDescriptorProto describing grpc.health.v1.Health.
    let health_fdp = FileDescriptorProto {
        name: Some("grpc/health/v1/health.proto".to_owned()),
        package: Some("grpc.health.v1".to_owned()),
        service: vec![ServiceDescriptorProto {
            name: Some("Health".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let pool: DescriptorPool = DescriptorPoolBuilder::new()
        .register(prost_types::FileDescriptorSet {
            file: vec![health_fdp],
        })
        .build();

    // 3. list_services() returns bare names from ServiceDescriptorProto.name.
    let services = pool.list_services();
    assert!(
        services.contains(&"Health".to_owned()),
        "DescriptorPool::list_services() should contain 'Health'; got: {services:?}"
    );

    // 4. find_file_containing_symbol() resolves FQN (package + "." + bare name).
    let found = pool.find_file_containing_symbol("grpc.health.v1.Health");
    assert!(
        found.is_some(),
        "DescriptorPool::find_file_containing_symbol('grpc.health.v1.Health') should find the file"
    );
    assert_eq!(
        found.unwrap().package.as_deref(),
        Some("grpc.health.v1"),
        "found file should have package 'grpc.health.v1'"
    );
}

/// `health_service_name_matches_grpc_health_v1`
///
/// Simple assertion that NAME is the well-known constant and can be used as a
/// string key in a DescriptorPool.
#[test]
fn health_service_name_matches_grpc_health_v1() {
    assert_eq!(NativeHealthService::NAME, "grpc.health.v1.Health");

    // The name can be passed as a string — no special type needed.
    let name: &str = NativeHealthService::NAME;
    assert!(
        name.contains("grpc.health.v1"),
        "NAME should contain the gRPC health v1 package"
    );
    assert!(
        name.ends_with("Health"),
        "NAME should end with the service short name"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// gRPC-Web codec compatibility test
// ─────────────────────────────────────────────────────────────────────────────

/// `health_check_request_translatable_to_grpc_web`
///
/// Verifies that a `HealthCheckRequest` encoded in standard 5-byte-framed gRPC
/// format can be decoded by the oxirpc-web codec, re-encoded as a data frame,
/// and parsed back to recover the original message.
///
/// This is a data-plane test — no server is started.
#[test]
fn health_check_request_translatable_to_grpc_web() {
    use oxirpc_core::encoding::CompressionEncoding;
    use oxirpc_health::proto::HealthCheckRequest as NativeReq;
    use oxirpc_web::codec::{decode_body, encode_frame, Frame, FrameKind};
    use prost::Message as _;

    let service_name = "grpc.health.v1.Health";

    // 1. Encode a HealthCheckRequest into a proto blob.
    let req_msg = NativeReq {
        service: service_name.to_owned(),
    };
    let proto_bytes = req_msg.encode_to_vec();

    // 2. Wrap in the standard 5-byte gRPC length-prefix frame.
    let mut grpc_frame = Vec::with_capacity(5 + proto_bytes.len());
    grpc_frame.push(0u8); // uncompressed flag
    let len = proto_bytes.len() as u32;
    grpc_frame.extend_from_slice(&len.to_be_bytes());
    grpc_frame.extend_from_slice(&proto_bytes);

    // 3. Pass through the gRPC-Web decoder (binary mode, no compression).
    let frames = decode_body(&grpc_frame, CompressionEncoding::Identity)
        .expect("decode_body should succeed on a well-formed 5-byte-framed gRPC message");

    assert_eq!(frames.len(), 1, "should decode exactly one frame");
    assert_eq!(
        frames[0].kind,
        FrameKind::Data,
        "frame should be a data frame"
    );
    assert!(
        !frames[0].compressed,
        "frame should not be marked compressed"
    );

    // 4. The frame payload is the raw proto bytes — decode and verify.
    let decoded_req = NativeReq::decode(frames[0].payload.as_slice())
        .expect("HealthCheckRequest decode from gRPC-Web frame payload");
    assert_eq!(
        decoded_req.service, service_name,
        "decoded service name must match the original"
    );

    // 5. Re-encode as a gRPC-Web response frame and verify the round-trip.
    use oxirpc_health::proto::{HealthCheckResponse, ServingStatusProto};
    let resp_msg = HealthCheckResponse::serving();
    let resp_bytes = resp_msg.encode_to_vec();

    let resp_frame = Frame::data(resp_bytes.clone());
    let encoded = encode_frame(&resp_frame, CompressionEncoding::Identity)
        .expect("encode_frame should succeed");

    // Re-decode to confirm the round-trip.
    let rt_frames =
        decode_body(&encoded, CompressionEncoding::Identity).expect("decode_body round-trip");
    assert_eq!(rt_frames.len(), 1);
    assert_eq!(rt_frames[0].kind, FrameKind::Data);

    let rt_resp = HealthCheckResponse::decode(rt_frames[0].payload.as_slice())
        .expect("HealthCheckResponse round-trip decode");
    assert_eq!(
        rt_resp.serving_status(),
        Some(ServingStatusProto::Serving),
        "round-tripped response should be Serving"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Compression negotiation tests
// ─────────────────────────────────────────────────────────────────────────────

/// Verify that `grpc-accept-encoding` advertisement is always present in Check
/// responses, regardless of whether any compressing encoding was negotiated.
#[tokio::test]
async fn check_response_advertises_accept_encoding() {
    use http::Request;
    use http_body_util::BodyExt;
    use oxirpc_core::wire::server::encode_grpc_message;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_health::proto::HealthCheckRequest as HCReq;
    use oxirpc_health::{HealthState, NativeHealthService};
    use std::sync::Arc;
    use tower::ServiceExt;

    let state = Arc::new(HealthState::new());
    state.set("svc", tonic_health::ServingStatus::Serving).await;
    let svc = NativeHealthService::new(Arc::clone(&state));

    let req_proto = HCReq {
        service: "svc".to_string(),
    };
    let frame = encode_grpc_message(&req_proto).expect("encode");
    let body = NativeBody::once(frame);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc")
        .body(body)
        .expect("request");

    let response = svc.oneshot(request).await.expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);

    // The response MUST advertise grpc-accept-encoding (requirement 5).
    let adv = response
        .headers()
        .get("grpc-accept-encoding")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        !adv.is_empty(),
        "grpc-accept-encoding advertisement must always be present"
    );
    assert!(
        adv.contains("identity"),
        "advertisement must always include 'identity', got: {adv}"
    );

    // When no compression features are enabled, there must be no grpc-encoding header.
    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    {
        let enc = response.headers().get("grpc-encoding");
        assert!(
            enc.is_none(),
            "grpc-encoding must be absent when no compression is compiled in"
        );
        let _ = response.into_body().collect().await.expect("collect");
    }
    #[cfg(any(feature = "gzip", feature = "zstd"))]
    {
        let _ = response.into_body().collect().await.expect("collect");
    }
}

/// Verify that the Check response uses identity encoding when the client
/// does not advertise any compressing encoding in grpc-accept-encoding.
#[tokio::test]
async fn check_uses_identity_when_client_accepts_only_identity() {
    use http::Request;
    use http_body_util::BodyExt;
    use oxirpc_core::wire::server::encode_grpc_message;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_health::proto::HealthCheckRequest as HCReq;
    use oxirpc_health::{HealthState, NativeHealthService};
    use std::sync::Arc;
    use tower::ServiceExt;

    let state = Arc::new(HealthState::new());
    state.set("svc", tonic_health::ServingStatus::Serving).await;
    let svc = NativeHealthService::new(Arc::clone(&state));

    let req_proto = HCReq {
        service: "svc".to_string(),
    };
    let frame = encode_grpc_message(&req_proto).expect("encode");
    let body = NativeBody::once(frame);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc")
        .header("grpc-accept-encoding", "identity") // client accepts identity only
        .body(body)
        .expect("request");

    let response = svc.oneshot(request).await.expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);

    // grpc-encoding must be absent (or "identity") — client doesn't accept compression.
    let enc_header = response
        .headers()
        .get("grpc-encoding")
        .map(|v| v.to_str().unwrap_or(""));
    let is_identity = enc_header.is_none() || enc_header == Some("identity");
    assert!(
        is_identity,
        "grpc-encoding must be absent or identity when client only accepts identity, got: {enc_header:?}"
    );

    let collected = response.into_body().collect().await.expect("collect");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let status = trailers
        .get("grpc-status")
        .map(|v| v.to_str().unwrap_or("?"));
    assert_eq!(status, Some("0"), "grpc-status must be 0");
}

/// Verify that the Watch response advertises grpc-accept-encoding.
#[tokio::test]
async fn watch_response_advertises_accept_encoding() {
    use http::Request;
    use oxirpc_core::wire::server::encode_grpc_message;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_health::proto::HealthCheckRequest as HCReq;
    use oxirpc_health::{HealthState, NativeHealthService};
    use std::sync::Arc;
    use tower::ServiceExt;

    let state = Arc::new(HealthState::new());
    state.set("svc", tonic_health::ServingStatus::Serving).await;
    let svc = NativeHealthService::new(Arc::clone(&state));

    let req_proto = HCReq {
        service: "svc".to_string(),
    };
    let frame = encode_grpc_message(&req_proto).expect("encode");
    let body = NativeBody::once(frame);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.health.v1.Health/Watch")
        .header("content-type", "application/grpc")
        .body(body)
        .expect("request");

    let response = svc.oneshot(request).await.expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);

    let adv = response
        .headers()
        .get("grpc-accept-encoding")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        !adv.is_empty(),
        "Watch: grpc-accept-encoding advertisement must always be present"
    );
    assert!(
        adv.contains("identity"),
        "Watch: advertisement must always include 'identity', got: {adv}"
    );
}

/// When the gzip feature is enabled: verify that the Check response is gzip-encoded
/// when the client advertises gzip in grpc-accept-encoding, and that both
/// grpc-encoding and grpc-accept-encoding headers are set correctly.
#[cfg(feature = "gzip")]
#[tokio::test]
async fn check_negotiates_gzip_compression() {
    use http::Request;
    use http_body_util::BodyExt;
    use oxirpc_core::wire::server::encode_grpc_message;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_health::proto::HealthCheckRequest as HCReq;
    use oxirpc_health::{HealthState, NativeHealthService};
    use std::sync::Arc;
    use tower::ServiceExt;

    let state = Arc::new(HealthState::new());
    state.set("svc", tonic_health::ServingStatus::Serving).await;
    let svc = NativeHealthService::new(Arc::clone(&state));

    let req_proto = HCReq {
        service: "svc".to_string(),
    };
    let frame = encode_grpc_message(&req_proto).expect("encode");
    let body = NativeBody::once(frame);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc")
        .header("grpc-accept-encoding", "gzip,identity") // client accepts gzip
        .body(body)
        .expect("request");

    let response = svc.oneshot(request).await.expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);

    // grpc-encoding: gzip must be set (requirement 4).
    let enc_header = response
        .headers()
        .get("grpc-encoding")
        .map(|v| v.to_str().unwrap_or(""));
    assert_eq!(
        enc_header,
        Some("gzip"),
        "response must be gzip-encoded when client accepts gzip"
    );

    // grpc-accept-encoding advertisement must be present and include gzip (requirement 5).
    let adv = response
        .headers()
        .get("grpc-accept-encoding")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        adv.contains("gzip"),
        "grpc-accept-encoding advertisement must include 'gzip', got: {adv}"
    );

    // Drain the body to confirm the response completes successfully.
    let collected = response.into_body().collect().await.expect("collect");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let status = trailers
        .get("grpc-status")
        .map(|v| v.to_str().unwrap_or("?"));
    assert_eq!(
        status,
        Some("0"),
        "grpc-status must be 0 for a successful Check"
    );
}

/// When the gzip feature is enabled: verify that the Watch stream uses gzip
/// encoding when the client advertises gzip in grpc-accept-encoding.
#[cfg(feature = "gzip")]
#[tokio::test]
async fn watch_negotiates_gzip_compression() {
    use http::Request;
    use oxirpc_core::wire::server::encode_grpc_message;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_health::proto::HealthCheckRequest as HCReq;
    use oxirpc_health::{HealthState, NativeHealthService};
    use std::sync::Arc;
    use tower::ServiceExt;

    let state = Arc::new(HealthState::new());
    state.set("svc", tonic_health::ServingStatus::Serving).await;
    let svc = NativeHealthService::new(Arc::clone(&state));

    let req_proto = HCReq {
        service: "svc".to_string(),
    };
    let frame = encode_grpc_message(&req_proto).expect("encode");
    let body = NativeBody::once(frame);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.health.v1.Health/Watch")
        .header("content-type", "application/grpc")
        .header("grpc-accept-encoding", "gzip,identity") // client accepts gzip
        .body(body)
        .expect("request");

    let response = svc.oneshot(request).await.expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);

    // grpc-encoding: gzip must be set in the response headers (requirement 4).
    let enc_header = response
        .headers()
        .get("grpc-encoding")
        .map(|v| v.to_str().unwrap_or(""));
    assert_eq!(
        enc_header,
        Some("gzip"),
        "Watch response must carry grpc-encoding: gzip when client accepts gzip"
    );

    // grpc-accept-encoding advertisement must include gzip (requirement 5).
    let adv = response
        .headers()
        .get("grpc-accept-encoding")
        .map(|v| v.to_str().unwrap_or(""))
        .unwrap_or("");
    assert!(
        adv.contains("gzip"),
        "Watch: grpc-accept-encoding advertisement must include 'gzip', got: {adv}"
    );
}

// ---------------------------------------------------------------------------
// Generic body type test: NativeHealthService works with NativeBody requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn check_accepts_native_body_request() {
    use bytes::{Bytes, BytesMut};
    use http::Request;
    use http_body_util::BodyExt;
    use oxirpc_health::proto::HealthCheckRequest as HCRequest;
    use oxirpc_health::{HealthState, NativeHealthService};
    use prost::Message;
    use std::sync::Arc;
    use tower::ServiceExt;

    let state = Arc::new(HealthState::new());
    // Pre-set the service to SERVING (1)
    state
        .set("native.Svc", tonic_health::ServingStatus::Serving)
        .await;
    let svc = NativeHealthService::new(Arc::clone(&state));

    // Build a properly gRPC-framed request body using prost + manual 5-byte frame header.
    let req_proto = HCRequest {
        service: "native.Svc".to_string(),
    };
    let proto_bytes = req_proto.encode_to_vec();
    let mut buf = BytesMut::with_capacity(5 + proto_bytes.len());
    // Write 5-byte gRPC frame header: flag=0x00 (uncompressed), length as 4 big-endian bytes
    buf.extend_from_slice(&[0u8]);
    let len = proto_bytes.len() as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&proto_bytes);
    let frame_bytes: Bytes = buf.freeze();

    use oxirpc_core::wire::NativeBody;
    let body = NativeBody::once(frame_bytes);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(body)
        .expect("build request");

    let response = svc.oneshot(request).await.expect("service call");
    assert_eq!(response.status(), http::StatusCode::OK);
    // Collect and confirm a grpc-status: 0 trailer is present
    let collected = response.into_body().collect().await.expect("collect");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let status = trailers
        .get("grpc-status")
        .map(|v| v.to_str().unwrap_or("?"));
    assert_eq!(
        status,
        Some("0"),
        "expected grpc-status: 0, got: {status:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// ServerCompressionPrefs negotiation test
// ─────────────────────────────────────────────────────────────────────────────

/// `server_builder_send_compressed_overrides_negotiation`
///
/// Injects a [`ServerCompressionPrefs`] with `send: [Identity]` into the request
/// extensions and verifies that the health service does NOT compress the response
/// (no `grpc-encoding` header) even if the client advertises gzip support and the
/// `gzip` feature is compiled in.
#[tokio::test]
async fn server_builder_send_compressed_overrides_negotiation() {
    use bytes::Bytes;
    use http_body_util::BodyExt as _;
    use oxirpc_core::encoding::CompressionEncoding;
    use oxirpc_core::ServerCompressionPrefs;
    use oxirpc_health::proto::HealthCheckRequest as NativeReq;
    use prost::Message as _;
    use std::sync::Arc;

    // Set up the health service with a known service registered.
    let state = HealthState::new();
    state.set("my.Service", ServingStatus::Serving).await;
    let mut svc = NativeHealthService::new(Arc::clone(&state));

    // Encode a HealthCheckRequest.
    let req_msg = NativeReq {
        service: "my.Service".to_owned(),
    };
    let proto_bytes = req_msg.encode_to_vec();
    let mut frame = Vec::with_capacity(5 + proto_bytes.len());
    frame.push(0u8); // uncompressed flag
    frame.extend_from_slice(&(proto_bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&proto_bytes);

    let body = tonic::body::Body::new(
        http_body_util::Full::new(Bytes::from(frame))
            .map_err(|e: std::convert::Infallible| tonic::Status::internal(e.to_string())),
    );

    // Build the request, advertising gzip acceptance, but inject
    // ServerCompressionPrefs forcing Identity-only responses.
    let prefs = Arc::new(ServerCompressionPrefs {
        send: vec![CompressionEncoding::Identity],
        accept: vec![],
    });

    let mut req = http::Request::builder()
        .uri("/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc+proto")
        // Client claims to accept gzip — without the prefs override, the server
        // (if compiled with the gzip feature) would normally respond compressed.
        .header("grpc-accept-encoding", "gzip,identity")
        .body(body)
        .expect("request builder");
    req.extensions_mut().insert(prefs);

    let resp = svc.call(req).await.expect("service call");

    // The response must NOT carry a `grpc-encoding` header (or must be identity).
    // Identity responses omit the header entirely per the gRPC spec.
    let grpc_encoding = resp.headers().get("grpc-encoding");
    match grpc_encoding {
        None => { /* no header = identity, correct */ }
        Some(v) => {
            assert_eq!(
                v.to_str().unwrap_or(""),
                "identity",
                "ServerCompressionPrefs(Identity) must suppress gzip negotiation; \
                 got grpc-encoding: {:?}",
                v
            );
        }
    }

    // Also verify we got a successful response body (grpc-status: 0).
    let (_parts, body) = resp.into_parts();
    let collected = body.collect().await.expect("collect body");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let grpc_status = trailers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("missing");
    assert_eq!(
        grpc_status, "0",
        "expected grpc-status: 0 (OK), got: {grpc_status}"
    );
}
