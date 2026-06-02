//! Tests for bidirectional conversion helpers between OxiRpcError, tonic::Status,
//! StatusCode, and GrpcStatusCode (Round 6 Slice 4).

use oxirpc_core::h2::GrpcStatusCode;
use oxirpc_core::status::StatusCode;
use oxirpc_core::OxiRpcError;
use tonic::Code;

// ─── OxiRpcError → tonic::Status ─────────────────────────────────────────────

#[test]
fn oxirpc_error_to_tonic_status_transport_maps_to_unavailable() {
    let e = OxiRpcError::Transport("bad".to_owned());
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::Unavailable);
    assert_eq!(s.message(), "bad");
}

#[test]
fn oxirpc_error_to_tonic_status_timeout_maps_to_deadline_exceeded() {
    let e = OxiRpcError::Timeout;
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::DeadlineExceeded);
}

#[test]
fn oxirpc_error_to_tonic_status_cancelled_maps_correctly() {
    let e = OxiRpcError::Cancelled;
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::Cancelled);
}

#[test]
fn oxirpc_error_to_tonic_status_passthrough_preserves_status() {
    let original = tonic::Status::not_found("not here");
    let e = OxiRpcError::Status(original.clone());
    let s2: tonic::Status = e.into();
    assert_eq!(s2.code(), original.code());
    assert_eq!(s2.message(), original.message());
}

#[test]
fn oxirpc_error_to_tonic_status_build_maps_to_internal() {
    let e = OxiRpcError::Build("build failure".to_owned());
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::Internal);
}

#[test]
fn oxirpc_error_to_tonic_status_tls_maps_to_unavailable() {
    let e = OxiRpcError::Tls("tls failure".to_owned());
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::Unavailable);
}

#[test]
fn oxirpc_error_to_tonic_status_compression_maps_to_internal() {
    let e = OxiRpcError::Compression("compress failure".to_owned());
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::Internal);
}

#[test]
fn oxirpc_error_to_tonic_status_proto_maps_to_internal() {
    let e = OxiRpcError::Proto("decode error".to_owned());
    let s: tonic::Status = e.into();
    assert_eq!(s.code(), Code::Internal);
}

// ─── OxiRpcError::from_status_code ───────────────────────────────────────────

#[test]
fn oxirpc_error_from_status_code_roundtrips() {
    let e = OxiRpcError::from_status_code(StatusCode::NotFound, "not here");
    if let OxiRpcError::Status(s) = e {
        assert_eq!(s.code(), Code::NotFound);
        assert_eq!(s.message(), "not here");
    } else {
        panic!("expected OxiRpcError::Status variant");
    }
}

#[test]
fn from_status_code_constructs_correct_error() {
    let e = OxiRpcError::from_status_code(StatusCode::Internal, "oops");
    if let OxiRpcError::Status(s) = &e {
        assert_eq!(s.code(), Code::Internal);
        assert_eq!(s.message(), "oops");
    } else {
        panic!("expected OxiRpcError::Status variant");
    }
}

// ─── StatusCode::from_i32 / as_i32 ────────────────────────────────────────────

#[test]
fn status_code_from_i32_all_17_codes() {
    for i in 0..=16 {
        assert!(
            StatusCode::from_i32(i).is_some(),
            "from_i32({i}) should return Some"
        );
    }
}

#[test]
fn status_code_from_i32_out_of_range_returns_none() {
    assert!(StatusCode::from_i32(17).is_none());
    assert!(StatusCode::from_i32(-1).is_none());
    assert!(StatusCode::from_i32(i32::MAX).is_none());
}

#[test]
fn status_code_as_i32_matches_wire_values() {
    for code in StatusCode::ALL {
        assert_eq!(code.as_i32(), code as i32, "as_i32() mismatch for {code:?}");
    }
}

// ─── GrpcStatusCode ↔ StatusCode ─────────────────────────────────────────────

#[test]
fn grpc_status_code_to_status_code_roundtrip() {
    for n in 0u32..=16 {
        let grpc = GrpcStatusCode::from_u32(n).expect("valid n");
        let sc: StatusCode = grpc.into();
        assert_eq!(
            sc as i32, n as i32,
            "GrpcStatusCode({n}) -> StatusCode mismatch"
        );
    }
}

#[test]
fn status_code_to_grpc_status_code_roundtrip() {
    for sc in StatusCode::ALL {
        let grpc: GrpcStatusCode = sc.into();
        assert_eq!(
            grpc as u32, sc as i32 as u32,
            "StatusCode({sc:?}) -> GrpcStatusCode mismatch"
        );
    }
}

// ─── Display helpers ──────────────────────────────────────────────────────────

#[test]
fn oxirpc_error_display_includes_msg() {
    let msg = "network fail";
    let s = OxiRpcError::Transport(msg.to_owned()).to_string();
    assert!(s.contains(msg), "display '{s}' should contain '{msg}'");
}
