//! Integration and unit tests for the xDS ADS streaming client (Slice 4).
//!
//! These tests cover:
//! 1. Proto round-trip for `DiscoveryRequest`.
//! 2. Manual decoding of a hand-crafted `ClusterLoadAssignment` byte string.
//! 3. State machine: initial request has empty version_info and nonce.
//! 4. State machine: ACK advances version and nonce.
//! 5. State machine: NACK preserves old version and sets error_detail.
//! 6. Backoff resets to base after `reset()`.
//! 7. `AdsClient::new` constructs without panicking.

use prost::Message as _;

use oxirpc_client::xds::ads::{initial_request, process_response, TypeState};
use oxirpc_client::xds::backoff::Backoff;
use oxirpc_client::xds::proto::{
    Address, ClusterLoadAssignment, DiscoveryRequest, DiscoveryResponse, Endpoint, GoogleAny,
    LbEndpoint, LocalityLbEndpoints, Node, SocketAddress, UInt32Value, CDS_TYPE_URL, EDS_TYPE_URL,
};
use oxirpc_client::xds::xds_resolver;

// ── Test 1: proto round-trip DiscoveryRequest ─────────────────────────────────

#[test]
fn proto_roundtrip_discovery_request() {
    let req = DiscoveryRequest {
        version_info: "v42".to_owned(),
        node: Some(Node {
            id: "test-node".to_owned(),
            cluster: "test-cluster".to_owned(),
            user_agent_name: "oxirpc".to_owned(),
            user_agent_version: "0.1.0".to_owned(),
        }),
        resource_names: vec!["cluster-a".to_owned(), "cluster-b".to_owned()],
        type_url: CDS_TYPE_URL.to_owned(),
        response_nonce: "nonce-1".to_owned(),
        error_detail: None,
    };

    // Encode to bytes.
    let mut buf = Vec::with_capacity(req.encoded_len());
    req.encode(&mut buf).expect("encode should succeed");

    // Decode from bytes.
    let decoded = DiscoveryRequest::decode(buf.as_slice()).expect("decode should succeed");

    assert_eq!(decoded.version_info, "v42");
    assert_eq!(decoded.response_nonce, "nonce-1");
    assert_eq!(decoded.resource_names, vec!["cluster-a", "cluster-b"]);
    assert_eq!(decoded.type_url, CDS_TYPE_URL);

    let node = decoded.node.expect("node should be present");
    assert_eq!(node.id, "test-node");
    assert_eq!(node.cluster, "test-cluster");
    assert_eq!(node.user_agent_name, "oxirpc");
    assert_eq!(node.user_agent_version, "0.1.0");
}

// ── Test 2: decode hand-crafted ClusterLoadAssignment ────────────────────────

#[test]
fn proto_decode_cluster_load_assignment() {
    // Construct a CLA with one endpoint and encode it.
    let cla = ClusterLoadAssignment {
        cluster_name: "my-service".to_owned(),
        endpoints: vec![LocalityLbEndpoints {
            lb_endpoints: vec![LbEndpoint {
                endpoint: Some(Endpoint {
                    address: Some(Address {
                        socket_address: Some(SocketAddress {
                            address: "10.0.0.1".to_owned(),
                            port_value: 8080,
                        }),
                    }),
                }),
                health_status: 1, // HEALTHY
                load_balancing_weight: Some(UInt32Value { value: 5 }),
            }],
            load_balancing_weight: Some(UInt32Value { value: 1 }),
        }],
    };

    let mut encoded = Vec::with_capacity(cla.encoded_len());
    cla.encode(&mut encoded).expect("encode should succeed");

    // Decode back.
    let decoded = ClusterLoadAssignment::decode(encoded.as_slice()).expect("decode should succeed");

    assert_eq!(decoded.cluster_name, "my-service");
    assert_eq!(decoded.endpoints.len(), 1);

    let locality = &decoded.endpoints[0];
    assert_eq!(locality.lb_endpoints.len(), 1);

    let ep = &locality.lb_endpoints[0];
    assert_eq!(ep.health_status, 1);
    assert_eq!(ep.load_balancing_weight.as_ref().map(|w| w.value), Some(5));

    let sock = ep
        .endpoint
        .as_ref()
        .and_then(|e| e.address.as_ref())
        .and_then(|a| a.socket_address.as_ref())
        .expect("socket_address should be present");

    assert_eq!(sock.address, "10.0.0.1");
    assert_eq!(sock.port_value, 8080);
}

// ── Test 3: initial request has empty version and nonce ───────────────────────

#[test]
fn state_machine_initial_request_empty_version_nonce() {
    let names = vec!["cluster-x".to_owned()];
    let node = Node {
        id: "n1".to_owned(),
        ..Default::default()
    };
    let req = initial_request(CDS_TYPE_URL, &names, node);

    assert_eq!(req.version_info, "", "initial version_info must be empty");
    assert_eq!(req.response_nonce, "", "initial nonce must be empty");
    assert_eq!(req.type_url, CDS_TYPE_URL);
    assert_eq!(req.resource_names, vec!["cluster-x"]);
    assert!(
        req.error_detail.is_none(),
        "no error_detail on initial request"
    );
}

// ── Test 4: ACK advances version and nonce ───────────────────────────────────

#[test]
fn state_machine_ack_advances_version() {
    let (_, watcher) = xds_resolver();

    // Build a valid EDS DiscoveryResponse.
    let cla = ClusterLoadAssignment {
        cluster_name: "svc-a".to_owned(),
        endpoints: vec![],
    };
    let mut cla_bytes = Vec::with_capacity(cla.encoded_len());
    cla.encode(&mut cla_bytes).unwrap();

    let resp = DiscoveryResponse {
        version_info: "v1".to_owned(),
        resources: vec![GoogleAny {
            type_url: EDS_TYPE_URL.to_owned(),
            value: cla_bytes,
        }],
        type_url: EDS_TYPE_URL.to_owned(),
        nonce: "n1".to_owned(),
    };

    let mut resp_bytes = Vec::with_capacity(resp.encoded_len());
    resp.encode(&mut resp_bytes).unwrap();

    let mut cds_state = TypeState::new(CDS_TYPE_URL, vec!["svc-a".to_owned()]);
    let mut eds_state = TypeState::new(EDS_TYPE_URL, vec!["svc-a".to_owned()]);

    let outcome = process_response(&resp_bytes, &mut cds_state, &mut eds_state, &watcher);

    // The ACK request should carry the new version and nonce.
    match outcome {
        oxirpc_client::xds::ads::ResponseOutcome::AckEds(ack) => {
            assert_eq!(ack.version_info, "v1", "ACK must carry the new version");
            assert_eq!(ack.response_nonce, "n1", "ACK must carry the new nonce");
            assert!(
                ack.error_detail.is_none(),
                "ACK must not carry error_detail"
            );
            // ACK must echo back the subscribed resource names.
            assert_eq!(
                ack.resource_names,
                vec!["svc-a"],
                "ACK must echo back resource_names"
            );
        }
        _other => panic!("expected AckEds, got something else"),
    }

    // EDS state should have been updated.
    assert_eq!(eds_state.version_info, "v1");
    assert_eq!(eds_state.nonce, "n1");
}

// ── Test 5: NACK preserves old version, sets error_detail ────────────────────

#[test]
fn state_machine_nack_preserves_old_version() {
    let (_, watcher) = xds_resolver();

    // Seed EDS state with an existing version.
    let mut cds_state = TypeState::new(CDS_TYPE_URL, vec!["svc-b".to_owned()]);
    let mut eds_state = TypeState::new(EDS_TYPE_URL, vec!["svc-b".to_owned()]);
    eds_state.version_info = "v0".to_owned();
    eds_state.nonce = "n0".to_owned();

    // Build a corrupted EDS response (non-proto bytes in the Any payload).
    let resp = DiscoveryResponse {
        version_info: "v1".to_owned(),
        resources: vec![GoogleAny {
            type_url: EDS_TYPE_URL.to_owned(),
            value: vec![0xFF, 0xFE, 0xFD, 0xFC], // garbage bytes
        }],
        type_url: EDS_TYPE_URL.to_owned(),
        nonce: "n1".to_owned(),
    };

    let mut resp_bytes = Vec::with_capacity(resp.encoded_len());
    resp.encode(&mut resp_bytes).unwrap();

    let outcome = process_response(&resp_bytes, &mut cds_state, &mut eds_state, &watcher);

    match outcome {
        oxirpc_client::xds::ads::ResponseOutcome::NackEds(nack) => {
            assert_eq!(
                nack.version_info, "v0",
                "NACK must preserve the old version"
            );
            assert_eq!(nack.response_nonce, "n1", "NACK must echo the new nonce");
            assert!(nack.error_detail.is_some(), "NACK must carry error_detail");
            let detail = nack.error_detail.unwrap();
            assert!(
                !detail.message.is_empty(),
                "error_detail.message must not be empty"
            );
            // NACK must also echo back the subscribed resource names.
            assert_eq!(
                nack.resource_names,
                vec!["svc-b"],
                "NACK must echo back resource_names"
            );
        }
        _other => panic!("expected NackEds, got something else"),
    }

    // EDS state version must NOT have been updated on NACK.
    assert_eq!(
        eds_state.version_info, "v0",
        "state must retain old version"
    );
}

// ── Test 6: backoff resets after ACK ─────────────────────────────────────────

#[test]
fn backoff_resets_after_ack() {
    let mut b = Backoff::default_grpc();

    // Advance several attempts.
    let d0 = b.next_duration();
    let d1 = b.next_duration();
    let d2 = b.next_duration();

    // Delays should be non-decreasing (on average) as attempts grow.
    // We only assert the first is non-zero.
    assert!(d0.as_millis() > 0, "first delay must be > 0 ms");
    // d1 and d2 are typically larger but jitter can theoretically flip them,
    // so we only assert they're positive.
    assert!(d1.as_millis() > 0);
    assert!(d2.as_millis() > 0);

    // Reset, then the next delay should match attempt-0 range again.
    b.reset();
    let d_after_reset = b.next_duration();

    // After reset, delay should be in the rough base range (< 300 ms with jitter).
    // Exact value depends on jitter; just check it is <= the max.
    assert!(
        d_after_reset.as_millis() < 300,
        "after reset, delay should be near base (100 ms ± jitter), got {:?}",
        d_after_reset
    );
}

// ── Test 7: AdsClient builds without panic ────────────────────────────────────

#[tokio::test]
async fn ads_client_builds_without_panic() {
    use oxirpc_client::balance::StaticResolver;
    use oxirpc_client::native_channel::NativeChannelBuilder;
    use oxirpc_client::xds::ads::{AdsClient, AdsConfig};
    use oxirpc_client::xds::{proto::Node, xds_resolver};

    // Build a NativeChannel with a no-endpoint static resolver.
    // We never actually call it, so no real server is needed.
    let resolver = StaticResolver::new(vec![]);
    let channel = NativeChannelBuilder::new()
        .resolver(resolver)
        .build()
        .await
        .expect("channel build should not fail with empty resolver");

    let (_resolver, watcher) = xds_resolver();

    let cfg = AdsConfig {
        initial_resource_names: vec!["test-cluster".to_owned()],
        node: Node {
            id: "test-node-id".to_owned(),
            cluster: "my-cluster".to_owned(),
            user_agent_name: "oxirpc-test".to_owned(),
            user_agent_version: "0.0.1".to_owned(),
        },
        backoff: Backoff::default_grpc(),
    };

    // Just constructing the client should not panic.
    let _client = AdsClient::new(channel, watcher, cfg);
}
