//! Integration tests for the xDS resolver module.

use oxirpc_client::balance::{Endpoint, Resolver};
use oxirpc_client::xds::{
    xds_resolver, ClusterLoadAssignment, HealthStatus, LbEndpoint, LocalityLbEndpoints,
    SocketAddress,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_endpoint(host: &str, port: u32, status: HealthStatus, weight: u32) -> LbEndpoint {
    LbEndpoint {
        address: SocketAddress {
            address: host.to_string(),
            port,
        },
        health_status: status,
        load_balancing_weight: weight,
    }
}

fn make_cla(cluster: &str, lb_eps: Vec<LbEndpoint>) -> ClusterLoadAssignment {
    ClusterLoadAssignment {
        cluster_name: cluster.to_string(),
        endpoints: vec![LocalityLbEndpoints {
            lb_endpoints: lb_eps,
            load_balancing_weight: 1,
        }],
    }
}

// ── Test 1: initial resolve returns empty Vec ─────────────────────────────────

#[tokio::test]
async fn xds_resolver_returns_initial_endpoints() {
    let (resolver, _watcher) = xds_resolver();
    let endpoints = resolver.resolve().await.expect("resolve should not fail");
    assert!(
        endpoints.is_empty(),
        "initial resolve should return empty Vec"
    );
}

// ── Test 2: watcher update propagates to resolver ────────────────────────────

#[tokio::test]
async fn xds_watcher_update_propagates_to_resolver() {
    let (resolver, watcher) = xds_resolver();

    let uri: http::Uri = "http://10.0.0.1:50051".parse().expect("valid URI");
    watcher.update_endpoints(vec![Endpoint::new(uri.clone())]);

    let endpoints = resolver.resolve().await.expect("resolve should not fail");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].uri, uri);
}

// ── Test 3: CLA conversion produces correct URI ───────────────────────────────

#[tokio::test]
async fn xds_resolver_converts_cla_to_endpoints() {
    let (resolver, watcher) = xds_resolver();

    let cla = make_cla(
        "my-service",
        vec![make_endpoint("10.0.0.1", 50051, HealthStatus::Healthy, 2)],
    );
    watcher.update(&cla);

    let endpoints = resolver.resolve().await.expect("resolve should not fail");
    assert_eq!(endpoints.len(), 1);
    // http::Uri normalizes bare URIs by appending a "/", so compare the authority part.
    assert_eq!(
        endpoints[0].uri.authority().map(|a| a.as_str()),
        Some("10.0.0.1:50051"),
        "URI authority should match the socket address"
    );
    assert_eq!(endpoints[0].weight, 2, "weight should propagate from CLA");
}

// ── Test 4: unhealthy / draining endpoints are filtered ──────────────────────

#[tokio::test]
async fn xds_health_status_filters_unhealthy() {
    let (resolver, watcher) = xds_resolver();

    let cla = make_cla(
        "filtered-service",
        vec![
            make_endpoint("10.0.0.1", 50051, HealthStatus::Healthy, 1),
            make_endpoint("10.0.0.2", 50052, HealthStatus::Unhealthy, 1),
            make_endpoint("10.0.0.3", 50053, HealthStatus::Draining, 1),
        ],
    );
    watcher.update(&cla);

    let endpoints = resolver.resolve().await.expect("resolve should not fail");
    assert_eq!(
        endpoints.len(),
        1,
        "only Healthy endpoint should pass the filter"
    );
    assert_eq!(
        endpoints[0].uri.authority().map(|a| a.as_str()),
        Some("10.0.0.1:50051"),
        "the surviving endpoint should be the healthy one"
    );
}

// ── Test 5: empty CLA returns empty endpoint list ────────────────────────────

#[tokio::test]
async fn xds_empty_cla_returns_empty_endpoints() {
    let (resolver, watcher) = xds_resolver();

    let cla = ClusterLoadAssignment {
        cluster_name: "empty-service".to_string(),
        endpoints: vec![],
    };
    watcher.update(&cla);

    let endpoints = resolver.resolve().await.expect("resolve should not fail");
    assert!(
        endpoints.is_empty(),
        "empty CLA should produce empty endpoint list"
    );
}

// ── Test 6: XdsResolver feeds endpoints into ChannelPool ─────────────────────

#[tokio::test]
async fn xds_resolver_works_with_channel_pool() {
    use oxirpc_client::balance::RoundRobin;
    use oxirpc_client::ChannelPool;

    let (resolver, watcher) = xds_resolver();

    let cla = make_cla(
        "pool-service",
        vec![
            make_endpoint("127.0.0.1", 50051, HealthStatus::Healthy, 1),
            make_endpoint("127.0.0.1", 50052, HealthStatus::Healthy, 1),
        ],
    );
    watcher.update(&cla);

    // Resolve endpoints and convert each to a tonic transport endpoint.
    let endpoints = resolver.resolve().await.expect("resolve should not fail");
    let tonic_endpoints: Vec<tonic::transport::Endpoint> = endpoints
        .iter()
        .filter_map(|ep| tonic::transport::Endpoint::from_shared(ep.uri.to_string()).ok())
        .collect();

    let pool = ChannelPool::from_endpoints(tonic_endpoints, RoundRobin::new())
        .await
        .expect("pool creation should not fail");

    assert_eq!(
        pool.len(),
        2,
        "pool should contain one channel per resolved endpoint"
    );
}
