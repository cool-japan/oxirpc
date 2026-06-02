//! ADS (Aggregated Discovery Service) streaming client for xDS v3.
//!
//! [`AdsClient`] connects to an Envoy-compatible control plane over a
//! bidirectional gRPC stream and drives the CDS → EDS resource discovery
//! loop.  It uses [`crate::NativeChannel`] for the underlying HTTP/2
//! transport so no tonic dependency is required in the streaming path.
//!
//! # State machine
//!
//! The pure logic lives in the `state` functions to keep them unit-testable
//! without any IO.  [`AdsClient::run`] drives the IO loop and delegates each
//! decoded `DiscoveryResponse` to [`process_response`].
//!
//! # ADS gRPC path
//!
//! ```text
//! POST /envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources
//! content-type: application/grpc+proto
//! te: trailers
//! ```

use std::collections::HashSet;

use bytes::{BufMut, Bytes, BytesMut};
use http_body::Body as _;
use prost::Message as _;

use oxirpc_core::wire::frame as wire_frame;
use oxirpc_core::OxiRpcError;

use crate::native_channel::body::body_channel;
use crate::xds::resolver::XdsWatcher;
use crate::xds::types::ClusterLoadAssignment as XdsCla;
use crate::NativeChannel;

use super::backoff::Backoff;
use super::proto::{
    Cluster, ClusterLoadAssignment as ProtoCla, DiscoveryRequest, DiscoveryResponse,
    GoogleRpcStatus, Node, CDS_TYPE_URL, EDS_TYPE_URL,
};

/// The ADS gRPC RPC path.
const ADS_PATH: &str =
    "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources";

// ── AdsConfig ─────────────────────────────────────────────────────────────────

/// Configuration for an [`AdsClient`] session.
pub struct AdsConfig {
    /// CDS cluster names this client subscribes to initially.
    pub initial_resource_names: Vec<String>,
    /// Node identity sent in every `DiscoveryRequest`.
    pub node: Node,
    /// Reconnect backoff policy.
    pub backoff: Backoff,
}

// ── AdsClient ─────────────────────────────────────────────────────────────────

/// An xDS ADS streaming client.
///
/// Connect to a control plane by calling [`AdsClient::run`] with a shutdown
/// future.  The client reconnects with exponential backoff on stream errors.
pub struct AdsClient {
    channel: NativeChannel,
    watcher: XdsWatcher,
    cfg: AdsConfig,
}

impl AdsClient {
    /// Construct a new client with the given channel, watcher, and config.
    pub fn new(channel: NativeChannel, watcher: XdsWatcher, cfg: AdsConfig) -> Self {
        Self {
            channel,
            watcher,
            cfg,
        }
    }

    /// Run the ADS client loop until `shutdown` resolves.
    ///
    /// Establishes a bidirectional gRPC stream, runs the CDS→EDS discovery
    /// state machine, and reconnects with backoff on any transport or decode
    /// error.
    pub async fn run(
        mut self,
        shutdown: impl std::future::Future<Output = ()> + Send,
    ) -> Result<(), OxiRpcError> {
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    return Ok(());
                }
                result = self.run_one_session() => {
                    match result {
                        Ok(()) => {
                            // Clean server-initiated close; back off then retry.
                        }
                        Err(e) => {
                            // Log the session error; reconnect with backoff.
                            let _ = e;
                        }
                    }
                    // Reset backoff only after the first successful ACK
                    // (run_one_session calls backoff.reset internally).
                    let delay = self.cfg.backoff.next_duration();
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => return Ok(()),
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    /// Establish one bidirectional ADS session and run until the stream closes.
    async fn run_one_session(&mut self) -> Result<(), OxiRpcError> {
        // Build the streaming HTTP/2 request.
        let (body_tx, req_body) = body_channel(32);

        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(ADS_PATH)
            .version(http::Version::HTTP_2)
            .header(http::header::CONTENT_TYPE, "application/grpc+proto")
            .header("te", "trailers")
            .body(req_body)
            .map_err(|e| OxiRpcError::Transport(e.to_string()))?;

        // Send initial CDS request.
        let initial = initial_request(
            CDS_TYPE_URL,
            &self.cfg.initial_resource_names,
            self.cfg.node.clone(),
        );
        send_request(&body_tx, &initial).await?;

        // Fire the call.
        let resp = self.channel.call(req).await?;
        let (_parts, resp_body) = resp.into_parts();

        // Per-session state.
        let mut cds_state = TypeState::new(CDS_TYPE_URL, self.cfg.initial_resource_names.clone());
        let mut eds_state = TypeState::new(EDS_TYPE_URL, Vec::new());
        let mut buf = BytesMut::new();

        // Pin the response body so we can poll it with poll_frame.
        tokio::pin!(resp_body);

        loop {
            // Poll the next data frame from the response body.
            let poll_result = futures_util::future::poll_fn(|cx| {
                use std::pin::Pin;
                Pin::new(&mut resp_body).poll_frame(cx)
            })
            .await;

            match poll_result {
                None => {
                    // Server closed the stream cleanly.
                    return Ok(());
                }
                Some(Err(e)) => {
                    return Err(e);
                }
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => {
                        buf.put_slice(&data);
                    }
                    Err(_) => {
                        // Trailer or non-data frame — stream is ending.
                        return Ok(());
                    }
                },
            }

            // Decode as many complete gRPC frames as available.
            loop {
                match try_decode_grpc_frame(&mut buf) {
                    None => break,
                    Some(payload) => {
                        let outcome = process_response(
                            &payload,
                            &mut cds_state,
                            &mut eds_state,
                            &self.watcher,
                        );

                        match outcome {
                            ResponseOutcome::AckCds(ack) | ResponseOutcome::AckEds(ack) => {
                                send_request(&body_tx, &ack).await?;
                                self.cfg.backoff.reset();
                            }
                            ResponseOutcome::NackCds(nack) | ResponseOutcome::NackEds(nack) => {
                                send_request(&body_tx, &nack).await?;
                            }
                            ResponseOutcome::NewEdsSubscription(ack, subscriptions) => {
                                // Update the EDS subscription list.
                                eds_state.resource_names = subscriptions
                                    .into_iter()
                                    .collect::<HashSet<_>>()
                                    .into_iter()
                                    .collect();
                                send_request(&body_tx, &ack).await?;
                                // Send EDS subscription request.
                                let eds_req = initial_request(
                                    EDS_TYPE_URL,
                                    &eds_state.resource_names,
                                    Node::default(),
                                );
                                send_request(&body_tx, &eds_req).await?;
                                self.cfg.backoff.reset();
                            }
                            ResponseOutcome::Unknown => {
                                // Ignore unknown type URLs gracefully.
                            }
                        }
                    }
                }
            }
        }
    }
}

// ── TypeState ─────────────────────────────────────────────────────────────────

/// Per-type-URL session state.
pub struct TypeState {
    /// The xDS type URL this state tracks.
    pub type_url: &'static str,
    /// Version from the last accepted response.
    pub version_info: String,
    /// Nonce from the last accepted response.
    pub nonce: String,
    /// Currently subscribed resource names.
    pub resource_names: Vec<String>,
}

impl TypeState {
    /// Create initial state with empty version/nonce.
    pub fn new(type_url: &'static str, resource_names: Vec<String>) -> Self {
        Self {
            type_url,
            version_info: String::new(),
            nonce: String::new(),
            resource_names,
        }
    }
}

// ── ResponseOutcome ───────────────────────────────────────────────────────────

/// Result of processing one `DiscoveryResponse` frame.
pub enum ResponseOutcome {
    /// CDS response decoded successfully; send this ACK.
    AckCds(DiscoveryRequest),
    /// EDS response decoded successfully; send this ACK.
    AckEds(DiscoveryRequest),
    /// CDS response could not be decoded; send this NACK.
    NackCds(DiscoveryRequest),
    /// EDS response could not be decoded; send this NACK.
    NackEds(DiscoveryRequest),
    /// A CDS response that produced new EDS subscriptions.
    /// Carries the CDS ACK request and the new EDS service names.
    NewEdsSubscription(DiscoveryRequest, Vec<String>),
    /// Unrecognised type URL — ignored.
    Unknown,
}

// ── Pure state-machine logic ──────────────────────────────────────────────────

/// Build the initial `DiscoveryRequest` for a given type URL.
///
/// version_info and response_nonce are both empty strings, as required by
/// the xDS protocol for the first request on a stream.
pub fn initial_request(type_url: &str, resource_names: &[String], node: Node) -> DiscoveryRequest {
    DiscoveryRequest {
        version_info: String::new(),
        node: Some(node),
        resource_names: resource_names.to_vec(),
        type_url: type_url.to_owned(),
        response_nonce: String::new(),
        error_detail: None,
    }
}

/// Build an ACK request for a successfully decoded response.
pub fn build_ack(state: &TypeState, new_version: &str, new_nonce: &str) -> DiscoveryRequest {
    DiscoveryRequest {
        version_info: new_version.to_owned(),
        node: None,
        resource_names: state.resource_names.clone(),
        type_url: state.type_url.to_owned(),
        response_nonce: new_nonce.to_owned(),
        error_detail: None,
    }
}

/// Build a NACK request for a response we could not decode.
///
/// The old `version_info` is preserved (per xDS spec) so the server knows
/// which version the client is still on.
pub fn build_nack(state: &TypeState, new_nonce: &str, error_message: &str) -> DiscoveryRequest {
    DiscoveryRequest {
        version_info: state.version_info.clone(),
        node: None,
        resource_names: state.resource_names.clone(),
        type_url: state.type_url.to_owned(),
        response_nonce: new_nonce.to_owned(),
        error_detail: Some(GoogleRpcStatus {
            code: 3, // INVALID_ARGUMENT
            message: error_message.to_owned(),
        }),
    }
}

/// Process one decoded `DiscoveryResponse` payload.
///
/// This function is intentionally pure (no IO) so it can be unit-tested
/// without setting up a real network connection.
pub fn process_response(
    payload: &[u8],
    cds_state: &mut TypeState,
    eds_state: &mut TypeState,
    watcher: &XdsWatcher,
) -> ResponseOutcome {
    let resp = match DiscoveryResponse::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            // Cannot determine type_url; respond with a generic NACK on CDS.
            let nack = build_nack(cds_state, "", &e.to_string());
            return ResponseOutcome::NackCds(nack);
        }
    };

    let version = resp.version_info.clone();
    let nonce = resp.nonce.clone();

    match resp.type_url.as_str() {
        CDS_TYPE_URL => handle_cds_response(resp, cds_state, version, nonce),
        EDS_TYPE_URL => handle_eds_response(resp, eds_state, version, nonce, watcher),
        _ => ResponseOutcome::Unknown,
    }
}

/// Decode and handle a CDS response.
fn handle_cds_response(
    resp: DiscoveryResponse,
    state: &mut TypeState,
    version: String,
    nonce: String,
) -> ResponseOutcome {
    let mut new_eds_names: Vec<String> = Vec::new();
    let mut decode_errors: Vec<String> = Vec::new();

    for any in &resp.resources {
        match Cluster::decode(any.value.as_slice()) {
            Ok(cluster) => {
                // Extract EDS service name (fall back to cluster name).
                let svc_name = if cluster
                    .eds_cluster_config
                    .as_ref()
                    .map(|c| !c.service_name.is_empty())
                    .unwrap_or(false)
                {
                    cluster
                        .eds_cluster_config
                        .as_ref()
                        .map(|c| c.service_name.clone())
                        .unwrap_or_else(|| cluster.name.clone())
                } else {
                    cluster.name.clone()
                };
                new_eds_names.push(svc_name);
            }
            Err(e) => {
                decode_errors.push(format!("cluster decode: {e}"));
            }
        }
    }

    if !decode_errors.is_empty() {
        let err_msg = decode_errors.join("; ");
        let nack = build_nack(state, &nonce, &err_msg);
        return ResponseOutcome::NackCds(nack);
    }

    // Update state to the new version.
    state.version_info = version.clone();
    state.nonce = nonce.clone();

    let ack = build_ack(state, &version, &nonce);

    if !new_eds_names.is_empty() {
        ResponseOutcome::NewEdsSubscription(ack, new_eds_names)
    } else {
        ResponseOutcome::AckCds(ack)
    }
}

/// Decode and handle an EDS response.
fn handle_eds_response(
    resp: DiscoveryResponse,
    state: &mut TypeState,
    version: String,
    nonce: String,
    watcher: &XdsWatcher,
) -> ResponseOutcome {
    let mut decode_errors: Vec<String> = Vec::new();

    for any in &resp.resources {
        match ProtoCla::decode(any.value.as_slice()) {
            Ok(proto_cla) => {
                let xds_cla: XdsCla = proto_cla.into();
                watcher.update(&xds_cla);
            }
            Err(e) => {
                decode_errors.push(format!("CLA decode: {e}"));
            }
        }
    }

    if !decode_errors.is_empty() {
        let err_msg = decode_errors.join("; ");
        let nack = build_nack(state, &nonce, &err_msg);
        return ResponseOutcome::NackEds(nack);
    }

    state.version_info = version.clone();
    state.nonce = nonce.clone();

    let ack = build_ack(state, &version, &nonce);
    ResponseOutcome::AckEds(ack)
}

// ── Wire helpers ──────────────────────────────────────────────────────────────

/// Encode a `DiscoveryRequest` as a gRPC length-prefixed frame and send it.
async fn send_request(
    tx: &crate::native_channel::body::NativeBodySender,
    req: &DiscoveryRequest,
) -> Result<(), OxiRpcError> {
    let encoded = encode_proto_frame(req)?;
    tx.send_data(encoded).await
}

/// Encode a prost message as a gRPC frame (5-byte header + protobuf payload).
pub(crate) fn encode_proto_frame<M: prost::Message>(msg: &M) -> Result<Bytes, OxiRpcError> {
    let mut proto_buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut proto_buf)
        .map_err(|e| OxiRpcError::Transport(format!("proto encode: {e}")))?;
    wire_frame::encode_frame(&proto_buf, false)
        .map_err(|e| OxiRpcError::Transport(format!("frame encode: {e}")))
}

/// Attempt to decode one complete gRPC frame from `buf`.
///
/// Returns `Some(payload_bytes)` if a full frame is available, or `None` if
/// more data is needed.
fn try_decode_grpc_frame(buf: &mut BytesMut) -> Option<Bytes> {
    const HDR: usize = 5;
    if buf.len() < HDR {
        return None;
    }
    let payload_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let total = HDR + payload_len;
    if buf.len() < total {
        return None;
    }
    // Consume and discard the 5-byte header.
    let _ = buf.split_to(HDR);
    Some(buf.split_to(payload_len).freeze())
}
