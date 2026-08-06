//! Example: HTTP/3 (gRPC-over-QUIC) client + server round trip, driven
//! entirely through the [`oxirpc::http3`] facade module — the same surface
//! documented in the README's HTTP/3 section and in `oxirpc::http3`'s own
//! rustdoc example, compiled here as a real, runnable program instead of a
//! `no_run` snippet.
//!
//! Generates a Pure-Rust Ed25519 self-signed certificate for `localhost` (the
//! cert type proven to complete the OxiQUIC handshake), binds a QUIC server
//! endpoint serving the built-in `grpc.health.v1.Health` service, dials it
//! with [`oxirpc::http3::H3ChannelBuilder`], issues a `Health/Check` RPC, and
//! shuts the server down gracefully.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example http3_client_server --features "http3,native,health,client"
//! ```

use http_body::Body as _;
use prost::Message as _;
use rustls::RootCertStore;

use oxirpc::http3::{
    bind_h3_endpoint, client_config_h3_arc, server_config_h3_arc, H3ChannelBuilder,
};
use oxirpc::ServerBuilder;
use oxirpc_core::wire::{encode_grpc_message, NativeBody};
use oxirpc_health::HealthBuilder;
use oxirpc_server::{NativeServiceRegistry, TransportConfig};

use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Self-signed Ed25519 cert (Pure Rust; the cert type proven to complete
    //    the OxiQUIC handshake with the pure-Rust crypto provider) ──────────
    let ck = oxitls_rcgen::generate_self_signed_ed25519(&["localhost"])?;
    let key_pem = pkcs8_der_to_pem(&ck.pkcs8_der);

    // ── Server: bind a QUIC endpoint and serve Health natively over HTTP/3 ──
    let server_cfg = server_config_h3_arc(ck.cert_pem.as_bytes(), key_pem.as_bytes())?;
    let endpoint = bind_h3_endpoint(
        "127.0.0.1:0".parse()?,
        server_cfg,
        TransportConfig::default(),
    )
    .await?;
    let addr = endpoint.local_addr()?;
    println!("[server] listening on {addr} (QUIC/h3)");

    let (health_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("greeter.Greeter").await;
    let registry = NativeServiceRegistry::new().add_service(health_svc);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_h3_with_endpoint(endpoint, registry, Some(shutdown_rx))
            .await
    });

    // ── Client: trust the self-signed cert, dial over QUIC ──────────────────
    let mut roots = RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(ck.cert_der.clone()))?;
    let tls = client_config_h3_arc(roots)?;
    let channel = H3ChannelBuilder::new()
        .addr(addr)
        .server_name("localhost")
        .tls(tls)
        .build()?;
    channel.ready().await?;

    // ── Issue a Health/Check RPC by hand (no generated client stub) ────────
    let request = HealthCheckRequest {
        service: "greeter.Greeter".to_string(),
    };
    let framed = encode_grpc_message(&request)?;
    let req = http::Request::builder()
        .method("POST")
        .uri("https://localhost/grpc.health.v1.Health/Check")
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(NativeBody::once(framed))?;

    let resp = channel.call(req).await?;
    println!("[client] response status: {}", resp.status());

    let mut body = std::pin::pin!(resp.into_body());
    let mut payload = Vec::new();
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        if let Ok(f) = frame {
            if f.is_data() {
                if let Ok(data) = f.into_data() {
                    payload.extend_from_slice(&data);
                }
            }
        }
    }
    let decoded = HealthCheckResponse::decode(payload.as_slice())?;
    println!(
        "[client] greeter.Greeter serving status = {} (1 = SERVING)",
        decoded.status
    );
    assert_eq!(decoded.status, 1, "expected SERVING");

    // ── Graceful shutdown ────────────────────────────────────────────────
    shutdown_tx.send(()).ok();
    server.await??;
    println!("[server] shut down cleanly");

    Ok(())
}

// ── PKCS#8 DER → PEM helper (example-only; avoids an extra dependency) ────

/// Wrap PKCS#8 DER key bytes in a `PRIVATE KEY` PEM envelope.
fn pkcs8_der_to_pem(der: &[u8]) -> String {
    let b64 = base64_encode(der);
    let mut out = String::from("-----BEGIN PRIVATE KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        out.push('\n');
    }
    out.push_str("-----END PRIVATE KEY-----\n");
    out
}

/// Standard RFC 4648 base64 encoder (example-only; avoids a dependency).
fn base64_encode(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[((n >> 18) & 0x3f) as usize] as char);
        out.push(T[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            T[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}
