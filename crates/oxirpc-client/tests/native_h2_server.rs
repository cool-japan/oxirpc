//! Minimal h2-server fixture for native_channel integration tests.
//!
//! Provides a programmable HTTP/2 server that exercises:
//! - Unary OK responses (200 + grpc-status:0 in trailers)
//! - Trailers-only responses (grpc-status in initial HEADERS+END_STREAM)
//! - Server-streaming (N data frames + trailer)
//! - GOAWAY (graceful shutdown)

use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use h2::server::SendResponse;
use http::{Request, Response};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

// ── Frame helpers ─────────────────────────────────────────────────────────────

/// Encode a gRPC length-prefixed frame (flag=0, 4-byte big-endian length, payload).
pub fn grpc_frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(0x00);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

// ── ServerHandle ──────────────────────────────────────────────────────────────

/// A handle to a running test H2 server.
pub struct ServerHandle {
    pub addr: SocketAddr,
    pub task: JoinHandle<()>,
}

impl ServerHandle {
    /// Abort the server task and release the port.
    pub fn shutdown(self) {
        self.task.abort();
    }
}

// ── spawn_unary_ok_server ─────────────────────────────────────────────────────

/// Spawn a server that responds to every request with:
///   HEADERS (200) → DATA (gRPC frame of `response_payload`) → TRAILERS (grpc-status:0)
pub async fn spawn_unary_ok_server(response_payload: Bytes) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let payload = response_payload.clone();
            tokio::spawn(async move {
                handle_unary_ok(stream, payload).await;
            });
        }
    });

    ServerHandle { addr, task }
}

async fn handle_unary_ok(stream: TcpStream, payload: Bytes) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };

    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        // Drain the request body (we don't actually inspect it).
        drain_request(req).await;
        send_unary_response(&mut respond, payload.clone()).await;
    }
}

async fn send_unary_response(respond: &mut SendResponse<Bytes>, payload: Bytes) {
    let response = Response::builder()
        .status(200)
        .header("content-type", "application/grpc+proto")
        .body(())
        .unwrap();

    let mut send_stream = match respond.send_response(response, false) {
        Ok(s) => s,
        Err(_) => return,
    };

    let frame = grpc_frame(&payload);
    if send_stream.send_data(frame, false).is_err() {
        return;
    }

    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    let _ = send_stream.send_trailers(trailers);
}

// ── spawn_trailer_only_server ─────────────────────────────────────────────────

/// Spawn a server that responds to every request with a trailers-only response
/// containing `grpc-status: <code>` in the initial HEADERS+END_STREAM frame.
pub async fn spawn_trailer_only_server(grpc_status_code: u32) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                handle_trailer_only(stream, grpc_status_code).await;
            });
        }
    });

    ServerHandle { addr, task }
}

async fn handle_trailer_only(stream: TcpStream, code: u32) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };

    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        drain_request(req).await;
        // Send trailers-only: HEADERS+END_STREAM with grpc-status in initial headers.
        let status_str = code.to_string();
        let response = Response::builder()
            .status(200)
            .header("content-type", "application/grpc+proto")
            .header("grpc-status", status_str.as_str())
            .body(())
            .unwrap();
        // `end_stream = true` makes this a trailers-only response.
        let _ = respond.send_response(response, true);
    }
}

// ── spawn_streaming_server ────────────────────────────────────────────────────

/// Spawn a server that sends `frame_count` gRPC data frames then grpc-status:0.
pub async fn spawn_streaming_server(frame_count: usize, frame_payload: Bytes) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let payload = frame_payload.clone();
            tokio::spawn(async move {
                handle_streaming(stream, frame_count, payload).await;
            });
        }
    });

    ServerHandle { addr, task }
}

async fn handle_streaming(stream: TcpStream, frame_count: usize, payload: Bytes) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };

    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        drain_request(req).await;

        let response = Response::builder()
            .status(200)
            .header("content-type", "application/grpc+proto")
            .body(())
            .unwrap();

        let mut send_stream = match respond.send_response(response, false) {
            Ok(s) => s,
            Err(_) => return,
        };

        for _ in 0..frame_count {
            let frame = grpc_frame(&payload);
            if send_stream.send_data(frame, false).is_err() {
                return;
            }
        }

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        let _ = send_stream.send_trailers(trailers);
    }
}

// ── spawn_goaway_server ───────────────────────────────────────────────────────

/// Spawn a server that immediately sends GOAWAY then closes the connection.
pub async fn spawn_goaway_server() -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut conn = match h2::server::handshake(stream).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                // Accept one request, then gracefully close.
                if let Some(Ok((req, _respond))) = conn.accept().await {
                    drain_request(req).await;
                }
                // Drive the connection to completion (triggers GOAWAY on client).
                drop(conn);
            });
        }
    });

    ServerHandle { addr, task }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Drain all DATA frames from the request body (required by h2 protocol).
async fn drain_request(req: Request<h2::RecvStream>) {
    let (_, mut body) = req.into_parts();
    while let Some(chunk) = body.data().await {
        if let Ok(chunk) = chunk {
            let _ = body.flow_control().release_capacity(chunk.len());
        }
    }
}

// ── spawn_no_trailer_server ───────────────────────────────────────────────────

/// Spawn a server that sends a `200` HEADERS frame, one gRPC data frame, then
/// ends the stream via `send_data(_, true)` WITHOUT ever sending a `grpc-status`
/// trailer. This is a protocol violation (or a non-gRPC-aware proxy in front of
/// a broken backend) that the client must surface as an error rather than a
/// silently successful empty stream.
pub async fn spawn_no_trailer_server(response_payload: Bytes) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let payload = response_payload.clone();
            tokio::spawn(async move {
                handle_no_trailer(stream, payload).await;
            });
        }
    });

    ServerHandle { addr, task }
}

async fn handle_no_trailer(stream: TcpStream, payload: Bytes) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };

    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        drain_request(req).await;

        let response = Response::builder()
            .status(200)
            .header("content-type", "application/grpc+proto")
            .body(())
            .unwrap();

        let mut send_stream = match respond.send_response(response, false) {
            Ok(s) => s,
            Err(_) => return,
        };

        let frame = grpc_frame(&payload);
        if send_stream.send_data(frame, false).is_err() {
            return;
        }
        // End the stream via a plain END_STREAM data frame — no HEADERS
        // trailer frame is ever sent, so `grpc-status` is never present.
        let _ = send_stream.send_data(Bytes::new(), true);
    }
}

// ── spawn_wrong_content_type_server ───────────────────────────────────────────

/// Spawn a server that responds to every request with a `200` HEADERS frame
/// whose `content-type` is `text/html` (simulating a reverse-proxy error page
/// or a plain HTTP endpoint reachable at the same address), followed by a
/// data frame and normal-looking gRPC trailers. The client must reject this
/// on the content-type check alone, never attempting to decode the body as
/// gRPC frames.
pub async fn spawn_wrong_content_type_server() -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                handle_wrong_content_type(stream).await;
            });
        }
    });

    ServerHandle { addr, task }
}

async fn handle_wrong_content_type(stream: TcpStream) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };

    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        drain_request(req).await;

        let response = Response::builder()
            .status(200)
            .header("content-type", "text/html")
            .body(())
            .unwrap();

        let mut send_stream = match respond.send_response(response, false) {
            Ok(s) => s,
            Err(_) => return,
        };

        let _ = send_stream.send_data(Bytes::from_static(b"<html>not gRPC</html>"), false);

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        let _ = send_stream.send_trailers(trailers);
    }
}

// ── spawn_missing_content_type_server ─────────────────────────────────────────

/// Spawn a server that responds with a `200` HEADERS frame carrying **no**
/// `content-type` header at all, then ends the stream immediately
/// (trailers-only shape). Simulates a bare HTTP server or a load balancer
/// health-check responder with no notion of gRPC.
pub async fn spawn_missing_content_type_server() -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                handle_missing_content_type(stream).await;
            });
        }
    });

    ServerHandle { addr, task }
}

async fn handle_missing_content_type(stream: TcpStream) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };

    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        drain_request(req).await;

        // No content-type header at all — end_stream = true (trailers-only shape).
        let response = Response::builder().status(200).body(()).unwrap();
        let _ = respond.send_response(response, true);
    }
}

// ── spawn_header_echo_server ────────────────────────────────────────────────

/// Spawn a server that echoes the request header `echo_name` value back in a
/// response header `x-echoed` (or `"<absent>"` if the header was missing),
/// then returns an empty OK (grpc-status:0) response. Used to verify that a
/// client interceptor injected a header onto the outgoing request.
pub async fn spawn_header_echo_server(echo_name: &'static str) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            tokio::spawn(async move {
                handle_header_echo(stream, echo_name).await;
            });
        }
    });
    ServerHandle { addr, task }
}

async fn handle_header_echo(stream: TcpStream, echo_name: &'static str) {
    let mut conn = match h2::server::handshake(stream).await {
        Ok(c) => c,
        Err(_) => return,
    };
    while let Some(result) = conn.accept().await {
        let (req, mut respond) = match result {
            Ok(v) => v,
            Err(_) => return,
        };
        let echoed = req
            .headers()
            .get(echo_name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<absent>")
            .to_string();
        drain_request(req).await;
        let response = Response::builder()
            .status(200)
            .header("content-type", "application/grpc+proto")
            .header("x-echoed", echoed.as_str())
            .body(())
            .unwrap();
        let mut send_stream = match respond.send_response(response, false) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        let _ = send_stream.send_trailers(trailers);
    }
}
