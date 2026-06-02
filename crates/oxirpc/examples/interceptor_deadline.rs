//! Example: Deadline / timeout propagation interceptor.
//!
//! Demonstrates [`oxirpc::interceptors::DeadlineInterceptor`], which injects a
//! default `grpc-timeout` metadata header when the caller has not already set
//! one.  This ensures all outbound RPCs carry a bounded deadline, preventing
//! indefinite hangs on unresponsive upstreams.
//!
//! The timeout uses the gRPC wire format: `<value><unit>` where unit is `H`
//! (hours), `M` (minutes), `S` (seconds), `m` (milliseconds), etc.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example interceptor_deadline
//! ```

use oxirpc::interceptors::DeadlineInterceptor;
use std::time::Duration;
use tonic::service::Interceptor;
use tonic::Request;

fn main() {
    let mut interceptor = DeadlineInterceptor::new(Duration::from_secs(5));

    // --- No existing timeout: interceptor injects default ---
    let req: Request<()> = Request::new(());
    let req = interceptor
        .call(req)
        .expect("deadline interceptor must not fail");
    let timeout = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .expect("grpc-timeout must be injected");
    println!("[injected] grpc-timeout = {timeout}");

    // --- Existing timeout: interceptor leaves it untouched ---
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("grpc-timeout", "1000m".parse().unwrap());
    let req = interceptor
        .call(req)
        .expect("deadline interceptor must not fail");
    let timeout = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .expect("grpc-timeout must remain");
    println!("[preserved] grpc-timeout = {timeout}  (caller's original value kept)");

    println!("Deadline interceptor example complete.");
    println!("See oxirpc::interceptors::DeadlineInterceptor for the implementation.");
}
