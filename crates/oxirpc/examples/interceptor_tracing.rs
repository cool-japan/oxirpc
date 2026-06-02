//! Example: Request tracing interceptor.
//!
//! Demonstrates [`oxirpc::interceptors::TracingInterceptor`], which attaches a
//! monotonically-increasing `x-request-id` metadata header to every outgoing RPC.
//!
//! The request ID can be correlated with server-side logs to trace individual
//! calls end-to-end.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example interceptor_tracing
//! ```

use oxirpc::interceptors::TracingInterceptor;
use tonic::service::Interceptor;
use tonic::Request;

fn main() {
    let mut interceptor = TracingInterceptor;

    for _ in 0..3 {
        let req: Request<()> = Request::new(());
        let req = interceptor
            .call(req)
            .expect("tracing interceptor must not fail");
        let id = req
            .metadata()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .expect("x-request-id must be present");
        println!("[traced] x-request-id = {id}");
    }

    println!("Tracing interceptor example complete.");
    println!("See oxirpc::interceptors::TracingInterceptor for the implementation.");
}
