//! Example: JWT/bearer token auth interceptor.
//!
//! Demonstrates [`oxirpc::interceptors::BearerAuthInterceptor`], which rejects
//! requests missing a valid `Authorization: Bearer <token>` metadata header.
//!
//! In production, replace the static token with a JWT-validation call.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example interceptor_auth
//! ```

use oxirpc::interceptors::BearerAuthInterceptor;
use tonic::service::Interceptor;
use tonic::Request;

fn main() {
    let mut interceptor = BearerAuthInterceptor::new("super-secret-jwt-token");

    // --- Rejected: no authorization header ---
    let req: Request<()> = Request::new(());
    match interceptor.call(req) {
        Err(status) => println!("[rejected] {status}"),
        Ok(_) => panic!("should have been rejected"),
    }

    // --- Rejected: wrong token ---
    let mut req: Request<()> = Request::new(());
    req.metadata_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    match interceptor.call(req) {
        Err(status) => println!("[rejected] {status}"),
        Ok(_) => panic!("should have been rejected"),
    }

    // --- Accepted: correct token ---
    let mut req: Request<()> = Request::new(());
    req.metadata_mut().insert(
        "authorization",
        "Bearer super-secret-jwt-token".parse().unwrap(),
    );
    match interceptor.call(req) {
        Ok(_) => println!("[accepted] request passed auth check"),
        Err(e) => panic!("should have passed: {e}"),
    }

    println!("Bearer auth interceptor example complete.");
    println!("See oxirpc::interceptors::BearerAuthInterceptor for the implementation.");
}
