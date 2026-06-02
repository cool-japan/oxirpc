//! TLS server config construction benchmark.
//!
//! Requires the `tls` feature (`--features tls` or `--all-features`).
//!
//! Measures the cold-construction cost of `tls::server_config(cert, key)` —
//! the per-process setup path rather than a per-request hot path.  A
//! self-signed cert is generated once outside the measurement loop via rcgen.

use criterion::{criterion_group, criterion_main, Criterion};

#[cfg(feature = "tls")]
fn localhost_cert_pem() -> (Vec<u8>, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("rcgen::generate_simple_self_signed");
    (
        cert.cert.pem().into_bytes(),
        cert.signing_key.serialize_pem().into_bytes(),
    )
}

#[cfg(feature = "tls")]
fn bench_server_config(c: &mut Criterion) {
    use oxirpc_core::tls::server_config;

    let (cert_pem, key_pem) = localhost_cert_pem();
    c.bench_function("tls_server_config_cold", |b| {
        b.iter(|| server_config(&cert_pem, &key_pem).expect("server_config ok"));
    });
}

#[cfg(feature = "tls")]
fn bench_client_config(c: &mut Criterion) {
    use oxirpc_core::tls::client_config;
    use rustls::RootCertStore;

    c.bench_function("tls_client_config_empty_roots", |b| {
        b.iter(|| {
            let roots = RootCertStore::empty();
            client_config(roots).expect("client_config ok")
        });
    });
}

#[cfg(not(feature = "tls"))]
fn bench_server_config(_c: &mut Criterion) {}

#[cfg(not(feature = "tls"))]
fn bench_client_config(_c: &mut Criterion) {}

criterion_group!(benches, bench_server_config, bench_client_config);
criterion_main!(benches);
