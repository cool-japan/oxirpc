//! Compatibility test: bytes produced by `protox` (the oxirpc-build parser)
//! are accepted by `oxirpc-reflect`'s [`DescriptorPoolBuilder`].
//!
//! This verifies end-to-end that:
//! 1. `protox::compile` produces a valid `prost_types::FileDescriptorSet`.
//! 2. The encoded bytes can be decoded by `DescriptorPoolBuilder::register_bytes`.
//! 3. The resulting pool lists the expected service name.
//!
//! Uses the same `greeter.proto` fixture already present for compile_link tests.

use oxirpc_reflect::DescriptorPoolBuilder;
use prost::Message as _;

#[test]
fn oxirpc_build_fds_reflection_compat() {
    // ── 1. Parse greeter.proto with protox ───────────────────────────────────
    let proto_path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/greeter.proto");

    // The include path must be the directory containing the proto file so
    // protox can resolve the file by its relative name.
    let fixtures_dir = proto_path
        .parent()
        .expect("proto_path should have a parent directory");

    let fds = protox::compile([proto_path.as_path()], [fixtures_dir])
        .expect("protox should compile greeter.proto without error");

    // ── 2. Encode to proto-binary bytes ──────────────────────────────────────
    let fds_bytes = fds.encode_to_vec();
    assert!(!fds_bytes.is_empty(), "encoded FDS must not be empty");

    // ── 3. Feed to oxirpc-reflect DescriptorPoolBuilder ──────────────────────
    let pool = DescriptorPoolBuilder::new()
        .register_bytes(&fds_bytes)
        .expect("DescriptorPoolBuilder should accept protox-produced FDS bytes")
        .build();

    // ── 4. Assert the expected service is present ─────────────────────────────
    let services = pool.list_services();
    assert!(
        !services.is_empty(),
        "pool should contain at least one service; got empty list"
    );
    assert!(
        services.iter().any(|s| s.contains("Greeter")),
        "service list {:?} should contain 'Greeter'",
        services
    );
}
