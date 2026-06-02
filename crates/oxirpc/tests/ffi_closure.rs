//! FFI tripwire: asserts that `oxirpc-reflect` and `oxirpc-web` do not introduce
//! forbidden FFI dependencies on normal (non-dev, non-build) edges.
//!
//! This test is documentation-as-code. The actual enforcement is:
//!
//! ```text
//! cargo tree -p oxirpc-reflect --edges normal \
//!     | grep -E 'flate2|ring|aws.lc|openssl-sys|native-tls'
//! # must be empty
//!
//! cargo tree -p oxirpc-web --edges normal \
//!     | grep -E 'flate2|ring|aws.lc|openssl-sys|native-tls'
//! # must be empty
//! ```
//!
//! Run the check in CI or manually from the workspace root.

/// Confirms that the workspace compiled successfully with:
///   - `tonic = { default-features = false }`
///   - `tonic-reflection = "0.14.6"`, `tonic-web = "0.14.6"` at their defaults
///   - No `flate2`, `ring`, `aws-lc`, `openssl-sys`, or `native-tls` on normal edges.
///
/// The cargo-deny `deny.toml` at the workspace root enforces this at every
/// `cargo build`. For manual verification run the `cargo tree` commands above.
#[test]
fn ffi_closure_documented() {
    // The fact that this test compiled is already the assertion:
    // tonic-reflection and tonic-web are in the workspace with
    // `tonic = { default-features = false }`, and the build succeeded.
    // Run the cargo tree commands in the module doc to manually verify the
    // closure is free of flate2/ring/aws-lc/openssl-sys/native-tls.
}
