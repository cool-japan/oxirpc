//! Purity tripwire: verifies that the default (no-feature) `oxirpc` closure
//! does **not** contain aws-lc or openssl.
//!
//! If this test fails it means some dependency path has leaked C/FFI code into
//! the `oxirpc` default feature set, breaking the Pure Rust guarantee.

#[test]
fn oxirpc_default_closure_pure() {
    // Walk up two levels: crates/oxirpc-adapter-aws-lc → crates/ → workspace root.
    let ws_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("parent of crates/oxirpc-adapter-aws-lc should be crates/")
        .parent()
        .expect("parent of crates/ should be workspace root");

    let output = std::process::Command::new("cargo")
        .args(["tree", "-p", "oxirpc", "--edges", "normal"])
        .current_dir(ws_root)
        .output()
        .expect("cargo tree failed — is cargo in PATH?");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("aws-lc"),
        "aws-lc leaked into oxirpc default closure:\n{stdout}"
    );
    assert!(
        !stdout.contains("openssl"),
        "openssl leaked into oxirpc default closure:\n{stdout}"
    );
}
