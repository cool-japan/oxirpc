//! Integration layer: generate service stubs from a `FileDescriptorSet` and
//! write them to an output directory.
//!
//! # Example
//!
//! ```rust,no_run
//! use oxirpc_build::{Builder, codegen::ServiceCodegen, file_gen};
//! use prost_types::FileDescriptorSet;
//!
//! let codegen = ServiceCodegen::new();
//! let fds: FileDescriptorSet = unimplemented!("parse your .proto files");
//! let out_dir = std::path::Path::new("/tmp/generated");
//! let files = file_gen::generate_services(&fds, out_dir, &codegen)
//!     .expect("code generation failed");
//! for f in files { println!("wrote {}", f.display()); }
//! ```

use crate::{codegen::ServiceCodegen, OxiRpcBuildError};
use std::path::{Path, PathBuf};

/// Generate service stubs from every file in a [`prost_types::FileDescriptorSet`]
/// and write them to `out_dir`.
///
/// For each `.proto` file that contains at least one `service` declaration,
/// one Rust source file is emitted at
/// `<out_dir>/<proto_stem>.services.rs`.
///
/// Returns the list of paths that were written.
///
/// # Errors
///
/// Returns an [`OxiRpcBuildError::Io`] if `out_dir` cannot be created or a
/// file cannot be written.
/// Returns an [`OxiRpcBuildError::Codegen`] if any proto file lacks a
/// `name` field (this is always set by `protox`; the error is a safety net).
pub fn generate_services(
    fds: &prost_types::FileDescriptorSet,
    out_dir: &Path,
    config: &ServiceCodegen,
) -> Result<Vec<PathBuf>, OxiRpcBuildError> {
    std::fs::create_dir_all(out_dir)?;

    let mut written = Vec::new();

    for file in &fds.file {
        // Skip files that declare no services.
        if file.service.is_empty() {
            continue;
        }

        let proto_name = file
            .name
            .as_deref()
            .filter(|n| !n.is_empty())
            .ok_or_else(|| OxiRpcBuildError::Codegen("proto file has no name field".into()))?;

        // Derive a Rust-friendly stem from the proto file name.
        // e.g. "helloworld/greeter.proto" → "greeter"
        let stem = Path::new(proto_name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("generated");

        let out_path = out_dir.join(format!("{stem}.services.rs"));

        let source = config.generate(file);
        std::fs::write(&out_path, source.as_bytes())?;

        written.push(out_path);
    }

    Ok(written)
}
