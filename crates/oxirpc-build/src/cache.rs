/// Incremental build cache for the `oxirpc-build` proto compiler.
///
/// Caches a parsed [`prost_types::FileDescriptorSet`] to avoid re-running
/// `protox` when no input files have changed.  The cache is stored in
/// `$OUT_DIR/.oxirpc-cache/fds-<hash>.bin` and is invalidated whenever any proto file
/// or include directory has a modification time equal to or newer than the
/// cache file.
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::time::SystemTime;

use prost_types::FileDescriptorSet;

const CACHE_SUBDIR: &str = ".oxirpc-cache";

/// Compute the cache file name for a given set of proto inputs.
///
/// A single `$OUT_DIR` may host several independent `compile_to_fds` calls in
/// one build script (e.g. two different `.proto` files compiled in sequence).
/// Keying the cache solely on `$OUT_DIR` would let one proto set's descriptor
/// set be returned for an unrelated one. To keep each proto set isolated, the
/// file name embeds a stable hash of the sorted proto paths.
fn cache_file_name(proto_files: &[impl AsRef<Path>]) -> String {
    let mut names: Vec<String> = proto_files
        .iter()
        .map(|p| p.as_ref().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    names.hash(&mut hasher);
    format!("fds-{:016x}.bin", hasher.finish())
}

/// Returns the mtime of the most recently modified file among `paths`.
///
/// Paths that cannot be stat'd are silently skipped.
fn max_mtime(paths: &[&Path]) -> Option<SystemTime> {
    paths
        .iter()
        .filter_map(|p| p.metadata().ok()?.modified().ok())
        .max()
}

/// Try to load a cached [`FileDescriptorSet`] from
/// `$OUT_DIR/.oxirpc-cache/fds-<hash>.bin`.
///
/// Returns `Some(fds)` on a cache hit and `None` on any miss or error
/// (missing file, I/O failure, parse error, or stale mtime).
///
/// Invalidation rule: the cache is considered stale if any file in
/// `proto_files` or `include_dirs` has a modification time that is **equal to
/// or newer** than the cache file.
pub fn try_load(
    out_dir: &Path,
    proto_files: &[impl AsRef<Path>],
    include_dirs: &[impl AsRef<Path>],
) -> Option<FileDescriptorSet> {
    let cache_path = out_dir
        .join(CACHE_SUBDIR)
        .join(cache_file_name(proto_files));
    let cache_mtime = cache_path.metadata().ok()?.modified().ok()?;

    // Collect all input paths into a flat slice for mtime comparison.
    let all_inputs: Vec<&Path> = proto_files
        .iter()
        .map(|p| p.as_ref())
        .chain(include_dirs.iter().map(|p| p.as_ref()))
        .collect();

    let input_mtime = max_mtime(&all_inputs)?;

    // Invalidate if any input is as new as or newer than the cache.
    if input_mtime >= cache_mtime {
        return None;
    }

    let bytes = std::fs::read(&cache_path).ok()?;
    use prost::Message;
    FileDescriptorSet::decode(bytes.as_slice()).ok()
}

/// Write a [`FileDescriptorSet`] to the cache at
/// `$OUT_DIR/.oxirpc-cache/fds-<hash>.bin`, where `<hash>` is derived from the
/// `proto_files` set so distinct proto sets compiled into the same `$OUT_DIR`
/// do not clobber one another.
///
/// Creates the cache directory if it does not already exist.  This is a
/// best-effort operation — callers should use `let _ = cache::write(...)` to
/// avoid failing the build on a cache write error.
pub fn write(
    out_dir: &Path,
    proto_files: &[impl AsRef<Path>],
    fds: &FileDescriptorSet,
) -> std::io::Result<()> {
    use prost::Message;
    let cache_dir = out_dir.join(CACHE_SUBDIR);
    std::fs::create_dir_all(&cache_dir)?;
    let bytes = fds.encode_to_vec();
    std::fs::write(cache_dir.join(cache_file_name(proto_files)), bytes)
}
