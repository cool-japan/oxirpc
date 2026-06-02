/// Incremental build cache for the `oxirpc-build` proto compiler.
///
/// Caches a parsed [`prost_types::FileDescriptorSet`] to avoid re-running
/// `protox` when no input files have changed.  The cache is stored in
/// `$OUT_DIR/.oxirpc-cache/fds.bin` and is invalidated whenever any proto file
/// or include directory has a modification time equal to or newer than the
/// cache file.
use std::path::Path;
use std::time::SystemTime;

use prost_types::FileDescriptorSet;

const CACHE_SUBDIR: &str = ".oxirpc-cache";
const CACHE_FILE: &str = "fds.bin";

/// Returns the mtime of the most recently modified file among `paths`.
///
/// Paths that cannot be stat'd are silently skipped.
fn max_mtime(paths: &[&Path]) -> Option<SystemTime> {
    paths
        .iter()
        .filter_map(|p| p.metadata().ok()?.modified().ok())
        .max()
}

/// Try to load a cached [`FileDescriptorSet`] from `$OUT_DIR/.oxirpc-cache/fds.bin`.
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
    let cache_path = out_dir.join(CACHE_SUBDIR).join(CACHE_FILE);
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

/// Write a [`FileDescriptorSet`] to the cache at `$OUT_DIR/.oxirpc-cache/fds.bin`.
///
/// Creates the cache directory if it does not already exist.  This is a
/// best-effort operation — callers should use `let _ = cache::write(...)` to
/// avoid failing the build on a cache write error.
pub fn write(out_dir: &Path, fds: &FileDescriptorSet) -> std::io::Result<()> {
    use prost::Message;
    let cache_dir = out_dir.join(CACHE_SUBDIR);
    std::fs::create_dir_all(&cache_dir)?;
    let bytes = fds.encode_to_vec();
    std::fs::write(cache_dir.join(CACHE_FILE), bytes)
}
