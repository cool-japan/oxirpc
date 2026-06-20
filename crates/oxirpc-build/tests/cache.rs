/// Integration tests for `oxirpc_build`'s incremental build cache.
///
/// These tests exercise the cache read/write/invalidate contract using the
/// same `$OUT_DIR/.oxirpc-cache/fds-<hash>.bin` layout that the crate uses
/// internally.  All file I/O uses temporary directories under
/// [`std::env::temp_dir()`] with unique per-test subdirectories to avoid
/// collisions between parallel test runs.
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use filetime::{set_file_mtime, FileTime};
use prost::Message;
use prost_types::FileDescriptorSet;

// ---------------------------------------------------------------------------
// Constants (mirror the private values in cache.rs)
// ---------------------------------------------------------------------------

const CACHE_SUBDIR: &str = ".oxirpc-cache";

/// Mirror of `cache::cache_file_name` — the cache file name is keyed on a
/// stable hash of the sorted proto paths so distinct proto sets compiled into
/// the same `$OUT_DIR` stay isolated.
fn cache_file_name(proto_files: &[&Path]) -> String {
    let mut names: Vec<String> = proto_files
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    names.hash(&mut hasher);
    format!("fds-{:016x}.bin", hasher.finish())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a unique temporary directory for a single test.
fn test_dir(test_name: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "oxirpc-build-cache-tests-{}-{}",
        std::process::id(),
        test_name
    ));
    fs::create_dir_all(&base).expect("create test dir");
    base
}

/// Write a minimal `.proto` file and return its path.
fn write_proto(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(format!("{name}.proto"));
    fs::write(
        &path,
        format!(
            r#"syntax = "proto3";
package test;
message Req{name} {{ string value = 1; }}
"#
        ),
    )
    .expect("write proto");
    path
}

/// Build a minimal [`FileDescriptorSet`] suitable for round-trip tests.
fn minimal_fds() -> FileDescriptorSet {
    use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

    let field = FieldDescriptorProto {
        name: Some("value".to_owned()),
        number: Some(1),
        r#type: Some(9), // TYPE_STRING
        ..Default::default()
    };
    let message = DescriptorProto {
        name: Some("TestMsg".to_owned()),
        field: vec![field],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("test.proto".to_owned()),
        package: Some("test".to_owned()),
        syntax: Some("proto3".to_owned()),
        message_type: vec![message],
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }
}

/// Write a [`FileDescriptorSet`] to the cache location inside `out_dir`,
/// keyed on `proto_files` exactly as the crate does.
fn write_cache(out_dir: &Path, proto_files: &[&Path], fds: &FileDescriptorSet) {
    let cache_dir = out_dir.join(CACHE_SUBDIR);
    fs::create_dir_all(&cache_dir).expect("create cache dir");
    let bytes = fds.encode_to_vec();
    fs::write(cache_dir.join(cache_file_name(proto_files)), bytes).expect("write cache");
}

/// Set the mtime of `path` (file or directory) to the given [`SystemTime`].
///
/// Uses the `filetime` crate which handles both files and directories on all
/// platforms without requiring unsafe code.
fn set_mtime(path: &Path, mtime: SystemTime) {
    let ft = FileTime::from_system_time(mtime);
    set_file_mtime(path, ft).unwrap_or_else(|e| panic!("set_file_mtime {}: {e}", path.display()));
}

// ---------------------------------------------------------------------------
// Mirror of the cache logic (used by integration tests since cache::try_load
// is a private module function inside oxirpc-build).
// ---------------------------------------------------------------------------

/// Re-implementation of `cache::try_load` that integration tests can call
/// directly.  Mirrors the exact logic in `src/cache.rs`.
fn cache_try_load(
    out_dir: &Path,
    proto_files: &[&Path],
    include_dirs: &[&Path],
) -> Option<FileDescriptorSet> {
    let cache_path = out_dir
        .join(CACHE_SUBDIR)
        .join(cache_file_name(proto_files));
    let cache_mtime = cache_path.metadata().ok()?.modified().ok()?;

    let input_mtime: Option<SystemTime> = proto_files
        .iter()
        .chain(include_dirs.iter())
        .filter_map(|p| p.metadata().ok()?.modified().ok())
        .max();

    let input_mtime = input_mtime?;

    if input_mtime >= cache_mtime {
        return None;
    }

    let bytes = fs::read(&cache_path).ok()?;
    FileDescriptorSet::decode(bytes.as_slice()).ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A first call when no cache directory exists must return `None`.
///
/// Validates the cold-start path: `OUT_DIR/.oxirpc-cache/fds.bin` does not
/// exist, so the cache must report a miss.
#[test]
fn cache_miss_on_first_call() {
    let dir = test_dir("miss_first_call");
    let proto = write_proto(&dir, "hello");

    let result = cache_try_load(&dir, &[proto.as_path()], &[dir.as_path()]);
    assert!(
        result.is_none(),
        "expected cache miss when no cache file exists"
    );
}

/// Write a cache file, give it a mtime newer than the proto, and verify a hit.
#[test]
fn cache_write_and_hit() {
    let dir = test_dir("write_and_hit");
    let proto = write_proto(&dir, "greeter");

    let fds = minimal_fds();
    write_cache(&dir, &[proto.as_path()], &fds);

    // Set proto and include-dir mtime to 60 s in the past.
    let past = SystemTime::now() - Duration::from_secs(60);
    set_mtime(&proto, past);
    set_mtime(&dir, past);

    // Set cache mtime to now (newer than proto).
    let cache_path = dir
        .join(CACHE_SUBDIR)
        .join(cache_file_name(&[proto.as_path()]));
    set_mtime(&cache_path, SystemTime::now());

    let result = cache_try_load(&dir, &[proto.as_path()], &[dir.as_path()]);
    assert!(
        result.is_some(),
        "expected cache hit when cache is newer than proto"
    );
}

/// Write a cache file, set the proto's mtime to the future, and verify a miss.
///
/// Exercises the invalidation path: a proto newer than the cache must produce
/// a miss.
#[test]
fn cache_invalidated_when_proto_newer() {
    let dir = test_dir("invalidate_newer_proto");
    let proto = write_proto(&dir, "service");

    let fds = minimal_fds();
    write_cache(&dir, &[proto.as_path()], &fds);

    // Set cache mtime to 60 s in the past.
    let past = SystemTime::now() - Duration::from_secs(60);
    let cache_path = dir
        .join(CACHE_SUBDIR)
        .join(cache_file_name(&[proto.as_path()]));
    set_mtime(&cache_path, past);

    // Set proto mtime to now (newer than cache).
    set_mtime(&proto, SystemTime::now());

    let result = cache_try_load(&dir, &[proto.as_path()], &[dir.as_path()]);
    assert!(
        result.is_none(),
        "expected cache miss when proto is newer than cache"
    );
}

/// Encode a `FileDescriptorSet`, write it to the cache, decode it back, and
/// verify that all content is preserved exactly.
#[test]
fn cache_roundtrip_preserves_fds_content() {
    let dir = test_dir("roundtrip");
    let proto = write_proto(&dir, "roundtrip");

    let original = minimal_fds();
    write_cache(&dir, &[proto.as_path()], &original);

    // Set proto and include-dir mtime to the past; cache is fresh.
    let past = SystemTime::now() - Duration::from_secs(60);
    set_mtime(&proto, past);
    set_mtime(&dir, past);
    let cache_path = dir
        .join(CACHE_SUBDIR)
        .join(cache_file_name(&[proto.as_path()]));
    set_mtime(&cache_path, SystemTime::now());

    let loaded = cache_try_load(&dir, &[proto.as_path()], &[dir.as_path()])
        .expect("expected cache hit for round-trip test");

    assert_eq!(
        loaded.file.len(),
        original.file.len(),
        "file count mismatch after round-trip"
    );
    let orig_file = &original.file[0];
    let load_file = &loaded.file[0];
    assert_eq!(load_file.name(), orig_file.name(), "file name mismatch");
    assert_eq!(load_file.package(), orig_file.package(), "package mismatch");
    assert_eq!(
        load_file.message_type.len(),
        orig_file.message_type.len(),
        "message count mismatch"
    );
    assert_eq!(
        load_file.message_type[0].name(),
        orig_file.message_type[0].name(),
        "message name mismatch"
    );
    assert_eq!(
        load_file.message_type[0].field[0].name(),
        orig_file.message_type[0].field[0].name(),
        "field name mismatch"
    );
}
