# Contributing to OxiRPC

Thanks for your interest in improving OxiRPC — the Pure-Rust gRPC stack. This
document covers how to build, test, and land changes.

## Building & Testing

OxiRPC is a Cargo workspace. From the repository root:

```bash
# Build everything (default features)
cargo build --workspace

# Run the full test suite (default features)
cargo nextest run --workspace

# Exercise an opt-in feature closure, e.g. HTTP/3:
cargo nextest run -p oxirpc --features "http3 native health client"

# Lint — this must be warning-free before a change is accepted:
cargo clippy --all-targets -- -D warnings
```

Both `cargo nextest run` and `cargo clippy --all-targets -- -D warnings` must be
green before a change is merged.

## Project Rules

These rules are enforced (in review and, where possible, in CI):

- **Pure Rust by default.** The default feature closure must contain no C / C++ /
  Fortran dependencies. Any FFI must live behind an explicit opt-in feature; the
  default build stays 100% Pure Rust. Verify with
  `cargo tree -p <crate> --edges normal`.
- **Zero clippy warnings.** `cargo clippy --all-targets -- -D warnings` must
  produce no output on the crate's real feature set.
- **No panics on untrusted input.** Do not use `unwrap()` / `expect()` / `panic!`
  / `unreachable!` / `assert!` on data derived from file, network, or otherwise
  attacker-controlled input. Return typed errors (`OxiRpcError`) instead.
- **Workspace dependency inheritance.** Add dependencies to the root
  `[workspace.dependencies]` and reference them with `dep.workspace = true` in
  member crates. Prefer the latest versions available on crates.io. New
  dependencies must be Pure Rust.
- **Keep files small.** Source files should stay under 2000 lines; split larger
  modules (preserving the public API) rather than growing a single file.
- **Tests use temporary directories.** Use `std::env::temp_dir()` for scratch
  files; never hardcode absolute paths in code, tests, or docs.
- **English only.** Write code, comments, and documentation in English.

## Commit & PR Etiquette

- Keep commits focused and describe the *why*, not just the *what*.
- Update `CHANGELOG.md` (the `[Unreleased]` section) for any user-visible change.
- Add or update tests that would fail without your change.

By contributing, you agree that your contributions are licensed under the
project's Apache-2.0 license.
