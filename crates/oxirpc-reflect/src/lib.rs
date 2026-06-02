#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! `oxirpc-reflect` — gRPC server reflection (v1 + v1alpha) for OxiRPC.
//!
//! Wraps [`tonic_reflection`]'s `server::Builder` into an ergonomic helper that
//! accepts encoded `FileDescriptorSet` bytes (the output of `prost::Message::encode_to_vec`
//! on a `prost_types::FileDescriptorSet`) and returns a mountable reflection service.
//!
//! A fully **native** implementation is also available via
//! [`NativeReflectionServiceV1`] / [`NativeReflectionServiceV1Alpha`] — these
//! speak the standard bidi-streaming `ServerReflectionInfo` RPC without depending
//! on `tonic_reflection` internally.
//!
//! # Example — static bytes
//!
//! ```rust,no_run
//! # fn get_fds_bytes() -> &'static [u8] { &[] }
//! use oxirpc_reflect::reflection_service_from_static;
//!
//! let svc = reflection_service_from_static(get_fds_bytes())
//!     .expect("reflection_service");
//! // Mount `svc` on your tonic Server via `add_service(svc)`.
//! ```
//!
//! # Example — combined builder
//!
//! ```rust,no_run
//! use oxirpc_reflect::ReflectionBuilder;
//!
//! let services = ReflectionBuilder::new()
//!     .register_file_descriptor_set(vec![])   // your encoded FDS bytes
//!     .include_v1alpha(true)
//!     .build()
//!     .expect("reflection services");
//! // Mount services.v1 and (optionally) services.v1alpha.
//! ```
//!
//! # Example — pre-decoded pool
//!
//! ```rust,no_run
//! use oxirpc_reflect::{DescriptorPoolBuilder, reflection_service_from_pool};
//!
//! let pool = DescriptorPoolBuilder::new()
//!     .register(prost_types::FileDescriptorSet { file: vec![] })
//!     .build();
//! let svc = reflection_service_from_pool(pool).expect("reflection_service");
//! ```
//!
//! # Example — native service (no tonic-reflection)
//!
//! ```rust,no_run
//! use oxirpc_reflect::ReflectionBuilder;
//!
//! let (v1, _v1alpha) = ReflectionBuilder::new()
//!     .register_file_descriptor_set(vec![])
//!     .build_native();
//! // Mount v1 via tonic::transport::Server::builder().add_service(v1).
//! ```

use std::sync::Arc;

use prost::Message as _;
use prost_types::FileDescriptorSet;

pub use tonic_reflection::server::v1::{ServerReflection, ServerReflectionServer};

use tonic_reflection::server::Builder;

pub mod proto;
pub mod service;

pub use service::{NativeReflectionServiceV1, NativeReflectionServiceV1Alpha};

// ─── DescriptorPool ──────────────────────────────────────────────────────────

/// A collection of pre-decoded `FileDescriptorSet` values ready for reflection registration.
///
/// Construct via [`DescriptorPoolBuilder`] or directly from `Vec<FileDescriptorSet>`.
#[derive(Debug, Clone)]
pub struct DescriptorPool(Vec<prost_types::FileDescriptorSet>);

impl DescriptorPool {
    /// Returns the raw file descriptor sets in this pool.
    pub fn file_descriptor_sets(&self) -> &[prost_types::FileDescriptorSet] {
        &self.0
    }

    /// Append a [`FileDescriptorSet`] to the pool.
    ///
    /// This enables dynamic service registration at runtime after initial construction.
    pub fn add_service(&mut self, fds: prost_types::FileDescriptorSet) {
        self.0.push(fds);
    }

    /// Remove [`FileDescriptorSet`] entries that contain the named service.
    ///
    /// Only removes an FDS entry if it contains **exactly one** service and that
    /// service matches `name`. Entries containing multiple services are left
    /// unchanged (documented limitation).
    ///
    /// Returns `true` if at least one entry was removed.
    pub fn remove_service(&mut self, name: &str) -> bool {
        let before = self.0.len();
        self.0.retain(|fds| {
            let services: Vec<&str> = fds
                .file
                .iter()
                .flat_map(|f| f.service.iter())
                .filter_map(|s| s.name.as_deref())
                .collect();
            // Keep this FDS if it has multiple services, OR its sole service doesn't match
            !(services.len() == 1 && services[0] == name)
        });
        self.0.len() < before
    }

    /// List all service names across all registered [`FileDescriptorSet`] entries.
    pub fn list_services(&self) -> Vec<String> {
        self.0
            .iter()
            .flat_map(|fds| fds.file.iter())
            .flat_map(|f| f.service.iter())
            .filter_map(|s| s.name.clone())
            .collect()
    }

    /// Encode all [`FileDescriptorSet`]s into a single merged proto-binary blob.
    ///
    /// The result is a proto-encoded `FileDescriptorSet` that merges all
    /// registered file descriptors into one flat list.
    pub fn encode(&self) -> Vec<u8> {
        let merged = prost_types::FileDescriptorSet {
            file: self.0.iter().flat_map(|fds| fds.file.clone()).collect(),
        };
        merged.encode_to_vec()
    }

    /// Decode a proto-binary blob into a [`DescriptorPool`].
    ///
    /// The bytes must contain a proto-encoded `FileDescriptorSet`.
    ///
    /// # Errors
    ///
    /// Returns [`ReflectError::Decode`] if the bytes are not a valid proto-encoded
    /// `FileDescriptorSet`.
    pub fn decode(bytes: &[u8]) -> Result<Self, ReflectError> {
        let fds = prost_types::FileDescriptorSet::decode(bytes).map_err(ReflectError::Decode)?;
        Ok(DescriptorPool(vec![fds]))
    }

    /// Return all extension field numbers defined in this pool that target `message_name`.
    ///
    /// Extension fields are collected from `FileDescriptorProto::extension` (top-level extensions).
    /// Nested message extensions are not included in this implementation.
    ///
    /// The `message_name` comparison is done after stripping any leading `.` from both sides.
    pub fn extension_numbers(&self, message_name: &str) -> Vec<i32> {
        self.0
            .iter()
            .flat_map(|fds| fds.file.iter())
            .flat_map(|f| f.extension.iter())
            .filter(|ext| {
                ext.extendee
                    .as_deref()
                    .map(|e| e.trim_start_matches('.') == message_name.trim_start_matches('.'))
                    .unwrap_or(false)
            })
            .filter_map(|ext| ext.number)
            .collect()
    }

    /// Find the first `FileDescriptorProto` in this pool whose `name` field equals `name`.
    pub fn find_file_by_name(&self, name: &str) -> Option<&prost_types::FileDescriptorProto> {
        self.0
            .iter()
            .flat_map(|fds| fds.file.iter())
            .find(|f| f.name.as_deref() == Some(name))
    }

    /// Find the first `FileDescriptorProto` that defines the given fully-qualified symbol.
    ///
    /// `symbol` may optionally start with `.` (it is stripped before comparison).
    ///
    /// The following symbol kinds are searched:
    /// - Top-level and nested message types.
    /// - Enum types (top-level only in this implementation).
    /// - Services and their methods.
    pub fn find_file_containing_symbol(
        &self,
        symbol: &str,
    ) -> Option<&prost_types::FileDescriptorProto> {
        let symbol = symbol.trim_start_matches('.');
        self.0
            .iter()
            .flat_map(|fds| fds.file.iter())
            .find(|f| file_contains_symbol(f, symbol))
    }

    /// Find the first `FileDescriptorProto` that defines an extension field
    /// `extension_number` on `containing_type`.
    ///
    /// `containing_type` may optionally start with `.`.
    pub fn find_file_containing_extension(
        &self,
        containing_type: &str,
        extension_number: i32,
    ) -> Option<&prost_types::FileDescriptorProto> {
        let containing_type = containing_type.trim_start_matches('.');
        self.0.iter().flat_map(|fds| fds.file.iter()).find(|f| {
            f.extension.iter().any(|ext| {
                ext.number == Some(extension_number)
                    && ext
                        .extendee
                        .as_deref()
                        .map(|e| e.trim_start_matches('.') == containing_type)
                        .unwrap_or(false)
            })
        })
    }
}

// ─── Symbol search helpers ───────────────────────────────────────────────────

/// Returns `true` if `file` contains a definition for `symbol` (no leading dot).
fn file_contains_symbol(file: &prost_types::FileDescriptorProto, symbol: &str) -> bool {
    let pkg = file.package.as_deref().unwrap_or("");

    // Check services and their methods.
    for svc in &file.service {
        let svc_name = svc.name.as_deref().unwrap_or("");
        let fqn = if pkg.is_empty() {
            svc_name.to_owned()
        } else {
            format!("{pkg}.{svc_name}")
        };
        if fqn == symbol {
            return true;
        }
        for method in &svc.method {
            let method_name = method.name.as_deref().unwrap_or("");
            let method_fqn = format!("{fqn}.{method_name}");
            if method_fqn == symbol {
                return true;
            }
        }
    }

    // Check top-level message types (and nested recursively).
    for msg in &file.message_type {
        if message_contains_symbol(msg, pkg, symbol) {
            return true;
        }
    }

    // Check top-level enum types.
    for en in &file.enum_type {
        let enum_name = en.name.as_deref().unwrap_or("");
        let fqn = if pkg.is_empty() {
            enum_name.to_owned()
        } else {
            format!("{pkg}.{enum_name}")
        };
        if fqn == symbol {
            return true;
        }
    }

    false
}

/// Recursively check if `msg` (or any nested type) matches `symbol`.
/// `parent_fqn` is the already-resolved fully-qualified prefix for the parent scope.
fn message_contains_symbol(
    msg: &prost_types::DescriptorProto,
    parent_fqn: &str,
    symbol: &str,
) -> bool {
    let msg_name = msg.name.as_deref().unwrap_or("");
    let fqn = if parent_fqn.is_empty() {
        msg_name.to_owned()
    } else {
        format!("{parent_fqn}.{msg_name}")
    };
    if fqn == symbol {
        return true;
    }
    for nested in &msg.nested_type {
        if message_contains_symbol(nested, &fqn, symbol) {
            return true;
        }
    }
    false
}

// ─── DescriptorPoolBuilder ───────────────────────────────────────────────────

/// Builder for [`DescriptorPool`] — registers pre-decoded descriptors.
///
/// Prefer this over [`ReflectionBuilder`] when you already have decoded
/// `FileDescriptorSet` values (e.g., from `oxiproto` or protox).
#[derive(Debug, Default)]
pub struct DescriptorPoolBuilder {
    sets: Vec<prost_types::FileDescriptorSet>,
}

impl DescriptorPoolBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self { sets: Vec::new() }
    }

    /// Register a pre-decoded `FileDescriptorSet`.
    pub fn register(mut self, fds: prost_types::FileDescriptorSet) -> Self {
        self.sets.push(fds);
        self
    }

    /// Register from proto-binary bytes, decoding immediately.
    ///
    /// Returns [`ReflectError::Decode`] if the bytes are not a valid `FileDescriptorSet`.
    pub fn register_bytes(mut self, bytes: &[u8]) -> Result<Self, ReflectError> {
        let fds = prost_types::FileDescriptorSet::decode(bytes).map_err(ReflectError::Decode)?;
        self.sets.push(fds);
        Ok(self)
    }

    /// Build the [`DescriptorPool`].
    pub fn build(self) -> DescriptorPool {
        DescriptorPool(self.sets)
    }
}

// ─── free functions ──────────────────────────────────────────────────────────

/// Build a gRPC server reflection service (v1 protocol) from encoded
/// `FileDescriptorSet` bytes with a `'static` lifetime.
///
/// `fds_bytes` must contain a `FileDescriptorSet` serialized as proto binary
/// (e.g., from `include_bytes!("path/to/fds.bin")` or a `pub const FDS: &[u8] = ...`).
///
/// The returned [`ServerReflectionServer`] can be added to a tonic server via
/// `tonic::transport::Server::builder().add_service(svc)`.
///
/// See [`ReflectionBuilder`] if you need combined v1 + v1alpha services, or
/// [`reflection_service_owned`] if you only have a `Vec<u8>`.
pub fn reflection_service_from_static(
    fds_bytes: &'static [u8],
) -> Result<ServerReflectionServer<impl ServerReflection>, ReflectError> {
    Builder::configure()
        .register_encoded_file_descriptor_set(fds_bytes)
        .build_v1()
        .map_err(ReflectError::Build)
}

/// Build a gRPC server reflection service (v1 protocol) from owned bytes.
///
/// The bytes are decoded into a `FileDescriptorSet` immediately; no memory is
/// leaked and no `'static` constraint is needed.  Returns [`ReflectError::Decode`]
/// if the bytes are not a valid proto-encoded `FileDescriptorSet`.
///
/// Prefer [`reflection_service_from_static`] when the bytes already have a
/// `'static` lifetime (e.g., from `include_bytes!`), or use [`ReflectionBuilder`]
/// when you need combined v1 + v1alpha output.
pub fn reflection_service_owned(
    fds_bytes: Vec<u8>,
) -> Result<ServerReflectionServer<impl ServerReflection>, ReflectError> {
    // Decode the bytes immediately so no lifetime constraint is carried into build_v1().
    let fds = FileDescriptorSet::decode(fds_bytes.as_slice()).map_err(ReflectError::Decode)?;
    Builder::configure()
        .register_file_descriptor_set(fds)
        .build_v1()
        .map_err(ReflectError::Build)
}

/// Build a gRPC server reflection service (v1alpha protocol) from encoded
/// `FileDescriptorSet` bytes with a `'static` lifetime.
///
/// Use this when the client supports only the legacy v1alpha reflection protocol.
/// For new deployments, prefer [`reflection_service_from_static`] (v1).
pub fn reflection_service_v1alpha_from_static(
    fds_bytes: &'static [u8],
) -> Result<
    tonic_reflection::server::v1alpha::ServerReflectionServer<
        impl tonic_reflection::server::v1alpha::ServerReflection,
    >,
    ReflectError,
> {
    Builder::configure()
        .register_encoded_file_descriptor_set(fds_bytes)
        .build_v1alpha()
        .map_err(ReflectError::Build)
}

/// Build a gRPC reflection service (v1) from a pre-built [`DescriptorPool`].
///
/// Iterates the pool's `FileDescriptorSet` values, registering each with
/// `tonic_reflection`. No byte decoding is performed here.
pub fn reflection_service_from_pool(
    pool: DescriptorPool,
) -> Result<ServerReflectionServer<impl ServerReflection>, ReflectError> {
    let mut builder = Builder::configure();
    for fds in pool.0 {
        builder = builder.register_file_descriptor_set(fds);
    }
    builder.build_v1().map_err(ReflectError::Build)
}

// ─── ReflectionBuilder ───────────────────────────────────────────────────────

/// A combined builder for gRPC server reflection services.
///
/// Accepts multiple `FileDescriptorSet` byte slices (owned or shared) and
/// builds v1 (and optionally v1alpha) reflection services in a single call.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_reflect::ReflectionBuilder;
/// const MY_FDS_BYTES: &[u8] = &[];
///
/// let services = ReflectionBuilder::new()
///     .register_file_descriptor_set(MY_FDS_BYTES.to_vec())
///     .include_v1alpha(true)
///     .build()
///     .unwrap();
/// // mount services.v1 and services.v1alpha.unwrap()
/// ```
///
/// # Example — filtering by service name
///
/// ```rust,no_run
/// use oxirpc_reflect::ReflectionBuilder;
///
/// // Assume `MyService` implements `tonic::server::NamedService`.
/// // Only file descriptors that contain a service named "MyService" will be exposed.
/// // (In real code `MyService` would be a generated tonic service struct.)
/// struct MyService;
/// impl tonic::server::NamedService for MyService {
///     const NAME: &'static str = "my.MyService";
/// }
///
/// let services = ReflectionBuilder::new()
///     .register_file_descriptor_set(vec![])   // your encoded FDS bytes
///     .register_service_named::<MyService>()
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Default)]
pub struct ReflectionBuilder {
    /// Shared byte buffers for each registered FileDescriptorSet.
    fds_list: Vec<Arc<[u8]>>,
    /// Pre-decoded FileDescriptorSet values registered via [`register_pool`](Self::register_pool).
    decoded_sets: Vec<prost_types::FileDescriptorSet>,
    include_v1alpha: bool,
    /// Optional allowlist of service names (bare name only, not package-qualified).
    ///
    /// When non-empty, only file descriptors containing at least one service whose
    /// *bare* name (`ServiceDescriptorProto.name`) is in this list will be passed to
    /// the tonic-reflection builder.  File descriptors with no matching service are
    /// silently dropped.  If empty, all registered file descriptors are exposed
    /// (backward-compatible default).
    service_filter: Vec<String>,
    /// Optional oxiproto-reflect DescriptorPool backend (feature = "oxiproto").
    ///
    /// When set, [`build_native`](Self::build_native) and
    /// [`build_native_v1`](Self::build_native_v1) will use this pool instead of
    /// the built-in prost-types pool.  Overrides any byte or decoded-set
    /// registrations for the native path only.
    #[cfg(feature = "oxiproto")]
    oxiproto_pool: Option<Arc<oxiproto_reflect::DescriptorPool>>,
}

impl ReflectionBuilder {
    /// Create a new, empty [`ReflectionBuilder`].
    pub fn new() -> Self {
        Self {
            fds_list: Vec::new(),
            decoded_sets: Vec::new(),
            include_v1alpha: false,
            service_filter: Vec::new(),
            #[cfg(feature = "oxiproto")]
            oxiproto_pool: None,
        }
    }

    /// Register an owned `Vec<u8>` (or anything `Into<Vec<u8>>`) as an
    /// encoded `FileDescriptorSet`.
    ///
    /// The bytes are stored in an `Arc<[u8]>` for cheap sharing.  They are
    /// decoded once per [`build`](Self::build) call.
    pub fn register_file_descriptor_set(mut self, fds: impl Into<Vec<u8>>) -> Self {
        let arc: Arc<[u8]> = fds.into().into();
        self.fds_list.push(arc);
        self
    }

    /// Register a pre-existing `Arc<[u8]>` as an encoded `FileDescriptorSet`.
    pub fn register_encoded_file_descriptor_set(mut self, fds: Arc<[u8]>) -> Self {
        self.fds_list.push(fds);
        self
    }

    /// Register a pre-built [`DescriptorPool`] alongside any byte-based registrations.
    ///
    /// This is the preferred path when you already have decoded `FileDescriptorSet`
    /// values (e.g., built via [`DescriptorPoolBuilder`]).
    pub fn register_pool(mut self, pool: DescriptorPool) -> Self {
        self.decoded_sets.extend(pool.0);
        self
    }

    /// Register an [`oxiproto_reflect::DescriptorPool`] as the backend for the
    /// native reflection service.
    ///
    /// When set, [`build_native`](Self::build_native) and
    /// [`build_native_v1`](Self::build_native_v1) will back the service with the
    /// richer prost-reflect pool rather than the built-in prost-types pool.
    ///
    /// This method is only available when the `oxiproto` feature is enabled.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "oxiproto")]
    /// # {
    /// use std::sync::Arc;
    /// use oxirpc_reflect::ReflectionBuilder;
    ///
    /// let fds_bytes: &[u8] = &[]; // replace with real FDS bytes
    /// let pool = oxiproto_reflect::pool_from_fds_bytes(fds_bytes)
    ///     .expect("valid FDS");
    /// let (v1, _v1alpha) = ReflectionBuilder::new()
    ///     .register_oxiproto_pool(Arc::new(pool))
    ///     .build_native();
    /// # }
    /// ```
    #[cfg(feature = "oxiproto")]
    pub fn register_oxiproto_pool(mut self, pool: Arc<oxiproto_reflect::DescriptorPool>) -> Self {
        self.oxiproto_pool = Some(pool);
        self
    }

    /// Register a service name filter using the [`tonic::server::NamedService`] trait.
    ///
    /// When one or more calls to this method are made, only file descriptors containing
    /// at least one service whose **bare** name matches `S::NAME` will be exposed by the
    /// reflection service.  All other file descriptors are silently dropped during
    /// [`build`](Self::build).
    ///
    /// If this method is never called (the default), **all** registered file descriptors
    /// are exposed — preserving the original behavior.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use oxirpc_reflect::ReflectionBuilder;
    ///
    /// struct MyService;
    /// impl tonic::server::NamedService for MyService {
    ///     const NAME: &'static str = "my.MyService";
    /// }
    ///
    /// let builder = ReflectionBuilder::new()
    ///     .register_file_descriptor_set(vec![])   // encoded FDS bytes
    ///     .register_service_named::<MyService>();
    /// ```
    pub fn register_service_named<S: tonic::server::NamedService>(mut self) -> Self {
        self.service_filter.push(S::NAME.to_owned());
        self
    }

    /// Whether to also build a v1alpha reflection service (default: `false`).
    ///
    /// When `true`, [`ReflectionServices::v1alpha`] will be `Some(...)`.
    pub fn include_v1alpha(mut self, v: bool) -> Self {
        self.include_v1alpha = v;
        self
    }

    /// Build the native reflection services.
    ///
    /// Returns a `(v1, v1alpha)` pair of [`NativeReflectionServiceV1`] and
    /// [`NativeReflectionServiceV1Alpha`].  Both are backed by the same
    /// descriptor pool.
    ///
    /// When the `oxiproto` feature is enabled and
    /// `register_oxiproto_pool` was called, the
    /// returned services will use the oxiproto-reflect pool as their backend.
    /// Otherwise the built-in prost-types pool is used.
    pub fn build_native(self) -> (NativeReflectionServiceV1, NativeReflectionServiceV1Alpha) {
        #[cfg(feature = "oxiproto")]
        if let Some(oxi_pool) = self.oxiproto_pool {
            let v1 =
                NativeReflectionServiceV1(service::NativeReflectionService::with_oxiproto_pool(
                    Arc::clone(&oxi_pool),
                    service::ReflectVersion::V1,
                ));
            let v1alpha = NativeReflectionServiceV1Alpha(
                service::NativeReflectionService::with_oxiproto_pool(
                    oxi_pool,
                    service::ReflectVersion::V1Alpha,
                ),
            );
            return (v1, v1alpha);
        }

        let pool = Arc::new(self.build_pool());
        let v1 = NativeReflectionServiceV1(service::NativeReflectionService::new(
            Arc::clone(&pool),
            service::ReflectVersion::V1,
        ));
        let v1alpha = NativeReflectionServiceV1Alpha(service::NativeReflectionService::new(
            pool,
            service::ReflectVersion::V1Alpha,
        ));
        (v1, v1alpha)
    }

    /// Build only the native v1 reflection service.
    ///
    /// Returns a [`NativeReflectionServiceV1`] backed by the registered descriptors.
    ///
    /// When the `oxiproto` feature is enabled and
    /// `register_oxiproto_pool` was called, the
    /// returned service will use the oxiproto-reflect pool as its backend.
    /// Otherwise the built-in prost-types pool is used.
    pub fn build_native_v1(self) -> NativeReflectionServiceV1 {
        #[cfg(feature = "oxiproto")]
        if let Some(oxi_pool) = self.oxiproto_pool {
            return NativeReflectionServiceV1(
                service::NativeReflectionService::with_oxiproto_pool(
                    oxi_pool,
                    service::ReflectVersion::V1,
                ),
            );
        }

        let pool = Arc::new(self.build_pool());
        NativeReflectionServiceV1(service::NativeReflectionService::new(
            pool,
            service::ReflectVersion::V1,
        ))
    }

    /// Consume this builder and produce a [`DescriptorPool`] from all registered descriptors.
    ///
    /// Byte-based registrations are decoded; pre-decoded pool entries are merged in.
    /// Decoding errors are silently swallowed (invalid FDS entries are skipped).
    fn build_pool(self) -> DescriptorPool {
        let Self {
            fds_list,
            decoded_sets,
            // remaining fields are not needed for the prost-types pool
            ..
        } = self;
        let mut files: Vec<prost_types::FileDescriptorSet> = Vec::new();
        for arc in &fds_list {
            if let Ok(fds) = prost_types::FileDescriptorSet::decode(arc.as_ref()) {
                files.push(fds);
            }
        }
        files.extend(decoded_sets);
        DescriptorPool(files)
    }

    /// Build the tonic-reflection-backed reflection service(s).
    ///
    /// Returns [`ReflectionServices`] containing a v1 service and, when
    /// [`include_v1alpha`](Self::include_v1alpha) was set, an optional v1alpha service.
    ///
    /// # Deprecation
    ///
    /// Prefer [`build_native`](Self::build_native) or [`build_native_v1`](Self::build_native_v1)
    /// for the fully-native implementation that does not depend on `tonic_reflection`.
    ///
    /// # Errors
    ///
    /// - [`ReflectError::Decode`] — if any registered byte slice is not a valid
    ///   proto-encoded `FileDescriptorSet`.
    /// - [`ReflectError::Build`] — if `tonic_reflection` rejects the decoded
    ///   descriptors (e.g., missing file name field).
    #[deprecated(
        note = "Use build_native() or build_native_v1() for the fully-native implementation"
    )]
    pub fn build(
        self,
    ) -> Result<
        ReflectionServices<
            impl ServerReflection,
            impl tonic_reflection::server::v1alpha::ServerReflection,
        >,
        ReflectError,
    > {
        // Destructure self to avoid partial-move issues when consuming both
        // fds_list (needs borrowing for decode) and decoded_sets (moved directly).
        let Self {
            fds_list,
            decoded_sets,
            include_v1alpha,
            service_filter,
            // oxiproto_pool is not used by the tonic-reflection path
            ..
        } = self;

        // Decode byte-based registrations.
        let decoded_from_bytes: Vec<FileDescriptorSet> = fds_list
            .iter()
            .map(|arc| FileDescriptorSet::decode(arc.as_ref()).map_err(ReflectError::Decode))
            .collect::<Result<_, _>>()?;

        // Merge pre-decoded pool entries.
        let all_decoded: Vec<FileDescriptorSet> =
            decoded_from_bytes.into_iter().chain(decoded_sets).collect();

        // Apply service name filter when one or more names were registered.
        //
        // For each FileDescriptorSet, keep only the FileDescriptorProto entries that
        // contain at least one service whose bare name is in `service_filter`.
        // FDS values that become empty after filtering are dropped entirely.
        // When `service_filter` is empty all descriptors pass through unchanged.
        let decoded: Vec<FileDescriptorSet> = if service_filter.is_empty() {
            all_decoded
        } else {
            all_decoded
                .into_iter()
                .map(|fds| {
                    let filtered_files: Vec<_> = fds
                        .file
                        .into_iter()
                        .filter(|f| {
                            f.service.iter().any(|s| {
                                s.name
                                    .as_deref()
                                    .map(|n| service_filter.iter().any(|sf| sf == n))
                                    .unwrap_or(false)
                            })
                        })
                        .collect();
                    prost_types::FileDescriptorSet {
                        file: filtered_files,
                    }
                })
                .filter(|fds| !fds.file.is_empty())
                .collect()
        };

        // Build v1
        let v1 = {
            let mut builder = Builder::configure();
            for fds in decoded.iter().cloned() {
                builder = builder.register_file_descriptor_set(fds);
            }
            builder.build_v1().map_err(ReflectError::Build)?
        };

        // Optionally build v1alpha
        let v1alpha = if include_v1alpha {
            let mut builder = Builder::configure();
            for fds in decoded {
                builder = builder.register_file_descriptor_set(fds);
            }
            Some(builder.build_v1alpha().map_err(ReflectError::Build)?)
        } else {
            None
        };

        Ok(ReflectionServices { v1, v1alpha })
    }
}

// ─── ReflectionServices ──────────────────────────────────────────────────────

/// The result of [`ReflectionBuilder::build`]: a v1 service plus an optional v1alpha service.
///
/// The type parameters `V1` and `VA` are opaque (the concrete types are internal
/// to `tonic_reflection`) — treat this struct as the named output of
/// [`ReflectionBuilder::build`].
pub struct ReflectionServices<V1, VA>
where
    V1: ServerReflection,
    VA: tonic_reflection::server::v1alpha::ServerReflection,
{
    /// gRPC reflection v1 service — mount via `tonic::Server::add_service`.
    pub v1: ServerReflectionServer<V1>,
    /// gRPC reflection v1alpha service (present when
    /// [`ReflectionBuilder::include_v1alpha`] was set to `true`).
    pub v1alpha: Option<tonic_reflection::server::v1alpha::ServerReflectionServer<VA>>,
}

// ─── ReflectError ────────────────────────────────────────────────────────────

/// Errors that can occur while constructing a gRPC reflection service.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReflectError {
    /// An error returned by the [`tonic_reflection`] builder (e.g., malformed
    /// `FileDescriptorSet`, missing file name).
    Build(tonic_reflection::server::Error),
    /// A `prost` decoding error encountered while parsing a `FileDescriptorSet`.
    Decode(prost::DecodeError),
}

impl std::fmt::Display for ReflectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(e) => write!(f, "reflection build error: {e}"),
            Self::Decode(e) => write!(f, "reflection decode error (invalid proto binary): {e}"),
        }
    }
}

impl std::error::Error for ReflectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Build(e) => Some(e),
            Self::Decode(e) => Some(e),
        }
    }
}
