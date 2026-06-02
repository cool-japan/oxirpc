//! Native async Rust client and server stub code generator.
//!
//! Generates gRPC service stubs directly from
//! [`prost_types::ServiceDescriptorProto`] without requiring `protoc` or any
//! external code-generation binary.
//!
//! # Design
//!
//! Generated code uses:
//! - `oxirpc_core::{Request, Response, Status}` for user-visible signatures
//!   (these are `pub use tonic::{...}` re-exports, so tonic interop is seamless)
//! - `tonic_prost::ProstCodec` as the wire codec (tonic-compatible two-type-param codec)
//! - `tonic::server::NamedService` + `tonic::codegen::Service` for the server wrapper
//! - `tonic::client::Grpc<T>` for the client wrapper
//! - `#[tonic::async_trait]` on the server trait (tonic re-exports `async_trait`)
//!
//! # Usage
//!
//! ```rust
//! use oxirpc_build::codegen::ServiceCodegen;
//! use prost_types::{ServiceDescriptorProto, MethodDescriptorProto};
//!
//! let codegen = ServiceCodegen::new();
//! // Pass a FileDescriptorProto or call generate_server/generate_client directly.
//! ```

use prost_types::{FileDescriptorProto, MethodDescriptorProto, ServiceDescriptorProto};

// ---------------------------------------------------------------------------
// Snake-case conversion
// ---------------------------------------------------------------------------

/// Convert a `CamelCase` identifier to `snake_case`.
///
/// `SayHello` → `say_hello`, `GetHTTPResponse` → `get_h_t_t_p_response`
/// (follows standard Rust snake-case rules for consecutive uppercase letters).
pub fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Type-name helpers
// ---------------------------------------------------------------------------

/// Extract the simple (unqualified) Rust type name from a proto fully-qualified
/// type reference such as `.helloworld.HelloRequest`.
///
/// Leading `.` and package prefix are stripped; only the last component is
/// returned.  For example:
/// - `.helloworld.HelloRequest` → `HelloRequest`
/// - `.HelloReply` → `HelloReply`
/// - `HelloRequest` → `HelloRequest`
fn simple_type_name(fq_name: &str) -> &str {
    let without_leading = fq_name.trim_start_matches('.');
    // Take the last dot-separated component.
    match without_leading.rfind('.') {
        Some(pos) => &without_leading[pos + 1..],
        None => without_leading,
    }
}

/// Determine the RPC streaming kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcKind {
    /// Unary: single request, single response.
    Unary,
    /// Server streaming: single request, response stream.
    ServerStreaming,
    /// Client streaming: request stream, single response.
    ClientStreaming,
    /// Bidirectional streaming: both sides stream.
    Bidi,
}

impl RpcKind {
    fn from_method(m: &MethodDescriptorProto) -> Self {
        match (m.client_streaming(), m.server_streaming()) {
            (false, false) => RpcKind::Unary,
            (false, true) => RpcKind::ServerStreaming,
            (true, false) => RpcKind::ClientStreaming,
            (true, true) => RpcKind::Bidi,
        }
    }
}

// ---------------------------------------------------------------------------
// ServiceCodegen
// ---------------------------------------------------------------------------

/// Configuration for generating gRPC service stubs from a
/// [`FileDescriptorProto`].
///
/// The generated Rust source uses:
/// - `oxirpc_core::{Request, Response, Status}` in user-visible signatures.
/// - `tonic_prost::ProstCodec` as the wire codec.
/// - `tonic::server::NamedService` and `tonic::codegen::Service` for the
///   server wrapper.
/// - `tonic::client::Grpc<T>` for the client wrapper.
/// - `#[tonic::async_trait]` on the generated server trait.
///
/// Downstream crates that `include!` the generated file must add
/// `tonic`, `tonic_prost`, and `oxirpc_core` as dependencies.
#[derive(Debug, Clone)]
pub struct ServiceCodegen {
    /// Whether to emit the client module (default: `true`).
    pub emit_client: bool,
    /// Whether to emit the server module (default: `true`).
    pub emit_server: bool,
    /// Proto path prefix for message types (default: `"super"`).
    ///
    /// Determines how generated method signatures refer to proto messages.
    /// `"super"` works when the generated file is `include!`-ed inside a
    /// module that lives next to the prost-generated message types.
    pub type_path: String,
    /// The codec path used in generated code (default: `"tonic_prost::ProstCodec"`).
    ///
    /// Override only if you provide a custom codec that implements
    /// `tonic::codec::Codec`.
    pub codec_path: String,
}

impl ServiceCodegen {
    /// Create a new [`ServiceCodegen`] with default settings.
    pub fn new() -> Self {
        Self {
            emit_client: true,
            emit_server: true,
            type_path: "super".to_owned(),
            codec_path: "tonic_prost::ProstCodec".to_owned(),
        }
    }

    /// Generate all service stubs for every service declared in `file`.
    ///
    /// Returns a string of valid Rust source code.  Write it to a `.rs` file
    /// and `include!` it in your crate.
    pub fn generate(&self, file: &FileDescriptorProto) -> String {
        let pkg = file.package().to_owned();
        let mut out = String::new();

        for svc in &file.service {
            if self.emit_server {
                out.push_str(&self.generate_server(&pkg, svc));
                out.push('\n');
            }
            if self.emit_client {
                out.push_str(&self.generate_client(&pkg, svc));
                out.push('\n');
            }
        }

        out
    }

    /// Generate the server module for a single service.
    ///
    /// The returned string is a `pub mod <name>_server { … }` block.
    pub fn generate_server(&self, pkg: &str, svc: &ServiceDescriptorProto) -> String {
        let svc_name = svc.name().to_owned();
        let mod_name = format!("{}_server", to_snake_case(&svc_name));
        let server_struct = format!("{svc_name}Server");
        let trait_name = svc_name.clone();

        // Full service name (e.g. "helloworld.Greeter")
        let full_svc_name = if pkg.is_empty() {
            svc_name.clone()
        } else {
            format!("{pkg}.{svc_name}")
        };

        // ── trait methods ──
        let trait_methods = self.generate_server_trait_methods(svc);

        // ── dispatch match arms ──
        let dispatch_arms = self.generate_server_dispatch_arms(pkg, &svc_name, svc);

        format!(
            r#"/// Generated server module for `{full_svc_name}`.
pub mod {mod_name} {{
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
    )]
    use oxirpc_core::{{Request, Response, Status}};
    use tonic::codegen::*;

    /// Server trait — implement this for your service logic.
    #[tonic::async_trait]
    pub trait {trait_name} : std::marker::Send + std::marker::Sync + 'static {{
{trait_methods}
    }}

    /// Tower [`Service`](tonic::codegen::Service) wrapper that dispatches to a
    /// [`{trait_name}`] implementation.
    #[derive(Debug)]
    pub struct {server_struct}<T> {{
        inner: std::sync::Arc<T>,
        accept_compression_encodings: tonic::codec::EnabledCompressionEncodings,
        send_compression_encodings: tonic::codec::EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }}

    impl<T: {trait_name}> {server_struct}<T> {{
        /// Wrap an implementation in a new server wrapper.
        pub fn new(inner: T) -> Self {{
            Self::from_arc(std::sync::Arc::new(inner))
        }}

        /// Wrap a pre-existing `Arc<T>`.
        pub fn from_arc(inner: std::sync::Arc<T>) -> Self {{
            Self {{
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }}
        }}

        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {{
            self.accept_compression_encodings.enable(encoding);
            self
        }}

        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {{
            self.send_compression_encodings.enable(encoding);
            self
        }}

        /// Limits the maximum size of a decoded message (default: 4 MB).
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {{
            self.max_decoding_message_size = Some(limit);
            self
        }}

        /// Limits the maximum size of an encoded message (default: `usize::MAX`).
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {{
            self.max_encoding_message_size = Some(limit);
            self
        }}
    }}

    impl<T> Clone for {server_struct}<T> {{
        fn clone(&self) -> Self {{
            Self {{
                inner: self.inner.clone(),
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }}
        }}
    }}

    impl<T, B> tonic::codegen::Service<http::Request<B>> for {server_struct}<T>
    where
        T: {trait_name},
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {{
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {{
            std::task::Poll::Ready(Ok(()))
        }}

        fn call(&mut self, req: http::Request<B>) -> Self::Future {{
            match req.uri().path() {{
{dispatch_arms}
                _ => Box::pin(async move {{
                    let mut response = http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }}),
            }}
        }}
    }}

    /// Generated gRPC service name.
    pub const SERVICE_NAME: &str = "{full_svc_name}";

    impl<T: {trait_name}> tonic::server::NamedService for {server_struct}<T> {{
        const NAME: &'static str = SERVICE_NAME;
    }}
}}
"#,
        )
    }

    /// Generate all `async fn` declarations for the server trait.
    fn generate_server_trait_methods(&self, svc: &ServiceDescriptorProto) -> String {
        let mut out = String::new();
        for method in &svc.method {
            out.push_str(&self.generate_server_trait_method(method, svc));
        }
        out
    }

    /// Generate a single server trait method declaration (not the body).
    fn generate_server_trait_method(
        &self,
        method: &MethodDescriptorProto,
        svc: &ServiceDescriptorProto,
    ) -> String {
        let method_name = to_snake_case(method.name());
        let req_type = simple_type_name(method.input_type());
        let res_type = simple_type_name(method.output_type());
        let type_path = &self.type_path;
        // svc is unused in the match but kept for the method signature for context.
        let _ = svc;

        match RpcKind::from_method(method) {
            RpcKind::Unary => format!(
                "        async fn {method_name}(\n\
                 \t\t    &self,\n\
                 \t\t    request: Request<{type_path}::{req_type}>,\n\
                 \t\t) -> std::result::Result<Response<{type_path}::{res_type}>, Status>;\n"
            ),
            RpcKind::ServerStreaming => {
                // Associated type for the response stream.
                let stream_type = format!("{}Stream", method.name());
                format!(
                    "        /// Server streaming response type for the `{method_name}` method.\n\
                     \t\ttype {stream_type}: tonic::codegen::tokio_stream::Stream<\n\
                     \t\t    Item = std::result::Result<{type_path}::{res_type}, Status>,\n\
                     \t\t> + std::marker::Send + 'static;\n\n\
                     \t\tasync fn {method_name}(\n\
                     \t\t    &self,\n\
                     \t\t    request: Request<{type_path}::{req_type}>,\n\
                     \t\t) -> std::result::Result<Response<Self::{stream_type}>, Status>;\n"
                )
            }
            RpcKind::ClientStreaming => format!(
                "        async fn {method_name}(\n\
                 \t\t    &self,\n\
                 \t\t    request: Request<tonic::Streaming<{type_path}::{req_type}>>,\n\
                 \t\t) -> std::result::Result<Response<{type_path}::{res_type}>, Status>;\n"
            ),
            RpcKind::Bidi => {
                let stream_type = format!("{}Stream", method.name());
                format!(
                    "        /// Server streaming response type for the `{method_name}` method.\n\
                     \t\ttype {stream_type}: tonic::codegen::tokio_stream::Stream<\n\
                     \t\t    Item = std::result::Result<{type_path}::{res_type}, Status>,\n\
                     \t\t> + std::marker::Send + 'static;\n\n\
                     \t\tasync fn {method_name}(\n\
                     \t\t    &self,\n\
                     \t\t    request: Request<tonic::Streaming<{type_path}::{req_type}>>,\n\
                     \t\t) -> std::result::Result<Response<Self::{stream_type}>, Status>;\n"
                )
            }
        }
    }

    /// Generate the `match req.uri().path()` dispatch arms for each method.
    fn generate_server_dispatch_arms(
        &self,
        pkg: &str,
        svc_name: &str,
        svc: &ServiceDescriptorProto,
    ) -> String {
        let mut out = String::new();
        for method in &svc.method {
            let method_proto_name = method.name();
            let full_svc = if pkg.is_empty() {
                svc_name.to_owned()
            } else {
                format!("{pkg}.{svc_name}")
            };
            let path = format!("/{full_svc}/{method_proto_name}");
            let arm = self.generate_server_dispatch_arm(&path, method, svc_name);
            out.push_str(&arm);
        }
        out
    }

    /// Generate one match arm for a single method.
    fn generate_server_dispatch_arm(
        &self,
        path: &str,
        method: &MethodDescriptorProto,
        svc_name: &str,
    ) -> String {
        let method_fn = to_snake_case(method.name());
        let method_proto_name = method.name();
        let req_type = simple_type_name(method.input_type());
        let res_type = simple_type_name(method.output_type());
        let type_path = &self.type_path;
        let codec_path = &self.codec_path;

        match RpcKind::from_method(method) {
            RpcKind::Unary => format!(
                "                \"{path}\" => {{\n\
                 \t\t\t\t\t#[allow(non_camel_case_types)]\n\
                 \t\t\t\t\tstruct {method_proto_name}Svc<T: {svc_name}>(pub std::sync::Arc<T>);\n\
                 \n\
                 \t\t\t\t\timpl<T: {svc_name}> tonic::server::UnaryService<{type_path}::{req_type}>\n\
                 \t\t\t\t\t\tfor {method_proto_name}Svc<T>\n\
                 \t\t\t\t\t{{\n\
                 \t\t\t\t\t\ttype Response = {type_path}::{res_type};\n\
                 \t\t\t\t\t\ttype Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;\n\
                 \n\
                 \t\t\t\t\t\tfn call(\n\
                 \t\t\t\t\t\t\t&mut self,\n\
                 \t\t\t\t\t\t\trequest: tonic::Request<{type_path}::{req_type}>,\n\
                 \t\t\t\t\t\t) -> Self::Future {{\n\
                 \t\t\t\t\t\t\tlet inner = std::sync::Arc::clone(&self.0);\n\
                 \t\t\t\t\t\t\tlet fut = async move {{\n\
                 \t\t\t\t\t\t\t\t<T as {svc_name}>::{method_fn}(&inner, request).await\n\
                 \t\t\t\t\t\t\t}};\n\
                 \t\t\t\t\t\t\tBox::pin(fut)\n\
                 \t\t\t\t\t\t}}\n\
                 \t\t\t\t\t}}\n\
                 \n\
                 \t\t\t\t\tlet accept_compression_encodings = self.accept_compression_encodings;\n\
                 \t\t\t\t\tlet send_compression_encodings = self.send_compression_encodings;\n\
                 \t\t\t\t\tlet max_decoding_message_size = self.max_decoding_message_size;\n\
                 \t\t\t\t\tlet max_encoding_message_size = self.max_encoding_message_size;\n\
                 \t\t\t\t\tlet inner = self.inner.clone();\n\
                 \t\t\t\t\tlet fut = async move {{\n\
                 \t\t\t\t\t\tlet method = {method_proto_name}Svc(inner);\n\
                 \t\t\t\t\t\tlet codec = {codec_path}::<\n\
                 \t\t\t\t\t\t\t{type_path}::{res_type},\n\
                 \t\t\t\t\t\t\t{type_path}::{req_type},\n\
                 \t\t\t\t\t\t>::default();\n\
                 \t\t\t\t\t\tlet mut grpc = tonic::server::Grpc::new(codec)\n\
                 \t\t\t\t\t\t\t.apply_compression_config(\n\
                 \t\t\t\t\t\t\t\taccept_compression_encodings,\n\
                 \t\t\t\t\t\t\t\tsend_compression_encodings,\n\
                 \t\t\t\t\t\t\t)\n\
                 \t\t\t\t\t\t\t.apply_max_message_size_config(\n\
                 \t\t\t\t\t\t\t\tmax_decoding_message_size,\n\
                 \t\t\t\t\t\t\t\tmax_encoding_message_size,\n\
                 \t\t\t\t\t\t\t);\n\
                 \t\t\t\t\t\tlet res = grpc.unary(method, req).await;\n\
                 \t\t\t\t\t\tOk(res)\n\
                 \t\t\t\t\t}};\n\
                 \t\t\t\t\tBox::pin(fut)\n\
                 \t\t\t\t}}\n"
            ),

            RpcKind::ServerStreaming => {
                let stream_type = format!("{}Stream", method_proto_name);
                format!(
                    "                \"{path}\" => {{\n\
                     \t\t\t\t\t#[allow(non_camel_case_types)]\n\
                     \t\t\t\t\tstruct {method_proto_name}Svc<T: {svc_name}>(pub std::sync::Arc<T>);\n\
                     \n\
                     \t\t\t\t\timpl<T: {svc_name}>\n\
                     \t\t\t\t\t\ttonic::server::ServerStreamingService<{type_path}::{req_type}>\n\
                     \t\t\t\t\t\tfor {method_proto_name}Svc<T>\n\
                     \t\t\t\t\t{{\n\
                     \t\t\t\t\t\ttype Response = {type_path}::{res_type};\n\
                     \t\t\t\t\t\ttype ResponseStream = T::{stream_type};\n\
                     \t\t\t\t\t\ttype Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;\n\
                     \n\
                     \t\t\t\t\t\tfn call(\n\
                     \t\t\t\t\t\t\t&mut self,\n\
                     \t\t\t\t\t\t\trequest: tonic::Request<{type_path}::{req_type}>,\n\
                     \t\t\t\t\t\t) -> Self::Future {{\n\
                     \t\t\t\t\t\t\tlet inner = std::sync::Arc::clone(&self.0);\n\
                     \t\t\t\t\t\t\tlet fut = async move {{\n\
                     \t\t\t\t\t\t\t\t<T as {svc_name}>::{method_fn}(&inner, request).await\n\
                     \t\t\t\t\t\t\t}};\n\
                     \t\t\t\t\t\t\tBox::pin(fut)\n\
                     \t\t\t\t\t\t}}\n\
                     \t\t\t\t\t}}\n\
                     \n\
                     \t\t\t\t\tlet accept_compression_encodings = self.accept_compression_encodings;\n\
                     \t\t\t\t\tlet send_compression_encodings = self.send_compression_encodings;\n\
                     \t\t\t\t\tlet max_decoding_message_size = self.max_decoding_message_size;\n\
                     \t\t\t\t\tlet max_encoding_message_size = self.max_encoding_message_size;\n\
                     \t\t\t\t\tlet inner = self.inner.clone();\n\
                     \t\t\t\t\tlet fut = async move {{\n\
                     \t\t\t\t\t\tlet method = {method_proto_name}Svc(inner);\n\
                     \t\t\t\t\t\tlet codec = {codec_path}::<\n\
                     \t\t\t\t\t\t\t{type_path}::{res_type},\n\
                     \t\t\t\t\t\t\t{type_path}::{req_type},\n\
                     \t\t\t\t\t\t>::default();\n\
                     \t\t\t\t\t\tlet mut grpc = tonic::server::Grpc::new(codec)\n\
                     \t\t\t\t\t\t\t.apply_compression_config(\n\
                     \t\t\t\t\t\t\t\taccept_compression_encodings,\n\
                     \t\t\t\t\t\t\t\tsend_compression_encodings,\n\
                     \t\t\t\t\t\t\t)\n\
                     \t\t\t\t\t\t\t.apply_max_message_size_config(\n\
                     \t\t\t\t\t\t\t\tmax_decoding_message_size,\n\
                     \t\t\t\t\t\t\t\tmax_encoding_message_size,\n\
                     \t\t\t\t\t\t\t);\n\
                     \t\t\t\t\t\tlet res = grpc.server_streaming(method, req).await;\n\
                     \t\t\t\t\t\tOk(res)\n\
                     \t\t\t\t\t}};\n\
                     \t\t\t\t\tBox::pin(fut)\n\
                     \t\t\t\t}}\n"
                )
            }

            RpcKind::ClientStreaming => format!(
                "                \"{path}\" => {{\n\
                 \t\t\t\t\t#[allow(non_camel_case_types)]\n\
                 \t\t\t\t\tstruct {method_proto_name}Svc<T: {svc_name}>(pub std::sync::Arc<T>);\n\
                 \n\
                 \t\t\t\t\timpl<T: {svc_name}>\n\
                 \t\t\t\t\t\ttonic::server::ClientStreamingService<{type_path}::{req_type}>\n\
                 \t\t\t\t\t\tfor {method_proto_name}Svc<T>\n\
                 \t\t\t\t\t{{\n\
                 \t\t\t\t\t\ttype Response = {type_path}::{res_type};\n\
                 \t\t\t\t\t\ttype Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;\n\
                 \n\
                 \t\t\t\t\t\tfn call(\n\
                 \t\t\t\t\t\t\t&mut self,\n\
                 \t\t\t\t\t\t\trequest: tonic::Request<tonic::Streaming<{type_path}::{req_type}>>,\n\
                 \t\t\t\t\t\t) -> Self::Future {{\n\
                 \t\t\t\t\t\t\tlet inner = std::sync::Arc::clone(&self.0);\n\
                 \t\t\t\t\t\t\tlet fut = async move {{\n\
                 \t\t\t\t\t\t\t\t<T as {svc_name}>::{method_fn}(&inner, request).await\n\
                 \t\t\t\t\t\t\t}};\n\
                 \t\t\t\t\t\t\tBox::pin(fut)\n\
                 \t\t\t\t\t\t}}\n\
                 \t\t\t\t\t}}\n\
                 \n\
                 \t\t\t\t\tlet accept_compression_encodings = self.accept_compression_encodings;\n\
                 \t\t\t\t\tlet send_compression_encodings = self.send_compression_encodings;\n\
                 \t\t\t\t\tlet max_decoding_message_size = self.max_decoding_message_size;\n\
                 \t\t\t\t\tlet max_encoding_message_size = self.max_encoding_message_size;\n\
                 \t\t\t\t\tlet inner = self.inner.clone();\n\
                 \t\t\t\t\tlet fut = async move {{\n\
                 \t\t\t\t\t\tlet method = {method_proto_name}Svc(inner);\n\
                 \t\t\t\t\t\tlet codec = {codec_path}::<\n\
                 \t\t\t\t\t\t\t{type_path}::{res_type},\n\
                 \t\t\t\t\t\t\t{type_path}::{req_type},\n\
                 \t\t\t\t\t\t>::default();\n\
                 \t\t\t\t\t\tlet mut grpc = tonic::server::Grpc::new(codec)\n\
                 \t\t\t\t\t\t\t.apply_compression_config(\n\
                 \t\t\t\t\t\t\t\taccept_compression_encodings,\n\
                 \t\t\t\t\t\t\t\tsend_compression_encodings,\n\
                 \t\t\t\t\t\t\t)\n\
                 \t\t\t\t\t\t\t.apply_max_message_size_config(\n\
                 \t\t\t\t\t\t\t\tmax_decoding_message_size,\n\
                 \t\t\t\t\t\t\t\tmax_encoding_message_size,\n\
                 \t\t\t\t\t\t\t);\n\
                 \t\t\t\t\t\tlet res = grpc.client_streaming(method, req).await;\n\
                 \t\t\t\t\t\tOk(res)\n\
                 \t\t\t\t\t}};\n\
                 \t\t\t\t\tBox::pin(fut)\n\
                 \t\t\t\t}}\n"
            ),

            RpcKind::Bidi => {
                let stream_type = format!("{}Stream", method_proto_name);
                format!(
                    "                \"{path}\" => {{\n\
                     \t\t\t\t\t#[allow(non_camel_case_types)]\n\
                     \t\t\t\t\tstruct {method_proto_name}Svc<T: {svc_name}>(pub std::sync::Arc<T>);\n\
                     \n\
                     \t\t\t\t\timpl<T: {svc_name}>\n\
                     \t\t\t\t\t\ttonic::server::StreamingService<{type_path}::{req_type}>\n\
                     \t\t\t\t\t\tfor {method_proto_name}Svc<T>\n\
                     \t\t\t\t\t{{\n\
                     \t\t\t\t\t\ttype Response = {type_path}::{res_type};\n\
                     \t\t\t\t\t\ttype ResponseStream = T::{stream_type};\n\
                     \t\t\t\t\t\ttype Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;\n\
                     \n\
                     \t\t\t\t\t\tfn call(\n\
                     \t\t\t\t\t\t\t&mut self,\n\
                     \t\t\t\t\t\t\trequest: tonic::Request<tonic::Streaming<{type_path}::{req_type}>>,\n\
                     \t\t\t\t\t\t) -> Self::Future {{\n\
                     \t\t\t\t\t\t\tlet inner = std::sync::Arc::clone(&self.0);\n\
                     \t\t\t\t\t\t\tlet fut = async move {{\n\
                     \t\t\t\t\t\t\t\t<T as {svc_name}>::{method_fn}(&inner, request).await\n\
                     \t\t\t\t\t\t\t}};\n\
                     \t\t\t\t\t\t\tBox::pin(fut)\n\
                     \t\t\t\t\t\t}}\n\
                     \t\t\t\t\t}}\n\
                     \n\
                     \t\t\t\t\tlet accept_compression_encodings = self.accept_compression_encodings;\n\
                     \t\t\t\t\tlet send_compression_encodings = self.send_compression_encodings;\n\
                     \t\t\t\t\tlet max_decoding_message_size = self.max_decoding_message_size;\n\
                     \t\t\t\t\tlet max_encoding_message_size = self.max_encoding_message_size;\n\
                     \t\t\t\t\tlet inner = self.inner.clone();\n\
                     \t\t\t\t\tlet fut = async move {{\n\
                     \t\t\t\t\t\tlet method = {method_proto_name}Svc(inner);\n\
                     \t\t\t\t\t\tlet codec = {codec_path}::<\n\
                     \t\t\t\t\t\t\t{type_path}::{res_type},\n\
                     \t\t\t\t\t\t\t{type_path}::{req_type},\n\
                     \t\t\t\t\t\t>::default();\n\
                     \t\t\t\t\t\tlet mut grpc = tonic::server::Grpc::new(codec)\n\
                     \t\t\t\t\t\t\t.apply_compression_config(\n\
                     \t\t\t\t\t\t\t\taccept_compression_encodings,\n\
                     \t\t\t\t\t\t\t\tsend_compression_encodings,\n\
                     \t\t\t\t\t\t\t)\n\
                     \t\t\t\t\t\t\t.apply_max_message_size_config(\n\
                     \t\t\t\t\t\t\t\tmax_decoding_message_size,\n\
                     \t\t\t\t\t\t\t\tmax_encoding_message_size,\n\
                     \t\t\t\t\t\t\t);\n\
                     \t\t\t\t\t\tlet res = grpc.streaming(method, req).await;\n\
                     \t\t\t\t\t\tOk(res)\n\
                     \t\t\t\t\t}};\n\
                     \t\t\t\t\tBox::pin(fut)\n\
                     \t\t\t\t}}\n"
                )
            }
        }
    }

    // -------------------------------------------------------------------------
    // Client codegen
    // -------------------------------------------------------------------------

    /// Generate the client module for a single service.
    ///
    /// The returned string is a `pub mod <name>_client { … }` block.
    pub fn generate_client(&self, pkg: &str, svc: &ServiceDescriptorProto) -> String {
        let svc_name = svc.name().to_owned();
        let mod_name = format!("{}_client", to_snake_case(&svc_name));
        let client_struct = format!("{svc_name}Client");

        let full_svc_name = if pkg.is_empty() {
            svc_name.clone()
        } else {
            format!("{pkg}.{svc_name}")
        };

        let methods = self.generate_client_methods(pkg, &svc_name, svc);

        format!(
            r#"/// Generated client module for `{full_svc_name}`.
pub mod {mod_name} {{
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        unused_imports,
        clippy::wildcard_imports,
    )]
    use oxirpc_core::{{Request, Response, Status}};
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;

    /// Generated gRPC client for `{full_svc_name}`.
    #[derive(Debug, Clone)]
    pub struct {client_struct}<T> {{
        inner: tonic::client::Grpc<T>,
    }}

    impl {client_struct}<tonic::transport::Channel> {{
        /// Attempt to create a new client by connecting to the given endpoint.
        pub async fn connect<D>(dst: D) -> std::result::Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {{
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }}
    }}

    impl<T> {client_struct}<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {{
        /// Create a new client from an inner service.
        pub fn new(inner: T) -> Self {{
            let inner = tonic::client::Grpc::new(inner);
            Self {{ inner }}
        }}

        /// Create a new client from an inner service, overriding the URI origin.
        pub fn with_origin(inner: T, origin: Uri) -> Self {{
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self {{ inner }}
        }}

        /// Compress requests with the given encoding.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {{
            self.inner = self.inner.send_compressed(encoding);
            self
        }}

        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {{
            self.inner = self.inner.accept_compressed(encoding);
            self
        }}

        /// Limits the maximum size of a decoded message (default: 4 MB).
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {{
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }}

        /// Limits the maximum size of an encoded message (default: `usize::MAX`).
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {{
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }}

{methods}
    }}
}}
"#,
        )
    }

    /// Generate all `pub async fn` method bodies for the client impl block.
    fn generate_client_methods(
        &self,
        pkg: &str,
        svc_name: &str,
        svc: &ServiceDescriptorProto,
    ) -> String {
        let mut out = String::new();
        let full_svc_name = if pkg.is_empty() {
            svc_name.to_owned()
        } else {
            format!("{pkg}.{svc_name}")
        };

        for method in &svc.method {
            out.push_str(&self.generate_client_method(&full_svc_name, method));
        }
        out
    }

    /// Generate a single client method.
    fn generate_client_method(
        &self,
        full_svc_name: &str,
        method: &MethodDescriptorProto,
    ) -> String {
        let method_fn = to_snake_case(method.name());
        let method_proto_name = method.name();
        let req_type = simple_type_name(method.input_type());
        let res_type = simple_type_name(method.output_type());
        let type_path = &self.type_path;
        let codec_path = &self.codec_path;
        // Build the static path string e.g. "/helloworld.Greeter/SayHello"
        let path = format!("/{full_svc_name}/{method_proto_name}");

        match RpcKind::from_method(method) {
            // Client-side codec: ProstCodec<Req, Res> because the client
            // *encodes* requests (Encode = Req) and *decodes* responses (Decode = Res).
            RpcKind::Unary => format!(
                "        /// Unary RPC: `{method_proto_name}`.\n\
                 \t\tpub async fn {method_fn}(\n\
                 \t\t    &mut self,\n\
                 \t\t    request: impl tonic::IntoRequest<{type_path}::{req_type}>,\n\
                 \t\t) -> std::result::Result<Response<{type_path}::{res_type}>, Status> {{\n\
                 \t\t    self.inner.ready().await.map_err(|e| {{\n\
                 \t\t        tonic::Status::unknown(format!(\"Service was not ready: {{}}\", e.into()))\n\
                 \t\t    }})?;\n\
                 \t\t    let codec = {codec_path}::<\n\
                 \t\t        {type_path}::{req_type},\n\
                 \t\t        {type_path}::{res_type},\n\
                 \t\t    >::default();\n\
                 \t\t    let path = http::uri::PathAndQuery::from_static(\"{path}\");\n\
                 \t\t    let mut req = request.into_request();\n\
                 \t\t    req.extensions_mut().insert(tonic::GrpcMethod::new(\"{full_svc_name}\", \"{method_proto_name}\"));\n\
                 \t\t    self.inner.unary(req, path, codec).await\n\
                 \t\t}}\n\n"
            ),
            RpcKind::ServerStreaming => format!(
                "        /// Server-streaming RPC: `{method_proto_name}`.\n\
                 \t\tpub async fn {method_fn}(\n\
                 \t\t    &mut self,\n\
                 \t\t    request: impl tonic::IntoRequest<{type_path}::{req_type}>,\n\
                 \t\t) -> std::result::Result<Response<tonic::codec::Streaming<{type_path}::{res_type}>>, Status> {{\n\
                 \t\t    self.inner.ready().await.map_err(|e| {{\n\
                 \t\t        tonic::Status::unknown(format!(\"Service was not ready: {{}}\", e.into()))\n\
                 \t\t    }})?;\n\
                 \t\t    let codec = {codec_path}::<\n\
                 \t\t        {type_path}::{req_type},\n\
                 \t\t        {type_path}::{res_type},\n\
                 \t\t    >::default();\n\
                 \t\t    let path = http::uri::PathAndQuery::from_static(\"{path}\");\n\
                 \t\t    let mut req = request.into_request();\n\
                 \t\t    req.extensions_mut().insert(tonic::GrpcMethod::new(\"{full_svc_name}\", \"{method_proto_name}\"));\n\
                 \t\t    self.inner.server_streaming(req, path, codec).await\n\
                 \t\t}}\n\n"
            ),
            RpcKind::ClientStreaming => format!(
                "        /// Client-streaming RPC: `{method_proto_name}`.\n\
                 \t\tpub async fn {method_fn}(\n\
                 \t\t    &mut self,\n\
                 \t\t    request: impl tonic::IntoStreamingRequest<Message = {type_path}::{req_type}>,\n\
                 \t\t) -> std::result::Result<Response<{type_path}::{res_type}>, Status> {{\n\
                 \t\t    self.inner.ready().await.map_err(|e| {{\n\
                 \t\t        tonic::Status::unknown(format!(\"Service was not ready: {{}}\", e.into()))\n\
                 \t\t    }})?;\n\
                 \t\t    let codec = {codec_path}::<\n\
                 \t\t        {type_path}::{req_type},\n\
                 \t\t        {type_path}::{res_type},\n\
                 \t\t    >::default();\n\
                 \t\t    let path = http::uri::PathAndQuery::from_static(\"{path}\");\n\
                 \t\t    let mut req = request.into_streaming_request();\n\
                 \t\t    req.extensions_mut().insert(tonic::GrpcMethod::new(\"{full_svc_name}\", \"{method_proto_name}\"));\n\
                 \t\t    self.inner.client_streaming(req, path, codec).await\n\
                 \t\t}}\n\n"
            ),
            RpcKind::Bidi => format!(
                "        /// Bidirectional-streaming RPC: `{method_proto_name}`.\n\
                 \t\tpub async fn {method_fn}(\n\
                 \t\t    &mut self,\n\
                 \t\t    request: impl tonic::IntoStreamingRequest<Message = {type_path}::{req_type}>,\n\
                 \t\t) -> std::result::Result<Response<tonic::codec::Streaming<{type_path}::{res_type}>>, Status> {{\n\
                 \t\t    self.inner.ready().await.map_err(|e| {{\n\
                 \t\t        tonic::Status::unknown(format!(\"Service was not ready: {{}}\", e.into()))\n\
                 \t\t    }})?;\n\
                 \t\t    let codec = {codec_path}::<\n\
                 \t\t        {type_path}::{req_type},\n\
                 \t\t        {type_path}::{res_type},\n\
                 \t\t    >::default();\n\
                 \t\t    let path = http::uri::PathAndQuery::from_static(\"{path}\");\n\
                 \t\t    let mut req = request.into_streaming_request();\n\
                 \t\t    req.extensions_mut().insert(tonic::GrpcMethod::new(\"{full_svc_name}\", \"{method_proto_name}\"));\n\
                 \t\t    self.inner.streaming(req, path, codec).await\n\
                 \t\t}}\n\n"
            ),
        }
    }
}

impl Default for ServiceCodegen {
    fn default() -> Self {
        Self::new()
    }
}
