//! Integration-style compile tests verifying cross-crate type compatibility.
//!
//! These tests verify that the oxirpc facade correctly re-exports types from
//! oxirpc-client and oxirpc-server, and that cross-feature type compatibility
//! is preserved. No running server is required.

// ─── client + server cross-feature ──────────────────────────────────────────

#[cfg(all(feature = "client", feature = "server"))]
mod client_server_types {
    #[test]
    fn client_and_server_builders_co_accessible() {
        // Verify both builders are accessible through the facade at the same time.
        // The facade provides them as top-level re-exports; this test exercises
        // the code path where both features are enabled simultaneously.
        let _client = oxirpc::ClientBuilder::new("http://localhost:50051");
        let _server = oxirpc::ServerBuilder::new();
    }

    #[test]
    fn core_types_shared_between_client_and_server_features() {
        use oxirpc::{Code, Status};
        // Status and Code must be unambiguous with both features enabled.
        let s = Status::ok("all good");
        assert_eq!(s.code(), Code::Ok);
        let s2 = Status::internal("error");
        assert_eq!(s2.code(), Code::Internal);
    }

    #[test]
    fn client_module_and_server_module_both_accessible() {
        // Verify the sub-modules are accessible alongside the top-level builders.
        let _ = oxirpc::client::ClientBuilder::new("http://localhost");
        let _ = oxirpc::server::ServerBuilder::new();
    }
}

// ─── client module re-exports ────────────────────────────────────────────────

#[cfg(feature = "client")]
mod client_module {
    #[test]
    fn channel_pool_type_accessible_through_client_module() {
        // ChannelPool requires connected channels to instantiate; just verify
        // the type path compiles through the facade's `client` module.
        use oxirpc::client::ChannelPool;
        let _ = std::mem::size_of::<ChannelPool>();
    }

    #[test]
    fn typed_channel_type_accessible_through_client_module() {
        use oxirpc::client::TypedChannel;
        struct MySvc;
        let _ = std::mem::size_of::<TypedChannel<MySvc>>();
    }

    #[test]
    fn rpc_metrics_type_accessible_through_client_module() {
        use oxirpc::client::RpcMetrics;
        let m = RpcMetrics::new();
        m.record_started();
        m.record_completed();
        assert_eq!(m.started(), 1);
        assert_eq!(m.completed(), 1);
        assert_eq!(m.failed(), 0);
    }

    #[test]
    fn client_builder_accessible_through_both_top_level_and_client_module() {
        // Top-level (existing path used by downstream stubs)
        let _a = oxirpc::ClientBuilder::new("http://localhost:50051");
        // Sub-module path (for users who prefer explicit namespacing)
        let _b = oxirpc::client::ClientBuilder::new("http://localhost:50051");
    }
}

// ─── server module re-exports ────────────────────────────────────────────────

#[cfg(feature = "server")]
mod server_module {
    #[test]
    fn server_builder_accessible_through_server_module() {
        let _ = oxirpc::server::ServerBuilder::new();
    }

    #[test]
    fn method_interceptor_layer_accessible_through_server_module() {
        use oxirpc::server::MethodInterceptorBuilder;
        // Verify the builder type is accessible; instantiation requires a service.
        let _ = std::mem::size_of::<MethodInterceptorBuilder>();
    }

    #[test]
    fn server_builder_accessible_through_both_top_level_and_server_module() {
        let _a = oxirpc::ServerBuilder::new();
        let _b = oxirpc::server::ServerBuilder::new();
    }
}

// ─── health + reflect cross-feature ─────────────────────────────────────────

#[cfg(all(feature = "health", feature = "reflect"))]
mod health_reflect_integration {
    #[test]
    fn health_and_reflect_builders_dont_conflict() {
        let _health = oxirpc::health::HealthBuilder::new();
        let _reflect = oxirpc::reflect::ReflectionBuilder::new();
    }
}

// ─── always-available trait bounds ───────────────────────────────────────────

#[test]
fn error_type_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<oxirpc::OxiRpcError>();
}

#[test]
fn status_implements_clone_and_debug() {
    let s = oxirpc::Status::ok("test");
    let s2 = s.clone();
    // Debug impl must not panic.
    let _ = format!("{:?}", s2);
}

#[test]
fn code_implements_eq_and_debug() {
    let c = oxirpc::Code::NotFound;
    assert_eq!(c, oxirpc::Code::NotFound);
    let _ = format!("{:?}", c);
}
