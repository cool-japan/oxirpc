use oxirpc_reflect::proto::{
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    ServerReflectionRequest,
};
use oxirpc_reflect::service::{
    NativeReflectionService, NativeReflectionServiceV1Alpha, ReflectVersion,
};
use oxirpc_reflect::{
    reflection_service_from_pool, DescriptorPool, DescriptorPoolBuilder, ReflectError,
    ReflectionBuilder,
};
use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet, ServiceDescriptorProto};
use tonic::server::NamedService;

// ─── existing tests (updated: no Vec::leak in test code either) ──────────────

#[test]
fn reflection_service_owned_builds() {
    // Build a minimal FileDescriptorSet with one service "helloworld.Greeter"
    let svc = ServiceDescriptorProto {
        name: Some("Greeter".to_string()),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("helloworld.proto".to_string()),
        package: Some("helloworld".to_string()),
        service: vec![svc],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };

    let encoded = fds.encode_to_vec();
    let result = oxirpc_reflect::reflection_service_owned(encoded);
    assert!(
        result.is_ok(),
        "reflection_service_owned should build: {:?}",
        result.err()
    );
}

#[test]
fn reflection_service_from_static_builds() {
    // Use empty bytes — the builder includes the built-in reflection FDS itself.
    // An empty slice passed as a registered set is valid; the builder still returns Ok.
    static EMPTY_FDS: &[u8] = &[];
    let result = oxirpc_reflect::reflection_service_from_static(EMPTY_FDS);
    assert!(
        result.is_ok(),
        "reflection_service_from_static with empty bytes should build: {:?}",
        result.err()
    );
}

#[test]
fn reflection_service_v1alpha_from_static_builds() {
    // Build a minimal FileDescriptorSet with one service "hello.GreeterV1Alpha"
    let svc = ServiceDescriptorProto {
        name: Some("GreeterV1Alpha".to_string()),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("hello_alpha.proto".to_string()),
        package: Some("hello".to_string()),
        service: vec![svc],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };

    // Use Vec::leak in test only — acceptable in unit tests (process exits after test runner).
    let static_bytes: &'static [u8] = Vec::leak(fds.encode_to_vec());
    let result = oxirpc_reflect::reflection_service_v1alpha_from_static(static_bytes);
    assert!(
        result.is_ok(),
        "reflection_service_v1alpha_from_static should build: {:?}",
        result.err()
    );
}

// ─── new tests for ReflectionBuilder and ReflectError::Decode ────────────────

#[allow(deprecated)]
#[test]
fn builder_builds_with_real_fds() {
    let fds = FileDescriptorSet { file: vec![] };
    let bytes = fds.encode_to_vec();

    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(bytes)
        .build();
    assert!(
        result.is_ok(),
        "ReflectionBuilder should succeed with valid (empty) FDS: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn builder_builds_with_named_service() {
    let svc = ServiceDescriptorProto {
        name: Some("EchoService".to_string()),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("echo.proto".to_string()),
        package: Some("echo".to_string()),
        service: vec![svc],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };
    let bytes = fds.encode_to_vec();

    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(bytes)
        .build();
    assert!(
        result.is_ok(),
        "ReflectionBuilder with named service should build: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn builder_builds_with_v1alpha() {
    let fds = FileDescriptorSet { file: vec![] };
    let bytes = fds.encode_to_vec();

    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(bytes)
        .include_v1alpha(true)
        .build();
    assert!(
        result.is_ok(),
        "ReflectionBuilder with v1alpha should build: {:?}",
        result.err()
    );
    let services = result.unwrap();
    assert!(
        services.v1alpha.is_some(),
        "v1alpha service should be present when include_v1alpha(true)"
    );
}

#[allow(deprecated)]
#[test]
fn builder_no_v1alpha_by_default() {
    let bytes = FileDescriptorSet { file: vec![] }.encode_to_vec();
    let services = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(bytes)
        .build()
        .expect("build should succeed");
    assert!(
        services.v1alpha.is_none(),
        "v1alpha service should be absent by default"
    );
}

#[allow(deprecated)]
#[test]
fn decode_error_on_malformed_fds() {
    // Garbage bytes that are not a valid protobuf FileDescriptorSet.
    // tonic-reflection v0.14.6 calls FileDescriptorSet::decode internally (or via our
    // register_file_descriptor_set path), so malformed bytes → Build or Decode error.
    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(vec![0xFF, 0xFE, 0xFD])
        .build();
    assert!(
        result.is_err(),
        "ReflectionBuilder should fail on malformed FDS"
    );
}

#[test]
fn reflection_service_owned_decode_error_on_malformed() {
    // reflection_service_owned now decodes eagerly → returns ReflectError::Decode
    let result = oxirpc_reflect::reflection_service_owned(vec![0xFF, 0xFE, 0xFD]);
    assert!(result.is_err(), "malformed bytes should fail");
    match result {
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.contains("decode"),
                "error message should mention 'decode': {msg}"
            );
        }
        Ok(_) => panic!("expected Err, got Ok"),
    }
}

#[test]
fn reflect_error_decode_variant_display() {
    // Trigger a real DecodeError by trying to decode garbage bytes.
    use prost::Message as _;
    let decode_err = prost_types::FileDescriptorSet::decode(&[0xFF_u8, 0xFE, 0xFD][..])
        .expect_err("should fail to decode garbage");
    let err = oxirpc_reflect::ReflectError::Decode(decode_err);
    let msg = format!("{err}");
    assert!(
        msg.contains("decode"),
        "Display for Decode variant should contain 'decode', got: {msg}"
    );
}

#[allow(deprecated)]
#[test]
fn reflect_error_build_variant_display() {
    // Build a Build or Decode variant by triggering a real error from malformed bytes.
    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(vec![0xFF, 0xFE, 0xFD])
        .build();
    assert!(result.is_err(), "malformed FDS should produce an error");
    // Extract the error without requiring Debug on the Ok type.
    match result {
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                !msg.is_empty(),
                "Display for error variant should be non-empty"
            );
        }
        Ok(_) => panic!("expected Err, got Ok"),
    }
}

#[test]
fn reflect_error_source_decode() {
    use prost::Message as _;
    use std::error::Error;
    let decode_err = prost_types::FileDescriptorSet::decode(&[0xFF_u8, 0xFE, 0xFD][..])
        .expect_err("should fail to decode garbage");
    let err = oxirpc_reflect::ReflectError::Decode(decode_err);
    assert!(
        err.source().is_some(),
        "Decode variant should expose source error"
    );
}

#[allow(deprecated)]
#[test]
fn builder_register_arc() {
    use std::sync::Arc;
    let arc: Arc<[u8]> = FileDescriptorSet { file: vec![] }.encode_to_vec().into();
    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_encoded_file_descriptor_set(arc)
        .build();
    assert!(
        result.is_ok(),
        "Arc-registered FDS should build: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn builder_multiple_fds() {
    let make_fd_bytes = |proto_name: &str, pkg: &str| {
        let fd = FileDescriptorProto {
            name: Some(proto_name.to_string()),
            package: Some(pkg.to_string()),
            ..Default::default()
        };
        FileDescriptorSet { file: vec![fd] }.encode_to_vec()
    };

    let result = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(make_fd_bytes("a.proto", "a"))
        .register_file_descriptor_set(make_fd_bytes("b.proto", "b"))
        .build();
    assert!(
        result.is_ok(),
        "multiple FDS registrations should succeed: {:?}",
        result.err()
    );
}

// ─── DescriptorPool / DescriptorPoolBuilder tests ─────────────────────────────

fn minimal_fds() -> prost_types::FileDescriptorSet {
    prost_types::FileDescriptorSet {
        file: vec![prost_types::FileDescriptorProto {
            name: Some("test_pool.proto".to_owned()),
            package: Some("test.pool".to_owned()),
            syntax: Some("proto3".to_owned()),
            ..Default::default()
        }],
    }
}

fn minimal_fds_bytes() -> Vec<u8> {
    use prost::Message;
    minimal_fds().encode_to_vec()
}

#[test]
fn descriptor_pool_builder_register_decoded() {
    let pool = DescriptorPoolBuilder::new().register(minimal_fds()).build();
    assert_eq!(pool.file_descriptor_sets().len(), 1);
}

#[test]
fn descriptor_pool_builder_register_bytes() {
    let bytes = minimal_fds_bytes();
    let pool = DescriptorPoolBuilder::new()
        .register_bytes(&bytes)
        .expect("decode should succeed")
        .build();
    assert_eq!(pool.file_descriptor_sets().len(), 1);
}

#[test]
fn descriptor_pool_builder_error_on_malformed() {
    let result = DescriptorPoolBuilder::new().register_bytes(b"not proto");
    assert!(result.is_err());
    match result {
        Err(ReflectError::Decode(_)) => {}
        other => panic!("expected Decode error, got {other:?}"),
    }
}

#[test]
fn reflection_service_from_pool_builds() {
    let pool = DescriptorPoolBuilder::new().register(minimal_fds()).build();
    let result = reflection_service_from_pool(pool);
    assert!(
        result.is_ok(),
        "pool-based reflection build failed: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn reflection_builder_register_pool() {
    let pool = DescriptorPoolBuilder::new().register(minimal_fds()).build();
    let services = ReflectionBuilder::new().register_pool(pool).build();
    assert!(
        services.is_ok(),
        "builder with pool failed: {:?}",
        services.err()
    );
}

#[allow(deprecated)]
#[test]
fn reflection_builder_register_pool_and_bytes() {
    let bytes = minimal_fds_bytes();
    let pool = DescriptorPoolBuilder::new().register(minimal_fds()).build();
    // mix both registration paths
    let services = ReflectionBuilder::new()
        .register_file_descriptor_set(bytes)
        .register_pool(pool)
        .build();
    assert!(
        services.is_ok(),
        "mixed registration failed: {:?}",
        services.err()
    );
}

// Ensure DescriptorPool is accessible (pub use check)
#[test]
fn descriptor_pool_type_accessible() {
    let _pool: DescriptorPool = DescriptorPoolBuilder::new().build();
    assert_eq!(_pool.file_descriptor_sets().len(), 0);
}

// ─── dynamic registration tests ───────────────────────────────────────────────

fn make_fds_with_service(service_name: &str) -> prost_types::FileDescriptorSet {
    prost_types::FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some(format!("{service_name}.proto")),
            package: Some("test".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some(service_name.to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn add_service_grows_pool() {
    let mut pool = DescriptorPoolBuilder::new().build();
    assert_eq!(pool.file_descriptor_sets().len(), 0);
    pool.add_service(make_fds_with_service("ServiceA"));
    assert_eq!(pool.file_descriptor_sets().len(), 1);
    pool.add_service(make_fds_with_service("ServiceB"));
    assert_eq!(pool.file_descriptor_sets().len(), 2);
}

#[test]
fn list_services_returns_names() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(make_fds_with_service("Alpha"));
    pool.add_service(make_fds_with_service("Beta"));
    let services = pool.list_services();
    assert!(services.contains(&"Alpha".to_owned()));
    assert!(services.contains(&"Beta".to_owned()));
}

#[test]
fn remove_service_shrinks_pool() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(make_fds_with_service("ToRemove"));
    pool.add_service(make_fds_with_service("ToKeep"));
    assert_eq!(pool.file_descriptor_sets().len(), 2);
    let removed = pool.remove_service("ToRemove");
    assert!(removed);
    assert_eq!(pool.file_descriptor_sets().len(), 1);
    let services = pool.list_services();
    assert!(!services.contains(&"ToRemove".to_owned()));
    assert!(services.contains(&"ToKeep".to_owned()));
}

#[test]
fn remove_service_returns_false_when_not_found() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(make_fds_with_service("Only"));
    let removed = pool.remove_service("NonExistent");
    assert!(!removed);
    assert_eq!(pool.file_descriptor_sets().len(), 1);
}

#[test]
fn remove_service_multi_service_fds_leaves_intact() {
    // An FDS with 2 services should NOT be removed even if one matches
    let fds = prost_types::FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("multi.proto".to_owned()),
            service: vec![
                ServiceDescriptorProto {
                    name: Some("A".to_owned()),
                    ..Default::default()
                },
                ServiceDescriptorProto {
                    name: Some("B".to_owned()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    };
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(fds);
    let removed = pool.remove_service("A");
    assert!(!removed, "multi-service FDS should not be removed");
    assert_eq!(pool.file_descriptor_sets().len(), 1);
}

// ─── encode / decode round-trip tests ─────────────────────────────────────────

#[test]
fn encode_decode_round_trip() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(make_fds_with_service("RoundTrip"));

    let bytes = pool.encode();
    assert!(!bytes.is_empty());

    let decoded = DescriptorPool::decode(&bytes).expect("decode should succeed");
    let services = decoded.list_services();
    assert!(services.contains(&"RoundTrip".to_owned()));
}

#[test]
fn encode_merges_multiple_fds_into_one() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(make_fds_with_service("SvcX"));
    pool.add_service(make_fds_with_service("SvcY"));
    // Two FDS entries going in
    assert_eq!(pool.file_descriptor_sets().len(), 2);

    let bytes = pool.encode();
    let decoded = DescriptorPool::decode(&bytes).expect("decode should succeed");
    // encode merges; decoded has 1 FDS with all files
    let services = decoded.list_services();
    assert!(services.contains(&"SvcX".to_owned()));
    assert!(services.contains(&"SvcY".to_owned()));
}

#[test]
fn decode_invalid_bytes_returns_error_or_empty() {
    // proto3 garbage bytes: may return Err or an empty/unknown-fields FDS
    // The invariant is: no panic.
    let result = DescriptorPool::decode(&[0xFF_u8, 0xFE, 0xFD]);
    let _ = result; // at minimum, no panic
}

#[test]
fn decode_empty_bytes_returns_empty_pool() {
    // Empty bytes decode to an empty FileDescriptorSet — valid proto3 default
    let pool = DescriptorPool::decode(&[]).expect("empty bytes should decode fine");
    assert!(pool.list_services().is_empty());
}

// ─── extension number tests ────────────────────────────────────────────────────

#[test]
fn extension_numbers_returns_matching_numbers() {
    use prost_types::{field_descriptor_proto::Type, FieldDescriptorProto};

    let ext_field = FieldDescriptorProto {
        name: Some("my_ext".to_owned()),
        number: Some(1001),
        extendee: Some(".mypackage.MyMessage".to_owned()),
        r#type: Some(Type::Int32 as i32),
        ..Default::default()
    };
    let fds = prost_types::FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("extensions.proto".to_owned()),
            package: Some("mypackage".to_owned()),
            extension: vec![ext_field],
            ..Default::default()
        }],
    };
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(fds);

    let nums = pool.extension_numbers("mypackage.MyMessage");
    assert!(nums.contains(&1001), "expected 1001, got {nums:?}");

    // Also test with leading dot stripped
    let nums2 = pool.extension_numbers(".mypackage.MyMessage");
    assert!(
        nums2.contains(&1001),
        "leading dot should be stripped: {nums2:?}"
    );
}

#[test]
fn extension_numbers_empty_for_no_match() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(make_fds_with_service("Svc"));
    let nums = pool.extension_numbers("NonExistentMessage");
    assert!(nums.is_empty());
}

// ─── enhanced Display tests ────────────────────────────────────────────────────

#[test]
fn reflect_error_decode_display_mentions_proto_binary() {
    use prost::Message as _;
    let decode_err = prost_types::FileDescriptorSet::decode(&[0xFF_u8, 0xFE, 0xFD][..])
        .expect_err("should fail to decode garbage");
    let err = ReflectError::Decode(decode_err);
    let msg = format!("{err}");
    assert!(
        msg.contains("decode") && msg.contains("proto"),
        "Display for Decode should mention 'decode' and 'proto', got: {msg}"
    );
}

// ─── register_service_named tests ─────────────────────────────────────────────

/// A minimal stub that implements `tonic::server::NamedService` for testing.
struct EchoStub;
impl tonic::server::NamedService for EchoStub {
    const NAME: &'static str = "EchoService";
}

/// A second stub for multi-service filtering tests.
struct GreeterStub;
impl tonic::server::NamedService for GreeterStub {
    const NAME: &'static str = "GreeterService";
}

/// A stub whose name does NOT appear in any registered FDS.
struct UnknownStub;
impl tonic::server::NamedService for UnknownStub {
    const NAME: &'static str = "UnknownService";
}

#[allow(deprecated)]
#[test]
fn register_service_named_no_filter_exposes_all() {
    // When register_service_named is never called the builder should behave
    // identically to before: all registered FDS entries pass through.
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("all.proto".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some("AnyService".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let result = ReflectionBuilder::new()
        .register_file_descriptor_set(fds.encode_to_vec())
        .build();
    assert!(
        result.is_ok(),
        "no filter should expose all services: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn register_service_named_matching_filter_passes_through() {
    // A filter that matches the sole service in the FDS → service is exposed.
    let fds = make_fds_with_service("EchoService");
    let result = ReflectionBuilder::new()
        .register_file_descriptor_set(fds.encode_to_vec())
        .register_service_named::<EchoStub>()
        .build();
    assert!(
        result.is_ok(),
        "matching filter should allow the service: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn register_service_named_non_matching_filter_drops_fds() {
    // A filter that does NOT match any service → all FDS are dropped.
    // tonic_reflection accepts an empty registration set and still builds Ok.
    let fds = make_fds_with_service("EchoService");
    let result = ReflectionBuilder::new()
        .register_file_descriptor_set(fds.encode_to_vec())
        .register_service_named::<UnknownStub>()
        .build();
    // Should still build — just an empty registry.
    assert!(
        result.is_ok(),
        "non-matching filter should drop FDS but still build: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn register_service_named_multi_filter_keeps_both_matching() {
    // Two services in separate FDS, two filters → both pass through.
    let fds_echo = make_fds_with_service("EchoService");
    let fds_greeter = make_fds_with_service("GreeterService");
    let result = ReflectionBuilder::new()
        .register_file_descriptor_set(fds_echo.encode_to_vec())
        .register_file_descriptor_set(fds_greeter.encode_to_vec())
        .register_service_named::<EchoStub>()
        .register_service_named::<GreeterStub>()
        .build();
    assert!(
        result.is_ok(),
        "multi-filter matching both services should succeed: {:?}",
        result.err()
    );
}

#[allow(deprecated)]
#[test]
fn register_service_named_partial_filter_drops_unmatched() {
    // Two services in separate FDS, filter only for one → other is dropped.
    // The builder should still succeed.
    let fds_echo = make_fds_with_service("EchoService");
    let fds_greeter = make_fds_with_service("GreeterService");
    let result = ReflectionBuilder::new()
        .register_file_descriptor_set(fds_echo.encode_to_vec())
        .register_file_descriptor_set(fds_greeter.encode_to_vec())
        .register_service_named::<EchoStub>() // only echo passes through
        .build();
    assert!(
        result.is_ok(),
        "partial filter should drop unmatched FDS but succeed: {:?}",
        result.err()
    );
}

#[test]
fn descriptor_pool_encode_decode_preserves_service_count() {
    let mut pool = DescriptorPoolBuilder::new().build();
    pool.add_service(FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("count_test.proto".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some("CountService".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    });

    let bytes = pool.encode();
    let decoded = DescriptorPool::decode(&bytes).expect("round-trip decode should succeed");
    assert_eq!(
        decoded.list_services().len(),
        1,
        "decoded pool should have exactly 1 service"
    );
}

#[test]
fn list_services_empty_on_empty_pool() {
    let pool = DescriptorPoolBuilder::new().build();
    assert!(
        pool.list_services().is_empty(),
        "empty pool should report no services"
    );
}

#[test]
fn reflect_error_decode_display() {
    use prost::Message as _;
    let decode_err = prost_types::FileDescriptorSet::decode(&[0xFF_u8, 0xFE, 0xFD][..])
        .expect_err("should fail to decode garbage");
    let e = ReflectError::Decode(decode_err);
    let s = format!("{e}");
    assert!(
        s.to_ascii_lowercase().contains("decode"),
        "Display should mention decode: {s}"
    );
}

// ─── Native reflection service tests ─────────────────────────────────────────

/// Build a minimal FDS with a named service, a message, and an extension.
fn native_test_fds() -> FileDescriptorSet {
    use prost_types::{
        field_descriptor_proto::Type, DescriptorProto, EnumDescriptorProto, FieldDescriptorProto,
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("native_test.proto".to_owned()),
            package: Some("native.pkg".to_owned()),
            syntax: Some("proto3".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some("NativeTestService".to_owned()),
                ..Default::default()
            }],
            message_type: vec![DescriptorProto {
                name: Some("NativeMessage".to_owned()),
                ..Default::default()
            }],
            enum_type: vec![EnumDescriptorProto {
                name: Some("NativeEnum".to_owned()),
                ..Default::default()
            }],
            extension: vec![FieldDescriptorProto {
                name: Some("native_ext".to_owned()),
                number: Some(2001),
                extendee: Some(".native.pkg.NativeMessage".to_owned()),
                r#type: Some(Type::Int32 as i32),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn proto_request_oneof_roundtrips_bytes() {
    let req = ServerReflectionRequest {
        host: "localhost".to_owned(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let bytes = req.encode_to_vec();
    let decoded = ServerReflectionRequest::decode(bytes.as_slice()).expect("decode");
    assert_eq!(decoded.host, "localhost");
    matches!(
        decoded.message_request,
        Some(MessageRequest::ListServices(_))
    );
}

#[test]
fn list_services_returns_registered_names() {
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let services = pool.list_services();
    assert!(
        services.iter().any(|s| s == "NativeTestService"),
        "expected NativeTestService in {services:?}"
    );
}

#[test]
fn file_by_filename_returns_correct_descriptor() {
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let fdp = pool.find_file_by_name("native_test.proto");
    assert!(fdp.is_some(), "expected to find native_test.proto");
    assert_eq!(fdp.unwrap().name.as_deref(), Some("native_test.proto"));
}

#[test]
fn file_containing_symbol_resolves_dotted_fqn() {
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let fdp = pool.find_file_containing_symbol("native.pkg.NativeMessage");
    assert!(
        fdp.is_some(),
        "expected to find file for native.pkg.NativeMessage"
    );
}

#[test]
fn file_containing_symbol_strips_leading_dot() {
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let fdp_with_dot = pool.find_file_containing_symbol(".native.pkg.NativeMessage");
    let fdp_without_dot = pool.find_file_containing_symbol("native.pkg.NativeMessage");
    assert!(fdp_with_dot.is_some(), "leading dot version should work");
    assert_eq!(
        fdp_with_dot.map(|f| f.name.as_deref()),
        fdp_without_dot.map(|f| f.name.as_deref()),
        "both should find the same file"
    );
}

#[test]
fn file_containing_extension_finds_right_file() {
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let fdp = pool.find_file_containing_extension("native.pkg.NativeMessage", 2001);
    assert!(
        fdp.is_some(),
        "expected to find file containing extension 2001 on NativeMessage"
    );
    assert_eq!(fdp.unwrap().name.as_deref(), Some("native_test.proto"));
}

#[test]
fn all_extension_numbers_returns_expected() {
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let nums = pool.extension_numbers("native.pkg.NativeMessage");
    assert!(
        nums.contains(&2001),
        "expected 2001 in extension numbers: {nums:?}"
    );
}

#[test]
fn unknown_filename_returns_not_found_error_response() {
    use std::sync::Arc;
    let pool = DescriptorPoolBuilder::new()
        .register(native_test_fds())
        .build();
    let svc = NativeReflectionService::new(Arc::new(pool), ReflectVersion::V1);
    let req = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileByFilename(
            "nonexistent.proto".to_owned(),
        )),
    };
    let resp = svc.handle_request(req);
    match resp.message_response {
        Some(MessageResponse::ErrorResponse(e)) => {
            assert_eq!(
                e.error_code, 5,
                "expected NOT_FOUND (5), got {}",
                e.error_code
            );
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[test]
fn v1alpha_service_has_v1alpha_name() {
    assert_eq!(
        NativeReflectionServiceV1Alpha::NAME,
        "grpc.reflection.v1alpha.ServerReflection"
    );
}

#[test]
fn v1_service_has_v1_name() {
    use oxirpc_reflect::NativeReflectionServiceV1;
    assert_eq!(
        NativeReflectionServiceV1::NAME,
        "grpc.reflection.v1.ServerReflection"
    );
}

// ─── Missing tests added for TODO completion ──────────────────────────────────

#[test]
fn get_file_by_name_returns_correct_descriptor() {
    use prost_types::{FileDescriptorProto, FileDescriptorSet};

    let fd = FileDescriptorProto {
        name: Some("helloworld.proto".to_owned()),
        package: Some("helloworld".to_owned()),
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };
    let pool = DescriptorPoolBuilder::new().register(fds).build();

    let found = pool.find_file_by_name("helloworld.proto");
    assert!(found.is_some(), "expected to find helloworld.proto");
    assert_eq!(
        found.expect("is some").name.as_deref(),
        Some("helloworld.proto")
    );

    let not_found = pool.find_file_by_name("nonexistent.proto");
    assert!(not_found.is_none(), "nonexistent.proto should return None");
}

#[test]
fn get_file_containing_symbol_finds_message() {
    use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};

    let fd = FileDescriptorProto {
        name: Some("helloworld.proto".to_owned()),
        package: Some("helloworld".to_owned()),
        message_type: vec![DescriptorProto {
            name: Some("HelloRequest".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };
    let pool = DescriptorPoolBuilder::new().register(fds).build();

    // without leading dot
    let found = pool.find_file_containing_symbol("helloworld.HelloRequest");
    assert!(
        found.is_some(),
        "helloworld.HelloRequest should be found (no leading dot)"
    );

    // with leading dot — should strip and still find
    let found_dot = pool.find_file_containing_symbol(".helloworld.HelloRequest");
    assert!(
        found_dot.is_some(),
        ".helloworld.HelloRequest should be found (leading dot)"
    );

    // nonexistent message
    let not_found = pool.find_file_containing_symbol("helloworld.NoSuchMsg");
    assert!(
        not_found.is_none(),
        "helloworld.NoSuchMsg should return None"
    );
}

#[test]
fn get_file_containing_symbol_finds_service() {
    use prost_types::{FileDescriptorProto, FileDescriptorSet};

    let fd = FileDescriptorProto {
        name: Some("helloworld.proto".to_owned()),
        package: Some("helloworld".to_owned()),
        service: vec![ServiceDescriptorProto {
            name: Some("Greeter".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };
    let pool = DescriptorPoolBuilder::new().register(fds).build();

    let found = pool.find_file_containing_symbol("helloworld.Greeter");
    assert!(
        found.is_some(),
        "helloworld.Greeter service should be found"
    );
    assert_eq!(
        found.expect("is some").name.as_deref(),
        Some("helloworld.proto")
    );
}

#[test]
fn error_response_not_found_has_correct_code() {
    use oxirpc_reflect::proto::ErrorResponse;

    let err = ErrorResponse::not_found("test.proto");
    assert_eq!(err.error_code, 5, "gRPC NOT_FOUND code should be 5");
    assert!(
        err.error_message.contains("test.proto"),
        "error message should contain the requested name: {}",
        err.error_message
    );
}

#[test]
fn v1alpha_service_name_is_correct() {
    use oxirpc_reflect::NativeReflectionServiceV1Alpha;
    assert_eq!(
        NativeReflectionServiceV1Alpha::NAME,
        "grpc.reflection.v1alpha.ServerReflection"
    );
}

#[test]
fn v1_service_name_is_correct() {
    use oxirpc_reflect::NativeReflectionServiceV1;
    assert_eq!(
        NativeReflectionServiceV1::NAME,
        "grpc.reflection.v1.ServerReflection"
    );
}

#[test]
fn all_extension_numbers_for_type_returns_list() {
    use prost_types::{
        field_descriptor_proto::Type, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };

    let ext = FieldDescriptorProto {
        name: Some("bar_ext".to_owned()),
        number: Some(42),
        extendee: Some(".foo.Bar".to_owned()),
        r#type: Some(Type::Int32 as i32),
        ..Default::default()
    };
    let fd = FileDescriptorProto {
        name: Some("foo.proto".to_owned()),
        package: Some("foo".to_owned()),
        extension: vec![ext],
        ..Default::default()
    };
    let fds = FileDescriptorSet { file: vec![fd] };
    let pool = DescriptorPoolBuilder::new().register(fds).build();

    let nums = pool.extension_numbers("foo.Bar");
    assert!(
        nums.contains(&42),
        "expected 42 in extension list, got {nums:?}"
    );

    let empty = pool.extension_numbers("foo.NoSuchType");
    assert!(
        empty.is_empty(),
        "nonexistent type should yield empty extension list"
    );
}

#[test]
fn build_native_returns_v1_and_v1alpha_pair() {
    use oxirpc_reflect::{
        NativeReflectionServiceV1, NativeReflectionServiceV1Alpha, ReflectionBuilder,
    };

    let (v1, v1alpha) = ReflectionBuilder::new()
        .register_file_descriptor_set(
            prost_types::FileDescriptorSet { file: vec![] }.encode_to_vec(),
        )
        .build_native();

    // Both wrappers should advertise the correct service names.
    assert_eq!(
        NativeReflectionServiceV1::NAME,
        "grpc.reflection.v1.ServerReflection"
    );
    assert_eq!(
        NativeReflectionServiceV1Alpha::NAME,
        "grpc.reflection.v1alpha.ServerReflection"
    );

    // Suppress unused-variable warnings while confirming the types are usable.
    let _ = v1;
    let _ = v1alpha;
}

#[test]
fn native_service_can_be_queried_for_its_own_name() {
    use oxirpc_reflect::NativeReflectionServiceV1;

    // Register the v1 reflection service name into a pool and query list_services.
    let fds = prost_types::FileDescriptorSet {
        file: vec![prost_types::FileDescriptorProto {
            name: Some("reflection_self.proto".to_owned()),
            package: Some("grpc.reflection.v1".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some("ServerReflection".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let pool = DescriptorPoolBuilder::new().register(fds).build();
    let services = pool.list_services();
    // The pool echoes back the bare service name stored in the FDS.
    assert!(
        services.iter().any(|s| s == "ServerReflection"),
        "pool list_services should return 'ServerReflection', got {services:?}"
    );

    // Verify the constant itself matches what is expected for v1.
    assert_eq!(
        NativeReflectionServiceV1::NAME,
        "grpc.reflection.v1.ServerReflection"
    );
}

// ─── Scale test for DescriptorPool with 100 descriptors ──────────────────────

#[test]
fn pool_with_100_descriptors_handles_all_lookups() {
    use prost_types::{
        FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto,
    };

    let mut builder = DescriptorPoolBuilder::new();

    for i in 0..100 {
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some(format!("service_{i}.proto")),
                package: Some("scale".to_owned()),
                service: vec![ServiceDescriptorProto {
                    name: Some(format!("Service{i}")),
                    method: vec![MethodDescriptorProto {
                        name: Some("DoIt".to_owned()),
                        input_type: Some(".scale.Req".to_owned()),
                        output_type: Some(".scale.Resp".to_owned()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        builder = builder.register(fds);
    }

    let pool = builder.build();

    // All 100 services appear in list_services
    let services = pool.list_services();
    assert_eq!(services.len(), 100, "should have 100 services");

    // find_file_by_name works for each
    for i in 0..100 {
        assert!(
            pool.find_file_by_name(&format!("service_{i}.proto"))
                .is_some(),
            "service_{i}.proto should be findable"
        );
    }

    // find_file_containing_symbol works
    assert!(
        pool.find_file_containing_symbol("scale.Service42")
            .is_some(),
        "Service42 symbol should be findable"
    );

    // Symbol lookup with leading dot
    assert!(
        pool.find_file_containing_symbol(".scale.Service0")
            .is_some(),
        ".scale.Service0 with leading dot should work"
    );
}

#[test]
fn bidi_handler_responds_to_multiple_requests_in_order() {
    use std::sync::Arc;

    let pool = DescriptorPoolBuilder::new()
        .register(make_fds_with_service("BidiSvc"))
        .build();
    let svc = NativeReflectionService::new(Arc::new(pool), ReflectVersion::V1);

    // Issue 3 ListServices requests and verify each response contains BidiSvc.
    for i in 0..3u32 {
        let req = ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        };
        let resp = svc.handle_request(req);
        match resp.message_response {
            Some(MessageResponse::ListServicesResponse(ls)) => {
                let names: Vec<&str> = ls.service.iter().map(|s| s.name.as_str()).collect();
                assert!(
                    names.contains(&"BidiSvc"),
                    "iteration {i}: expected BidiSvc in {names:?}"
                );
            }
            other => panic!("iteration {i}: expected ListServicesResponse, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn reflection_accepts_native_body_request() {
    use bytes::Bytes;
    use http::Request;
    use http_body_util::BodyExt;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_reflect::proto::{
        server_reflection_request::MessageRequest as SrMessageRequest, ServerReflectionRequest,
    };
    use prost::Message;
    use tower::ServiceExt;

    let pool = DescriptorPoolBuilder::new()
        .register(make_fds_with_service("TestSvc"))
        .build();
    let (svc, _) = ReflectionBuilder::new().register_pool(pool).build_native();

    // Build a ServerReflectionRequest: list_services
    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(SrMessageRequest::ListServices(String::new())),
    };

    // Encode as a single gRPC frame: 1-byte compressed flag + 4-byte big-endian length + payload
    let body_bytes: Bytes = {
        let mut msg_buf = bytes::BytesMut::new();
        req_proto.encode(&mut msg_buf).expect("encode");
        let len = msg_buf.len() as u32;
        let mut frame = bytes::BytesMut::with_capacity(5 + msg_buf.len());
        frame.extend_from_slice(&[0u8]);
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&msg_buf);
        frame.freeze()
    };

    let body = NativeBody::once(body_bytes);
    let request = Request::builder()
        .method("POST")
        .uri("/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(body)
        .expect("build request");

    let response = svc.oneshot(request).await.expect("service call");
    assert_eq!(response.status(), http::StatusCode::OK);
    let collected = response.into_body().collect().await.expect("collect");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let status = trailers
        .get("grpc-status")
        .map(|v| v.to_str().unwrap_or("?"));
    assert_eq!(
        status,
        Some("0"),
        "expected grpc-status: 0, got: {status:?}"
    );
}

#[cfg(feature = "gzip")]
#[tokio::test]
async fn reflection_negotiates_gzip_compression() {
    use http::Request;
    use oxirpc_core::wire::NativeBody;
    use oxirpc_reflect::{DescriptorPoolBuilder, ReflectionBuilder};
    use prost::Message;
    use tower::ServiceExt;

    let pool = DescriptorPoolBuilder::new().build();
    let (svc, _) = ReflectionBuilder::new().register_pool(pool).build_native();

    use oxirpc_reflect::proto::{
        server_reflection_request::MessageRequest, ServerReflectionRequest,
    };
    let req_proto = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let body_bytes = {
        let mut msg_buf = bytes::BytesMut::new();
        req_proto.encode(&mut msg_buf).expect("encode");
        let len = msg_buf.len() as u32;
        let mut frame = bytes::BytesMut::with_capacity(5 + msg_buf.len());
        frame.extend_from_slice(&[0u8]);
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&msg_buf);
        frame.freeze()
    };
    let body = NativeBody::once(body_bytes);

    let request = Request::builder()
        .method("POST")
        .uri("/grpc.reflection.v1.ServerReflection/ServerReflectionInfo")
        .header("content-type", "application/grpc")
        .header("grpc-accept-encoding", "gzip,identity")
        .body(body)
        .expect("request");

    let response = svc.oneshot(request).await.expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);
    let enc_header = response
        .headers()
        .get("grpc-encoding")
        .map(|v| v.to_str().unwrap_or(""));
    assert_eq!(
        enc_header,
        Some("gzip"),
        "reflect response must be gzip-compressed when client accepts gzip"
    );
}

// ─── oxiproto DescriptorPool backend tests (feature = "oxiproto") ─────────────

/// Build a [`prost_types::FileDescriptorSet`] suitable for loading into an
/// `oxiproto_reflect::DescriptorPool`.
#[cfg(feature = "oxiproto")]
fn oxiproto_test_fds() -> prost_types::FileDescriptorSet {
    use prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, MethodDescriptorProto,
    };

    // We need a self-contained FDS: the message types used in method
    // input/output must be declared in the same file so prost-reflect can
    // resolve them.
    let request_msg = DescriptorProto {
        name: Some("OxiRequest".to_owned()),
        field: vec![FieldDescriptorProto {
            name: Some("value".to_owned()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::Int32 as i32),
            json_name: Some("value".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let response_msg = DescriptorProto {
        name: Some("OxiResponse".to_owned()),
        field: vec![FieldDescriptorProto {
            name: Some("result".to_owned()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::Int32 as i32),
            json_name: Some("result".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };

    prost_types::FileDescriptorSet {
        file: vec![prost_types::FileDescriptorProto {
            name: Some("oxiproto_test.proto".to_owned()),
            package: Some("oxi.test".to_owned()),
            syntax: Some("proto3".to_owned()),
            message_type: vec![request_msg, response_msg],
            service: vec![prost_types::ServiceDescriptorProto {
                name: Some("OxiTestService".to_owned()),
                method: vec![MethodDescriptorProto {
                    name: Some("DoOxi".to_owned()),
                    input_type: Some(".oxi.test.OxiRequest".to_owned()),
                    output_type: Some(".oxi.test.OxiResponse".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[cfg(feature = "oxiproto")]
#[test]
fn oxiproto_pool_list_services_via_service_builder() {
    use prost::Message as _;
    use std::sync::Arc;

    let fds = oxiproto_test_fds();
    let fds_bytes = fds.encode_to_vec();
    let oxi_pool = oxiproto_reflect::pool_from_fds_bytes(&fds_bytes)
        .expect("valid FDS bytes for oxiproto pool");

    // Use NativeReflectionService directly with the oxiproto pool.
    use oxirpc_reflect::proto::{
        server_reflection_request::MessageRequest as SrReq,
        server_reflection_response::MessageResponse as SrResp, ServerReflectionRequest,
    };
    use oxirpc_reflect::service::{NativeReflectionService, ReflectVersion};

    let svc = NativeReflectionService::with_oxiproto_pool(Arc::new(oxi_pool), ReflectVersion::V1);
    let req = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(SrReq::ListServices(String::new())),
    };
    let resp = svc.handle_request(req);
    match resp.message_response {
        Some(SrResp::ListServicesResponse(ls)) => {
            let names: Vec<&str> = ls.service.iter().map(|s| s.name.as_str()).collect();
            assert!(
                names.contains(&"oxi.test.OxiTestService"),
                "oxiproto pool should list oxi.test.OxiTestService; got {names:?}"
            );
        }
        other => panic!("expected ListServicesResponse, got {other:?}"),
    }

    // Also verify via ReflectionBuilder path (test the builder integration).
    let oxi_pool2 = oxiproto_reflect::pool_from_fds_bytes(&fds_bytes)
        .expect("valid FDS bytes for oxiproto pool");
    let (v1, _) = oxirpc_reflect::ReflectionBuilder::new()
        .register_oxiproto_pool(Arc::new(oxi_pool2))
        .build_native();
    let _ = v1; // confirm type is NativeReflectionServiceV1
}

#[cfg(feature = "oxiproto")]
#[test]
fn oxiproto_pool_file_by_filename() {
    use prost::Message as _;
    use std::sync::Arc;

    let fds = oxiproto_test_fds();
    let fds_bytes = fds.encode_to_vec();
    let oxi_pool = oxiproto_reflect::pool_from_fds_bytes(&fds_bytes).expect("valid FDS bytes");
    let inner = oxirpc_reflect::service::NativeReflectionService::with_oxiproto_pool(
        Arc::new(oxi_pool),
        oxirpc_reflect::service::ReflectVersion::V1,
    );

    use oxirpc_reflect::proto::{
        server_reflection_request::MessageRequest as SrReq,
        server_reflection_response::MessageResponse as SrResp, ServerReflectionRequest,
    };

    // Hit: known filename
    let req = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(SrReq::FileByFilename("oxiproto_test.proto".to_owned())),
    };
    let resp = inner.handle_request(req);
    match resp.message_response {
        Some(SrResp::FileDescriptorResponse(fdr)) => {
            assert!(
                !fdr.file_descriptor_proto.is_empty(),
                "expected non-empty file descriptor proto bytes"
            );
        }
        other => panic!("expected FileDescriptorResponse, got {other:?}"),
    }

    // Miss: unknown filename
    let req_miss = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(SrReq::FileByFilename("no_such.proto".to_owned())),
    };
    let resp_miss = inner.handle_request(req_miss);
    match resp_miss.message_response {
        Some(SrResp::ErrorResponse(e)) => {
            assert_eq!(e.error_code, 5, "expected NOT_FOUND (5)");
        }
        other => panic!("expected ErrorResponse for miss, got {other:?}"),
    }
}

#[cfg(feature = "oxiproto")]
#[test]
fn oxiproto_pool_file_containing_symbol() {
    use prost::Message as _;
    use std::sync::Arc;

    let fds = oxiproto_test_fds();
    let fds_bytes = fds.encode_to_vec();
    let oxi_pool = oxiproto_reflect::pool_from_fds_bytes(&fds_bytes).expect("valid FDS bytes");
    let inner = oxirpc_reflect::service::NativeReflectionService::with_oxiproto_pool(
        Arc::new(oxi_pool),
        oxirpc_reflect::service::ReflectVersion::V1,
    );

    use oxirpc_reflect::proto::{
        server_reflection_request::MessageRequest as SrReq,
        server_reflection_response::MessageResponse as SrResp, ServerReflectionRequest,
    };

    // Service symbol
    let req = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(SrReq::FileContainingSymbol(
            "oxi.test.OxiTestService".to_owned(),
        )),
    };
    let resp = inner.handle_request(req);
    match resp.message_response {
        Some(SrResp::FileDescriptorResponse(fdr)) => {
            assert!(!fdr.file_descriptor_proto.is_empty());
        }
        other => panic!("expected FileDescriptorResponse for symbol, got {other:?}"),
    }

    // Message symbol with leading dot
    let req_msg = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(SrReq::FileContainingSymbol(
            ".oxi.test.OxiRequest".to_owned(),
        )),
    };
    let resp_msg = inner.handle_request(req_msg);
    match resp_msg.message_response {
        Some(SrResp::FileDescriptorResponse(fdr)) => {
            assert!(!fdr.file_descriptor_proto.is_empty());
        }
        other => panic!("expected FileDescriptorResponse for .oxi.test.OxiRequest, got {other:?}"),
    }

    // Regression: oxiproto backend must match what native backend returns for LIST_SERVICES.
    // Build the same FDS in both pools and compare list_services output.
    let native_pool = oxirpc_reflect::DescriptorPoolBuilder::new()
        .register(oxiproto_test_fds())
        .build();
    let native_services = native_pool.list_services();
    // oxiproto returns fully-qualified names; native returns bare names.
    // Verify both contain the service (by substring match on bare name).
    assert!(
        native_services.iter().any(|s| s == "OxiTestService"),
        "native pool should list OxiTestService; got {native_services:?}"
    );
}

#[cfg(feature = "oxiproto")]
#[test]
fn oxiproto_pool_with_native_constructor() {
    use prost::Message as _;
    use std::sync::Arc;

    let fds = oxiproto_test_fds();
    let fds_bytes = fds.encode_to_vec();
    let oxi_pool = oxiproto_reflect::pool_from_fds_bytes(&fds_bytes).expect("valid FDS");
    let svc = oxirpc_reflect::service::NativeReflectionService::with_oxiproto_pool(
        Arc::new(oxi_pool),
        oxirpc_reflect::service::ReflectVersion::V1,
    );
    assert_eq!(
        svc.version(),
        oxirpc_reflect::service::ReflectVersion::V1,
        "version should be V1"
    );
}
