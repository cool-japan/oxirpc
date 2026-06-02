/// Integration tests: verify that oxiproto types are accessible via the oxirpc facade.
///
/// Enabled only when the `oxiproto` feature is active.
#[cfg(feature = "oxiproto")]
mod proto_integration {
    use oxirpc::proto::{
        prost_types, wire, Extensions, Message, Name, OxiMessage, OxiName, OxiOneof, OxiProtoError,
        OxiProtoResult,
    };

    /// Verify that `OxiProtoError` is re-exported and can be constructed via its
    /// `ParseError` variant (always available, no feature gate needed).
    #[test]
    fn facade_reexports_oxi_proto_error() {
        let err: OxiProtoError = OxiProtoError::ParseError("test".into());
        let msg = err.to_string();
        assert!(
            msg.contains("test"),
            "expected error message to contain 'test', got: {msg}"
        );
    }

    /// Verify `OxiProtoResult` type alias is accessible and wraps `OxiProtoError`.
    #[test]
    fn facade_reexports_oxi_proto_result() {
        let ok: OxiProtoResult<u32> = Ok(42);
        assert!(ok.is_ok());

        let err: OxiProtoResult<u32> = Err(OxiProtoError::CodegenError("boom".into()));
        assert!(err.is_err());
    }

    /// Verify `prost_types::FileDescriptorSet` is accessible via the facade.
    #[test]
    fn facade_reexports_prost_types() {
        let fds: prost_types::FileDescriptorSet = prost_types::FileDescriptorSet::default();
        assert!(
            fds.file.is_empty(),
            "empty default FileDescriptorSet must have no files"
        );
    }

    /// Verify `wire` module types are accessible via the facade.
    #[test]
    fn facade_reexports_wire_module() {
        use wire::WireType;
        // WireType::Varint == 0
        let _wt = WireType::Varint;
    }

    /// Verify that trait symbols are accessible by using them in concrete bounds
    /// (checked at type-check time; this test exists to prove the re-export paths resolve).
    #[test]
    fn facade_reexports_trait_symbols() {
        // Verify the trait names resolve as bounds — no instances needed.
        fn _accept_oxi_message<T: OxiMessage>() {}
        fn _accept_oxi_name<T: OxiName>() {}
        fn _accept_oxi_oneof<T: OxiOneof>() {}
        fn _accept_message<T: Message>() {}
        fn _accept_name<T: Name>() {}
        // Extensions is a concrete struct, not a trait — verify it can be constructed.
        let _ext = Extensions::default();
    }
}
