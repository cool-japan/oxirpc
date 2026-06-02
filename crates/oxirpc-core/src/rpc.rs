//! Native gRPC `Status` type — independent of tonic.
//!
//! [`Status`] carries a [`crate::status::StatusCode`], a human-readable message,
//! optional error detail bytes, and trailing metadata. Bridges to and from
//! `tonic::Status` allow interop during the migration period.

use crate::metadata::Metadata;
use crate::status::StatusCode;

/// A gRPC status value produced by an RPC call.
///
/// Lives in `oxirpc_core::rpc` (not at the crate root) so it does not collide
/// with the `pub use tonic::Status` re-export in `lib.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// The canonical gRPC status code.
    pub code: StatusCode,
    /// A developer-facing description of the error.
    pub message: String,
    /// Structured error detail payload (e.g. a serialised proto `google.rpc.Status`).
    pub details: Vec<u8>,
    /// Trailing metadata to propagate back to the caller.
    pub metadata: Metadata,
}

impl Status {
    /// Create an `OK` status with no message, details, or metadata.
    pub fn ok() -> Self {
        Self {
            code: StatusCode::Ok,
            message: String::new(),
            details: Vec::new(),
            metadata: Metadata::new(),
        }
    }

    /// Create a status with the given code and message.
    pub fn new(code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: Vec::new(),
            metadata: Metadata::new(),
        }
    }

    /// Attach structured detail bytes, consuming and returning `self`.
    pub fn with_details(mut self, details: Vec<u8>) -> Self {
        self.details = details;
        self
    }

    /// Attach trailing metadata, consuming and returning `self`.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Whether this status represents success.
    pub fn is_ok(&self) -> bool {
        self.code.is_ok()
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "status {} ({})", self.code, self.message)
    }
}

impl std::error::Error for Status {}

// ---------------------------------------------------------------------------
// Tonic bridge
// ---------------------------------------------------------------------------

impl From<tonic::Status> for Status {
    fn from(s: tonic::Status) -> Self {
        let code = StatusCode::from(s.code());
        let message = s.message().to_owned();
        let details = s.details().to_vec();

        let mut metadata = Metadata::new();
        for key_and_value in s.metadata().iter() {
            match key_and_value {
                tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                    if let Ok(val_str) = v.to_str() {
                        // Best-effort: skip entries that violate ASCII value rules.
                        let _ = metadata.insert(k.as_str(), val_str);
                    }
                }
                tonic::metadata::KeyAndValueRef::Binary(k, v) => {
                    let _ = metadata.insert_bin(k.as_str(), v.as_ref());
                }
            }
        }

        Self {
            code,
            message,
            details,
            metadata,
        }
    }
}

impl From<Status> for tonic::Status {
    fn from(s: Status) -> tonic::Status {
        let code: tonic::Code = s.code.into();
        let mut ts = if s.details.is_empty() {
            tonic::Status::new(code, s.message)
        } else {
            tonic::Status::with_details(code, s.message, s.details.into())
        };

        // Propagate metadata into the tonic status trailing metadata map.
        for (key, value_bytes) in s.metadata.iter() {
            if crate::metadata::Metadata::is_binary_key(key) {
                if let Ok(mkey) =
                    tonic::metadata::MetadataKey::<tonic::metadata::Binary>::from_bytes(
                        key.as_bytes(),
                    )
                {
                    let mval =
                        tonic::metadata::MetadataValue::<tonic::metadata::Binary>::from_bytes(
                            value_bytes,
                        );
                    ts.metadata_mut().insert_bin(mkey, mval);
                }
            } else if let Ok(mkey) =
                tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes())
            {
                if let Ok(mval) =
                    tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(value_bytes)
                {
                    ts.metadata_mut().insert(mkey, mval);
                }
            }
        }

        ts
    }
}
