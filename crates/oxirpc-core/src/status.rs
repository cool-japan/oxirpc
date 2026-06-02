//! Native gRPC status codes — independent of tonic.
//!
//! [`StatusCode`] enumerates all 17 canonical gRPC status codes as defined by
//! the gRPC specification (`grpc/status.proto`). The numeric values match the
//! wire representation exactly, so conversion to and from `i32` is lossless for
//! known codes.
//!
//! This type is provided so that downstream code can match on status codes
//! without depending on tonic. It interconverts freely with [`tonic::Code`].
//!
//! # Example
//!
//! ```rust
//! use oxirpc_core::status::StatusCode;
//!
//! assert_eq!(StatusCode::Ok as i32, 0);
//! assert_eq!(StatusCode::from_i32(5), Some(StatusCode::NotFound));
//! assert_eq!(StatusCode::NotFound.as_str(), "NOT_FOUND");
//! assert!(StatusCode::Unavailable.is_retryable());
//! ```

/// A canonical gRPC status code.
///
/// The discriminant of each variant equals its on-the-wire numeric value, so
/// `StatusCode::NotFound as i32 == 5`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum StatusCode {
    /// Not an error; returned on success.
    Ok = 0,
    /// The operation was cancelled, typically by the caller.
    Cancelled = 1,
    /// Unknown error.
    Unknown = 2,
    /// The client specified an invalid argument.
    InvalidArgument = 3,
    /// The deadline expired before the operation could complete.
    DeadlineExceeded = 4,
    /// Some requested entity was not found.
    NotFound = 5,
    /// The entity that a client attempted to create already exists.
    AlreadyExists = 6,
    /// The caller does not have permission to execute the specified operation.
    PermissionDenied = 7,
    /// Some resource has been exhausted (quota, disk space, …).
    ResourceExhausted = 8,
    /// The operation was rejected because the system is not in a state required.
    FailedPrecondition = 9,
    /// The operation was aborted (concurrency conflict).
    Aborted = 10,
    /// The operation was attempted past the valid range.
    OutOfRange = 11,
    /// The operation is not implemented or not supported.
    Unimplemented = 12,
    /// Internal error.
    Internal = 13,
    /// The service is currently unavailable.
    Unavailable = 14,
    /// Unrecoverable data loss or corruption.
    DataLoss = 15,
    /// The request does not have valid authentication credentials.
    Unauthenticated = 16,
}

impl StatusCode {
    /// All status codes, in numeric order.
    pub const ALL: [StatusCode; 17] = [
        StatusCode::Ok,
        StatusCode::Cancelled,
        StatusCode::Unknown,
        StatusCode::InvalidArgument,
        StatusCode::DeadlineExceeded,
        StatusCode::NotFound,
        StatusCode::AlreadyExists,
        StatusCode::PermissionDenied,
        StatusCode::ResourceExhausted,
        StatusCode::FailedPrecondition,
        StatusCode::Aborted,
        StatusCode::OutOfRange,
        StatusCode::Unimplemented,
        StatusCode::Internal,
        StatusCode::Unavailable,
        StatusCode::DataLoss,
        StatusCode::Unauthenticated,
    ];

    /// Convert a numeric wire value into a [`StatusCode`].
    ///
    /// Returns [`None`] if `value` is outside the valid range `0..=16`.
    pub const fn from_i32(value: i32) -> Option<StatusCode> {
        match value {
            0 => Some(StatusCode::Ok),
            1 => Some(StatusCode::Cancelled),
            2 => Some(StatusCode::Unknown),
            3 => Some(StatusCode::InvalidArgument),
            4 => Some(StatusCode::DeadlineExceeded),
            5 => Some(StatusCode::NotFound),
            6 => Some(StatusCode::AlreadyExists),
            7 => Some(StatusCode::PermissionDenied),
            8 => Some(StatusCode::ResourceExhausted),
            9 => Some(StatusCode::FailedPrecondition),
            10 => Some(StatusCode::Aborted),
            11 => Some(StatusCode::OutOfRange),
            12 => Some(StatusCode::Unimplemented),
            13 => Some(StatusCode::Internal),
            14 => Some(StatusCode::Unavailable),
            15 => Some(StatusCode::DataLoss),
            16 => Some(StatusCode::Unauthenticated),
            _ => None,
        }
    }

    /// Convert a numeric wire value into a [`StatusCode`], mapping any
    /// out-of-range value to [`StatusCode::Unknown`] (per gRPC convention).
    pub const fn from_i32_lossy(value: i32) -> StatusCode {
        match StatusCode::from_i32(value) {
            Some(code) => code,
            None => StatusCode::Unknown,
        }
    }

    /// The canonical SCREAMING_SNAKE_CASE name of this code.
    pub const fn as_str(self) -> &'static str {
        match self {
            StatusCode::Ok => "OK",
            StatusCode::Cancelled => "CANCELLED",
            StatusCode::Unknown => "UNKNOWN",
            StatusCode::InvalidArgument => "INVALID_ARGUMENT",
            StatusCode::DeadlineExceeded => "DEADLINE_EXCEEDED",
            StatusCode::NotFound => "NOT_FOUND",
            StatusCode::AlreadyExists => "ALREADY_EXISTS",
            StatusCode::PermissionDenied => "PERMISSION_DENIED",
            StatusCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
            StatusCode::FailedPrecondition => "FAILED_PRECONDITION",
            StatusCode::Aborted => "ABORTED",
            StatusCode::OutOfRange => "OUT_OF_RANGE",
            StatusCode::Unimplemented => "UNIMPLEMENTED",
            StatusCode::Internal => "INTERNAL",
            StatusCode::Unavailable => "UNAVAILABLE",
            StatusCode::DataLoss => "DATA_LOSS",
            StatusCode::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// Whether this code is conventionally safe to retry.
    ///
    /// Per the gRPC retry guidance, only [`StatusCode::Unavailable`] is
    /// universally retryable; [`StatusCode::ResourceExhausted`] may be retried
    /// with backoff. All other codes indicate the retry would also fail.
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            StatusCode::Unavailable | StatusCode::ResourceExhausted
        )
    }

    /// Whether this code represents success ([`StatusCode::Ok`]).
    pub const fn is_ok(self) -> bool {
        matches!(self, StatusCode::Ok)
    }

    /// The numeric wire value of this status code (`grpc-status` trailer).
    ///
    /// Equivalent to `self as i32`. Provided as a named method for clarity
    /// and symmetry with [`Self::from_i32`].
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}

impl core::fmt::Display for StatusCode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<StatusCode> for i32 {
    fn from(code: StatusCode) -> i32 {
        code as i32
    }
}

impl From<tonic::Code> for StatusCode {
    fn from(code: tonic::Code) -> StatusCode {
        StatusCode::from_i32_lossy(code as i32)
    }
}

impl From<StatusCode> for tonic::Code {
    fn from(code: StatusCode) -> tonic::Code {
        tonic::Code::from_i32(code as i32)
    }
}
