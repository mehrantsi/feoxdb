use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FeoxError {
    #[error(
        "Invalid key size: key must be 1-{} bytes",
        crate::constants::MAX_KEY_SIZE
    )]
    InvalidKeySize,

    #[error(
        "Invalid value size: value must be 1-{} bytes",
        crate::constants::MAX_VALUE_SIZE
    )]
    InvalidValueSize,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Database is full")]
    DatabaseFull,

    #[error("Out of memory")]
    OutOfMemory,

    #[error("Timestamp is older than existing record")]
    OlderTimestamp,

    #[error("Invalid numeric value")]
    InvalidNumericValue,

    #[error("Numeric overflow")]
    NumericOverflow,

    #[error("Invalid range: start key must be <= end key")]
    InvalidRange,

    #[error("Multi-tenant mode is disabled")]
    MultiTenantDisabled,

    #[error("No device configured")]
    NoDevice,

    #[error("Invalid metadata")]
    InvalidMetadata,

    #[error("Corrupted record")]
    CorruptedRecord,

    #[error("Invalid record")]
    InvalidRecord,

    #[error("Invalid device")]
    InvalidDevice,

    #[error("Invalid operation")]
    InvalidOperation,

    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("System error: {0}")]
    SystemError(i32),

    #[error("JSON patch error: {0}")]
    JsonPatchError(String),

    #[error("Allocation failed")]
    AllocationFailed,

    #[error("Lock poisoned")]
    LockPoisoned,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("Channel error")]
    ChannelError,

    #[error("System shutting down")]
    ShuttingDown,

    #[error("Operation timed out")]
    Timeout,

    #[error("Not implemented")]
    NotImplemented,

    #[error("Unsupported on this platform")]
    Unsupported,

    #[error("Size mismatch: expected {expected} bytes, got {actual} bytes")]
    SizeMismatch { expected: usize, actual: usize },

    #[error("Invalid argument")]
    InvalidArgument,

    #[error("Out of space")]
    OutOfSpace,

    #[error("Corrupted data")]
    CorruptedData,

    #[error("Duplicate key")]
    DuplicateKey,
}

pub type Result<T> = std::result::Result<T, FeoxError>;

impl From<i32> for FeoxError {
    fn from(errno: i32) -> Self {
        match errno {
            2 => FeoxError::KeyNotFound,
            12 => FeoxError::OutOfMemory,
            17 => FeoxError::OlderTimestamp,
            22 => FeoxError::InvalidKeySize,
            28 => FeoxError::DatabaseFull,
            75 => FeoxError::NumericOverflow,
            90 => FeoxError::SizeMismatch {
                expected: 0,
                actual: 0,
            },
            _ => FeoxError::SystemError(errno),
        }
    }
}

impl FeoxError {
    pub fn errno(&self) -> i32 {
        match self {
            FeoxError::InvalidKeySize | FeoxError::InvalidValueSize => crate::constants::EINVAL,
            FeoxError::KeyNotFound => crate::constants::ENOENT,
            FeoxError::DatabaseFull => crate::constants::ENOSPC,
            FeoxError::OutOfMemory | FeoxError::AllocationFailed => crate::constants::ENOMEM,
            FeoxError::OlderTimestamp => crate::constants::EEXIST,
            FeoxError::NumericOverflow => crate::constants::EOVERFLOW,
            FeoxError::InvalidRange => crate::constants::EINVAL,
            FeoxError::MultiTenantDisabled => crate::constants::ENODEV,
            FeoxError::NoDevice => crate::constants::ENODEV,
            FeoxError::InvalidMetadata | FeoxError::CorruptedRecord => crate::constants::EIO,
            FeoxError::IoError(_) => crate::constants::EIO,
            FeoxError::SystemError(e) => *e,
            FeoxError::Timeout => crate::constants::EAGAIN,
            FeoxError::SizeMismatch { .. } => crate::constants::EMSGSIZE,
            _ => crate::constants::EIO,
        }
    }
}
