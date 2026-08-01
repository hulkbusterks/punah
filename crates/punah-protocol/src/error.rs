//! Protocol codec failures.

use core::fmt;

/// A frame could not be encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeError {
    KeyTooLarge { length: usize, maximum: usize },
    ValueTooLarge { length: usize, maximum: usize },
    TtlTooLarge(u32),
    OutputTooSmall { needed: usize, available: usize },
}

/// A frame could not yet be decoded or is malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    Incomplete { needed: usize, available: usize },
    UnsupportedVersion(u8),
    UnknownOpcode(u8),
    ValueTooLarge { length: usize, maximum: usize },
    ChecksumMismatch { expected: u32, actual: u32 },
    KeyTooLarge { length: usize, maximum: usize },
    UnknownStatus(u8),
    InvalidHeaderLength(u16),
    ReservedFieldNonZero,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyTooLarge { length, maximum } => {
                write!(formatter, "key length {length} exceeds maximum {maximum}")
            }
            Self::ValueTooLarge { length, maximum } => {
                write!(formatter, "value length {length} exceeds maximum {maximum}")
            }
            Self::TtlTooLarge(ttl) => write!(formatter, "TTL {ttl} exceeds 24-bit maximum"),
            Self::OutputTooSmall { needed, available } => {
                write!(formatter, "output needs {needed} bytes but has {available}")
            }
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { needed, available } => {
                write!(formatter, "frame needs {needed} bytes but has {available}")
            }
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported protocol version {version}")
            }
            Self::UnknownOpcode(opcode) => write!(formatter, "unknown opcode {opcode:#04x}"),
            Self::ValueTooLarge { length, maximum } => {
                write!(formatter, "value length {length} exceeds maximum {maximum}")
            }
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "checksum mismatch: header has {expected:#010x}, computed {actual:#010x}"
            ),
            Self::KeyTooLarge { length, maximum } => {
                write!(formatter, "key length {length} exceeds maximum {maximum}")
            }
            Self::UnknownStatus(status) => write!(formatter, "unknown status {status:#04x}"),
            Self::InvalidHeaderLength(length) => {
                write!(formatter, "invalid header length {length}")
            }
            Self::ReservedFieldNonZero => write!(formatter, "reserved field is non-zero"),
        }
    }
}

impl std::error::Error for EncodeError {}
impl std::error::Error for DecodeError {}
