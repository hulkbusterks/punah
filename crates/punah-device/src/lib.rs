#![deny(unsafe_code)]

mod crash;
mod file;
mod mapped;
mod memory;
#[cfg(target_os = "linux")]
mod uring;

use std::fmt;

pub use crash::CrashDevice;
pub use file::FileDevice;
pub use mapped::{MappedReadError, MappedSegment};
pub use memory::MemoryDevice;
#[cfg(target_os = "linux")]
pub use uring::UringDevice;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RequestId(pub u64);

#[derive(Debug, Eq, PartialEq)]
pub enum IoOperation {
    Read { offset: u64, len: u32 },
    Write { offset: u64, data: Box<[u8]> },
    Flush,
}

#[derive(Debug, Eq, PartialEq)]
pub struct IoRequest {
    pub id: RequestId,
    pub operation: IoOperation,
}

#[derive(Debug, Eq, PartialEq)]
pub enum IoResult {
    Read(Box<[u8]>),
    Written(Box<[u8]>),
    Flushed,
    Failed(DeviceError),
}

#[derive(Debug, Eq, PartialEq)]
pub struct IoCompletion {
    pub id: RequestId,
    pub result: IoResult,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SubmitError {
    pub error: DeviceError,
    pub request: IoRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceError {
    QueueFull,
    OutOfBounds,
    Backend(String),
}

impl fmt::Display for DeviceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => formatter.write_str("device queue is full"),
            Self::OutOfBounds => formatter.write_str("I/O is outside device capacity"),
            Self::Backend(message) => write!(formatter, "device backend: {message}"),
        }
    }
}

impl std::error::Error for DeviceError {}

/// A queue-owned, polling-oriented device.
///
/// Implementations are intentionally mutable: one worker owns a device queue
/// and drives both submission and completion polling. Completions may arrive
/// in any order and must be correlated by [`RequestId`]. A successful flush
/// persists every successful write completion observed before that flush was
/// submitted; callers must wait for a write phase before submitting its
/// durability barrier.
pub trait BlockDevice {
    /// Submits an operation without blocking for its completion.
    ///
    /// # Errors
    ///
    /// Returns [`DeviceError::QueueFull`] when backpressure prevents immediate
    /// submission, or another device error when the request is invalid.
    fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError>;

    /// Appends at most `limit` newly observed completions and returns the
    /// number appended. Existing entries in `completions` do not consume the
    /// limit.
    fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize;
}
