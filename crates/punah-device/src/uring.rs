//! Linux `io_uring` ordinary-file baseline.
//!
//! This module owns every buffer until its CQE arrives. The single unsafe SQE
//! push is isolated here because the kernel stores raw buffer pointers between
//! submission and completion.

#![allow(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use io_uring::{IoUring, opcode, squeue, types};

use crate::{
    BlockDevice, DeviceError, IoCompletion, IoOperation, IoRequest, IoResult, RequestId,
    SubmitError,
};

/// Bounded `io_uring` adapter for ordinary Linux files.
///
/// This is deliberately a first baseline: no `O_DIRECT`, registered files or
/// buffers, SQPOLL, IOPOLL, or fixed-buffer claim. `submit` only transfers
/// ownership into a userspace queue; [`BlockDevice::poll`] drives SQ/CQ work.
pub struct UringDevice {
    file: File,
    capacity: u64,
    queue_capacity: usize,
    queued: VecDeque<IoRequest>,
    pending: HashMap<u64, Pending>,
    ring: IoUring,
}

impl UringDevice {
    /// Creates/truncates an ordinary file and creates a bounded ring.
    ///
    /// # Errors
    /// Returns for invalid queue depth or Linux file/ring setup failure.
    pub fn create(
        path: impl AsRef<Path>,
        capacity: u64,
        queue_capacity: usize,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(capacity)?;
        Self::from_file(file, capacity, queue_capacity)
    }

    /// Opens an existing ordinary file and creates a bounded ring.
    ///
    /// # Errors
    /// Returns if the file is too small, queue depth is invalid, or ring setup fails.
    pub fn open(path: impl AsRef<Path>, capacity: u64, queue_capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Self::from_file(file, capacity, queue_capacity)
    }

    /// Wraps an existing ordinary file.
    ///
    /// # Errors
    /// Returns if the file is too small, queue depth is zero/unrepresentable, or
    /// `io_uring_setup` is unavailable/rejected by the running kernel.
    pub fn from_file(file: File, capacity: u64, queue_capacity: usize) -> io::Result<Self> {
        let entries = u32::try_from(queue_capacity).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "queue capacity exceeds u32")
        })?;
        if entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue capacity must be nonzero",
            ));
        }
        if file.metadata()?.len() < capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file is smaller than device capacity",
            ));
        }
        Ok(Self {
            file,
            capacity,
            queue_capacity,
            queued: VecDeque::with_capacity(queue_capacity),
            pending: HashMap::with_capacity(queue_capacity),
            ring: IoUring::new(entries)?,
        })
    }

    #[must_use]
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    #[must_use]
    pub fn pending(&self) -> usize {
        self.queued.len() + self.pending.len()
    }

    /// Persists file contents and filename metadata from the control plane.
    ///
    /// # Errors
    /// Returns an ordinary filesystem synchronization error.
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn contains_id(&self, id: RequestId) -> bool {
        self.pending.contains_key(&id.0) || self.queued.iter().any(|request| request.id == id)
    }

    fn in_bounds(&self, offset: u64, length: usize) -> bool {
        u64::try_from(length)
            .ok()
            .and_then(|length| offset.checked_add(length))
            .is_some_and(|end| end <= self.capacity)
    }

    fn fill_submission(&mut self) {
        while self.pending.len() < self.queue_capacity {
            let Some(request) = self.queued.pop_front() else {
                break;
            };
            let mut pending = Pending::from_request(request);
            let entry = pending.entry(self.file.as_raw_fd());
            // SAFETY: `pending` owns each buffer referenced by the SQE. It is
            // inserted into `self.pending` before `ring.submit()` and remains
            // there until the matching CQE is drained. The file also outlives
            // the ring and all pending operations.
            if unsafe { self.ring.submission().push(&entry) }.is_err() {
                self.queued.push_front(pending.into_request());
                break;
            }
            let id = pending.id().0;
            debug_assert!(self.pending.insert(id, pending).is_none());
        }
    }

    fn drain_completions(&mut self, output: &mut Vec<IoCompletion>, limit: usize) {
        let available = limit.saturating_sub(output.len());
        let completed: Vec<_> = self
            .ring
            .completion()
            .take(available)
            .map(|entry| (entry.user_data(), entry.result()))
            .collect();
        for (id, result) in completed {
            let completion = self.pending.remove(&id).map_or_else(
                || IoCompletion {
                    id: RequestId(id),
                    result: IoResult::Failed(DeviceError::Backend(
                        "completion for unknown request id".to_owned(),
                    )),
                },
                |pending| pending.complete(result),
            );
            output.push(completion);
        }
    }
}

impl BlockDevice for UringDevice {
    fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError> {
        if self.pending() >= self.queue_capacity {
            return Err(SubmitError {
                error: DeviceError::QueueFull,
                request,
            });
        }
        if self.contains_id(request.id) {
            return Err(SubmitError {
                error: DeviceError::Backend("duplicate request id".to_owned()),
                request,
            });
        }
        let valid = match &request.operation {
            IoOperation::Read { offset, len } => self.in_bounds(*offset, *len as usize),
            IoOperation::Write { offset, data } => {
                u32::try_from(data.len()).is_ok() && self.in_bounds(*offset, data.len())
            }
            IoOperation::Flush => true,
        };
        if !valid {
            return Err(SubmitError {
                error: DeviceError::OutOfBounds,
                request,
            });
        }
        self.queued.push_back(request);
        Ok(())
    }

    fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize {
        let initial = completions.len();
        if limit == 0 {
            return 0;
        }
        self.fill_submission();
        // An enter failure leaves owned buffers in `pending`; a later poll can
        // retry without violating pointer lifetime. CQE failures are delivered
        // normally through `Pending::complete`.
        let _ = self.ring.submit();
        self.drain_completions(completions, initial.saturating_add(limit));
        completions.len() - initial
    }
}

impl Drop for UringDevice {
    fn drop(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        self.fill_submission();
        if self.ring.submit_and_wait(self.pending.len()).is_ok() {
            let mut ignored = Vec::with_capacity(self.pending.len());
            self.drain_completions(&mut ignored, usize::MAX);
        }
        if !self.pending.is_empty() {
            // A kernel/ring teardown error gives us no completion proof. Leak
            // the owned buffers rather than release memory the kernel may still
            // reference. This path is process-fatal-backend territory, not a
            // recoverable request error.
            for (_, pending) in self.pending.drain() {
                std::mem::forget(pending);
            }
        }
    }
}

enum Pending {
    Read {
        id: RequestId,
        offset: u64,
        bytes: Box<[u8]>,
    },
    Write {
        id: RequestId,
        offset: u64,
        bytes: Box<[u8]>,
    },
    Flush {
        id: RequestId,
    },
}

impl Pending {
    fn from_request(request: IoRequest) -> Self {
        match request.operation {
            IoOperation::Read { offset, len } => Self::Read {
                id: request.id,
                offset,
                bytes: vec![0; len as usize].into_boxed_slice(),
            },
            IoOperation::Write { offset, data } => Self::Write {
                id: request.id,
                offset,
                bytes: data,
            },
            IoOperation::Flush => Self::Flush { id: request.id },
        }
    }

    const fn id(&self) -> RequestId {
        match self {
            Self::Read { id, .. } | Self::Write { id, .. } | Self::Flush { id } => *id,
        }
    }

    fn entry(&mut self, fd: i32) -> squeue::Entry {
        match self {
            Self::Read { id, offset, bytes } => opcode::Read::new(
                types::Fd(fd),
                bytes.as_mut_ptr(),
                u32::try_from(bytes.len()).expect("read length originated as u32"),
            )
            .offset(*offset)
            .build()
            .user_data(id.0),
            Self::Write { id, offset, bytes } => opcode::Write::new(
                types::Fd(fd),
                bytes.as_ptr(),
                u32::try_from(bytes.len()).expect("write length validated at admission"),
            )
            .offset(*offset)
            .build()
            .user_data(id.0),
            Self::Flush { id } => opcode::Fsync::new(types::Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(id.0),
        }
    }

    fn into_request(self) -> IoRequest {
        match self {
            Self::Read {
                id, offset, bytes, ..
            } => IoRequest {
                id,
                operation: IoOperation::Read {
                    offset,
                    len: u32::try_from(bytes.len()).expect("read length originated as u32"),
                },
            },
            Self::Write {
                id, offset, bytes, ..
            } => IoRequest {
                id,
                operation: IoOperation::Write {
                    offset,
                    data: bytes,
                },
            },
            Self::Flush { id } => IoRequest {
                id,
                operation: IoOperation::Flush,
            },
        }
    }

    fn complete(self, result: i32) -> IoCompletion {
        let id = self.id();
        if result < 0 {
            return IoCompletion {
                id,
                result: IoResult::Failed(DeviceError::Backend(
                    io::Error::from_raw_os_error(-result).to_string(),
                )),
            };
        }
        let io_result = match self {
            Self::Read { bytes, .. } => complete_read(bytes, result),
            Self::Write { bytes, .. } => complete_write(bytes, result),
            Self::Flush { .. } if result == 0 => IoResult::Flushed,
            Self::Flush { .. } => IoResult::Failed(DeviceError::Backend(format!(
                "unexpected io_uring fsync result {result}"
            ))),
        };
        IoCompletion {
            id,
            result: io_result,
        }
    }
}

fn complete_read(bytes: Box<[u8]>, result: i32) -> IoResult {
    match usize::try_from(result) {
        Ok(length) if length == bytes.len() => IoResult::Read(bytes),
        Ok(length) => IoResult::Failed(DeviceError::Backend(format!(
            "short io_uring read: {length} of {} bytes",
            bytes.len()
        ))),
        Err(_) => IoResult::Failed(DeviceError::Backend(
            "negative io_uring read length".to_owned(),
        )),
    }
}

fn complete_write(bytes: Box<[u8]>, result: i32) -> IoResult {
    match usize::try_from(result) {
        Ok(length) if length == bytes.len() => IoResult::Written(bytes),
        Ok(length) => IoResult::Failed(DeviceError::Backend(format!(
            "short io_uring write: {length} of {} bytes",
            bytes.len()
        ))),
        Err(_) => IoResult::Failed(DeviceError::Backend(
            "negative io_uring write length".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "punah-uring-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn bounded_write_flush_read_round_trip() {
        let path = path();
        let mut device = match UringDevice::create(&path, 4096, 4) {
            Ok(device) => device,
            Err(error) if matches!(error.raw_os_error(), Some(1 | 38 | 95)) => {
                let _ = std::fs::remove_file(path);
                return;
            }
            Err(error) => panic!("create ring: {error}"),
        };
        device
            .submit(IoRequest {
                id: RequestId(1),
                operation: IoOperation::Write {
                    offset: 7,
                    data: Vec::from(&b"punah"[..]).into_boxed_slice(),
                },
            })
            .unwrap();
        finish(&mut device, 1);
        device
            .submit(IoRequest {
                id: RequestId(2),
                operation: IoOperation::Flush,
            })
            .unwrap();
        finish(&mut device, 1);
        device
            .submit(IoRequest {
                id: RequestId(3),
                operation: IoOperation::Read { offset: 7, len: 5 },
            })
            .unwrap();
        assert_eq!(
            finish(&mut device, 1).pop().unwrap().result,
            IoResult::Read(Vec::from(&b"punah"[..]).into_boxed_slice())
        );
        drop(device);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn poll_limit_counts_new_completions_not_existing_entries() {
        let path = path();
        let mut device = match UringDevice::create(&path, 4096, 2) {
            Ok(device) => device,
            Err(error) if matches!(error.raw_os_error(), Some(1 | 38 | 95)) => {
                let _ = std::fs::remove_file(path);
                return;
            }
            Err(error) => panic!("create ring: {error}"),
        };
        device
            .submit(IoRequest {
                id: RequestId(8),
                operation: IoOperation::Flush,
            })
            .unwrap();
        let mut completions = vec![IoCompletion {
            id: RequestId(7),
            result: IoResult::Flushed,
        }];
        while completions.len() == 1 {
            assert!(device.poll(&mut completions, 1) <= 1);
            std::thread::yield_now();
        }
        assert_eq!(completions[1].id, RequestId(8));
        drop(device);
        std::fs::remove_file(path).unwrap();
    }

    fn finish(device: &mut UringDevice, count: usize) -> Vec<IoCompletion> {
        let mut completions = Vec::new();
        while completions.len() < count {
            device.poll(&mut completions, count);
            std::thread::yield_now();
        }
        completions
    }
}
