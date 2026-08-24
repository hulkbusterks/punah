use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::{
    BlockDevice, DeviceError, IoCompletion, IoOperation, IoRequest, IoResult, SubmitError,
};

/// A portable, polling file-backed block device.
///
/// Work is performed synchronously by [`BlockDevice::poll`], never by
/// `submit`. Flush uses [`File::sync_data`], persisting file contents without
/// requiring unrelated filesystem metadata to reach stable storage.
pub struct FileDevice {
    file: File,
    capacity: u64,
    queue: VecDeque<IoRequest>,
    queue_capacity: usize,
}

impl FileDevice {
    /// Creates or truncates a zero-length backing file to `capacity` bytes.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be created, sized, or inspected,
    /// or if `queue_capacity` is zero.
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

    /// Opens an existing backing file. It must be at least `capacity` bytes.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or inspected, if it is
    /// too small, or if `queue_capacity` is zero.
    pub fn open(path: impl AsRef<Path>, capacity: u64, queue_capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Self::from_file(file, capacity, queue_capacity)
    }

    /// Wraps an existing readable and writable file.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if metadata cannot be read, the file is too small,
    /// or `queue_capacity` is zero.
    pub fn from_file(file: File, capacity: u64, queue_capacity: usize) -> io::Result<Self> {
        if queue_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue capacity must be non-zero",
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
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
        })
    }

    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Persists file contents and the metadata required to reopen this device.
    ///
    /// Segment creation uses this control-plane operation before publishing a
    /// metadata root that references the new filename. Request-path durability
    /// continues to use queued [`IoOperation::Flush`] operations.
    ///
    /// # Errors
    ///
    /// Returns an OS error when the backing file cannot be synchronized.
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn in_bounds(&self, offset: u64, len: usize) -> bool {
        u64::try_from(len)
            .ok()
            .and_then(|len| offset.checked_add(len))
            .is_some_and(|end| end <= self.capacity)
    }

    fn execute(&mut self, request: IoRequest) -> IoCompletion {
        let result = match request.operation {
            IoOperation::Read { offset, len } => self.read(offset, len),
            IoOperation::Write { offset, data } => self.write(offset, data),
            IoOperation::Flush => self
                .file
                .sync_data()
                .map_or_else(|error| backend(&error), |()| IoResult::Flushed),
        };
        IoCompletion {
            id: request.id,
            result,
        }
    }

    fn read(&mut self, offset: u64, len: u32) -> IoResult {
        if !self.in_bounds(offset, len as usize) {
            return IoResult::Failed(DeviceError::OutOfBounds);
        }
        let mut bytes = vec![0; len as usize];
        if let Err(error) = self
            .file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.read_exact(&mut bytes))
        {
            return backend(&error);
        }
        IoResult::Read(bytes.into_boxed_slice())
    }

    fn write(&mut self, offset: u64, data: Box<[u8]>) -> IoResult {
        if !self.in_bounds(offset, data.len()) {
            return IoResult::Failed(DeviceError::OutOfBounds);
        }
        if let Err(error) = self
            .file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.write_all(&data))
        {
            return backend(&error);
        }
        IoResult::Written(data)
    }
}

fn backend(error: &io::Error) -> IoResult {
    IoResult::Failed(DeviceError::Backend(error.to_string()))
}

impl BlockDevice for FileDevice {
    fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError> {
        if self.queue.len() >= self.queue_capacity {
            return Err(SubmitError {
                error: DeviceError::QueueFull,
                request,
            });
        }
        self.queue.push_back(request);
        Ok(())
    }

    fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize {
        let count = limit.min(self.queue.len());
        for _ in 0..count {
            let request = self
                .queue
                .pop_front()
                .expect("count is bounded by queue length");
            let completion = self.execute(request);
            completions.push(completion);
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestId;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "punah-device-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ))
    }

    #[test]
    fn writes_reads_flushes_and_returns_owned_buffer() {
        let path = path();
        let mut device = FileDevice::create(&path, 32, 3).expect("create");
        let data = Vec::from(&b"punah"[..]).into_boxed_slice();
        device
            .submit(IoRequest {
                id: RequestId(1),
                operation: IoOperation::Write { offset: 7, data },
            })
            .expect("write");
        device
            .submit(IoRequest {
                id: RequestId(2),
                operation: IoOperation::Flush,
            })
            .expect("flush");
        device
            .submit(IoRequest {
                id: RequestId(3),
                operation: IoOperation::Read { offset: 7, len: 5 },
            })
            .expect("read");
        let mut completions = Vec::new();
        assert_eq!(device.poll(&mut completions, 8), 3);
        assert_eq!(
            completions[0].result,
            IoResult::Written(Vec::from(&b"punah"[..]).into_boxed_slice())
        );
        assert_eq!(completions[1].result, IoResult::Flushed);
        assert_eq!(
            completions[2].result,
            IoResult::Read(Vec::from(&b"punah"[..]).into_boxed_slice())
        );
        drop(device);
        assert_eq!(
            &std::fs::read(&path).expect("read backing")[7..12],
            b"punah"
        );
        std::fs::remove_file(path).expect("remove temp file");
    }

    #[test]
    fn validates_capacity_bounds_and_queue() {
        let path = path();
        std::fs::write(&path, [0; 4]).expect("seed");
        assert!(FileDevice::open(&path, 5, 1).is_err());
        assert!(FileDevice::open(&path, 4, 0).is_err());
        let mut device = FileDevice::open(&path, 4, 1).expect("open");
        device
            .submit(IoRequest {
                id: RequestId(1),
                operation: IoOperation::Read { offset: 4, len: 1 },
            })
            .expect("queued");
        let rejected = device
            .submit(IoRequest {
                id: RequestId(2),
                operation: IoOperation::Flush,
            })
            .expect_err("full");
        assert_eq!(rejected.error, DeviceError::QueueFull);
        let mut completions = Vec::new();
        device.poll(&mut completions, 1);
        assert_eq!(
            completions[0].result,
            IoResult::Failed(DeviceError::OutOfBounds)
        );
        drop(device);
        std::fs::remove_file(path).expect("remove temp file");
    }
}
