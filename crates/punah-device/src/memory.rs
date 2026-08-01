use std::collections::VecDeque;

use crate::{
    BlockDevice, DeviceError, IoCompletion, IoOperation, IoRequest, IoResult, SubmitError,
};

/// Deterministic bounded device used to test the polling engine.
pub struct MemoryDevice {
    bytes: Vec<u8>,
    queue: VecDeque<IoRequest>,
    queue_capacity: usize,
}

impl MemoryDevice {
    /// Creates a zero-filled device with a bounded queue.
    ///
    /// # Panics
    ///
    /// Panics if `queue_capacity` is zero.
    #[must_use]
    pub fn new(capacity: usize, queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0, "queue capacity must be non-zero");
        Self {
            bytes: vec![0; capacity],
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
        }
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    fn execute(&mut self, request: IoRequest) -> IoCompletion {
        let result = match request.operation {
            IoOperation::Read { offset, len } => self.read(offset, len),
            IoOperation::Write { offset, data } => self.write(offset, data),
            IoOperation::Flush => IoResult::Flushed,
        };
        IoCompletion {
            id: request.id,
            result,
        }
    }

    fn read(&self, offset: u64, len: u32) -> IoResult {
        let Ok(start) = usize::try_from(offset) else {
            return IoResult::Failed(DeviceError::OutOfBounds);
        };
        let Some(end) = start.checked_add(len as usize) else {
            return IoResult::Failed(DeviceError::OutOfBounds);
        };
        self.bytes
            .get(start..end)
            .map_or(IoResult::Failed(DeviceError::OutOfBounds), |bytes| {
                IoResult::Read(bytes.to_vec().into_boxed_slice())
            })
    }

    fn write(&mut self, offset: u64, data: Box<[u8]>) -> IoResult {
        let Ok(start) = usize::try_from(offset) else {
            return IoResult::Failed(DeviceError::OutOfBounds);
        };
        let Some(end) = start.checked_add(data.len()) else {
            return IoResult::Failed(DeviceError::OutOfBounds);
        };
        let Some(destination) = self.bytes.get_mut(start..end) else {
            return IoResult::Failed(DeviceError::OutOfBounds);
        };
        destination.copy_from_slice(&data);
        IoResult::Written(data)
    }
}

impl BlockDevice for MemoryDevice {
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

    #[test]
    fn work_completes_only_when_polled() {
        let mut device = MemoryDevice::new(64, 2);
        device
            .submit(IoRequest {
                id: RequestId(1),
                operation: IoOperation::Write {
                    offset: 4,
                    data: Vec::from(&b"punah"[..]).into_boxed_slice(),
                },
            })
            .expect("submit write");
        device
            .submit(IoRequest {
                id: RequestId(2),
                operation: IoOperation::Read { offset: 4, len: 5 },
            })
            .expect("submit read");

        let mut completions = Vec::new();
        assert_eq!(device.poll(&mut completions, 1), 1);
        assert_eq!(device.pending(), 1);
        assert_eq!(device.poll(&mut completions, 8), 1);
        assert_eq!(
            completions[1].result,
            IoResult::Read(Vec::from(&b"punah"[..]).into_boxed_slice())
        );
    }

    #[test]
    fn reports_backpressure_and_bounds() {
        let mut device = MemoryDevice::new(8, 1);
        device
            .submit(IoRequest {
                id: RequestId(1),
                operation: IoOperation::Read { offset: 7, len: 2 },
            })
            .expect("first request");
        let rejected = device
            .submit(IoRequest {
                id: RequestId(2),
                operation: IoOperation::Flush,
            })
            .expect_err("queue full");
        assert_eq!(rejected.error, DeviceError::QueueFull);

        let mut completions = Vec::new();
        device.poll(&mut completions, 1);
        assert_eq!(
            completions[0].result,
            IoResult::Failed(DeviceError::OutOfBounds)
        );
    }
}
