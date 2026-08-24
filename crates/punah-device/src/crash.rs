use std::collections::VecDeque;

use crate::{
    BlockDevice, DeviceError, IoCompletion, IoOperation, IoRequest, IoResult, SubmitError,
};

const POWER_LOSS_MESSAGE: &str = "simulated power loss";

/// Deterministic device with separate volatile and durable byte images.
///
/// Completed writes modify only the volatile image. A completed flush copies
/// the volatile image to the durable image. [`Self::power_loss`] discards
/// volatile state and queued requests, modelling a restart from durable bytes.
/// This backend is intentionally simple and intended for crash-consistency
/// tests rather than throughput measurements. Each write effect is atomic and
/// Flush promotes the complete image; it does not model torn sectors, partial
/// failed-Flush persistence, controller reordering, or SPDK/FUA semantics.
pub struct CrashDevice {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    queue: VecDeque<IoRequest>,
    queue_capacity: usize,
    effects_until_power_loss: Option<usize>,
}

impl CrashDevice {
    /// Creates a zero-filled device with a bounded queue.
    ///
    /// # Panics
    ///
    /// Panics if `queue_capacity` is zero.
    #[must_use]
    pub fn new(capacity: usize, queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0, "queue capacity must be non-zero");
        Self {
            volatile: vec![0; capacity],
            durable: vec![0; capacity],
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
            effects_until_power_loss: None,
        }
    }

    /// Reopens a device from a previously captured durable image.
    ///
    /// # Panics
    ///
    /// Panics if `queue_capacity` is zero.
    #[must_use]
    pub fn from_durable(durable: Vec<u8>, queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0, "queue capacity must be non-zero");
        Self {
            volatile: durable.clone(),
            durable,
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
            effects_until_power_loss: None,
        }
    }

    /// Injects power loss at the selected future operation-effect boundary.
    ///
    /// Zero loses power before the first queued operation takes effect. A
    /// positive value executes that many operations, then loses power before
    /// the triggering operation's successful completion is observed. The
    /// triggering request instead receives one backend-failure completion so a
    /// synchronous polling state machine can terminate deterministically.
    pub fn arm_power_loss_after(&mut self, completed_effects: usize) {
        self.effects_until_power_loss = Some(completed_effects);
    }

    /// Discards volatile bytes and all submitted but incomplete operations.
    ///
    /// The device is immediately usable as a restarted device. Returns the
    /// number of queued requests that were lost.
    pub fn power_loss(&mut self) -> usize {
        let dropped = self.queue.len();
        self.queue.clear();
        self.volatile.copy_from_slice(&self.durable);
        self.effects_until_power_loss = None;
        dropped
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.volatile.len()
    }

    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Bytes visible before a simulated restart.
    #[must_use]
    pub fn volatile_bytes(&self) -> &[u8] {
        &self.volatile
    }

    /// Bytes guaranteed to survive a simulated restart.
    #[must_use]
    pub fn durable_bytes(&self) -> &[u8] {
        &self.durable
    }

    #[must_use]
    pub fn into_durable_bytes(self) -> Vec<u8> {
        self.durable
    }

    fn execute(&mut self, request: IoRequest) -> IoCompletion {
        let result = match request.operation {
            IoOperation::Read { offset, len } => self.read(offset, len),
            IoOperation::Write { offset, data } => self.write(offset, data),
            IoOperation::Flush => {
                self.durable.copy_from_slice(&self.volatile);
                IoResult::Flushed
            }
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
        self.volatile
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
        let Some(destination) = self.volatile.get_mut(start..end) else {
            return IoResult::Failed(DeviceError::OutOfBounds);
        };
        destination.copy_from_slice(&data);
        IoResult::Written(data)
    }

    fn should_lose_power_before_effect(&self) -> bool {
        self.effects_until_power_loss == Some(0)
    }

    fn should_lose_power_after_effect(&mut self) -> bool {
        let Some(remaining) = self.effects_until_power_loss.as_mut() else {
            return false;
        };
        if *remaining == 0 {
            return false;
        }
        *remaining -= 1;
        *remaining == 0
    }
}

impl BlockDevice for CrashDevice {
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
        let mut completed = 0;
        for _ in 0..count {
            let request = self
                .queue
                .pop_front()
                .expect("count is bounded by queue length");
            if self.should_lose_power_before_effect() {
                let id = request.id;
                self.power_loss();
                completions.push(IoCompletion {
                    id,
                    result: IoResult::Failed(DeviceError::Backend(POWER_LOSS_MESSAGE.to_owned())),
                });
                completed += 1;
                break;
            }
            let mut completion = self.execute(request);
            completed += 1;
            if self.should_lose_power_after_effect() {
                self.power_loss();
                completion.result =
                    IoResult::Failed(DeviceError::Backend(POWER_LOSS_MESSAGE.to_owned()));
                completions.push(completion);
                break;
            }
            completions.push(completion);
        }
        completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestId;

    fn request(id: u64, operation: IoOperation) -> IoRequest {
        IoRequest {
            id: RequestId(id),
            operation,
        }
    }

    #[test]
    fn flush_advances_durable_image_and_power_loss_discards_later_writes() {
        let mut device = CrashDevice::new(16, 4);
        device
            .submit(request(
                1,
                IoOperation::Write {
                    offset: 2,
                    data: Vec::from(&b"body"[..]).into_boxed_slice(),
                },
            ))
            .unwrap();
        let mut completions = Vec::new();
        device.poll(&mut completions, 1);
        assert_eq!(&device.volatile_bytes()[2..6], b"body");
        assert_eq!(&device.durable_bytes()[2..6], &[0; 4]);

        device.submit(request(2, IoOperation::Flush)).unwrap();
        device.poll(&mut completions, 1);
        assert_eq!(&device.durable_bytes()[2..6], b"body");

        device
            .submit(request(
                3,
                IoOperation::Write {
                    offset: 2,
                    data: Vec::from(&b"lost"[..]).into_boxed_slice(),
                },
            ))
            .unwrap();
        device.poll(&mut completions, 1);
        assert_eq!(&device.volatile_bytes()[2..6], b"lost");
        assert_eq!(device.power_loss(), 0);
        assert_eq!(&device.volatile_bytes()[2..6], b"body");
    }

    #[test]
    fn injected_power_loss_after_an_effect_fails_one_completion_and_drops_the_queue() {
        let mut device = CrashDevice::new(16, 2);
        device
            .submit(request(
                1,
                IoOperation::Write {
                    offset: 0,
                    data: Box::new([1]),
                },
            ))
            .unwrap();
        device
            .submit(request(
                2,
                IoOperation::Write {
                    offset: 1,
                    data: Box::new([2]),
                },
            ))
            .unwrap();
        device.arm_power_loss_after(1);

        let mut completions = Vec::new();
        assert_eq!(device.poll(&mut completions, 2), 1);
        assert_eq!(device.pending(), 0);
        assert!(matches!(
            completions[0].result,
            IoResult::Failed(DeviceError::Backend(ref message)) if message == POWER_LOSS_MESSAGE
        ));
        assert_eq!(device.volatile_bytes(), &[0; 16]);
        assert_eq!(device.durable_bytes(), &[0; 16]);
    }

    #[test]
    fn target_zero_loses_power_before_a_flush_effect() {
        let mut device = CrashDevice::new(16, 2);
        device
            .submit(request(
                1,
                IoOperation::Write {
                    offset: 0,
                    data: Box::new([1]),
                },
            ))
            .unwrap();
        let mut completions = Vec::new();
        device.poll(&mut completions, 1);
        assert_eq!(device.volatile_bytes()[0], 1);
        assert_eq!(device.durable_bytes()[0], 0);

        device.submit(request(2, IoOperation::Flush)).unwrap();
        device.arm_power_loss_after(0);
        completions.clear();
        assert_eq!(device.poll(&mut completions, 1), 1);
        assert!(matches!(
            completions[0].result,
            IoResult::Failed(DeviceError::Backend(ref message)) if message == POWER_LOSS_MESSAGE
        ));
        assert_eq!(device.volatile_bytes()[0], 0);
        assert_eq!(device.durable_bytes()[0], 0);
    }

    #[test]
    fn a_flush_effect_can_be_durable_even_when_its_acknowledgement_is_lost() {
        let mut device = CrashDevice::new(16, 2);
        device
            .submit(request(
                1,
                IoOperation::Write {
                    offset: 0,
                    data: Box::new([1]),
                },
            ))
            .unwrap();
        let mut completions = Vec::new();
        device.poll(&mut completions, 1);

        device.submit(request(2, IoOperation::Flush)).unwrap();
        device.arm_power_loss_after(1);
        completions.clear();
        assert_eq!(device.poll(&mut completions, 1), 1);
        assert!(matches!(
            completions[0].result,
            IoResult::Failed(DeviceError::Backend(ref message)) if message == POWER_LOSS_MESSAGE
        ));
        assert_eq!(device.volatile_bytes()[0], 1);
        assert_eq!(device.durable_bytes()[0], 1);
    }
}
