use punah_device::{
    BlockDevice, DeviceError, IoCompletion, IoOperation, IoRequest, IoResult, RequestId,
};
use punah_record::{
    COMMIT_MARKER_OFFSET, EncodeError, Record, commit_marker_bytes, encode_uncommitted,
};
use punah_types::{Durability, RecordLocation, SegmentId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct AppendTicket(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendCompletion {
    pub ticket: AppendTicket,
    pub record: Record,
    pub location: RecordLocation,
    pub durability: Durability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendError {
    Busy,
    WriterFailed,
    SegmentFull,
    UnsupportedDurability(Durability),
    Encode(EncodeError),
    Submission(DeviceError),
    Io(DeviceError),
    UnexpectedCompletion,
    OffsetOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRejected {
    pub error: AppendError,
    pub record: Box<Record>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AppendStage {
    WritingRecord,
    FlushingRecord,
    WritingCommit,
    FlushingCommit,
}

struct PendingAppend {
    ticket: AppendTicket,
    request_id: RequestId,
    record: Record,
    location: RecordLocation,
    durability: Durability,
    stage: AppendStage,
}

/// Single-queue reference segment writer.
///
/// It deliberately permits one append at a time. Batching and multiple
/// in-flight records belong in the optimized worker after the commit protocol
/// is proven correct.
pub struct SegmentWriter<D> {
    device: D,
    segment_id: SegmentId,
    generation: u32,
    capacity: u64,
    next_offset: u64,
    next_request_id: u64,
    pending: Option<PendingAppend>,
    failed: bool,
}

impl<D: BlockDevice> SegmentWriter<D> {
    #[must_use]
    pub fn new(device: D, segment_id: SegmentId, generation: u32, capacity: u64) -> Self {
        Self::new_at(device, segment_id, generation, capacity, 0)
    }

    /// Reopens a segment at a previously recovered committed boundary.
    ///
    /// Callers must obtain `next_offset` by scanning committed records rather
    /// than trusting file length, because segment files may be preallocated.
    #[must_use]
    pub fn new_at(
        device: D,
        segment_id: SegmentId,
        generation: u32,
        capacity: u64,
        next_offset: u64,
    ) -> Self {
        Self {
            device,
            segment_id,
            generation,
            capacity,
            next_offset,
            next_request_id: 1,
            pending: None,
            failed: false,
        }
    }

    /// Submits an uncommitted record body to the owned device queue.
    ///
    /// # Errors
    ///
    /// Returns the original record with an error when the writer is busy, the
    /// segment is full, encoding fails, durability belongs to a future
    /// replication layer, or the device rejects submission.
    pub fn try_append(
        &mut self,
        record: Record,
        durability: Durability,
    ) -> Result<AppendTicket, AppendRejected> {
        if self.failed {
            return Err(AppendRejected {
                error: AppendError::WriterFailed,
                record: Box::new(record),
            });
        }
        if self.pending.is_some() {
            return Err(AppendRejected {
                error: AppendError::Busy,
                record: Box::new(record),
            });
        }
        if matches!(durability, Durability::Replicated { .. }) {
            return Err(AppendRejected {
                error: AppendError::UnsupportedDurability(durability),
                record: Box::new(record),
            });
        }
        let encoded = match encode_uncommitted(&record) {
            Ok(encoded) => encoded,
            Err(error) => {
                return Err(AppendRejected {
                    error: AppendError::Encode(error),
                    record: Box::new(record),
                });
            }
        };
        let Ok(stored_len) = u32::try_from(encoded.len()) else {
            return Err(AppendRejected {
                error: AppendError::OffsetOverflow,
                record: Box::new(record),
            });
        };
        let Some(end) = self.next_offset.checked_add(u64::from(stored_len)) else {
            return Err(AppendRejected {
                error: AppendError::OffsetOverflow,
                record: Box::new(record),
            });
        };
        if end > self.capacity {
            return Err(AppendRejected {
                error: AppendError::SegmentFull,
                record: Box::new(record),
            });
        }
        let Ok(logical_len) = u32::try_from(record.payload.len()) else {
            return Err(AppendRejected {
                error: AppendError::OffsetOverflow,
                record: Box::new(record),
            });
        };

        let ticket = AppendTicket(self.next_request_id);
        let request_id = self.allocate_request_id();
        let request = IoRequest {
            id: request_id,
            operation: IoOperation::Write {
                offset: self.next_offset,
                data: encoded.into_boxed_slice(),
            },
        };
        if let Err(error) = self.device.submit(request) {
            return Err(AppendRejected {
                error: AppendError::Submission(error.error),
                record: Box::new(record),
            });
        }
        let location = RecordLocation {
            segment_id: self.segment_id,
            generation: self.generation,
            offset: self.next_offset,
            stored_len,
            logical_len,
        };
        self.next_offset = end;
        self.pending = Some(PendingAppend {
            ticket,
            request_id,
            record,
            location,
            durability,
            stage: AppendStage::WritingRecord,
        });
        Ok(ticket)
    }

    /// Advances one device completion and possibly emits a durable append.
    ///
    /// The caller may publish the returned record in the index only after this
    /// method returns [`AppendCompletion`].
    pub fn poll(&mut self) -> Option<Result<AppendCompletion, AppendError>> {
        let mut completions = Vec::with_capacity(1);
        if self.device.poll(&mut completions, 1) == 0 {
            return None;
        }
        let Some(completion) = completions.pop() else {
            self.failed = true;
            return Some(Err(AppendError::UnexpectedCompletion));
        };
        let Some(pending) = self.pending.take() else {
            self.failed = true;
            return Some(Err(AppendError::UnexpectedCompletion));
        };
        if completion.id != pending.request_id {
            self.failed = true;
            return Some(Err(AppendError::UnexpectedCompletion));
        }
        if let IoResult::Failed(error) = completion.result {
            self.failed = true;
            return Some(Err(AppendError::Io(error)));
        }
        if !completion_matches_stage(&completion, pending.stage) {
            self.failed = true;
            return Some(Err(AppendError::UnexpectedCompletion));
        }

        let outcome = self.advance(pending);
        if matches!(outcome, Some(Err(_))) {
            self.failed = true;
        }
        outcome
    }

    #[must_use]
    pub fn reserved_boundary(&self) -> u64 {
        self.next_offset
    }

    #[must_use]
    pub fn into_device(self) -> D {
        self.device
    }

    fn advance(&mut self, pending: PendingAppend) -> Option<Result<AppendCompletion, AppendError>> {
        match pending.stage {
            AppendStage::WritingRecord if requires_flush(pending.durability) => transition_result(
                self.submit_next(pending, AppendStage::FlushingRecord, IoOperation::Flush),
            ),
            AppendStage::WritingRecord | AppendStage::FlushingRecord => {
                let Some(offset) = pending.location.offset.checked_add(COMMIT_MARKER_OFFSET) else {
                    return Some(Err(AppendError::OffsetOverflow));
                };
                transition_result(self.submit_next(
                    pending,
                    AppendStage::WritingCommit,
                    IoOperation::Write {
                        offset,
                        data: Box::new(commit_marker_bytes()),
                    },
                ))
            }
            AppendStage::WritingCommit if requires_flush(pending.durability) => transition_result(
                self.submit_next(pending, AppendStage::FlushingCommit, IoOperation::Flush),
            ),
            AppendStage::WritingCommit | AppendStage::FlushingCommit => {
                Some(Ok(AppendCompletion {
                    ticket: pending.ticket,
                    record: pending.record,
                    location: pending.location,
                    durability: pending.durability,
                }))
            }
        }
    }

    fn submit_next(
        &mut self,
        mut pending: PendingAppend,
        stage: AppendStage,
        operation: IoOperation,
    ) -> Result<(), AppendError> {
        let request_id = self.allocate_request_id();
        let request = IoRequest {
            id: request_id,
            operation,
        };
        self.device
            .submit(request)
            .map_err(|error| AppendError::Submission(error.error))?;
        pending.request_id = request_id;
        pending.stage = stage;
        self.pending = Some(pending);
        Ok(())
    }

    fn allocate_request_id(&mut self) -> RequestId {
        let request_id = RequestId(self.next_request_id);
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        request_id
    }
}

fn transition_result(
    result: Result<(), AppendError>,
) -> Option<Result<AppendCompletion, AppendError>> {
    result.err().map(Err)
}

fn requires_flush(durability: Durability) -> bool {
    matches!(durability, Durability::LocalDurable)
}

fn completion_matches_stage(completion: &IoCompletion, stage: AppendStage) -> bool {
    matches!(
        (&completion.result, stage),
        (
            IoResult::Written(_),
            AppendStage::WritingRecord | AppendStage::WritingCommit
        ) | (
            IoResult::Flushed,
            AppendStage::FlushingRecord | AppendStage::FlushingCommit
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use punah_device::{FileDevice, MemoryDevice, SubmitError};
    use punah_record::{RecordType, decode};
    use punah_types::Namespace;
    use std::time::SystemTime;

    struct FailingDevice {
        inner: MemoryDevice,
        fail_completion: usize,
        completion_count: usize,
    }

    impl FailingDevice {
        fn new(fail_completion: usize) -> Self {
            Self {
                inner: MemoryDevice::new(4096, 4),
                fail_completion,
                completion_count: 0,
            }
        }
    }

    impl BlockDevice for FailingDevice {
        fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError> {
            self.inner.submit(request)
        }

        fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize {
            let before = completions.len();
            let count = self.inner.poll(completions, limit);
            for completion in &mut completions[before..] {
                self.completion_count += 1;
                if self.completion_count == self.fail_completion {
                    completion.result = IoResult::Failed(DeviceError::Backend(
                        "injected completion failure".to_owned(),
                    ));
                }
            }
            count
        }
    }

    fn record(payload: &[u8]) -> Record {
        Record {
            record_type: RecordType::Artifact,
            flags: 0,
            namespace: Namespace::new("swarm").expect("namespace"),
            key: b"key".to_vec(),
            sequence: 1,
            created_at_unix_ns: 10,
            expires_at_unix_ns: None,
            operation_hash: [1; 32],
            content_hash: [2; 32],
            dependencies: Vec::new(),
            payload: payload.to_vec(),
        }
    }

    fn finish_append(
        writer: &mut SegmentWriter<MemoryDevice>,
    ) -> Result<AppendCompletion, AppendError> {
        for _ in 0..8 {
            if let Some(result) = writer.poll() {
                return result;
            }
        }
        panic!("append did not complete");
    }

    #[test]
    fn buffered_append_becomes_a_decodable_record() {
        let device = MemoryDevice::new(4096, 4);
        let mut writer = SegmentWriter::new(device, SegmentId(3), 1, 4096);
        let original = record(b"result");
        let ticket = writer
            .try_append(original.clone(), Durability::Buffered)
            .expect("submit");
        let completion = finish_append(&mut writer).expect("complete");
        assert_eq!(completion.ticket, ticket);
        assert_eq!(completion.record, original);

        let mut device = writer.into_device();
        device
            .submit(IoRequest {
                id: RequestId(99),
                operation: IoOperation::Read {
                    offset: completion.location.offset,
                    len: completion.location.stored_len,
                },
            })
            .expect("read submission");
        let mut completions = Vec::new();
        device.poll(&mut completions, 1);
        let IoResult::Read(bytes) = &completions[0].result else {
            panic!("expected read completion");
        };
        assert_eq!(decode(bytes).expect("durable record").record, original);
    }

    #[test]
    fn locally_durable_append_crosses_both_flush_stages() {
        let device = MemoryDevice::new(4096, 4);
        let mut writer = SegmentWriter::new(device, SegmentId(1), 0, 4096);
        writer
            .try_append(record(b"durable"), Durability::LocalDurable)
            .expect("submit");

        assert!(writer.poll().is_none());
        assert!(writer.poll().is_none());
        assert!(writer.poll().is_none());
        assert!(matches!(writer.poll(), Some(Ok(_))));
    }

    #[test]
    fn full_segment_rejects_without_consuming_record() {
        let device = MemoryDevice::new(64, 1);
        let mut writer = SegmentWriter::new(device, SegmentId(1), 0, 64);
        let original = record(b"too large");
        let rejected = writer
            .try_append(original.clone(), Durability::Buffered)
            .expect_err("segment is too small");
        assert_eq!(rejected.error, AppendError::SegmentFull);
        assert_eq!(*rejected.record, original);
        assert_eq!(writer.reserved_boundary(), 0);
    }

    #[test]
    fn every_local_durability_failure_boundary_recovers_safely() {
        for failure in 1..=4 {
            let device = FailingDevice::new(failure);
            let mut writer = SegmentWriter::new(device, SegmentId(9), 0, 4096);
            writer
                .try_append(record(b"crash boundary"), Durability::LocalDurable)
                .expect("initial submission");

            let error = loop {
                if let Some(result) = writer.poll() {
                    break result.expect_err("injected failure must fail append");
                }
            };
            assert!(matches!(error, AppendError::Io(_)));
            assert_eq!(
                writer
                    .try_append(record(b"must be rejected"), Durability::Buffered)
                    .expect_err("failed writer cannot append")
                    .error,
                AppendError::WriterFailed
            );

            let mut device = writer.into_device();
            device
                .submit(IoRequest {
                    id: RequestId(100),
                    operation: IoOperation::Read {
                        offset: 0,
                        len: 4096,
                    },
                })
                .expect("read submission");
            let mut completions = Vec::new();
            device.poll(&mut completions, 1);
            let IoResult::Read(bytes) = &completions[0].result else {
                panic!("expected readable device after failure");
            };
            let recovered = crate::recover_segment(bytes, SegmentId(9), 0);
            assert!(
                recovered.is_ok(),
                "failure boundary {failure} corrupted segment"
            );
            let (_, report) = recovered.expect("safe recovery");
            assert!(report.records_scanned <= 1);
        }
    }

    #[test]
    fn locally_durable_file_segment_recovers_after_reopen() {
        let path = std::env::temp_dir().join(format!(
            "punah-segment-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let device = FileDevice::create(&path, 4096, 4).expect("file device");
        let mut writer = SegmentWriter::new(device, SegmentId(5), 2, 4096);
        writer
            .try_append(record(b"persistent artifact"), Durability::LocalDurable)
            .expect("submit");
        let completion = loop {
            if let Some(result) = writer.poll() {
                break result.expect("durable completion");
            }
        };
        assert_eq!(completion.location.offset, 0);
        drop(writer.into_device());

        let bytes = std::fs::read(&path).expect("reopen segment");
        let (index, report) = crate::recover_segment(&bytes, SegmentId(5), 2).expect("recover");
        assert_eq!(index.len(), 1);
        assert_eq!(report.records_scanned, 1);
        std::fs::remove_file(path).expect("remove segment");
    }
}
