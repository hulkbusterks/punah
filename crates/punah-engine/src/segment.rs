use std::collections::{HashMap, VecDeque};

use punah_device::{
    BlockDevice, DeviceError, IoCompletion, IoOperation, IoRequest, IoResult, RequestId,
};
use punah_record::{
    COMMIT_MARKER_OFFSET, EncodeError, Record, commit_marker_bytes, encode_uncommitted, encoded_len,
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

/// A cohort rejected before publication or failed at an ambiguous I/O boundary.
/// Records are returned for diagnostics; callers must not retry an I/O-failed
/// cohort without reopening because some bytes may already be durable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CohortRejected {
    pub error: AppendError,
    pub records: Vec<Record>,
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

struct CohortEntry {
    ticket: AppendTicket,
    record: Record,
    encoded: Option<Box<[u8]>>,
    location: RecordLocation,
}

#[derive(Clone, Copy)]
enum ExpectedCompletion {
    Written,
    Flushed,
}

struct PhaseFailure {
    error: AppendError,
    accepted_any: bool,
}

/// Queue-owned segment writer with singleton and bounded-cohort append paths.
///
/// A cohort pipelines record bodies and commit markers while retaining one
/// durability barrier per phase. Publication remains strictly in reservation
/// order, independent of device completion order.
pub struct SegmentWriter<D> {
    device: D,
    segment_id: SegmentId,
    generation: u32,
    capacity: u64,
    next_offset: u64,
    next_ticket_id: u64,
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
            next_ticket_id: 1,
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
        let encoded_len = match encoded_len(&record) {
            Ok(encoded_len) => encoded_len,
            Err(error) => {
                return Err(AppendRejected {
                    error: AppendError::Encode(error),
                    record: Box::new(record),
                });
            }
        };
        let Ok(stored_len) = u32::try_from(encoded_len) else {
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
        let encoded = match encode_uncommitted(&record) {
            Ok(encoded) => encoded,
            Err(error) => {
                return Err(AppendRejected {
                    error: AppendError::Encode(error),
                    record: Box::new(record),
                });
            }
        };
        debug_assert_eq!(encoded.len(), encoded_len);

        let ticket = self.allocate_ticket();
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

    /// Appends an ordered cohort using queue depth while sharing durability
    /// barriers across all records.
    ///
    /// `LocalDurable` cohorts perform exactly two flushes: one after every body
    /// write has completed and one after every commit-marker write has
    /// completed. Device completions may arrive in any order; returned
    /// completions always follow reservation order.
    ///
    /// # Errors
    ///
    /// Returns every original record when admission, encoding, submission, or
    /// I/O fails. Once any request has been accepted, a failure is ambiguous
    /// and fail-stops the writer until recovery.
    pub fn append_cohort(
        &mut self,
        records: Vec<Record>,
        durability: Durability,
    ) -> Result<Vec<AppendCompletion>, CohortRejected> {
        if self.failed {
            return Err(CohortRejected {
                error: AppendError::WriterFailed,
                records,
            });
        }
        if self.pending.is_some() {
            return Err(CohortRejected {
                error: AppendError::Busy,
                records,
            });
        }
        if matches!(durability, Durability::Replicated { .. }) {
            return Err(CohortRejected {
                error: AppendError::UnsupportedDurability(durability),
                records,
            });
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let measured = match measure_cohort(&records, self.next_offset, self.capacity) {
            Ok(measured) => measured,
            Err(error) => return Err(CohortRejected { error, records }),
        };
        let end = measured.last().map_or(self.next_offset, |entry| entry.end);

        let mut encoded = Vec::with_capacity(records.len());
        for (record, measurement) in records.iter().zip(&measured) {
            match encode_uncommitted(record) {
                Ok(bytes) => {
                    debug_assert_eq!(bytes.len(), measurement.encoded_len);
                    encoded.push(Some(bytes.into_boxed_slice()));
                }
                Err(error) => {
                    return Err(CohortRejected {
                        error: AppendError::Encode(error),
                        records,
                    });
                }
            }
        }

        let mut entries = Vec::with_capacity(records.len());
        for ((record, encoded), measurement) in records.into_iter().zip(encoded).zip(measured) {
            entries.push(CohortEntry {
                ticket: self.allocate_ticket(),
                record,
                encoded,
                location: RecordLocation {
                    segment_id: self.segment_id,
                    generation: self.generation,
                    offset: measurement.offset,
                    stored_len: measurement.stored_len,
                    logical_len: measurement.logical_len,
                },
            });
        }

        let original_boundary = self.next_offset;
        self.next_offset = end;
        if let Err(failure) = self.write_bodies(&mut entries) {
            if failure.accepted_any {
                self.failed = true;
            } else {
                self.next_offset = original_boundary;
            }
            return Err(cohort_failure(failure.error, entries));
        }
        if requires_flush(durability)
            && let Err(failure) = self.flush_phase()
        {
            self.failed = true;
            return Err(cohort_failure(failure.error, entries));
        }
        if let Err(failure) = self.write_commit_markers(&entries) {
            self.failed = true;
            return Err(cohort_failure(failure.error, entries));
        }
        if requires_flush(durability)
            && let Err(failure) = self.flush_phase()
        {
            self.failed = true;
            return Err(cohort_failure(failure.error, entries));
        }

        Ok(entries
            .into_iter()
            .map(|entry| AppendCompletion {
                ticket: entry.ticket,
                record: entry.record,
                location: entry.location,
                durability,
            })
            .collect())
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

    /// Bytes still available for reservation in this segment.
    #[must_use]
    pub fn remaining_capacity(&self) -> u64 {
        self.capacity.saturating_sub(self.next_offset)
    }

    #[must_use]
    pub fn into_device(self) -> D {
        self.device
    }

    /// Borrows the owned device for control-plane operations performed only
    /// while no append is pending, such as sealing a file-backed segment.
    #[must_use]
    pub const fn device(&self) -> &D {
        &self.device
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

    fn write_bodies(&mut self, entries: &mut [CohortEntry]) -> Result<(), PhaseFailure> {
        let mut requests = VecDeque::with_capacity(entries.len());
        for entry in entries {
            let data = entry
                .encoded
                .take()
                .expect("cohort body is submitted exactly once");
            requests.push_back(IoRequest {
                id: self.allocate_request_id(),
                operation: IoOperation::Write {
                    offset: entry.location.offset,
                    data,
                },
            });
        }
        self.execute_phase(requests, ExpectedCompletion::Written)
    }

    fn write_commit_markers(&mut self, entries: &[CohortEntry]) -> Result<(), PhaseFailure> {
        let mut requests = VecDeque::with_capacity(entries.len());
        for entry in entries {
            let Some(offset) = entry.location.offset.checked_add(COMMIT_MARKER_OFFSET) else {
                return Err(PhaseFailure {
                    error: AppendError::OffsetOverflow,
                    accepted_any: false,
                });
            };
            requests.push_back(IoRequest {
                id: self.allocate_request_id(),
                operation: IoOperation::Write {
                    offset,
                    data: Box::new(commit_marker_bytes()),
                },
            });
        }
        self.execute_phase(requests, ExpectedCompletion::Written)
    }

    fn flush_phase(&mut self) -> Result<(), PhaseFailure> {
        let request_id = self.allocate_request_id();
        self.execute_phase(
            VecDeque::from([IoRequest {
                id: request_id,
                operation: IoOperation::Flush,
            }]),
            ExpectedCompletion::Flushed,
        )
    }

    fn execute_phase(
        &mut self,
        mut requests: VecDeque<IoRequest>,
        expected: ExpectedCompletion,
    ) -> Result<(), PhaseFailure> {
        let mut outstanding = HashMap::with_capacity(requests.len());
        let mut completions = Vec::with_capacity(requests.len().min(64));
        let mut accepted_any = false;

        while !requests.is_empty() || !outstanding.is_empty() {
            while let Some(request) = requests.pop_front() {
                let request_id = request.id;
                match self.device.submit(request) {
                    Ok(()) => {
                        accepted_any = true;
                        if outstanding.insert(request_id, expected).is_some() {
                            return Err(PhaseFailure {
                                error: AppendError::UnexpectedCompletion,
                                accepted_any,
                            });
                        }
                    }
                    Err(rejected) if rejected.error == DeviceError::QueueFull => {
                        requests.push_front(rejected.request);
                        break;
                    }
                    Err(rejected) => {
                        return Err(PhaseFailure {
                            error: AppendError::Submission(rejected.error),
                            accepted_any,
                        });
                    }
                }
            }

            if outstanding.is_empty() {
                return Err(PhaseFailure {
                    error: AppendError::Submission(DeviceError::QueueFull),
                    accepted_any,
                });
            }

            completions.clear();
            let limit = outstanding.len().min(64);
            if self.device.poll(&mut completions, limit) == 0 {
                std::thread::yield_now();
                continue;
            }
            for completion in completions.drain(..) {
                let Some(expected) = outstanding.remove(&completion.id) else {
                    return Err(PhaseFailure {
                        error: AppendError::UnexpectedCompletion,
                        accepted_any,
                    });
                };
                match (expected, completion.result) {
                    (ExpectedCompletion::Written, IoResult::Written(_))
                    | (ExpectedCompletion::Flushed, IoResult::Flushed) => {}
                    (_, IoResult::Failed(error)) => {
                        return Err(PhaseFailure {
                            error: AppendError::Io(error),
                            accepted_any,
                        });
                    }
                    _ => {
                        return Err(PhaseFailure {
                            error: AppendError::UnexpectedCompletion,
                            accepted_any,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn allocate_ticket(&mut self) -> AppendTicket {
        let ticket = AppendTicket(self.next_ticket_id);
        self.next_ticket_id = self.next_ticket_id.wrapping_add(1).max(1);
        ticket
    }

    fn allocate_request_id(&mut self) -> RequestId {
        let request_id = RequestId(self.next_request_id);
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        request_id
    }
}

struct CohortMeasurement {
    encoded_len: usize,
    offset: u64,
    end: u64,
    stored_len: u32,
    logical_len: u32,
}

fn measure_cohort(
    records: &[Record],
    start: u64,
    capacity: u64,
) -> Result<Vec<CohortMeasurement>, AppendError> {
    let mut offset = start;
    let mut measured = Vec::with_capacity(records.len());
    for record in records {
        let encoded_len = encoded_len(record).map_err(AppendError::Encode)?;
        let stored_len = u32::try_from(encoded_len).map_err(|_| AppendError::OffsetOverflow)?;
        let logical_len =
            u32::try_from(record.payload.len()).map_err(|_| AppendError::OffsetOverflow)?;
        let end = offset
            .checked_add(u64::from(stored_len))
            .ok_or(AppendError::OffsetOverflow)?;
        if end > capacity {
            return Err(AppendError::SegmentFull);
        }
        measured.push(CohortMeasurement {
            encoded_len,
            offset,
            end,
            stored_len,
            logical_len,
        });
        offset = end;
    }
    Ok(measured)
}

fn cohort_failure(error: AppendError, entries: Vec<CohortEntry>) -> CohortRejected {
    CohortRejected {
        error,
        records: entries.into_iter().map(|entry| entry.record).collect(),
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
    use punah_device::{CrashDevice, FileDevice, MemoryDevice, SubmitError};
    use punah_record::{RecordType, decode};
    use punah_types::Namespace;
    use std::time::SystemTime;

    struct FailingDevice {
        inner: MemoryDevice,
        fail_completion: usize,
        completion_count: usize,
    }

    struct CountingDevice {
        inner: MemoryDevice,
        flushes: usize,
    }

    struct ReorderingDevice<D> {
        inner: D,
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

    impl CountingDevice {
        fn new(capacity: usize, queue_capacity: usize) -> Self {
            Self {
                inner: MemoryDevice::new(capacity, queue_capacity),
                flushes: 0,
            }
        }
    }

    impl BlockDevice for CountingDevice {
        fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError> {
            let is_flush = matches!(request.operation, IoOperation::Flush);
            self.inner.submit(request)?;
            self.flushes += usize::from(is_flush);
            Ok(())
        }

        fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize {
            self.inner.poll(completions, limit)
        }
    }

    impl<D> ReorderingDevice<D> {
        const fn new(inner: D) -> Self {
            Self { inner }
        }

        fn into_inner(self) -> D {
            self.inner
        }
    }

    impl<D: BlockDevice> BlockDevice for ReorderingDevice<D> {
        fn submit(&mut self, request: IoRequest) -> Result<(), SubmitError> {
            self.inner.submit(request)
        }

        fn poll(&mut self, completions: &mut Vec<IoCompletion>, limit: usize) -> usize {
            let mut reordered = Vec::with_capacity(limit);
            let count = self.inner.poll(&mut reordered, limit);
            reordered.reverse();
            completions.extend(reordered);
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

    fn finish_append<D: BlockDevice>(
        writer: &mut SegmentWriter<D>,
    ) -> Result<AppendCompletion, AppendError> {
        for _ in 0..8 {
            if let Some(result) = writer.poll() {
                return result;
            }
        }
        panic!("append did not complete");
    }

    fn durable_cohort_records(count: usize) -> Vec<Record> {
        (1_u64..=u64::try_from(count).expect("record count fits u64"))
            .map(|sequence| {
                let mut record = record(b"durable cohort");
                record.sequence = sequence;
                record.key = format!("key-{sequence}").into_bytes();
                record.content_hash = [u8::try_from(sequence).expect("small sequence"); 32];
                record
            })
            .collect()
    }

    fn assert_cohort_power_loss_boundary<D, F>(
        device: D,
        power_loss_after: usize,
        completion_boundaries: usize,
        record_count: usize,
        into_durable: F,
    ) where
        D: BlockDevice,
        F: FnOnce(D) -> Vec<u8>,
    {
        let mut writer = SegmentWriter::new(device, SegmentId(13), 0, 16 * 1024);
        let rejected = writer
            .append_cohort(
                durable_cohort_records(record_count),
                Durability::LocalDurable,
            )
            .expect_err("power loss must interrupt the cohort");
        assert!(matches!(
            rejected.error,
            AppendError::Io(DeviceError::Backend(_))
        ));

        let durable = into_durable(writer.into_device());
        let (index, report) = crate::recover_segment(&durable, SegmentId(13), 0).unwrap();
        let expected = if power_loss_after == completion_boundaries {
            u64::try_from(record_count).unwrap()
        } else {
            0
        };
        assert_eq!(
            report.records_scanned, expected,
            "power loss at operation-effect boundary {power_loss_after}"
        );
        assert_eq!(index.len(), usize::try_from(expected).unwrap());
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
    fn locally_durable_cohort_shares_exactly_two_flushes() {
        let device = CountingDevice::new(16 * 1024, 2);
        let mut writer = SegmentWriter::new(device, SegmentId(1), 0, 16 * 1024);
        let records: Vec<_> = (1..=5)
            .map(|sequence| {
                let mut record = record(b"durable cohort");
                record.sequence = sequence;
                record
            })
            .collect();

        let completions = writer
            .append_cohort(records, Durability::LocalDurable)
            .expect("cohort");
        assert_eq!(completions.len(), 5);
        assert_eq!(
            completions
                .iter()
                .map(|completion| completion.record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(writer.into_device().flushes, 2);
    }

    #[test]
    fn cohort_correlates_reordered_device_completions() {
        let device = ReorderingDevice::new(MemoryDevice::new(16 * 1024, 8));
        let mut writer = SegmentWriter::new(device, SegmentId(7), 3, 16 * 1024);
        let records: Vec<_> = (1..=5)
            .map(|sequence| {
                let mut record = record(b"out of order");
                record.sequence = sequence;
                record.key = format!("key-{sequence}").into_bytes();
                record
            })
            .collect();

        let completions = writer
            .append_cohort(records, Durability::Buffered)
            .expect("cohort");
        assert_eq!(
            completions
                .iter()
                .map(|completion| completion.record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert!(
            completions
                .windows(2)
                .all(|pair| pair[0].location.offset < pair[1].location.offset)
        );
    }

    #[test]
    fn full_cohort_rejects_every_record_without_reserving_space() {
        let device = MemoryDevice::new(256, 2);
        let mut writer = SegmentWriter::new(device, SegmentId(1), 0, 256);
        let originals = vec![record(b"one"), record(b"two")];
        let rejected = writer
            .append_cohort(originals.clone(), Durability::Buffered)
            .expect_err("cohort cannot fit");
        assert_eq!(rejected.error, AppendError::SegmentFull);
        assert_eq!(rejected.records, originals);
        assert_eq!(writer.reserved_boundary(), 0);
    }

    #[test]
    fn ambiguous_cohort_failure_fail_stops_writer() {
        let device = FailingDevice::new(6);
        let mut writer = SegmentWriter::new(device, SegmentId(9), 0, 16 * 1024);
        let originals = vec![
            record(b"one"),
            record(b"two"),
            record(b"three"),
            record(b"four"),
            record(b"five"),
        ];
        let rejected = writer
            .append_cohort(originals.clone(), Durability::LocalDurable)
            .expect_err("injected flush failure");
        assert!(matches!(rejected.error, AppendError::Io(_)));
        assert_eq!(rejected.records, originals);
        assert_eq!(
            writer
                .try_append(record(b"later"), Durability::Buffered)
                .expect_err("writer must fail stop")
                .error,
            AppendError::WriterFailed
        );
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
    fn local_durable_singleton_survives_only_after_the_final_flush() {
        for power_loss_after in 0..=4 {
            let mut device = CrashDevice::new(4096, 4);
            device.arm_power_loss_after(power_loss_after);
            let mut writer = SegmentWriter::new(device, SegmentId(12), 0, 4096);
            writer
                .try_append(record(b"durable singleton"), Durability::LocalDurable)
                .expect("initial submission");

            let error = loop {
                if let Some(result) = writer.poll() {
                    break result.expect_err("power loss must interrupt the writer");
                }
            };
            assert!(matches!(error, AppendError::Io(DeviceError::Backend(_))));

            let durable = writer.into_device().into_durable_bytes();
            let (_, report) = crate::recover_segment(&durable, SegmentId(12), 0).unwrap();
            assert_eq!(
                report.records_scanned,
                u64::from(power_loss_after == 4),
                "power loss at operation-effect boundary {power_loss_after}"
            );
        }
    }

    #[test]
    fn local_durable_cohort_survives_only_after_its_final_flush() {
        const RECORDS: usize = 3;
        const COMPLETION_BOUNDARIES: usize = 2 * RECORDS + 2;

        for power_loss_after in 0..=COMPLETION_BOUNDARIES {
            let mut device = CrashDevice::new(16 * 1024, 8);
            device.arm_power_loss_after(power_loss_after);
            assert_cohort_power_loss_boundary(
                device,
                power_loss_after,
                COMPLETION_BOUNDARIES,
                RECORDS,
                CrashDevice::into_durable_bytes,
            );

            let mut device = CrashDevice::new(16 * 1024, 8);
            device.arm_power_loss_after(power_loss_after);
            assert_cohort_power_loss_boundary(
                ReorderingDevice::new(device),
                power_loss_after,
                COMPLETION_BOUNDARIES,
                RECORDS,
                |device| device.into_inner().into_durable_bytes(),
            );
        }
    }

    #[test]
    fn acknowledged_local_durable_singleton_survives_later_power_loss() {
        let device = CrashDevice::new(4096, 4);
        let mut writer = SegmentWriter::new(device, SegmentId(15), 0, 4096);
        writer
            .try_append(record(b"acknowledged singleton"), Durability::LocalDurable)
            .expect("submit");
        finish_append(&mut writer).expect("acknowledge singleton");

        let mut device = writer.into_device();
        device.power_loss();
        let (_, report) = crate::recover_segment(device.durable_bytes(), SegmentId(15), 0).unwrap();
        assert_eq!(report.records_scanned, 1);
    }

    #[test]
    fn acknowledged_local_durable_cohort_survives_later_power_loss() {
        const RECORDS: usize = 3;
        let device = CrashDevice::new(16 * 1024, 8);
        let mut writer = SegmentWriter::new(device, SegmentId(16), 0, 16 * 1024);
        let completions = writer
            .append_cohort(durable_cohort_records(RECORDS), Durability::LocalDurable)
            .expect("acknowledge cohort");
        assert_eq!(completions.len(), RECORDS);

        let mut device = writer.into_device();
        device.power_loss();
        let (_, report) = crate::recover_segment(device.durable_bytes(), SegmentId(16), 0).unwrap();
        assert_eq!(report.records_scanned, u64::try_from(RECORDS).unwrap());
    }

    #[test]
    fn buffered_acknowledgement_is_lost_without_a_flush() {
        let device = CrashDevice::new(4096, 4);
        let mut writer = SegmentWriter::new(device, SegmentId(14), 0, 4096);
        writer
            .try_append(record(b"buffered"), Durability::Buffered)
            .expect("submit");
        loop {
            if let Some(result) = writer.poll() {
                result.expect("buffered append completes");
                break;
            }
        }

        let mut device = writer.into_device();
        assert!(punah_record::decode(device.volatile_bytes()).is_ok());
        device.power_loss();
        let (_, report) = crate::recover_segment(device.durable_bytes(), SegmentId(14), 0).unwrap();
        assert_eq!(report.records_scanned, 0);
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
