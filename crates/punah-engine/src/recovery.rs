use std::time::{Duration, SystemTime};

use punah_record::{DecodeError, Record, RecordType};
use punah_types::{ArtifactEntry, RecordLocation, SegmentId};

use crate::ReferenceIndex;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    Corrupt { offset: u64, source: DecodeError },
    OffsetOverflow,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub records_scanned: u64,
    pub artifacts_applied: u64,
    pub tombstones_applied: u64,
    pub committed_boundary: u64,
}

/// Rebuilds an index from a contiguous segment image.
///
/// Zero-filled and uncommitted trailing space is treated as the unused tail of
/// a preallocated segment. Corruption inside committed data is reported.
///
/// # Errors
///
/// Returns [`RecoveryError::Corrupt`] for invalid committed data and
/// [`RecoveryError::OffsetOverflow`] when a location cannot be represented.
pub fn recover_segment(
    bytes: &[u8],
    segment_id: SegmentId,
    generation: u32,
) -> Result<(ReferenceIndex, RecoveryReport), RecoveryError> {
    let mut index = ReferenceIndex::default();
    let mut report = RecoveryReport::default();
    let mut cursor = 0_usize;

    while cursor < bytes.len() {
        let tail = &bytes[cursor..];
        if tail.iter().all(|byte| *byte == 0) {
            break;
        }
        let decoded = match punah_record::decode(tail) {
            Ok(decoded) => decoded,
            Err(DecodeError::Incomplete | DecodeError::InvalidCommitMarker) => break,
            Err(source) => {
                return Err(RecoveryError::Corrupt {
                    offset: u64::try_from(cursor).map_err(|_| RecoveryError::OffsetOverflow)?,
                    source,
                });
            }
        };
        let offset = u64::try_from(cursor).map_err(|_| RecoveryError::OffsetOverflow)?;
        let stored_len =
            u32::try_from(decoded.encoded_len).map_err(|_| RecoveryError::OffsetOverflow)?;
        apply_recovered_record(
            &mut index,
            &mut report,
            decoded.record,
            RecordLocation {
                segment_id,
                generation,
                offset,
                stored_len,
                logical_len: stored_len,
            },
        );
        cursor = cursor
            .checked_add(decoded.encoded_len)
            .ok_or(RecoveryError::OffsetOverflow)?;
        report.records_scanned += 1;
        report.committed_boundary =
            u64::try_from(cursor).map_err(|_| RecoveryError::OffsetOverflow)?;
    }

    Ok((index, report))
}

fn apply_recovered_record(
    index: &mut ReferenceIndex,
    report: &mut RecoveryReport,
    record: Record,
    mut location: RecordLocation,
) {
    location.logical_len = u32::try_from(record.payload.len()).unwrap_or(u32::MAX);
    match record.record_type {
        RecordType::Artifact => {
            let created_at =
                SystemTime::UNIX_EPOCH + Duration::from_nanos(record.created_at_unix_ns);
            let expires_at = record
                .expires_at_unix_ns
                .map(|timestamp| SystemTime::UNIX_EPOCH + Duration::from_nanos(timestamp));
            if index.insert_latest(ArtifactEntry {
                namespace: record.namespace,
                content_hash: record.content_hash,
                operation_hash: record.operation_hash,
                sequence: record.sequence,
                location,
                created_at,
                expires_at,
                state: punah_types::EntryState::Live,
            }) {
                report.artifacts_applied += 1;
            }
        }
        RecordType::Tombstone => {
            if index.apply_tombstone(
                &record.namespace,
                &record.content_hash,
                record.sequence,
                location,
            ) {
                report.tombstones_applied += 1;
            }
        }
    }
}
