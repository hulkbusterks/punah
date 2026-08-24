use super::*;
use crate::{RecoveryError, recover_segment};
use punah_record::{DecodeError, Record, RecordType};
use punah_types::{EntryState, OperationHash};

fn location(offset: u64) -> RecordLocation {
    RecordLocation {
        segment_id: SegmentId(1),
        generation: 0,
        offset,
        stored_len: 10,
        logical_len: 10,
    }
}

fn entry(sequence: u64, offset: u64) -> ArtifactEntry {
    ArtifactEntry {
        namespace: Namespace::new("test").expect("namespace"),
        content_hash: [1; 32],
        operation_hash: OperationHash::default(),
        sequence,
        location: location(offset),
        created_at: SystemTime::UNIX_EPOCH,
        expires_at: None,
        state: EntryState::Live,
    }
}

#[test]
fn redirect_requires_matching_sequence_and_location() {
    let mut index = ReferenceIndex::default();
    index.insert(entry(2, 20));
    let namespace = Namespace::new("test").expect("namespace");

    assert!(!index.redirect_if_current(&namespace, &[1; 32], 1, location(20), location(30)));
    assert!(!index.redirect_if_current(&namespace, &[1; 32], 2, location(10), location(30)));
    assert!(index.redirect_if_current(&namespace, &[1; 32], 2, location(20), location(30)));
    assert_eq!(
        index
            .get(&namespace, &[1; 32], SystemTime::UNIX_EPOCH)
            .unwrap()
            .location,
        location(30)
    );
}

#[test]
fn tombstone_blocks_redirect_and_stale_resurrection() {
    let mut index = ReferenceIndex::default();
    index.insert_latest(entry(4, 10));
    let namespace = Namespace::new("test").expect("namespace");
    assert!(index.apply_tombstone(&namespace, &[1; 32], 5, location(20)));
    assert!(!index.redirect_if_current(&namespace, &[1; 32], 4, location(10), location(30)));
    assert!(!index.insert_latest(entry(5, 40)));
    assert!(!index.insert_latest(entry(4, 40)));
    assert!(
        matches!(index.version_metadata(&namespace, &[1; 32]), Some(IndexVersion::Tombstone(TombstoneMetadata { sequence: 5, source: TombstoneSource::Record(place) })) if *place == location(20))
    );
}

#[test]
fn snapshot_is_owned_and_contains_live_and_tombstone_versions() {
    let mut index = ReferenceIndex::default();
    index.insert_latest(entry(1, 10));
    let other = Namespace::new("other").expect("namespace");
    index.apply_tombstone(&other, &[9; 32], 7, location(70));
    let snapshot = index.snapshot();
    index.insert_latest(entry(2, 20));
    assert_eq!(snapshot.len(), 2);
    assert!(
        snapshot
            .iter()
            .any(|item| matches!(&item.version, IndexVersion::Live(value) if value.sequence == 1))
    );
    assert!(snapshot.iter().any(|item| matches!(
        item.version,
        IndexVersion::Tombstone(TombstoneMetadata { sequence: 7, .. })
    )));
}

fn record(record_type: RecordType, sequence: u64, payload: &[u8]) -> Record {
    Record {
        record_type,
        flags: 0,
        namespace: Namespace::new("test").unwrap(),
        key: b"key".to_vec(),
        sequence,
        created_at_unix_ns: 10,
        expires_at_unix_ns: None,
        operation_hash: [2; 32],
        content_hash: [1; 32],
        dependencies: Vec::new(),
        payload: payload.to_vec(),
    }
}

#[test]
fn recovery_applies_latest_record_and_retains_tombstone_location() {
    let first = punah_record::encode(&record(RecordType::Artifact, 1, b"old")).unwrap();
    let second = punah_record::encode(&record(RecordType::Artifact, 2, b"new")).unwrap();
    let tombstone = punah_record::encode(&record(RecordType::Tombstone, 3, b"")).unwrap();
    let tombstone_offset = (first.len() + second.len()) as u64;
    let mut segment = [first, second, tombstone].concat();
    segment.resize(segment.len() + 4096, 0);
    let (index, report) = recover_segment(&segment, SegmentId(7), 1).unwrap();
    let namespace = Namespace::new("test").unwrap();
    assert!(index.is_empty());
    assert_eq!(report.records_scanned, 3);
    assert!(
        matches!(index.version_metadata(&namespace, &[1; 32]), Some(IndexVersion::Tombstone(TombstoneMetadata { sequence: 3, source: TombstoneSource::Record(place) })) if place.segment_id == SegmentId(7) && place.generation == 1 && place.offset == tombstone_offset)
    );
}

#[test]
fn recovery_rejects_corruption_in_committed_data() {
    let mut encoded = punah_record::encode(&record(RecordType::Artifact, 1, b"payload")).unwrap();
    encoded[130] ^= 1;
    assert!(matches!(
        recover_segment(&encoded, SegmentId(1), 0),
        Err(RecoveryError::Corrupt {
            source: DecodeError::RecordChecksumMismatch,
            ..
        })
    ));
}

#[test]
fn recovered_tombstone_prevents_older_copy_resurrection() {
    let tombstone = punah_record::encode(&record(RecordType::Tombstone, 10, b"")).unwrap();
    let stale = punah_record::encode(&record(RecordType::Artifact, 9, b"stale")).unwrap();
    let (index, report) = recover_segment(&[tombstone, stale].concat(), SegmentId(1), 0).unwrap();
    let namespace = Namespace::new("test").unwrap();
    assert!(index.is_empty());
    assert_eq!(index.version(&namespace, &[1; 32]), Some(10));
    assert_eq!(report.tombstones_applied, 1);
    assert_eq!(report.artifacts_applied, 0);
}

#[test]
fn recovery_stops_at_first_zero_header_without_scanning_capacity_tail() {
    let encoded = punah_record::encode(&record(RecordType::Artifact, 1, b"payload")).unwrap();
    let committed_boundary = encoded.len();
    let mut segment = encoded;
    segment.resize(committed_boundary + punah_record::RECORD_HEADER_LEN, 0);
    segment.extend_from_slice(&[1; punah_record::RECORD_HEADER_LEN]);

    let (index, report) = recover_segment(&segment, SegmentId(1), 0).unwrap();

    assert_eq!(report.records_scanned, 1);
    assert_eq!(report.committed_boundary, committed_boundary as u64);
    assert_eq!(index.len(), 1);
}

#[test]
fn recovery_rejects_nonzero_corrupt_header_at_next_record() {
    let encoded = punah_record::encode(&record(RecordType::Artifact, 1, b"payload")).unwrap();
    let corrupt_offset = encoded.len();
    let mut segment = encoded;
    segment.extend_from_slice(&[1; punah_record::RECORD_HEADER_LEN]);

    assert!(matches!(
        recover_segment(&segment, SegmentId(1), 0),
        Err(RecoveryError::Corrupt {
            offset,
            source: DecodeError::BadMagic,
        }) if offset == corrupt_offset as u64
    ));
}

#[test]
fn recovered_location_tracks_logical_payload_length() {
    let payload = b"payload bytes";
    let encoded = punah_record::encode(&record(RecordType::Artifact, 1, payload)).unwrap();

    let (index, _) = recover_segment(&encoded, SegmentId(3), 2).unwrap();
    let namespace = Namespace::new("test").unwrap();
    let entry = index
        .get(&namespace, &[1; 32], SystemTime::UNIX_EPOCH)
        .unwrap();

    assert_eq!(
        entry.location.logical_len,
        u32::try_from(payload.len()).unwrap()
    );
    assert_eq!(entry.location.stored_len as usize, encoded.len());
}
