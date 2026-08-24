#![no_main]

use libfuzzer_sys::fuzz_target;
use punah_metadata::{
    JournalEntry, JournalOperation, ManifestCheckpoint, MetadataPointer, SegmentDescriptor,
    SegmentRef, Superblock,
};
use punah_protocol::{FrameRef, Opcode};
use punah_record::{Record, RecordType};
use punah_types::Namespace;

const MAX_KEY: usize = 512;
const MAX_VALUE: usize = 4 * 1024;
const MAX_DEPENDENCIES: usize = 4;
const MAX_SEGMENTS: usize = 8;

fuzz_target!(|data: &[u8]| {
    let mut input = Input::new(data);
    check_record(&mut input);
    check_protocol_v1(&mut input);
    check_protocol_v2(&mut input);
    check_manifest(&mut input);
    check_journal(&mut input);
    check_superblock(&mut input);
});

struct Input<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Input<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn byte(&mut self) -> u8 {
        let byte = self.bytes.get(self.cursor).copied().unwrap_or(0);
        self.cursor = self.cursor.saturating_add(1);
        byte
    }

    fn u32(&mut self) -> u32 {
        u32::from_le_bytes([self.byte(), self.byte(), self.byte(), self.byte()])
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes([
            self.byte(),
            self.byte(),
            self.byte(),
            self.byte(),
            self.byte(),
            self.byte(),
            self.byte(),
            self.byte(),
        ])
    }

    fn array32(&mut self) -> [u8; 32] {
        let mut value = [0_u8; 32];
        for byte in &mut value {
            *byte = self.byte();
        }
        value
    }

    fn slice(&mut self, maximum: usize) -> &'a [u8] {
        let requested = usize::try_from(self.u32()).unwrap_or(usize::MAX) % (maximum + 1);
        let available = self.bytes.len().saturating_sub(self.cursor);
        let length = requested.min(available);
        let start = self.cursor;
        self.cursor += length;
        &self.bytes[start..start + length]
    }
}

fn check_record(input: &mut Input<'_>) {
    let record_type = if input.byte() & 1 == 0 {
        RecordType::Artifact
    } else {
        RecordType::Tombstone
    };
    let dependency_count = usize::from(input.byte()) % (MAX_DEPENDENCIES + 1);
    let dependencies = (0..dependency_count).map(|_| input.array32()).collect();
    let record = Record {
        record_type,
        flags: input.byte(),
        namespace: Namespace::new(b"fuzz".to_vec()).expect("fixed namespace is valid"),
        key: input.slice(MAX_KEY).to_vec(),
        sequence: input.u64(),
        created_at_unix_ns: input.u64(),
        expires_at_unix_ns: (input.byte() & 1 != 0).then(|| input.u64()),
        operation_hash: input.array32(),
        content_hash: input.array32(),
        dependencies,
        payload: input.slice(MAX_VALUE).to_vec(),
    };

    let encoded = punah_record::encode(&record).expect("bounded record encodes");
    assert_eq!(
        punah_record::encoded_len(&record).expect("bounded record has a length"),
        encoded.len()
    );
    let owned = punah_record::decode(&encoded).expect("committed record decodes");
    let borrowed = punah_record::decode_ref(&encoded).expect("committed record borrows");
    assert_eq!(owned.record, record);
    assert_eq!(borrowed.encoded_len, encoded.len());
    assert_eq!(borrowed.record.payload, record.payload);

    let mut uncommitted = punah_record::encode_uncommitted(&record).expect("record encodes");
    assert!(punah_record::decode(&uncommitted).is_err());
    punah_record::commit_in_place(&mut uncommitted).expect("valid body commits");
    assert_eq!(
        punah_record::decode(&uncommitted)
            .expect("record decodes")
            .record,
        record
    );

    let alignment = 512_usize << (usize::from(input.byte()) % 4);
    let extent = punah_record::encode_aligned_extent(std::slice::from_ref(&record), alignment)
        .expect("bounded aligned extent encodes");
    let decoded = punah_record::decode_aligned_extent(&extent.body, &extent.footer)
        .expect("aligned extent decodes");
    assert_eq!(decoded.alignment, alignment);
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.records[0].record.sequence, record.sequence);
}

fn check_protocol_v1(input: &mut Input<'_>) {
    let opcodes = [
        Opcode::Get,
        Opcode::Set,
        Opcode::Delete,
        Opcode::Ping,
        Opcode::Pong,
        Opcode::Error,
    ];
    let opcode = opcodes[usize::from(input.byte()) % opcodes.len()];
    let flags = input.byte();
    let ttl = input.u32() & 0x00ff_ffff;
    let key = input.slice(MAX_KEY);
    let value = input.slice(MAX_VALUE);
    let frame = FrameRef {
        opcode,
        flags,
        ttl,
        key,
        value,
    };
    let encoded = punah_protocol::encode(frame).expect("bounded v1 frame encodes");
    let (decoded, consumed) = punah_protocol::decode(&encoded).expect("v1 frame decodes");
    assert_eq!(decoded, frame);
    assert_eq!(consumed, encoded.len());
    assert_eq!(
        punah_protocol::encode_header(frame).expect("v1 header encodes"),
        encoded[..punah_protocol::HEADER_LEN]
    );
}

fn check_protocol_v2(input: &mut Input<'_>) {
    use punah_protocol::v2::{FrameRef, Opcode, Status};

    let opcodes = [
        Opcode::ArtifactGet,
        Opcode::ArtifactPut,
        Opcode::OperationResolve,
        Opcode::LeaseAcquire,
        Opcode::LeaseRenew,
        Opcode::LeaseRelease,
        Opcode::ResultPublish,
    ];
    let statuses = [
        Status::None,
        Status::Hit,
        Status::Miss,
        Status::Stored,
        Status::Deleted,
        Status::LeaseGranted,
        Status::LeaseHeld,
        Status::Conflict,
        Status::Backpressure,
        Status::Invalid,
        Status::Error,
    ];
    let frame = FrameRef {
        opcode: opcodes[usize::from(input.byte()) % opcodes.len()],
        status: statuses[usize::from(input.byte()) % statuses.len()],
        flags: input.byte(),
        request_id: input.u64(),
        ttl_seconds: input.u32(),
        key: input.slice(MAX_KEY),
        value: input.slice(MAX_VALUE),
    };
    let encoded = punah_protocol::v2::encode(frame).expect("bounded v2 frame encodes");
    let (decoded, consumed) = punah_protocol::v2::decode(&encoded).expect("v2 frame decodes");
    assert_eq!(decoded, frame);
    assert_eq!(consumed, encoded.len());
    assert_eq!(
        punah_protocol::v2::encode_header(frame).expect("v2 header encodes"),
        encoded[..punah_protocol::v2::HEADER_LEN]
    );
}

fn check_manifest(input: &mut Input<'_>) {
    let count = usize::from(input.byte()) % (MAX_SEGMENTS + 1);
    let active_segments = (0..count)
        .map(|_| SegmentDescriptor {
            id: input.u32(),
            generation: input.u32(),
            start_offset: input.u64(),
            logical_length: input.u64(),
            sealed_sequence: input.u64(),
        })
        .collect();
    let checkpoint = ManifestCheckpoint {
        generation: input.u64(),
        active_segments,
    };
    let encoded = punah_metadata::encode_manifest(&checkpoint).expect("manifest encodes");
    assert_eq!(
        punah_metadata::decode_manifest(&encoded).expect("manifest decodes"),
        checkpoint
    );
    let mut uncommitted =
        punah_metadata::encode_uncommitted_manifest(&checkpoint).expect("manifest encodes");
    punah_metadata::commit_manifest(&mut uncommitted).expect("manifest commits");
    assert_eq!(uncommitted, encoded);
}

fn check_journal(input: &mut Input<'_>) {
    let refs = |input: &mut Input<'_>, count: usize| {
        (0..count)
            .map(|_| SegmentRef {
                id: input.u32(),
                generation: input.u32(),
            })
            .collect::<Vec<_>>()
    };
    let operation = if input.byte() & 1 == 0 {
        let old_count = 1 + usize::from(input.byte()) % 4;
        let new_count = 1 + usize::from(input.byte()) % 4;
        JournalOperation::Replace {
            old: refs(input, old_count),
            new: refs(input, new_count),
        }
    } else {
        let count = 1 + usize::from(input.byte()) % 4;
        JournalOperation::Retire {
            segments: refs(input, count),
        }
    };
    let entry = JournalEntry {
        generation: input.u64(),
        operation,
    };
    let encoded = punah_metadata::encode_journal_entry(&entry).expect("journal encodes");
    let (decoded, consumed) =
        punah_metadata::decode_journal_entry(&encoded).expect("journal decodes");
    assert_eq!(decoded, entry);
    assert_eq!(consumed, encoded.len());
    let mut uncommitted =
        punah_metadata::encode_uncommitted_journal_entry(&entry).expect("journal encodes");
    punah_metadata::commit_journal_entry(&mut uncommitted).expect("journal commits");
    assert_eq!(uncommitted, encoded);
}

fn check_superblock(input: &mut Input<'_>) {
    let pointer = |input: &mut Input<'_>| MetadataPointer {
        offset: input.u64(),
        length: input.u64(),
        checksum: input.u32(),
    };
    let superblock = Superblock {
        generation: input.u64(),
        checkpoint: pointer(input),
        journal_tail: pointer(input),
    };
    let encoded = punah_metadata::encode_superblock(&superblock);
    assert_eq!(
        punah_metadata::decode_superblock(&encoded).expect("superblock decodes"),
        superblock
    );
    let mut uncommitted = punah_metadata::encode_uncommitted_superblock(&superblock);
    punah_metadata::commit_superblock(&mut uncommitted).expect("superblock commits");
    assert_eq!(uncommitted, encoded);
}
