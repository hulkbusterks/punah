use crate::{
    JournalEntry, JournalOperation, ManifestCheckpoint, MetadataPointer, SegmentDescriptor,
    SegmentRef, Superblock, encode_journal_entry, encode_manifest, encode_superblock,
};

#[test]
fn durable_metadata_v1_matches_golden_bytes() {
    let manifest = ManifestCheckpoint {
        generation: 7,
        active_segments: vec![
            SegmentDescriptor {
                id: 1,
                generation: 2,
                start_offset: 4096,
                logical_length: 8192,
                sealed_sequence: 9,
            },
            SegmentDescriptor {
                id: 3,
                generation: 4,
                start_offset: 0,
                logical_length: 128,
                sealed_sequence: 0,
            },
        ],
    };
    let journal = JournalEntry {
        generation: 7,
        operation: JournalOperation::Replace {
            old: vec![SegmentRef {
                id: 1,
                generation: 2,
            }],
            new: vec![SegmentRef {
                id: 3,
                generation: 4,
            }],
        },
    };
    let superblock = Superblock {
        generation: 7,
        checkpoint: MetadataPointer {
            offset: 8192,
            length: 128,
            checksum: 0x1122_3344,
        },
        journal_tail: MetadataPointer {
            offset: 8320,
            length: 64,
            checksum: 0x5566_7788,
        },
    };

    assert_eq!(
        encode_manifest(&manifest).unwrap(),
        decode_hex(include_str!("../fixtures/manifest-v1.hex"))
    );
    assert_eq!(
        encode_journal_entry(&journal).unwrap(),
        decode_hex(include_str!("../fixtures/journal-v1.hex"))
    );
    let encoded_superblock = encode_superblock(&superblock);
    let expected_header = decode_hex(include_str!("../fixtures/superblock-v1-header.hex"));
    assert_eq!(
        &encoded_superblock[..expected_header.len()],
        expected_header
    );
    assert!(
        encoded_superblock[expected_header.len()..4088]
            .iter()
            .all(|byte| *byte == 0)
    );
    assert_eq!(
        &encoded_superblock[4088..],
        decode_hex(include_str!("../fixtures/superblock-v1-tail.hex"))
    );
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).expect("fixture hex")
        })
        .collect()
}
