#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let owned = punah_record::decode(data);
    let borrowed = punah_record::decode_ref(data);
    match (owned, borrowed) {
        (Ok(owned), Ok(borrowed)) => {
            assert_eq!(owned.encoded_len, borrowed.encoded_len);
            assert_eq!(owned.record.record_type, borrowed.record.record_type);
            assert_eq!(owned.record.flags, borrowed.record.flags);
            assert_eq!(owned.record.namespace.as_bytes(), borrowed.record.namespace);
            assert_eq!(owned.record.key, borrowed.record.key);
            assert_eq!(owned.record.sequence, borrowed.record.sequence);
            assert_eq!(
                owned.record.created_at_unix_ns,
                borrowed.record.created_at_unix_ns
            );
            assert_eq!(
                owned.record.expires_at_unix_ns,
                borrowed.record.expires_at_unix_ns
            );
            assert_eq!(owned.record.operation_hash, borrowed.record.operation_hash);
            assert_eq!(owned.record.content_hash, borrowed.record.content_hash);
            assert_eq!(
                owned.record.dependencies,
                borrowed.record.dependencies().collect::<Vec<_>>()
            );
            assert_eq!(owned.record.payload, borrowed.record.payload);
        }
        (Err(owned), Err(borrowed)) => assert_eq!(owned, borrowed),
        (owned, borrowed) => panic!("owned/borrowed disagreement: {owned:?} vs {borrowed:?}"),
    }

    let _ = punah_protocol::decode(data);
    let _ = punah_protocol::v2::decode(data);
    let _ = punah_metadata::decode_manifest(data);
    let _ = punah_metadata::decode_journal_entry(data);
    let _ = punah_metadata::decode_superblock(data);

    let split = data.len() / 2;
    let _ = punah_record::decode_aligned_extent(&data[..split], &data[split..]);

    let mut candidate = data.to_vec();
    let _ = punah_record::commit_in_place(&mut candidate);
    let mut candidate = data.to_vec();
    let _ = punah_metadata::commit_manifest(&mut candidate);
    let mut candidate = data.to_vec();
    let _ = punah_metadata::commit_journal_entry(&mut candidate);
    let mut candidate = data.to_vec();
    let _ = punah_metadata::commit_superblock(&mut candidate);

    let _ = punah_metadata::newest_valid_superblock(&data[..split], &data[split..]);
});
