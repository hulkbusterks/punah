use crate::DecodeError;
use crate::checksum::{crc32c, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64};

const MAGIC: &[u8; 8] = b"PUNASB01";
const VERSION: u16 = 1;
const HEADER_LENGTH: u16 = 64;
const CHECKSUM_OFFSET: usize = 4_088;
pub const SUPERBLOCK_SIZE: usize = 4_096;
pub const SUPERBLOCK_COMMIT_MARKER_OFFSET: usize = 4_092;
const COMMIT_MARKER: u32 = 0xC011_17ED;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataPointer {
    pub offset: u64,
    pub length: u64,
    pub checksum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub generation: u64,
    pub checkpoint: MetadataPointer,
    pub journal_tail: MetadataPointer,
}

#[must_use]
pub fn encode_uncommitted_superblock(value: &Superblock) -> [u8; SUPERBLOCK_SIZE] {
    let mut bytes = [0_u8; SUPERBLOCK_SIZE];
    bytes[0..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    put_u16(&mut bytes, 10, HEADER_LENGTH);
    put_u64(&mut bytes, 16, value.generation);
    put_pointer(&mut bytes, 24, value.checkpoint);
    put_pointer(&mut bytes, 44, value.journal_tail);
    let checksum = crc32c(&[&bytes[..CHECKSUM_OFFSET]]);
    put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
    bytes
}

#[must_use]
pub fn encode_superblock(value: &Superblock) -> [u8; SUPERBLOCK_SIZE] {
    let mut bytes = encode_uncommitted_superblock(value);
    put_u32(&mut bytes, SUPERBLOCK_COMMIT_MARKER_OFFSET, COMMIT_MARKER);
    bytes
}

/// Validates an uncommitted slot before installing its final marker.
///
/// # Errors
/// Returns an error when the slot is truncated, corrupt, or already committed.
pub fn commit_superblock(bytes: &mut [u8]) -> Result<(), DecodeError> {
    validate(bytes, false)?;
    put_u32(bytes, SUPERBLOCK_COMMIT_MARKER_OFFSET, COMMIT_MARKER);
    Ok(())
}

/// Decodes one committed fixed-size slot.
///
/// # Errors
/// Returns an error when the slot is truncated, corrupt, or unsupported.
pub fn decode_superblock(bytes: &[u8]) -> Result<Superblock, DecodeError> {
    validate(bytes, true)?;
    Ok(Superblock {
        generation: get_u64(bytes, 16),
        checkpoint: get_pointer(bytes, 24),
        journal_tail: get_pointer(bytes, 44),
    })
}

/// Selects the highest-generation valid slot. Equal generations prefer slot A.
#[must_use]
pub fn newest_valid_superblock(a: &[u8], b: &[u8]) -> Option<(usize, Superblock)> {
    match (decode_superblock(a), decode_superblock(b)) {
        (Ok(left), Ok(right)) if right.generation > left.generation => Some((1, right)),
        (Ok(left), Ok(_) | Err(_)) => Some((0, left)),
        (Err(_), Ok(right)) => Some((1, right)),
        (Err(_), Err(_)) => None,
    }
}

fn validate(bytes: &[u8], committed: bool) -> Result<(), DecodeError> {
    if bytes.len() < SUPERBLOCK_SIZE {
        return Err(DecodeError::Incomplete);
    }
    if &bytes[..8] != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = get_u16(bytes, 8);
    if version != VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let header = get_u16(bytes, 10);
    if header != HEADER_LENGTH {
        return Err(DecodeError::InvalidHeaderLength(header));
    }
    let marker = get_u32(bytes, SUPERBLOCK_COMMIT_MARKER_OFFSET);
    if (committed && marker != COMMIT_MARKER) || (!committed && marker != 0) {
        return Err(DecodeError::InvalidCommitMarker);
    }
    if get_u32(bytes, CHECKSUM_OFFSET) != crc32c(&[&bytes[..CHECKSUM_OFFSET]]) {
        return Err(DecodeError::ChecksumMismatch);
    }
    Ok(())
}

fn put_pointer(bytes: &mut [u8], offset: usize, pointer: MetadataPointer) {
    put_u64(bytes, offset, pointer.offset);
    put_u64(bytes, offset + 8, pointer.length);
    put_u32(bytes, offset + 16, pointer.checksum);
}
fn get_pointer(bytes: &[u8], offset: usize) -> MetadataPointer {
    MetadataPointer {
        offset: get_u64(bytes, offset),
        length: get_u64(bytes, offset + 8),
        checksum: get_u32(bytes, offset + 16),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(generation: u64) -> Superblock {
        Superblock {
            generation,
            checkpoint: MetadataPointer {
                offset: 8192,
                length: 512,
                checksum: 7,
            },
            journal_tail: MetadataPointer {
                offset: 8704,
                length: 128,
                checksum: 8,
            },
        }
    }

    #[test]
    fn round_trip_and_explicit_commit() {
        let value = sample(9);
        let mut bytes = encode_uncommitted_superblock(&value);
        assert_eq!(
            decode_superblock(&bytes),
            Err(DecodeError::InvalidCommitMarker)
        );
        commit_superblock(&mut bytes).expect("commit");
        assert_eq!(decode_superblock(&bytes), Ok(value));
    }

    #[test]
    fn every_truncated_prefix_is_incomplete() {
        let bytes = encode_superblock(&sample(1));
        for end in 0..SUPERBLOCK_SIZE {
            assert_eq!(
                decode_superblock(&bytes[..end]),
                Err(DecodeError::Incomplete)
            );
        }
    }

    #[test]
    fn corruption_and_torn_commit_are_rejected() {
        let mut corrupt = encode_superblock(&sample(1));
        corrupt[100] ^= 1;
        assert_eq!(
            decode_superblock(&corrupt),
            Err(DecodeError::ChecksumMismatch)
        );
        let mut torn = encode_superblock(&sample(1));
        torn[SUPERBLOCK_COMMIT_MARKER_OFFSET] = 0;
        assert_eq!(
            decode_superblock(&torn),
            Err(DecodeError::InvalidCommitMarker)
        );
    }

    #[test]
    fn newest_valid_slot_ignores_corruption() {
        let old = encode_superblock(&sample(4));
        let newest = encode_superblock(&sample(5));
        assert_eq!(newest_valid_superblock(&old, &newest), Some((1, sample(5))));
        let mut corrupt = newest;
        corrupt[200] ^= 1;
        assert_eq!(
            newest_valid_superblock(&old, &corrupt),
            Some((0, sample(4)))
        );
        assert_eq!(newest_valid_superblock(&corrupt, &corrupt), None);
    }
}
