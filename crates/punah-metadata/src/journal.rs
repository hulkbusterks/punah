use crate::checksum::{crc32c, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64};
use crate::{DecodeError, EncodeError};

const MAGIC: &[u8; 8] = b"PUNATJ01";
const VERSION: u16 = 1;
const HEADER_LENGTH: usize = 48;
const HEADER_LENGTH_FIELD: u16 = 48;
const REFERENCE_LENGTH: usize = 8;
const ALIGNMENT: usize = 8;
const CHECKSUM_OFFSET: usize = 40;
pub const JOURNAL_COMMIT_MARKER_OFFSET: usize = 44;
const COMMIT_MARKER: u32 = 0xC011_17ED;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentRef {
    pub id: u32,
    pub generation: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalOperation {
    Replace {
        old: Vec<SegmentRef>,
        new: Vec<SegmentRef>,
    },
    Retire {
        segments: Vec<SegmentRef>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalEntry {
    pub generation: u64,
    pub operation: JournalOperation,
}

/// Encodes a journal record whose commit marker remains clear.
///
/// # Errors
/// Returns an error for an empty/invalid operation or unrepresentable length.
pub fn encode_uncommitted_journal_entry(value: &JournalEntry) -> Result<Vec<u8>, EncodeError> {
    let (kind, old, new): (u8, &[SegmentRef], &[SegmentRef]) = match &value.operation {
        JournalOperation::Replace { old, new } if !old.is_empty() && !new.is_empty() => {
            (1, old, new)
        }
        JournalOperation::Retire { segments } if !segments.is_empty() => (2, segments, &[]),
        _ => return Err(EncodeError::InvalidOperationShape),
    };
    let old_count =
        u16::try_from(old.len()).map_err(|_| EncodeError::TooManySegments(old.len()))?;
    let new_count =
        u16::try_from(new.len()).map_err(|_| EncodeError::TooManySegments(new.len()))?;
    let count = old
        .len()
        .checked_add(new.len())
        .ok_or(EncodeError::LengthOverflow)?;
    let raw = HEADER_LENGTH
        .checked_add(
            count
                .checked_mul(REFERENCE_LENGTH)
                .ok_or(EncodeError::LengthOverflow)?,
        )
        .ok_or(EncodeError::LengthOverflow)?;
    let total = raw
        .checked_add(ALIGNMENT - 1)
        .ok_or(EncodeError::LengthOverflow)?
        / ALIGNMENT
        * ALIGNMENT;
    let total_u32 = u32::try_from(total).map_err(|_| EncodeError::LengthOverflow)?;
    let mut bytes = vec![0; total];
    bytes[..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    put_u16(&mut bytes, 10, HEADER_LENGTH_FIELD);
    bytes[12] = kind;
    put_u16(&mut bytes, 14, old_count);
    put_u16(&mut bytes, 16, new_count);
    put_u64(&mut bytes, 24, value.generation);
    put_u32(&mut bytes, 32, total_u32);
    for (index, reference) in old.iter().chain(new).enumerate() {
        put_ref(
            &mut bytes,
            HEADER_LENGTH + index * REFERENCE_LENGTH,
            *reference,
        );
    }
    let checksum = crc32c(&[&bytes[..CHECKSUM_OFFSET], &bytes[HEADER_LENGTH..]]);
    put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
    Ok(bytes)
}

/// Encodes a committed topology journal record.
///
/// # Errors
/// Returns an error for an empty/invalid operation or unrepresentable length.
pub fn encode_journal_entry(value: &JournalEntry) -> Result<Vec<u8>, EncodeError> {
    let mut bytes = encode_uncommitted_journal_entry(value)?;
    put_u32(&mut bytes, JOURNAL_COMMIT_MARKER_OFFSET, COMMIT_MARKER);
    Ok(bytes)
}
/// Validates an uncommitted journal record before installing its marker.
///
/// # Errors
/// Returns an error when the record is truncated, corrupt, or already committed.
pub fn commit_journal_entry(bytes: &mut [u8]) -> Result<(), DecodeError> {
    validate(bytes, false)?;
    put_u32(bytes, JOURNAL_COMMIT_MARKER_OFFSET, COMMIT_MARKER);
    Ok(())
}

/// Decodes one committed record and returns its consumed byte count.
///
/// # Errors
/// Returns an error when the record is truncated, corrupt, malformed, or unsupported.
pub fn decode_journal_entry(bytes: &[u8]) -> Result<(JournalEntry, usize), DecodeError> {
    let (total, old_count, new_count, kind) = validate(bytes, true)?;
    let old = refs(bytes, 0, old_count);
    let new = refs(bytes, old_count, new_count);
    let operation = match kind {
        1 => JournalOperation::Replace { old, new },
        2 => JournalOperation::Retire { segments: old },
        other => return Err(DecodeError::UnknownOperation(other)),
    };
    Ok((
        JournalEntry {
            generation: get_u64(bytes, 24),
            operation,
        },
        total,
    ))
}

fn validate(bytes: &[u8], committed: bool) -> Result<(usize, usize, usize, u8), DecodeError> {
    if bytes.len() < HEADER_LENGTH {
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
    if usize::from(header) != HEADER_LENGTH {
        return Err(DecodeError::InvalidHeaderLength(header));
    }
    let marker = get_u32(bytes, JOURNAL_COMMIT_MARKER_OFFSET);
    if (committed && marker != COMMIT_MARKER) || (!committed && marker != 0) {
        return Err(DecodeError::InvalidCommitMarker);
    }
    let kind = bytes[12];
    if kind != 1 && kind != 2 {
        return Err(DecodeError::UnknownOperation(kind));
    }
    let old = usize::from(get_u16(bytes, 14));
    let new = usize::from(get_u16(bytes, 16));
    if old == 0 || (kind == 1 && new == 0) || (kind == 2 && new != 0) {
        return Err(DecodeError::InvalidOperationShape);
    }
    let expected = HEADER_LENGTH
        .checked_add(
            (old + new)
                .checked_mul(REFERENCE_LENGTH)
                .ok_or(DecodeError::LengthOverflow)?,
        )
        .ok_or(DecodeError::LengthOverflow)?;
    let total_u32 = get_u32(bytes, 32);
    let total = usize::try_from(total_u32).map_err(|_| DecodeError::LengthOverflow)?;
    if total != expected || total % ALIGNMENT != 0 {
        return Err(DecodeError::InvalidTotalLength(total_u32));
    }
    if bytes.len() < total {
        return Err(DecodeError::Incomplete);
    }
    if get_u32(bytes, CHECKSUM_OFFSET)
        != crc32c(&[&bytes[..CHECKSUM_OFFSET], &bytes[HEADER_LENGTH..total]])
    {
        return Err(DecodeError::ChecksumMismatch);
    }
    Ok((total, old, new, kind))
}
fn refs(bytes: &[u8], start: usize, count: usize) -> Vec<SegmentRef> {
    (start..start + count)
        .map(|index| get_ref(bytes, HEADER_LENGTH + index * REFERENCE_LENGTH))
        .collect()
}
fn put_ref(bytes: &mut [u8], offset: usize, value: SegmentRef) {
    put_u32(bytes, offset, value.id);
    put_u32(bytes, offset + 4, value.generation);
}
fn get_ref(bytes: &[u8], offset: usize) -> SegmentRef {
    SegmentRef {
        id: get_u32(bytes, offset),
        generation: get_u32(bytes, offset + 4),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn replace() -> JournalEntry {
        JournalEntry {
            generation: 4,
            operation: JournalOperation::Replace {
                old: vec![SegmentRef {
                    id: 1,
                    generation: 1,
                }],
                new: vec![SegmentRef {
                    id: 3,
                    generation: 2,
                }],
            },
        }
    }
    #[test]
    fn operations_round_trip() {
        for value in [
            replace(),
            JournalEntry {
                generation: 5,
                operation: JournalOperation::Retire {
                    segments: vec![SegmentRef {
                        id: 1,
                        generation: 1,
                    }],
                },
            },
        ] {
            let bytes = encode_journal_entry(&value).expect("encode");
            assert_eq!(decode_journal_entry(&bytes), Ok((value, bytes.len())));
        }
    }
    #[test]
    fn explicit_commit() {
        let value = replace();
        let mut bytes = encode_uncommitted_journal_entry(&value).expect("encode");
        assert_eq!(
            decode_journal_entry(&bytes),
            Err(DecodeError::InvalidCommitMarker)
        );
        commit_journal_entry(&mut bytes).expect("commit");
        assert_eq!(decode_journal_entry(&bytes).expect("decode").0, value);
    }
    #[test]
    fn every_prefix_is_incomplete() {
        let bytes = encode_journal_entry(&replace()).expect("encode");
        for end in 0..bytes.len() {
            assert_eq!(
                decode_journal_entry(&bytes[..end]),
                Err(DecodeError::Incomplete)
            );
        }
    }
    #[test]
    fn corruption_is_detected() {
        let mut bytes = encode_journal_entry(&replace()).expect("encode");
        bytes[50] ^= 1;
        assert_eq!(
            decode_journal_entry(&bytes),
            Err(DecodeError::ChecksumMismatch)
        );
    }
    #[test]
    fn empty_operations_are_rejected() {
        let value = JournalEntry {
            generation: 1,
            operation: JournalOperation::Retire { segments: vec![] },
        };
        assert_eq!(
            encode_journal_entry(&value),
            Err(EncodeError::InvalidOperationShape)
        );
    }
}
