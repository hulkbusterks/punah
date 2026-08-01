use crate::checksum::{crc32c, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64};
use crate::{DecodeError, EncodeError};

const MAGIC: &[u8; 8] = b"PUNAMF01";
const VERSION: u16 = 1;
const HEADER_LENGTH: usize = 64;
const HEADER_LENGTH_FIELD: u16 = 64;
const DESCRIPTOR_LENGTH: usize = 32;
const CHECKSUM_OFFSET: usize = 56;
pub const MANIFEST_COMMIT_MARKER_OFFSET: usize = 60;
const COMMIT_MARKER: u32 = 0xC011_17ED;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentDescriptor {
    pub id: u32,
    pub generation: u32,
    pub start_offset: u64,
    pub logical_length: u64,
    pub sealed_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestCheckpoint {
    pub generation: u64,
    pub active_segments: Vec<SegmentDescriptor>,
}

/// Encodes a checkpoint whose commit marker remains clear.
///
/// # Errors
/// Returns an error if its descriptor count or encoded length is not representable.
pub fn encode_uncommitted_manifest(value: &ManifestCheckpoint) -> Result<Vec<u8>, EncodeError> {
    let count = u32::try_from(value.active_segments.len())
        .map_err(|_| EncodeError::TooManySegments(value.active_segments.len()))?;
    let total = HEADER_LENGTH
        .checked_add(
            value
                .active_segments
                .len()
                .checked_mul(DESCRIPTOR_LENGTH)
                .ok_or(EncodeError::LengthOverflow)?,
        )
        .ok_or(EncodeError::LengthOverflow)?;
    let total_u32 = u32::try_from(total).map_err(|_| EncodeError::LengthOverflow)?;
    let mut bytes = vec![0; total];
    bytes[..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    put_u16(&mut bytes, 10, HEADER_LENGTH_FIELD);
    put_u64(&mut bytes, 16, value.generation);
    put_u32(&mut bytes, 24, count);
    put_u32(&mut bytes, 28, total_u32);
    for (index, segment) in value.active_segments.iter().enumerate() {
        put_descriptor(
            &mut bytes,
            HEADER_LENGTH + index * DESCRIPTOR_LENGTH,
            *segment,
        );
    }
    let checksum = crc32c(&[&bytes[..CHECKSUM_OFFSET], &bytes[HEADER_LENGTH..]]);
    put_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
    Ok(bytes)
}

/// Encodes a committed checkpoint.
///
/// # Errors
/// Returns an error if its descriptor count or encoded length is not representable.
pub fn encode_manifest(value: &ManifestCheckpoint) -> Result<Vec<u8>, EncodeError> {
    let mut bytes = encode_uncommitted_manifest(value)?;
    put_u32(&mut bytes, MANIFEST_COMMIT_MARKER_OFFSET, COMMIT_MARKER);
    Ok(bytes)
}

/// Validates an uncommitted checkpoint before installing its marker.
///
/// # Errors
/// Returns an error when the checkpoint is truncated, corrupt, or already committed.
pub fn commit_manifest(bytes: &mut [u8]) -> Result<(), DecodeError> {
    validate(bytes, false)?;
    put_u32(bytes, MANIFEST_COMMIT_MARKER_OFFSET, COMMIT_MARKER);
    Ok(())
}

/// Decodes a committed checkpoint.
///
/// # Errors
/// Returns an error when the checkpoint is truncated, corrupt, or unsupported.
pub fn decode_manifest(bytes: &[u8]) -> Result<ManifestCheckpoint, DecodeError> {
    let (total, count) = validate(bytes, true)?;
    let mut active_segments = Vec::with_capacity(count);
    for index in 0..count {
        active_segments.push(get_descriptor(
            bytes,
            HEADER_LENGTH + index * DESCRIPTOR_LENGTH,
        ));
    }
    debug_assert_eq!(total, HEADER_LENGTH + count * DESCRIPTOR_LENGTH);
    Ok(ManifestCheckpoint {
        generation: get_u64(bytes, 16),
        active_segments,
    })
}

fn validate(bytes: &[u8], committed: bool) -> Result<(usize, usize), DecodeError> {
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
    let marker = get_u32(bytes, MANIFEST_COMMIT_MARKER_OFFSET);
    if (committed && marker != COMMIT_MARKER) || (!committed && marker != 0) {
        return Err(DecodeError::InvalidCommitMarker);
    }
    let total_u32 = get_u32(bytes, 28);
    let total = usize::try_from(total_u32).map_err(|_| DecodeError::LengthOverflow)?;
    let count = usize::try_from(get_u32(bytes, 24)).map_err(|_| DecodeError::LengthOverflow)?;
    let expected = HEADER_LENGTH
        .checked_add(
            count
                .checked_mul(DESCRIPTOR_LENGTH)
                .ok_or(DecodeError::LengthOverflow)?,
        )
        .ok_or(DecodeError::LengthOverflow)?;
    if total != expected {
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
    Ok((total, count))
}

fn put_descriptor(bytes: &mut [u8], offset: usize, value: SegmentDescriptor) {
    put_u32(bytes, offset, value.id);
    put_u32(bytes, offset + 4, value.generation);
    put_u64(bytes, offset + 8, value.start_offset);
    put_u64(bytes, offset + 16, value.logical_length);
    put_u64(bytes, offset + 24, value.sealed_sequence);
}
fn get_descriptor(bytes: &[u8], offset: usize) -> SegmentDescriptor {
    SegmentDescriptor {
        id: get_u32(bytes, offset),
        generation: get_u32(bytes, offset + 4),
        start_offset: get_u64(bytes, offset + 8),
        logical_length: get_u64(bytes, offset + 16),
        sealed_sequence: get_u64(bytes, offset + 24),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> ManifestCheckpoint {
        ManifestCheckpoint {
            generation: 3,
            active_segments: vec![SegmentDescriptor {
                id: 7,
                generation: 2,
                start_offset: 4096,
                logical_length: 800,
                sealed_sequence: 99,
            }],
        }
    }
    #[test]
    fn round_trip() {
        let value = sample();
        assert_eq!(
            decode_manifest(&encode_manifest(&value).expect("encode")),
            Ok(value)
        );
    }
    #[test]
    fn explicit_commit() {
        let value = sample();
        let mut bytes = encode_uncommitted_manifest(&value).expect("encode");
        assert_eq!(
            decode_manifest(&bytes),
            Err(DecodeError::InvalidCommitMarker)
        );
        commit_manifest(&mut bytes).expect("commit");
        assert_eq!(decode_manifest(&bytes), Ok(value));
    }
    #[test]
    fn every_prefix_is_incomplete() {
        let bytes = encode_manifest(&sample()).expect("encode");
        for end in 0..bytes.len() {
            assert_eq!(decode_manifest(&bytes[..end]), Err(DecodeError::Incomplete));
        }
    }
    #[test]
    fn descriptor_corruption_is_detected() {
        let mut bytes = encode_manifest(&sample()).expect("encode");
        bytes[70] ^= 1;
        assert_eq!(decode_manifest(&bytes), Err(DecodeError::ChecksumMismatch));
    }
}
