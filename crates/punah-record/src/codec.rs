use punah_checksum::{Checksum, checksum};
use punah_types::{ContentHash, Namespace, OperationHash};

const MAGIC: &[u8; 8] = b"PUNAH001";
const VERSION: u16 = 1;
const HEADER_LEN_FIELD: u16 = 128;
const HEADER_LEN: usize = 128;
const ALIGNMENT: usize = 8;
const COMMIT_MARKER: u32 = 0xC011_17ED;
pub const COMMIT_MARKER_OFFSET: u64 = 124;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Artifact = 1,
    Tombstone = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Artifact),
            2 => Ok(Self::Tombstone),
            other => Err(DecodeError::UnknownRecordType(other)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub record_type: RecordType,
    pub flags: u8,
    pub namespace: Namespace,
    /// Original lookup key. Hashes accelerate routing and identify immutable
    /// content, but arbitrary-key equality is defined by these bytes.
    pub key: Vec<u8>,
    pub sequence: u64,
    pub created_at_unix_ns: u64,
    pub expires_at_unix_ns: Option<u64>,
    pub operation_hash: OperationHash,
    pub content_hash: ContentHash,
    pub dependencies: Vec<ContentHash>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeError {
    NamespaceTooLong(usize),
    KeyTooLong(usize),
    TooManyDependencies(usize),
    PayloadTooLong(usize),
    RecordTooLong(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    Incomplete,
    BadMagic,
    UnsupportedVersion(u16),
    UnknownRecordType(u8),
    InvalidHeaderLength(u16),
    InvalidTotalLength(u32),
    InvalidCommitMarker,
    HeaderChecksumMismatch,
    RecordChecksumMismatch,
    InvalidNamespace,
    LengthOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRecord {
    pub record: Record,
    pub encoded_len: usize,
}

/// Encodes a committed version-1 record.
///
/// # Errors
///
/// Returns an error when a variable-length field cannot be represented by the
/// version-1 format or the aligned record would exceed `u32::MAX` bytes.
pub fn encode(record: &Record) -> Result<Vec<u8>, EncodeError> {
    let mut bytes = encode_uncommitted(record)?;
    put_u32(&mut bytes, 124, COMMIT_MARKER);
    Ok(bytes)
}

/// Encodes a version-1 record without its commit marker.
///
/// The returned bytes must not be made visible through the index. A storage
/// pipeline establishes durability and then calls [`commit_in_place`].
///
/// # Errors
///
/// Returns an error when a variable-length field cannot be represented by the
/// version-1 format or the aligned record would exceed `u32::MAX` bytes.
pub fn encode_uncommitted(record: &Record) -> Result<Vec<u8>, EncodeError> {
    let namespace = record.namespace.as_bytes();
    let namespace_len = u16::try_from(namespace.len())
        .map_err(|_| EncodeError::NamespaceTooLong(namespace.len()))?;
    let key_len =
        u16::try_from(record.key.len()).map_err(|_| EncodeError::KeyTooLong(record.key.len()))?;
    let dependency_count = u16::try_from(record.dependencies.len())
        .map_err(|_| EncodeError::TooManyDependencies(record.dependencies.len()))?;
    let payload_len = u32::try_from(record.payload.len())
        .map_err(|_| EncodeError::PayloadTooLong(record.payload.len()))?;
    let dependency_bytes = record
        .dependencies
        .len()
        .checked_mul(32)
        .ok_or(EncodeError::RecordTooLong(usize::MAX))?;
    let unaligned_len = HEADER_LEN
        .checked_add(namespace.len())
        .and_then(|length| length.checked_add(record.key.len()))
        .and_then(|length| length.checked_add(dependency_bytes))
        .and_then(|length| length.checked_add(record.payload.len()))
        .ok_or(EncodeError::RecordTooLong(usize::MAX))?;
    let total_len =
        align_up(unaligned_len, ALIGNMENT).ok_or(EncodeError::RecordTooLong(unaligned_len))?;
    let total_len_u32 =
        u32::try_from(total_len).map_err(|_| EncodeError::RecordTooLong(total_len))?;

    let mut bytes = vec![0; total_len];
    bytes[0..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    bytes[10] = record.record_type as u8;
    bytes[11] = record.flags;
    put_u16(&mut bytes, 12, HEADER_LEN_FIELD);
    put_u16(&mut bytes, 14, key_len);
    put_u16(&mut bytes, 16, namespace_len);
    put_u16(&mut bytes, 18, dependency_count);
    put_u32(&mut bytes, 20, payload_len);
    put_u64(&mut bytes, 24, record.sequence);
    put_u64(&mut bytes, 32, record.created_at_unix_ns);
    put_u64(&mut bytes, 40, record.expires_at_unix_ns.unwrap_or(0));
    bytes[48..80].copy_from_slice(&record.operation_hash);
    bytes[80..112].copy_from_slice(&record.content_hash);
    put_u32(&mut bytes, 120, total_len_u32);

    let mut cursor = HEADER_LEN;
    bytes[cursor..cursor + namespace.len()].copy_from_slice(namespace);
    cursor += namespace.len();
    bytes[cursor..cursor + record.key.len()].copy_from_slice(&record.key);
    cursor += record.key.len();
    for dependency in &record.dependencies {
        bytes[cursor..cursor + 32].copy_from_slice(dependency);
        cursor += 32;
    }
    bytes[cursor..cursor + record.payload.len()].copy_from_slice(&record.payload);

    let header_checksum = checksum(&bytes[0..112]);
    put_u32(&mut bytes, 112, header_checksum);
    let record_checksum = record_checksum(&bytes);
    put_u32(&mut bytes, 116, record_checksum);
    Ok(bytes)
}

/// Establishes the commit marker on an otherwise valid encoded record.
///
/// # Errors
///
/// Returns a decode error when the buffer is truncated or corrupt. The buffer
/// is unchanged on failure.
pub fn commit_in_place(bytes: &mut [u8]) -> Result<(), DecodeError> {
    validate_uncommitted(bytes)?;
    put_u32(bytes, 124, COMMIT_MARKER);
    Ok(())
}

#[must_use]
pub fn commit_marker_bytes() -> [u8; 4] {
    COMMIT_MARKER.to_le_bytes()
}

/// Decodes one committed version-1 record from the start of `bytes`.
///
/// # Errors
///
/// Returns [`DecodeError::Incomplete`] for a truncated record and a specific
/// validation error for corrupt or unsupported input.
pub fn decode(bytes: &[u8]) -> Result<DecodedRecord, DecodeError> {
    let total_len = validate_envelope(bytes, true)?;
    let encoded = &bytes[..total_len];
    decode_validated(encoded, total_len)
}

fn validate_uncommitted(bytes: &[u8]) -> Result<usize, DecodeError> {
    validate_envelope(bytes, false)
}

fn validate_envelope(bytes: &[u8], require_commit: bool) -> Result<usize, DecodeError> {
    if bytes.len() < HEADER_LEN {
        return Err(DecodeError::Incomplete);
    }
    if &bytes[0..8] != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = get_u16(bytes, 8);
    if version != VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    RecordType::try_from(bytes[10])?;
    let header_len = get_u16(bytes, 12);
    if header_len != HEADER_LEN_FIELD {
        return Err(DecodeError::InvalidHeaderLength(header_len));
    }
    let commit_marker = get_u32(bytes, 124);
    if require_commit && commit_marker != COMMIT_MARKER {
        return Err(DecodeError::InvalidCommitMarker);
    }
    if !require_commit && commit_marker != 0 {
        return Err(DecodeError::InvalidCommitMarker);
    }
    if checksum(&bytes[0..112]) != get_u32(bytes, 112) {
        return Err(DecodeError::HeaderChecksumMismatch);
    }

    let total_len_u32 = get_u32(bytes, 120);
    let total_len = usize::try_from(total_len_u32).map_err(|_| DecodeError::LengthOverflow)?;
    if total_len < HEADER_LEN || total_len % ALIGNMENT != 0 {
        return Err(DecodeError::InvalidTotalLength(total_len_u32));
    }
    if bytes.len() < total_len {
        return Err(DecodeError::Incomplete);
    }
    let encoded = &bytes[..total_len];
    if record_checksum(encoded) != get_u32(encoded, 116) {
        return Err(DecodeError::RecordChecksumMismatch);
    }
    Ok(total_len)
}

fn decode_validated(encoded: &[u8], total_len: usize) -> Result<DecodedRecord, DecodeError> {
    let total_len_u32 = get_u32(encoded, 120);
    let record_type = RecordType::try_from(encoded[10])?;
    let key_len = usize::from(get_u16(encoded, 14));
    let namespace_len = usize::from(get_u16(encoded, 16));
    let dependency_count = usize::from(get_u16(encoded, 18));
    let dependency_bytes = dependency_count
        .checked_mul(32)
        .ok_or(DecodeError::LengthOverflow)?;
    let payload_len =
        usize::try_from(get_u32(encoded, 20)).map_err(|_| DecodeError::LengthOverflow)?;
    let content_end = HEADER_LEN
        .checked_add(namespace_len)
        .and_then(|length| length.checked_add(key_len))
        .and_then(|length| length.checked_add(dependency_bytes))
        .and_then(|length| length.checked_add(payload_len))
        .ok_or(DecodeError::LengthOverflow)?;
    if content_end > total_len {
        return Err(DecodeError::InvalidTotalLength(total_len_u32));
    }

    let namespace = Namespace::new(encoded[HEADER_LEN..HEADER_LEN + namespace_len].to_vec())
        .map_err(|_| DecodeError::InvalidNamespace)?;
    let mut cursor = HEADER_LEN + namespace_len;
    let key = encoded[cursor..cursor + key_len].to_vec();
    cursor += key_len;
    let mut dependencies = Vec::with_capacity(dependency_count);
    for _ in 0..dependency_count {
        let mut dependency = [0; 32];
        dependency.copy_from_slice(&encoded[cursor..cursor + 32]);
        dependencies.push(dependency);
        cursor += 32;
    }
    let payload = encoded[cursor..cursor + payload_len].to_vec();
    let mut operation_hash = [0; 32];
    operation_hash.copy_from_slice(&encoded[48..80]);
    let mut content_hash = [0; 32];
    content_hash.copy_from_slice(&encoded[80..112]);
    let expiry = get_u64(encoded, 40);

    Ok(DecodedRecord {
        record: Record {
            record_type,
            flags: encoded[11],
            namespace,
            key,
            sequence: get_u64(encoded, 24),
            created_at_unix_ns: get_u64(encoded, 32),
            expires_at_unix_ns: (expiry != 0).then_some(expiry),
            operation_hash,
            content_hash,
            dependencies,
            payload,
        },
        encoded_len: total_len,
    })
}

fn record_checksum(bytes: &[u8]) -> u32 {
    let mut checksum = Checksum::new();
    checksum.update(&bytes[0..116]);
    checksum.update(&bytes[120..124]);
    checksum.update(&bytes[128..]);
    checksum.finish()
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|sum| sum / alignment * alignment)
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact() -> Record {
        Record {
            record_type: RecordType::Artifact,
            flags: 3,
            namespace: Namespace::new("swarm-a").expect("namespace"),
            key: b"operation-key".to_vec(),
            sequence: 42,
            created_at_unix_ns: 100,
            expires_at_unix_ns: Some(200),
            operation_hash: [1; 32],
            content_hash: [2; 32],
            dependencies: vec![[3; 32], [4; 32]],
            payload: b"cached tool output".to_vec(),
        }
    }

    #[test]
    fn crc32c_matches_standard_vector() {
        assert_eq!(checksum(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn record_round_trips() {
        let record = artifact();
        let bytes = encode(&record).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded.record, record);
        assert_eq!(decoded.encoded_len, bytes.len());
        assert_eq!(bytes.len() % ALIGNMENT, 0);
    }

    #[test]
    fn payload_corruption_is_detected() {
        let mut bytes = encode(&artifact()).expect("encode");
        bytes[HEADER_LEN + 8] ^= 0x80;
        assert_eq!(decode(&bytes), Err(DecodeError::RecordChecksumMismatch));
    }

    #[test]
    fn missing_commit_is_rejected() {
        let bytes = encode_uncommitted(&artifact()).expect("encode");
        assert_eq!(decode(&bytes), Err(DecodeError::InvalidCommitMarker));
    }

    #[test]
    fn explicit_commit_makes_record_visible() {
        let record = artifact();
        let mut bytes = encode_uncommitted(&record).expect("encode");
        assert_eq!(decode(&bytes), Err(DecodeError::InvalidCommitMarker));
        commit_in_place(&mut bytes).expect("commit");
        assert_eq!(decode(&bytes).expect("decode").record, record);
    }

    #[test]
    fn every_truncated_prefix_is_incomplete() {
        let bytes = encode(&artifact()).expect("encode");
        for end in 0..bytes.len() {
            assert_eq!(decode(&bytes[..end]), Err(DecodeError::Incomplete));
        }
    }
}
