use punah_checksum::{Checksum, checksum};
use punah_types::{ContentHash, Namespace, OperationHash};

const MAGIC: &[u8; 8] = b"PUNAH001";
const VERSION: u16 = 1;
const HEADER_LEN_FIELD: u16 = 128;
const HEADER_LEN: usize = 128;
pub const RECORD_HEADER_LEN: usize = HEADER_LEN;
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

/// Validated record fields borrowing variable-length bytes from an encoded
/// record. This avoids allocating payloads while scanning mapped segments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordRef<'a> {
    pub record_type: RecordType,
    pub flags: u8,
    pub namespace: &'a [u8],
    pub key: &'a [u8],
    pub sequence: u64,
    pub created_at_unix_ns: u64,
    pub expires_at_unix_ns: Option<u64>,
    pub operation_hash: OperationHash,
    pub content_hash: ContentHash,
    dependency_bytes: &'a [u8],
    pub payload: &'a [u8],
}

impl RecordRef<'_> {
    /// Iterates over copied dependency identities without allocating a vector.
    #[must_use]
    pub fn dependencies(&self) -> impl ExactSizeIterator<Item = ContentHash> + '_ {
        self.dependency_bytes
            .chunks_exact(32)
            .map(|bytes| <[u8; 32]>::try_from(bytes).unwrap_or([0; 32]))
    }

    fn to_owned(self) -> Result<Record, DecodeError> {
        Ok(Record {
            record_type: self.record_type,
            flags: self.flags,
            namespace: Namespace::new(self.namespace.to_vec())
                .map_err(|_| DecodeError::InvalidNamespace)?,
            key: self.key.to_vec(),
            sequence: self.sequence,
            created_at_unix_ns: self.created_at_unix_ns,
            expires_at_unix_ns: self.expires_at_unix_ns,
            operation_hash: self.operation_hash,
            content_hash: self.content_hash,
            dependencies: self.dependencies().collect(),
            payload: self.payload.to_vec(),
        })
    }
}

/// A validated borrowed record and the bytes consumed by it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedRecordRef<'a> {
    pub record: RecordRef<'a>,
    pub encoded_len: usize,
}

#[derive(Clone, Copy)]
struct EncodedLayout {
    namespace_len: u16,
    key_len: u16,
    dependency_count: u16,
    payload_len: u32,
    total_len: usize,
    total_len_u32: u32,
}

/// Returns the exact aligned size of an encoded record without allocating or
/// calculating its checksums.
///
/// # Errors
///
/// Returns the same representability errors as [`encode_uncommitted`].
pub fn encoded_len(record: &Record) -> Result<usize, EncodeError> {
    layout(record).map(|layout| layout.total_len)
}

/// Returns the exact aligned size for variable-length record fields without
/// constructing or copying a [`Record`].
///
/// This is useful for admission paths that need to reject an unrepresentable
/// request before it is merged into a storage cohort.
///
/// # Errors
///
/// Returns the same length and representability errors as [`encoded_len`].
pub fn encoded_len_for_fields(
    namespace_len: usize,
    key_len: usize,
    dependency_count: usize,
    payload_len: usize,
) -> Result<usize, EncodeError> {
    layout_for_fields(namespace_len, key_len, dependency_count, payload_len)
        .map(|layout| layout.total_len)
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
    let layout = layout(record)?;

    let mut bytes = vec![0; layout.total_len];
    bytes[0..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    bytes[10] = record.record_type as u8;
    bytes[11] = record.flags;
    put_u16(&mut bytes, 12, HEADER_LEN_FIELD);
    put_u16(&mut bytes, 14, layout.key_len);
    put_u16(&mut bytes, 16, layout.namespace_len);
    put_u16(&mut bytes, 18, layout.dependency_count);
    put_u32(&mut bytes, 20, layout.payload_len);
    put_u64(&mut bytes, 24, record.sequence);
    put_u64(&mut bytes, 32, record.created_at_unix_ns);
    put_u64(&mut bytes, 40, record.expires_at_unix_ns.unwrap_or(0));
    bytes[48..80].copy_from_slice(&record.operation_hash);
    bytes[80..112].copy_from_slice(&record.content_hash);
    put_u32(&mut bytes, 120, layout.total_len_u32);

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

fn layout(record: &Record) -> Result<EncodedLayout, EncodeError> {
    layout_for_fields(
        record.namespace.as_bytes().len(),
        record.key.len(),
        record.dependencies.len(),
        record.payload.len(),
    )
}

fn layout_for_fields(
    namespace_length: usize,
    key_length: usize,
    dependency_length: usize,
    payload_length: usize,
) -> Result<EncodedLayout, EncodeError> {
    let namespace_len = u16::try_from(namespace_length)
        .map_err(|_| EncodeError::NamespaceTooLong(namespace_length))?;
    let key_len = u16::try_from(key_length).map_err(|_| EncodeError::KeyTooLong(key_length))?;
    let dependency_count = u16::try_from(dependency_length)
        .map_err(|_| EncodeError::TooManyDependencies(dependency_length))?;
    let payload_len =
        u32::try_from(payload_length).map_err(|_| EncodeError::PayloadTooLong(payload_length))?;
    let dependency_bytes = dependency_length
        .checked_mul(32)
        .ok_or(EncodeError::RecordTooLong(usize::MAX))?;
    let unaligned_len = HEADER_LEN
        .checked_add(namespace_length)
        .and_then(|length| length.checked_add(key_length))
        .and_then(|length| length.checked_add(dependency_bytes))
        .and_then(|length| length.checked_add(payload_length))
        .ok_or(EncodeError::RecordTooLong(usize::MAX))?;
    let total_len =
        align_up(unaligned_len, ALIGNMENT).ok_or(EncodeError::RecordTooLong(unaligned_len))?;
    let total_len_u32 =
        u32::try_from(total_len).map_err(|_| EncodeError::RecordTooLong(total_len))?;
    Ok(EncodedLayout {
        namespace_len,
        key_len,
        dependency_count,
        payload_len,
        total_len,
        total_len_u32,
    })
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
    let decoded = decode_ref(bytes)?;
    Ok(DecodedRecord {
        record: decoded.record.to_owned()?,
        encoded_len: decoded.encoded_len,
    })
}

/// Validates one committed version-1 record without copying its variable-sized
/// fields.
///
/// # Errors
///
/// Returns [`DecodeError::Incomplete`] for a truncated record and a specific
/// validation error for corrupt or unsupported input.
pub fn decode_ref(bytes: &[u8]) -> Result<DecodedRecordRef<'_>, DecodeError> {
    let total_len = validate_envelope(bytes, true)?;
    decode_validated_ref(&bytes[..total_len], total_len)
}

/// Validates a committed fixed header and returns its declared aligned record
/// length without reading the variable region.
///
/// This is intended for bounded mapped-file scanners: copy one header, measure
/// the exact record, then copy and fully validate only those bytes. A successful
/// result does not validate the complete-record checksum.
///
/// # Errors
///
/// Returns an error for a truncated, uncommitted, corrupt, or unsupported
/// header, or for an invalid declared length.
pub fn committed_record_len(header: &[u8]) -> Result<usize, DecodeError> {
    validate_header(header, true)
}

fn validate_uncommitted(bytes: &[u8]) -> Result<usize, DecodeError> {
    validate_envelope(bytes, false)
}

fn validate_envelope(bytes: &[u8], require_commit: bool) -> Result<usize, DecodeError> {
    let total_len = validate_header(bytes, require_commit)?;
    if bytes.len() < total_len {
        return Err(DecodeError::Incomplete);
    }
    let encoded = &bytes[..total_len];
    if record_checksum(encoded) != get_u32(encoded, 116) {
        return Err(DecodeError::RecordChecksumMismatch);
    }
    Ok(total_len)
}

fn validate_header(bytes: &[u8], require_commit: bool) -> Result<usize, DecodeError> {
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
    Ok(total_len)
}

fn decode_validated_ref(
    encoded: &[u8],
    total_len: usize,
) -> Result<DecodedRecordRef<'_>, DecodeError> {
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

    if namespace_len == 0 {
        return Err(DecodeError::InvalidNamespace);
    }
    let namespace = &encoded[HEADER_LEN..HEADER_LEN + namespace_len];
    let mut cursor = HEADER_LEN + namespace_len;
    let key = &encoded[cursor..cursor + key_len];
    cursor += key_len;
    let dependencies = &encoded[cursor..cursor + dependency_bytes];
    cursor += dependency_bytes;
    let payload = &encoded[cursor..cursor + payload_len];
    let mut operation_hash = [0; 32];
    operation_hash.copy_from_slice(&encoded[48..80]);
    let mut content_hash = [0; 32];
    content_hash.copy_from_slice(&encoded[80..112]);
    let expiry = get_u64(encoded, 40);

    Ok(DecodedRecordRef {
        record: RecordRef {
            record_type,
            flags: encoded[11],
            namespace,
            key,
            sequence: get_u64(encoded, 24),
            created_at_unix_ns: get_u64(encoded, 32),
            expires_at_unix_ns: (expiry != 0).then_some(expiry),
            operation_hash,
            content_hash,
            dependency_bytes: dependencies,
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
        assert_eq!(encoded_len(&record), Ok(bytes.len()));
        assert_eq!(
            encoded_len_for_fields(
                record.namespace.as_bytes().len(),
                record.key.len(),
                record.dependencies.len(),
                record.payload.len(),
            ),
            Ok(bytes.len())
        );
        let borrowed = decode_ref(&bytes).expect("borrowed decode");
        assert_eq!(borrowed.record.namespace, record.namespace.as_bytes());
        assert_eq!(borrowed.record.key, record.key);
        assert_eq!(borrowed.record.payload, record.payload);
        assert_eq!(
            borrowed.record.dependencies().collect::<Vec<_>>(),
            record.dependencies
        );
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

    #[test]
    fn committed_header_reports_length_before_variable_bytes_are_present() {
        let record = artifact();
        let committed = encode(&record).expect("encode");
        assert_eq!(
            committed_record_len(&committed[..RECORD_HEADER_LEN]),
            Ok(committed.len())
        );

        let uncommitted = encode_uncommitted(&record).expect("encode");
        assert_eq!(
            committed_record_len(&uncommitted[..RECORD_HEADER_LEN]),
            Err(DecodeError::InvalidCommitMarker)
        );
        assert_eq!(
            committed_record_len(&committed[..RECORD_HEADER_LEN - 1]),
            Err(DecodeError::Incomplete)
        );
    }
}
