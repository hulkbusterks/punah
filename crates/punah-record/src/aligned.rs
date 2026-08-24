use punah_checksum::checksum;

use crate::{DecodeError, DecodedRecordRef, EncodeError, Record, decode_ref, encode};

const BODY_MAGIC: [u8; 8] = *b"PUNAXB02";
const FOOTER_MAGIC: [u8; 8] = *b"PUNAXC02";
const VERSION: u16 = 2;
const HEADER_LEN: usize = 64;
const HEADER_LEN_U16: u16 = 64;
const HEADER_CHECKSUM_OFFSET: usize = 60;
const MIN_ALIGNMENT: usize = 512;
const MAX_ALIGNMENT: usize = 64 * 1024;

/// Two buffers submitted by an aligned direct-device append protocol.
///
/// `body` is written and flushed first. `footer` is then written as one aligned
/// block and flushed. Neither buffer requires an in-place sub-block rewrite.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlignedExtent {
    pub alignment: usize,
    pub body: Vec<u8>,
    pub footer: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlignedEncodeError {
    Empty,
    InvalidAlignment(usize),
    TooManyRecords(usize),
    LengthOverflow,
    Record(EncodeError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlignedDecodeError {
    Incomplete,
    BadBodyMagic,
    BadFooterMagic,
    UnsupportedVersion(u16),
    InvalidHeaderLength(u16),
    InvalidAlignment(u32),
    InvalidLength,
    HeaderChecksumMismatch,
    BodyChecksumMismatch,
    FooterChecksumMismatch,
    FooterMismatch,
    NonZeroPadding,
    RecordCountMismatch,
    SequenceMismatch,
    Record(DecodeError),
}

/// Validated records borrowing their bytes from an aligned body extent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedAlignedExtent<'a> {
    pub alignment: usize,
    pub records: Vec<DecodedRecordRef<'a>>,
}

/// Packs committed record encodings densely into an aligned body and constructs
/// one aligned authorization footer.
///
/// The footer is not a transaction promise: records become individually
/// publishable in sequence after the complete extent is durable. It is a
/// recovery primitive that prevents a torn/missing footer from authorizing any
/// body record.
///
/// # Errors
/// Returns for an empty cohort, unsupported alignment, excessive count/length,
/// or a record that cannot be represented by record v1 inside the extent.
pub fn encode_aligned_extent(
    records: &[Record],
    alignment: usize,
) -> Result<AlignedExtent, AlignedEncodeError> {
    validate_alignment(alignment).map_err(|()| AlignedEncodeError::InvalidAlignment(alignment))?;
    if records.is_empty() {
        return Err(AlignedEncodeError::Empty);
    }
    let count = u16::try_from(records.len())
        .map_err(|_| AlignedEncodeError::TooManyRecords(records.len()))?;
    let mut encoded = Vec::with_capacity(records.len());
    let mut used = HEADER_LEN;
    for record in records {
        let bytes = encode(record).map_err(AlignedEncodeError::Record)?;
        used = used
            .checked_add(bytes.len())
            .ok_or(AlignedEncodeError::LengthOverflow)?;
        encoded.push(bytes);
    }
    let total = align_up(used, alignment).ok_or(AlignedEncodeError::LengthOverflow)?;
    let used_u32 = u32::try_from(used).map_err(|_| AlignedEncodeError::LengthOverflow)?;
    let total_u32 = u32::try_from(total).map_err(|_| AlignedEncodeError::LengthOverflow)?;
    let alignment_u32 = u32::try_from(alignment).map_err(|_| AlignedEncodeError::LengthOverflow)?;
    let first_sequence = records.first().map_or(0, |record| record.sequence);
    let last_sequence = records.last().map_or(0, |record| record.sequence);

    let mut body = vec![0; total];
    let mut cursor = HEADER_LEN;
    for bytes in encoded {
        let end = cursor
            .checked_add(bytes.len())
            .ok_or(AlignedEncodeError::LengthOverflow)?;
        body[cursor..end].copy_from_slice(&bytes);
        cursor = end;
    }
    let body_checksum = checksum(&body[HEADER_LEN..used]);
    write_header(
        &mut body[..HEADER_LEN],
        BODY_MAGIC,
        alignment_u32,
        count,
        used_u32,
        total_u32,
        body_checksum,
        first_sequence,
        last_sequence,
    );

    let mut footer = vec![0; alignment];
    write_header(
        &mut footer[..HEADER_LEN],
        FOOTER_MAGIC,
        alignment_u32,
        count,
        used_u32,
        total_u32,
        body_checksum,
        first_sequence,
        last_sequence,
    );
    Ok(AlignedExtent {
        alignment,
        body,
        footer,
    })
}

/// Validates an aligned body/footer pair and returns borrowed record views.
///
/// # Errors
/// Returns if either object is torn, corrupt, noncanonical, inconsistent, or
/// contains an invalid nested record.
pub fn decode_aligned_extent<'a>(
    body: &'a [u8],
    footer: &[u8],
) -> Result<DecodedAlignedExtent<'a>, AlignedDecodeError> {
    if body.len() < HEADER_LEN || footer.len() < HEADER_LEN {
        return Err(AlignedDecodeError::Incomplete);
    }
    let body_header = read_header(&body[..HEADER_LEN], BODY_MAGIC, true)?;
    let footer_header = read_header(&footer[..HEADER_LEN], FOOTER_MAGIC, false)?;
    if body_header != footer_header {
        return Err(AlignedDecodeError::FooterMismatch);
    }
    let alignment = usize::try_from(body_header.alignment)
        .map_err(|_| AlignedDecodeError::InvalidAlignment(body_header.alignment))?;
    validate_alignment(alignment)
        .map_err(|()| AlignedDecodeError::InvalidAlignment(body_header.alignment))?;
    if footer.len() != alignment
        || body.len() != usize::try_from(body_header.total).unwrap_or(usize::MAX)
        || !body.len().is_multiple_of(alignment)
    {
        return Err(AlignedDecodeError::InvalidLength);
    }
    let used = usize::try_from(body_header.used).map_err(|_| AlignedDecodeError::InvalidLength)?;
    if !(HEADER_LEN..=body.len()).contains(&used) {
        return Err(AlignedDecodeError::InvalidLength);
    }
    if body[used..].iter().any(|byte| *byte != 0)
        || footer[HEADER_LEN..].iter().any(|byte| *byte != 0)
    {
        return Err(AlignedDecodeError::NonZeroPadding);
    }
    if checksum(&body[HEADER_LEN..used]) != body_header.body_checksum {
        return Err(AlignedDecodeError::BodyChecksumMismatch);
    }

    let mut records = Vec::with_capacity(usize::from(body_header.count));
    let mut cursor = HEADER_LEN;
    while cursor < used && records.len() < usize::from(body_header.count) {
        let decoded = decode_ref(&body[cursor..used]).map_err(AlignedDecodeError::Record)?;
        cursor = cursor
            .checked_add(decoded.encoded_len)
            .ok_or(AlignedDecodeError::InvalidLength)?;
        records.push(decoded);
    }
    if cursor != used || records.len() != usize::from(body_header.count) {
        return Err(AlignedDecodeError::RecordCountMismatch);
    }
    if records.first().map_or(0, |record| record.record.sequence) != body_header.first_sequence
        || records.last().map_or(0, |record| record.record.sequence) != body_header.last_sequence
    {
        return Err(AlignedDecodeError::SequenceMismatch);
    }
    Ok(DecodedAlignedExtent { alignment, records })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Header {
    alignment: u32,
    count: u16,
    used: u32,
    total: u32,
    body_checksum: u32,
    first_sequence: u64,
    last_sequence: u64,
}

#[allow(clippy::too_many_arguments)]
fn write_header(
    output: &mut [u8],
    magic: [u8; 8],
    alignment: u32,
    count: u16,
    used: u32,
    total: u32,
    body_checksum: u32,
    first_sequence: u64,
    last_sequence: u64,
) {
    output[..8].copy_from_slice(&magic);
    put_u16(output, 8, VERSION);
    put_u16(output, 10, HEADER_LEN_U16);
    put_u32(output, 12, alignment);
    put_u16(output, 16, count);
    put_u32(output, 20, used);
    put_u32(output, 24, total);
    put_u32(output, 28, body_checksum);
    put_u64(output, 32, first_sequence);
    put_u64(output, 40, last_sequence);
    let header_checksum = checksum(&output[..HEADER_CHECKSUM_OFFSET]);
    put_u32(output, HEADER_CHECKSUM_OFFSET, header_checksum);
}

fn read_header(bytes: &[u8], magic: [u8; 8], body: bool) -> Result<Header, AlignedDecodeError> {
    if bytes[..8] != magic {
        return Err(if body {
            AlignedDecodeError::BadBodyMagic
        } else {
            AlignedDecodeError::BadFooterMagic
        });
    }
    let version = get_u16(bytes, 8);
    if version != VERSION {
        return Err(AlignedDecodeError::UnsupportedVersion(version));
    }
    let header_len = get_u16(bytes, 10);
    if header_len != HEADER_LEN_U16 {
        return Err(AlignedDecodeError::InvalidHeaderLength(header_len));
    }
    let expected = checksum(&bytes[..HEADER_CHECKSUM_OFFSET]);
    let actual = get_u32(bytes, HEADER_CHECKSUM_OFFSET);
    if expected != actual {
        return Err(if body {
            AlignedDecodeError::HeaderChecksumMismatch
        } else {
            AlignedDecodeError::FooterChecksumMismatch
        });
    }
    Ok(Header {
        alignment: get_u32(bytes, 12),
        count: get_u16(bytes, 16),
        used: get_u32(bytes, 20),
        total: get_u32(bytes, 24),
        body_checksum: get_u32(bytes, 28),
        first_sequence: get_u64(bytes, 32),
        last_sequence: get_u64(bytes, 40),
    })
}

fn validate_alignment(alignment: usize) -> Result<(), ()> {
    if (MIN_ALIGNMENT..=MAX_ALIGNMENT).contains(&alignment) && alignment.is_power_of_two() {
        Ok(())
    } else {
        Err(())
    }
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|value| value / alignment * alignment)
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

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap_or([0; 2]))
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap_or([0; 4]))
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap_or([0; 8]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RecordType;
    use punah_types::Namespace;

    fn record(sequence: u64, payload: usize) -> Record {
        Record {
            record_type: RecordType::Artifact,
            flags: 0,
            namespace: Namespace::new("default").unwrap(),
            key: format!("key-{sequence}").into_bytes(),
            sequence,
            created_at_unix_ns: sequence,
            expires_at_unix_ns: None,
            operation_hash: [1; 32],
            content_hash: [2; 32],
            dependencies: Vec::new(),
            payload: vec![3; payload],
        }
    }

    fn golden_record() -> Record {
        Record {
            record_type: RecordType::Artifact,
            flags: 3,
            namespace: Namespace::new("gold").unwrap(),
            key: b"key".to_vec(),
            sequence: 0x0102_0304_0506_0708,
            created_at_unix_ns: 11,
            expires_at_unix_ns: Some(22),
            operation_hash: [0x11; 32],
            content_hash: [0x22; 32],
            dependencies: vec![[0x33; 32]],
            payload: b"payload".to_vec(),
        }
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

    #[test]
    fn durable_v1_and_aligned_v2_headers_match_golden_bytes() {
        let expected_record = decode_hex(include_str!("../fixtures/record-v1.hex"));
        assert_eq!(encode(&golden_record()).unwrap(), expected_record);

        let extent = encode_aligned_extent(&[golden_record()], 512).unwrap();
        let body_header = decode_hex(include_str!("../fixtures/aligned-body-v2-header.hex"));
        let footer_header = decode_hex(include_str!("../fixtures/aligned-footer-v2-header.hex"));
        assert_eq!(&extent.body[..HEADER_LEN], body_header);
        assert_eq!(
            &extent.body[HEADER_LEN..HEADER_LEN + expected_record.len()],
            expected_record
        );
        assert!(
            extent.body[HEADER_LEN + expected_record.len()..]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert_eq!(&extent.footer[..HEADER_LEN], footer_header);
        assert!(extent.footer[HEADER_LEN..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn aligned_extent_round_trips_dense_records() {
        let records = vec![record(7, 16), record(8, 2048), record(9, 31)];
        let extent = encode_aligned_extent(&records, 4096).unwrap();
        assert_eq!(extent.body.len() % 4096, 0);
        assert_eq!(extent.footer.len(), 4096);
        let decoded = decode_aligned_extent(&extent.body, &extent.footer).unwrap();
        assert_eq!(decoded.alignment, 4096);
        assert_eq!(decoded.records.len(), records.len());
        assert_eq!(
            decoded
                .records
                .iter()
                .map(|decoded| decoded.record.sequence)
                .collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert_eq!(decoded.records[1].record.payload, records[1].payload);
    }

    #[test]
    fn missing_or_torn_footer_never_authorizes_the_body() {
        let extent = encode_aligned_extent(&[record(1, 16)], 4096).unwrap();
        assert!(matches!(
            decode_aligned_extent(&extent.body, &vec![0; 4096]),
            Err(AlignedDecodeError::BadFooterMagic)
        ));
        for boundary in [0, 8, 60, 63, 4095] {
            assert!(decode_aligned_extent(&extent.body, &extent.footer[..boundary]).is_err());
        }
    }

    #[test]
    fn corruption_and_noncanonical_padding_are_rejected() {
        let extent = encode_aligned_extent(&[record(1, 16), record(2, 16)], 512).unwrap();
        let mut body = extent.body.clone();
        body[HEADER_LEN + 140] ^= 1;
        assert!(matches!(
            decode_aligned_extent(&body, &extent.footer),
            Err(AlignedDecodeError::BodyChecksumMismatch)
        ));

        let mut footer = extent.footer.clone();
        footer[HEADER_LEN] = 1;
        assert!(matches!(
            decode_aligned_extent(&extent.body, &footer),
            Err(AlignedDecodeError::NonZeroPadding)
        ));
    }

    #[test]
    fn alignment_and_empty_cohort_are_rejected() {
        assert_eq!(
            encode_aligned_extent(&[], 4096),
            Err(AlignedEncodeError::Empty)
        );
        assert_eq!(
            encode_aligned_extent(&[record(1, 1)], 1000),
            Err(AlignedEncodeError::InvalidAlignment(1000))
        );
    }
}
