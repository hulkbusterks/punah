//! Redgo-compatible fixed-header frame encoding and incremental decoding.

use crate::{DecodeError, EncodeError, Opcode};
use punah_checksum::checksum_parts;

/// Redgo v1 header size: version, opcode, lengths, CRC, flags, and 24-bit TTL.
pub const HEADER_LEN: usize = 16;
pub const VERSION: u8 = 1;
pub const MAX_KEY_LEN: usize = u16::MAX as usize;
/// Defensive allocation bound below the representable `u32` wire length.
pub const MAX_VALUE_LEN: usize = 64 * 1024 * 1024;
pub const MAX_FRAME_LEN: usize = HEADER_LEN + MAX_KEY_LEN + MAX_VALUE_LEN;
const MAX_TTL: u32 = 0x00ff_ffff;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub opcode: Opcode,
    pub flags: u8,
    pub ttl: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Frame {
    #[must_use]
    pub const fn new(opcode: Opcode, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            opcode,
            flags: 0,
            ttl: 0,
            key,
            value,
        }
    }

    #[must_use]
    pub fn as_ref(&self) -> FrameRef<'_> {
        FrameRef {
            opcode: self.opcode,
            flags: self.flags,
            ttl: self.ttl,
            key: &self.key,
            value: &self.value,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameRef<'a> {
    pub opcode: Opcode,
    pub flags: u8,
    /// TTL in seconds, encoded as an unsigned 24-bit big-endian integer.
    pub ttl: u32,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl FrameRef<'_> {
    #[must_use]
    pub fn to_owned(self) -> Frame {
        Frame {
            opcode: self.opcode,
            flags: self.flags,
            ttl: self.ttl,
            key: self.key.to_vec(),
            value: self.value.to_vec(),
        }
    }
}

/// Encodes a frame into a newly allocated buffer.
///
/// # Errors
///
/// Returns an error when a field exceeds its wire or defensive size limit.
pub fn encode(frame: FrameRef<'_>) -> Result<Vec<u8>, EncodeError> {
    let len = checked_len(frame)?;
    let mut output = vec![0; len];
    encode_into(frame, &mut output)?;
    Ok(output)
}

/// Encodes and validates only the fixed header for scatter/gather output.
///
/// The returned checksum covers the borrowed key and value exactly as in a
/// contiguous frame. Callers may write the header, key, and value as separate
/// buffers without constructing a second payload-sized allocation.
///
/// # Errors
///
/// Returns an error when a field exceeds its wire or defensive size limit.
pub fn encode_header(frame: FrameRef<'_>) -> Result<[u8; HEADER_LEN], EncodeError> {
    checked_len(frame)?;
    let key_len = frame.key.len();
    let key_len_wire = u16::try_from(key_len).map_err(|_| EncodeError::KeyTooLarge {
        length: key_len,
        maximum: MAX_KEY_LEN,
    })?;
    let value_len_wire =
        u32::try_from(frame.value.len()).map_err(|_| EncodeError::ValueTooLarge {
            length: frame.value.len(),
            maximum: MAX_VALUE_LEN,
        })?;
    let mut header = [0_u8; HEADER_LEN];
    header[0] = VERSION;
    header[1] = frame.opcode as u8;
    header[2..4].copy_from_slice(&key_len_wire.to_be_bytes());
    header[4..8].copy_from_slice(&value_len_wire.to_be_bytes());
    header[8..12].copy_from_slice(&checksum_parts(&[frame.key, frame.value]).to_be_bytes());
    header[12] = frame.flags;
    let ttl = frame.ttl.to_be_bytes();
    header[13..16].copy_from_slice(&ttl[1..]);
    Ok(header)
}

/// Encodes a frame into the beginning of `output`.
///
/// # Errors
///
/// Returns an error when a field exceeds its limit or `output` is too short.
pub fn encode_into(frame: FrameRef<'_>, output: &mut [u8]) -> Result<usize, EncodeError> {
    let len = checked_len(frame)?;
    if output.len() < len {
        return Err(EncodeError::OutputTooSmall {
            needed: len,
            available: output.len(),
        });
    }
    let key_len = frame.key.len();
    let header = encode_header(frame)?;
    let bytes = &mut output[..len];
    bytes[..HEADER_LEN].copy_from_slice(&header);
    bytes[HEADER_LEN..HEADER_LEN + key_len].copy_from_slice(frame.key);
    bytes[HEADER_LEN + key_len..].copy_from_slice(frame.value);
    Ok(len)
}

/// Decodes one frame and returns its borrowed fields plus bytes consumed.
///
/// # Errors
///
/// Returns [`DecodeError::Incomplete`] when more TCP input is needed, and a
/// specific validation error for malformed or unsupported frames.
pub fn decode(input: &[u8]) -> Result<(FrameRef<'_>, usize), DecodeError> {
    if input.len() < HEADER_LEN {
        return Err(DecodeError::Incomplete {
            needed: HEADER_LEN,
            available: input.len(),
        });
    }
    if input[0] != VERSION {
        return Err(DecodeError::UnsupportedVersion(input[0]));
    }
    let opcode = Opcode::from_byte(input[1]).ok_or(DecodeError::UnknownOpcode(input[1]))?;
    let key_len = usize::from(u16::from_be_bytes([input[2], input[3]]));
    let value_len = u32::from_be_bytes([input[4], input[5], input[6], input[7]]) as usize;
    if value_len > MAX_VALUE_LEN {
        return Err(DecodeError::ValueTooLarge {
            length: value_len,
            maximum: MAX_VALUE_LEN,
        });
    }
    let len = HEADER_LEN
        .checked_add(key_len)
        .and_then(|n| n.checked_add(value_len))
        .ok_or(DecodeError::ValueTooLarge {
            length: value_len,
            maximum: MAX_VALUE_LEN,
        })?;
    if input.len() < len {
        return Err(DecodeError::Incomplete {
            needed: len,
            available: input.len(),
        });
    }
    let key_end = HEADER_LEN + key_len;
    let key = &input[HEADER_LEN..key_end];
    let value = &input[key_end..len];
    let expected = u32::from_be_bytes([input[8], input[9], input[10], input[11]]);
    let actual = checksum_parts(&[key, value]);
    if expected != actual {
        return Err(DecodeError::ChecksumMismatch { expected, actual });
    }
    let ttl = u32::from_be_bytes([0, input[13], input[14], input[15]]);
    Ok((
        FrameRef {
            opcode,
            flags: input[12],
            ttl,
            key,
            value,
        },
        len,
    ))
}

fn checked_len(frame: FrameRef<'_>) -> Result<usize, EncodeError> {
    if frame.key.len() > MAX_KEY_LEN {
        return Err(EncodeError::KeyTooLarge {
            length: frame.key.len(),
            maximum: MAX_KEY_LEN,
        });
    }
    if frame.value.len() > MAX_VALUE_LEN {
        return Err(EncodeError::ValueTooLarge {
            length: frame.value.len(),
            maximum: MAX_VALUE_LEN,
        });
    }
    if frame.ttl > MAX_TTL {
        return Err(EncodeError::TtlTooLarge(frame.ttl));
    }
    Ok(HEADER_LEN + frame.key.len() + frame.value.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Frame {
        Frame {
            opcode: Opcode::Set,
            flags: 0xa5,
            ttl: 0x12_3456,
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }
    }

    #[test]
    fn exact_v1_layout_and_round_trip() {
        let bytes = encode(sample().as_ref()).unwrap();
        assert_eq!(&bytes[..8], &[1, 2, 0, 3, 0, 0, 0, 5]);
        assert_eq!(bytes[12], 0xa5);
        assert_eq!(&bytes[13..16], &[0x12, 0x34, 0x56]);
        assert_eq!(&bytes[16..], b"keyvalue");
        let (frame, used) = decode(&bytes).unwrap();
        assert_eq!(frame, sample().as_ref());
        assert_eq!(used, bytes.len());
    }

    #[test]
    fn crc_covers_key_then_value_only() {
        let bytes = encode(sample().as_ref()).unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            checksum_parts(&[b"key", b"value"])
        );
        let mut changed_header = bytes.clone();
        changed_header[12] ^= 1;
        assert!(decode(&changed_header).is_ok());
        let mut changed_value = bytes;
        changed_value[HEADER_LEN + 3] ^= 1;
        assert!(matches!(
            decode(&changed_value),
            Err(DecodeError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn partial_header_and_body_report_exact_requirement() {
        assert_eq!(
            decode(&[0; 2]),
            Err(DecodeError::Incomplete {
                needed: 16,
                available: 2
            })
        );
        let bytes = encode(sample().as_ref()).unwrap();
        assert_eq!(
            decode(&bytes[..18]),
            Err(DecodeError::Incomplete {
                needed: 24,
                available: 18
            })
        );
    }

    #[test]
    fn rejects_version_opcode_and_declared_oversize() {
        let mut bytes = encode(sample().as_ref()).unwrap();
        bytes[0] = 2;
        assert_eq!(decode(&bytes), Err(DecodeError::UnsupportedVersion(2)));
        bytes[0] = 1;
        bytes[1] = 0;
        assert_eq!(decode(&bytes), Err(DecodeError::UnknownOpcode(0)));
        bytes[1] = 2;
        bytes[4..8].copy_from_slice(&u32::try_from(MAX_VALUE_LEN + 1).unwrap().to_be_bytes());
        assert_eq!(
            decode(&bytes),
            Err(DecodeError::ValueTooLarge {
                length: MAX_VALUE_LEN + 1,
                maximum: MAX_VALUE_LEN
            })
        );
    }

    #[test]
    fn rejects_ttl_outside_unsigned_24_bit_range() {
        let owned = sample();
        let frame = FrameRef {
            ttl: 0x0100_0000,
            ..owned.as_ref()
        };
        assert_eq!(encode(frame), Err(EncodeError::TtlTooLarge(0x0100_0000)));
    }

    #[test]
    fn encode_into_checks_capacity_and_preserves_suffix() {
        let frame = sample();
        assert_eq!(
            encode_into(frame.as_ref(), &mut [0; 20]),
            Err(EncodeError::OutputTooSmall {
                needed: 24,
                available: 20
            })
        );
        let mut output = [0xcc; 27];
        assert_eq!(encode_into(frame.as_ref(), &mut output).unwrap(), 24);
        assert_eq!(&output[24..], &[0xcc; 3]);
    }

    #[test]
    fn header_encoding_matches_contiguous_encoding() {
        let frame = sample();
        let encoded = encode(frame.as_ref()).unwrap();
        assert_eq!(
            encode_header(frame.as_ref()).unwrap(),
            encoded[..HEADER_LEN]
        );
    }

    #[test]
    fn trailing_bytes_are_left_for_next_frame() {
        let mut bytes = encode(sample().as_ref()).unwrap();
        bytes.extend_from_slice(b"next");
        assert_eq!(decode(&bytes).unwrap().1, 24);
    }

    #[test]
    fn empty_key_and_value_round_trip() {
        let frame = Frame::new(Opcode::Ping, Vec::new(), Vec::new());
        let bytes = encode(frame.as_ref()).unwrap();
        assert_eq!(bytes.len(), HEADER_LEN);
        assert_eq!(decode(&bytes).unwrap().0, frame.as_ref());
    }
}
