//! Punah v2 framing for artifacts, exact operations, and fenced leases.
//!
//! This module is intentionally independent from the Redgo-compatible v1
//! codec. A transport can select the decoder from the first version byte.

use crate::{DecodeError, EncodeError};
use punah_checksum::checksum_parts;

pub const VERSION: u8 = 2;
pub const HEADER_LEN: usize = 32;
const HEADER_LEN_WIRE: u16 = 32;
pub const MAX_KEY_LEN: usize = 1024 * 1024;
pub const MAX_VALUE_LEN: usize = 64 * 1024 * 1024;
pub const MAX_FRAME_LEN: usize = HEADER_LEN + MAX_KEY_LEN + MAX_VALUE_LEN;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum Opcode {
    ArtifactGet = 0x20,
    ArtifactPut = 0x21,
    OperationResolve = 0x22,
    LeaseAcquire = 0x23,
    LeaseRenew = 0x24,
    LeaseRelease = 0x25,
    ResultPublish = 0x26,
}

impl Opcode {
    const fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            0x20 => Self::ArtifactGet,
            0x21 => Self::ArtifactPut,
            0x22 => Self::OperationResolve,
            0x23 => Self::LeaseAcquire,
            0x24 => Self::LeaseRenew,
            0x25 => Self::LeaseRelease,
            0x26 => Self::ResultPublish,
            _ => return None,
        })
    }
}

/// `None` is used by requests; every response uses an explicit outcome.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum Status {
    None = 0x00,
    Hit = 0x01,
    Miss = 0x02,
    Stored = 0x03,
    Deleted = 0x04,
    LeaseGranted = 0x05,
    LeaseHeld = 0x06,
    Conflict = 0x07,
    Backpressure = 0x08,
    Invalid = 0x09,
    Error = 0xff,
}

impl Status {
    const fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            0x00 => Self::None,
            0x01 => Self::Hit,
            0x02 => Self::Miss,
            0x03 => Self::Stored,
            0x04 => Self::Deleted,
            0x05 => Self::LeaseGranted,
            0x06 => Self::LeaseHeld,
            0x07 => Self::Conflict,
            0x08 => Self::Backpressure,
            0x09 => Self::Invalid,
            0xff => Self::Error,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub opcode: Opcode,
    pub status: Status,
    pub flags: u8,
    pub request_id: u64,
    pub ttl_seconds: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Frame {
    #[must_use]
    pub fn as_ref(&self) -> FrameRef<'_> {
        FrameRef {
            opcode: self.opcode,
            status: self.status,
            flags: self.flags,
            request_id: self.request_id,
            ttl_seconds: self.ttl_seconds,
            key: &self.key,
            value: &self.value,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameRef<'a> {
    pub opcode: Opcode,
    pub status: Status,
    pub flags: u8,
    pub request_id: u64,
    pub ttl_seconds: u32,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl FrameRef<'_> {
    #[must_use]
    pub fn to_owned(self) -> Frame {
        Frame {
            opcode: self.opcode,
            status: self.status,
            flags: self.flags,
            request_id: self.request_id,
            ttl_seconds: self.ttl_seconds,
            key: self.key.to_vec(),
            value: self.value.to_vec(),
        }
    }
}

/// Encode one complete v2 frame into a fresh bounded allocation.
///
/// # Errors
///
/// Returns an error if either payload field exceeds its protocol bound.
pub fn encode(frame: FrameRef<'_>) -> Result<Vec<u8>, EncodeError> {
    let length = checked_len(frame)?;
    let mut output = vec![0; length];
    encode_into(frame, &mut output)?;
    Ok(output)
}

/// Encode one frame into `output`, preserving bytes after the encoded frame.
///
/// # Errors
///
/// Returns an error if a field exceeds its bound or `output` is too small.
pub fn encode_into(frame: FrameRef<'_>, output: &mut [u8]) -> Result<usize, EncodeError> {
    let length = checked_len(frame)?;
    if output.len() < length {
        return Err(EncodeError::OutputTooSmall {
            needed: length,
            available: output.len(),
        });
    }
    let key_len = u32::try_from(frame.key.len()).map_err(|_| EncodeError::KeyTooLarge {
        length: frame.key.len(),
        maximum: MAX_KEY_LEN,
    })?;
    let value_len = u32::try_from(frame.value.len()).map_err(|_| EncodeError::ValueTooLarge {
        length: frame.value.len(),
        maximum: MAX_VALUE_LEN,
    })?;
    let bytes = &mut output[..length];
    bytes.fill(0);
    bytes[0] = VERSION;
    bytes[1] = frame.opcode as u8;
    bytes[2] = frame.status as u8;
    bytes[3] = frame.flags;
    // 4..6 are reserved and must remain zero.
    bytes[6..8].copy_from_slice(&HEADER_LEN_WIRE.to_be_bytes());
    bytes[8..16].copy_from_slice(&frame.request_id.to_be_bytes());
    bytes[16..20].copy_from_slice(&key_len.to_be_bytes());
    bytes[20..24].copy_from_slice(&value_len.to_be_bytes());
    bytes[24..28].copy_from_slice(&frame.ttl_seconds.to_be_bytes());
    bytes[HEADER_LEN..HEADER_LEN + frame.key.len()].copy_from_slice(frame.key);
    bytes[HEADER_LEN + frame.key.len()..].copy_from_slice(frame.value);
    let checksum = checksum_parts(&[&bytes[..28], frame.key, frame.value]);
    bytes[28..32].copy_from_slice(&checksum.to_be_bytes());
    Ok(length)
}

/// Decode exactly one frame while borrowing its key and value from `input`.
///
/// An incomplete header reports 32 bytes. Once a complete header is present,
/// an incomplete body reports the exact total frame length required.
///
/// # Errors
///
/// Returns [`DecodeError::Incomplete`] with the precise required size for
/// partial input, or a specific error for malformed, unsupported, oversized,
/// or corrupt input.
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
    let status = Status::from_byte(input[2]).ok_or(DecodeError::UnknownStatus(input[2]))?;
    if input[4..6] != [0, 0] {
        return Err(DecodeError::ReservedFieldNonZero);
    }
    let header_len = u16::from_be_bytes([input[6], input[7]]);
    if usize::from(header_len) != HEADER_LEN {
        return Err(DecodeError::InvalidHeaderLength(header_len));
    }
    let key_len = u32::from_be_bytes([input[16], input[17], input[18], input[19]]) as usize;
    let value_len = u32::from_be_bytes([input[20], input[21], input[22], input[23]]) as usize;
    if key_len > MAX_KEY_LEN {
        return Err(DecodeError::KeyTooLarge {
            length: key_len,
            maximum: MAX_KEY_LEN,
        });
    }
    if value_len > MAX_VALUE_LEN {
        return Err(DecodeError::ValueTooLarge {
            length: value_len,
            maximum: MAX_VALUE_LEN,
        });
    }
    let length = HEADER_LEN + key_len + value_len;
    if input.len() < length {
        return Err(DecodeError::Incomplete {
            needed: length,
            available: input.len(),
        });
    }
    let key_end = HEADER_LEN + key_len;
    let key = &input[HEADER_LEN..key_end];
    let value = &input[key_end..length];
    let expected = u32::from_be_bytes([input[28], input[29], input[30], input[31]]);
    let actual = checksum_parts(&[&input[..28], key, value]);
    if expected != actual {
        return Err(DecodeError::ChecksumMismatch { expected, actual });
    }
    Ok((
        FrameRef {
            opcode,
            status,
            flags: input[3],
            request_id: u64::from_be_bytes([
                input[8], input[9], input[10], input[11], input[12], input[13], input[14],
                input[15],
            ]),
            ttl_seconds: u32::from_be_bytes([input[24], input[25], input[26], input[27]]),
            key,
            value,
        },
        length,
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
    Ok(HEADER_LEN + frame.key.len() + frame.value.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Frame {
        Frame {
            opcode: Opcode::LeaseAcquire,
            status: Status::LeaseGranted,
            flags: 0xa5,
            request_id: 0x0102_0304_0506_0708,
            ttl_seconds: 300,
            key: b"operation-id".to_vec(),
            value: b"fencing-token".to_vec(),
        }
    }

    #[test]
    fn exact_layout_and_borrowed_round_trip() {
        let frame = sample();
        let bytes = encode(frame.as_ref()).unwrap();
        assert_eq!(&bytes[..8], &[2, 0x23, 5, 0xa5, 0, 0, 0, 32]);
        assert_eq!(&bytes[8..16], &0x0102_0304_0506_0708_u64.to_be_bytes());
        assert_eq!(&bytes[16..20], &12_u32.to_be_bytes());
        assert_eq!(&bytes[20..24], &13_u32.to_be_bytes());
        assert_eq!(&bytes[24..28], &300_u32.to_be_bytes());
        let (decoded, consumed) = decode(&bytes).unwrap();
        assert_eq!(decoded, frame.as_ref());
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn every_opcode_and_status_has_a_stable_wire_value() {
        for opcode in [
            Opcode::ArtifactGet,
            Opcode::ArtifactPut,
            Opcode::OperationResolve,
            Opcode::LeaseAcquire,
            Opcode::LeaseRenew,
            Opcode::LeaseRelease,
            Opcode::ResultPublish,
        ] {
            assert_eq!(Opcode::from_byte(opcode as u8), Some(opcode));
        }
        for status in [
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
        ] {
            assert_eq!(Status::from_byte(status as u8), Some(status));
        }
    }

    #[test]
    fn truncation_reports_exact_required_length() {
        assert_eq!(
            decode(&[0; 31]),
            Err(DecodeError::Incomplete {
                needed: 32,
                available: 31
            })
        );
        let bytes = encode(sample().as_ref()).unwrap();
        for available in HEADER_LEN..bytes.len() {
            assert_eq!(
                decode(&bytes[..available]),
                Err(DecodeError::Incomplete {
                    needed: bytes.len(),
                    available
                })
            );
        }
    }

    #[test]
    fn checksum_covers_header_key_and_value() {
        let bytes = encode(sample().as_ref()).unwrap();
        for offset in [3, 8, 24, HEADER_LEN, bytes.len() - 1] {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 1;
            assert!(matches!(
                decode(&corrupt),
                Err(DecodeError::ChecksumMismatch { .. })
            ));
        }
    }

    #[test]
    fn malformed_header_is_rejected_before_body_access() {
        let mut bytes = encode(sample().as_ref()).unwrap();
        bytes[4] = 1;
        assert_eq!(decode(&bytes), Err(DecodeError::ReservedFieldNonZero));
        bytes[4] = 0;
        bytes[7] = 31;
        assert_eq!(decode(&bytes), Err(DecodeError::InvalidHeaderLength(31)));
        bytes[7] = 32;
        bytes[2] = 0x7f;
        assert_eq!(decode(&bytes), Err(DecodeError::UnknownStatus(0x7f)));
    }

    #[test]
    fn unsupported_version_and_opcode_are_rejected() {
        let mut bytes = encode(sample().as_ref()).unwrap();
        bytes[0] = 3;
        assert_eq!(decode(&bytes), Err(DecodeError::UnsupportedVersion(3)));
        bytes[0] = VERSION;
        bytes[1] = 0x7f;
        assert_eq!(decode(&bytes), Err(DecodeError::UnknownOpcode(0x7f)));
    }

    #[test]
    fn declared_lengths_are_bounded_before_incomplete_result() {
        let mut bytes = encode(sample().as_ref()).unwrap();
        let oversized_key = u32::try_from(MAX_KEY_LEN).unwrap() + 1;
        bytes[16..20].copy_from_slice(&oversized_key.to_be_bytes());
        assert_eq!(
            decode(&bytes),
            Err(DecodeError::KeyTooLarge {
                length: MAX_KEY_LEN + 1,
                maximum: MAX_KEY_LEN
            })
        );
        bytes[16..20].copy_from_slice(&0_u32.to_be_bytes());
        let oversized_value = u32::try_from(MAX_VALUE_LEN).unwrap() + 1;
        bytes[20..24].copy_from_slice(&oversized_value.to_be_bytes());
        assert_eq!(
            decode(&bytes),
            Err(DecodeError::ValueTooLarge {
                length: MAX_VALUE_LEN + 1,
                maximum: MAX_VALUE_LEN
            })
        );
    }

    #[test]
    fn trailing_bytes_and_output_suffix_are_preserved() {
        let frame = sample();
        let expected = HEADER_LEN + frame.key.len() + frame.value.len();
        let mut output = vec![0xcc; expected + 5];
        assert_eq!(encode_into(frame.as_ref(), &mut output).unwrap(), expected);
        assert_eq!(&output[expected..], &[0xcc; 5]);
        assert_eq!(decode(&output).unwrap().1, expected);
    }

    #[test]
    fn v1_layout_remains_distinct_and_unchanged() {
        let v1 = crate::Frame::new(crate::Opcode::Set, b"key".to_vec(), b"value".to_vec());
        let bytes = crate::encode(v1.as_ref()).unwrap();
        assert_eq!(&bytes[..8], &[1, 2, 0, 3, 0, 0, 0, 5]);
        assert_eq!(crate::decode(&bytes).unwrap().0, v1.as_ref());
        assert_eq!(
            decode(&bytes),
            Err(DecodeError::Incomplete {
                needed: 32,
                available: 24
            })
        );
    }
}
