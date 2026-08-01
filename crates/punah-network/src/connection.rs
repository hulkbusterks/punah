use std::io::{self, Read, Write};

use punah_protocol::{DecodeError, Frame, HEADER_LEN, decode, encode};

use crate::ConnectionError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VersionedFrame {
    V1(Frame),
    V2(punah_protocol::v2::Frame),
}

/// Reads exactly one frame, tolerating arbitrary TCP fragmentation.
///
/// `Ok(None)` means the peer closed between frames. Closing partway through a
/// frame is an `UnexpectedEof` error.
///
/// # Errors
///
/// Returns an I/O error for interrupted frames, or a decode error for malformed
/// protocol input.
pub fn read_frame(reader: &mut impl Read) -> Result<Option<Frame>, ConnectionError> {
    let mut header = [0_u8; HEADER_LEN];
    let mut read = 0;
    while read < header.len() {
        match reader.read(&mut header[read..]) {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer closed in the middle of a frame header",
                )
                .into());
            }
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }

    let total = match decode(&header) {
        Err(DecodeError::Incomplete { needed, .. }) => needed,
        Err(error) => return Err(error.into()),
        Ok(_) => HEADER_LEN,
    };
    let mut bytes = vec![0_u8; total];
    bytes[..HEADER_LEN].copy_from_slice(&header);
    reader.read_exact(&mut bytes[HEADER_LEN..])?;
    let (frame, used) = decode(&bytes)?;
    debug_assert_eq!(used, bytes.len());
    Ok(Some(frame.to_owned()))
}

/// Encodes and completely writes one frame.
///
/// # Errors
///
/// Returns an encoding error for an invalid frame or an I/O error if the full
/// frame cannot be written.
pub fn write_frame(writer: &mut impl Write, frame: &Frame) -> Result<(), ConnectionError> {
    let bytes = encode(frame.as_ref())?;
    writer.write_all(&bytes)?;
    Ok(())
}

/// Reads one v1 or v2 frame, selecting its bounded decoder from the first byte.
///
/// # Errors
/// Returns an I/O error for truncated input or a codec error for invalid frames.
pub fn read_versioned_frame(
    reader: &mut impl Read,
) -> Result<Option<VersionedFrame>, ConnectionError> {
    let Some(version) = read_first(reader)? else {
        return Ok(None);
    };
    match version {
        punah_protocol::VERSION => {
            let bytes = read_complete(reader, version, HEADER_LEN, |bytes| {
                decode(bytes).map(|(_, used)| used)
            })?;
            let (frame, _) = decode(&bytes)?;
            Ok(Some(VersionedFrame::V1(frame.to_owned())))
        }
        punah_protocol::v2::VERSION => {
            let bytes = read_complete(reader, version, punah_protocol::v2::HEADER_LEN, |bytes| {
                punah_protocol::v2::decode(bytes).map(|(_, used)| used)
            })?;
            let (frame, _) = punah_protocol::v2::decode(&bytes)?;
            Ok(Some(VersionedFrame::V2(frame.to_owned())))
        }
        other => Err(DecodeError::UnsupportedVersion(other).into()),
    }
}

/// Writes a frame using its own protocol version's encoder.
///
/// # Errors
/// Returns a codec error for invalid fields or an I/O error on write failure.
pub fn write_versioned_frame(
    writer: &mut impl Write,
    frame: &VersionedFrame,
) -> Result<(), ConnectionError> {
    let bytes = match frame {
        VersionedFrame::V1(frame) => encode(frame.as_ref())?,
        VersionedFrame::V2(frame) => punah_protocol::v2::encode(frame.as_ref())?,
    };
    writer.write_all(&bytes)?;
    Ok(())
}

fn read_first(reader: &mut impl Read) -> Result<Option<u8>, io::Error> {
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(byte[0])),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn read_complete<F>(
    reader: &mut impl Read,
    version: u8,
    header_len: usize,
    decode_frame: F,
) -> Result<Vec<u8>, ConnectionError>
where
    F: Fn(&[u8]) -> Result<usize, DecodeError>,
{
    let mut header = vec![0_u8; header_len];
    header[0] = version;
    reader.read_exact(&mut header[1..])?;
    let total = match decode_frame(&header) {
        Err(DecodeError::Incomplete { needed, .. }) => needed,
        Err(error) => return Err(error.into()),
        Ok(_) => header_len,
    };
    let mut bytes = vec![0_u8; total];
    bytes[..header_len].copy_from_slice(&header);
    reader.read_exact(&mut bytes[header_len..])?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use punah_protocol::{Frame, Opcode, encode};

    use super::*;

    struct OneByteAtATime<R>(R);

    impl<R: Read> Read for OneByteAtATime<R> {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let length = output.len().min(1);
            self.0.read(&mut output[..length])
        }
    }

    #[test]
    fn reads_fragmented_frame_exactly() {
        let expected = Frame::new(Opcode::Set, b"key".to_vec(), b"value".to_vec());
        let bytes = encode(expected.as_ref()).unwrap();
        let mut reader = OneByteAtATime(Cursor::new(bytes));
        assert_eq!(read_frame(&mut reader).unwrap(), Some(expected));
        assert_eq!(read_frame(&mut reader).unwrap(), None);
    }

    #[test]
    fn reports_partial_header_as_unexpected_eof() {
        let mut reader = Cursor::new([1_u8, 4]);
        let error = read_frame(&mut reader).unwrap_err();
        assert!(matches!(
            error,
            ConnectionError::Io(ref error) if error.kind() == io::ErrorKind::UnexpectedEof
        ));
    }

    #[test]
    fn reads_fragmented_v2_frame_and_following_v1_frame() {
        let v2 = punah_protocol::v2::Frame {
            opcode: punah_protocol::v2::Opcode::OperationResolve,
            status: punah_protocol::v2::Status::None,
            flags: 0,
            request_id: 42,
            ttl_seconds: 0,
            key: vec![7; 32],
            value: Vec::new(),
        };
        let v1 = Frame::new(Opcode::Ping, Vec::new(), Vec::new());
        let mut bytes = punah_protocol::v2::encode(v2.as_ref()).unwrap();
        bytes.extend_from_slice(&encode(v1.as_ref()).unwrap());
        let mut reader = OneByteAtATime(Cursor::new(bytes));
        assert_eq!(
            read_versioned_frame(&mut reader).unwrap(),
            Some(VersionedFrame::V2(v2))
        );
        assert_eq!(
            read_versioned_frame(&mut reader).unwrap(),
            Some(VersionedFrame::V1(v1))
        );
        assert_eq!(read_versioned_frame(&mut reader).unwrap(), None);
    }
}
