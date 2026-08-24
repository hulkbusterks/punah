use std::io::{self, IoSlice, Read, Write};

use punah_protocol::{DecodeError, Frame, FrameRef, HEADER_LEN, decode, encode, encode_header};

use crate::ConnectionError;

// Development crossover measurements were neutral around 32 KiB: small
// frames retain the cheaper contiguous path, while large responses avoid a
// second payload-sized allocation and copy.
const VECTORED_BODY_THRESHOLD: usize = 32 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VersionedFrame {
    V1(Frame),
    V2(punah_protocol::v2::Frame),
}

/// Reusable storage for reading one borrowed protocol-v1 frame at a time.
///
/// The returned frame borrows this buffer and therefore cannot outlive it or
/// overlap the next read. Benchmark clients can validate a response without
/// allocating/copying owned key and value vectors for every frame.
#[derive(Debug, Default)]
pub struct FrameBuffer {
    bytes: Vec<u8>,
}

impl FrameBuffer {
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    /// Reads one complete frame into reusable storage.
    ///
    /// `Ok(None)` means the peer closed between frames. The returned borrow is
    /// invalidated by the next mutable access to this buffer.
    ///
    /// # Errors
    /// Returns an I/O error for interrupted frames, or a decode error for
    /// malformed protocol input.
    pub fn read<'a>(
        &'a mut self,
        reader: &mut impl Read,
    ) -> Result<Option<FrameRef<'a>>, ConnectionError> {
        let mut header = [0_u8; HEADER_LEN];
        if !read_header(reader, &mut header)? {
            return Ok(None);
        }
        let total = match decode(&header) {
            Err(DecodeError::Incomplete { needed, .. }) => needed,
            Err(error) => return Err(error.into()),
            Ok(_) => HEADER_LEN,
        };
        self.bytes.resize(total, 0);
        self.bytes[..HEADER_LEN].copy_from_slice(&header);
        reader.read_exact(&mut self.bytes[HEADER_LEN..])?;
        let (frame, used) = decode(&self.bytes)?;
        debug_assert_eq!(used, self.bytes.len());
        Ok(Some(frame))
    }
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
    Ok(FrameBuffer::new().read(reader)?.map(FrameRef::to_owned))
}

fn read_header(reader: &mut impl Read, header: &mut [u8; HEADER_LEN]) -> Result<bool, io::Error> {
    let mut read = 0;
    while read < header.len() {
        match reader.read(&mut header[read..]) {
            Ok(0) if read == 0 => return Ok(false),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer closed in the middle of a frame header",
                ));
            }
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

/// Encodes and completely writes one frame.
///
/// # Errors
///
/// Returns an encoding error for an invalid frame or an I/O error if the full
/// frame cannot be written.
pub fn write_frame(writer: &mut impl Write, frame: &Frame) -> Result<(), ConnectionError> {
    let frame = frame.as_ref();
    if frame.key.len().saturating_add(frame.value.len()) < VECTORED_BODY_THRESHOLD {
        writer.write_all(&encode(frame)?)?;
        return Ok(());
    }
    let header = encode_header(frame)?;
    write_parts(writer, &header, frame.key, frame.value)?;
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
    match frame {
        VersionedFrame::V1(frame) => write_frame(writer, frame)?,
        VersionedFrame::V2(frame) => {
            let frame = frame.as_ref();
            if frame.key.len().saturating_add(frame.value.len()) < VECTORED_BODY_THRESHOLD {
                writer.write_all(&punah_protocol::v2::encode(frame)?)?;
                return Ok(());
            }
            let header = punah_protocol::v2::encode_header(frame)?;
            write_parts(writer, &header, frame.key, frame.value)?;
        }
    }
    Ok(())
}

fn write_parts(writer: &mut impl Write, header: &[u8], key: &[u8], value: &[u8]) -> io::Result<()> {
    let parts = [header, key, value];
    let mut part = 0_usize;
    let mut offset = 0_usize;
    while part < parts.len() {
        while part < parts.len() && offset == parts[part].len() {
            part += 1;
            offset = 0;
        }
        if part == parts.len() {
            break;
        }
        let empty = &[][..];
        let slices = match part {
            0 => [
                IoSlice::new(&parts[0][offset..]),
                IoSlice::new(parts[1]),
                IoSlice::new(parts[2]),
            ],
            1 => [
                IoSlice::new(&parts[1][offset..]),
                IoSlice::new(parts[2]),
                IoSlice::new(empty),
            ],
            2 => [
                IoSlice::new(&parts[2][offset..]),
                IoSlice::new(empty),
                IoSlice::new(empty),
            ],
            _ => unreachable!("part is bounded by the three frame buffers"),
        };
        let written = loop {
            match writer.write_vectored(&slices) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write complete frame",
                    ));
                }
                Ok(written) => break written,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        };

        let mut remaining = written;
        while remaining > 0 {
            let available = parts[part].len() - offset;
            if remaining < available {
                offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                part += 1;
                offset = 0;
                while part < parts.len() && parts[part].is_empty() {
                    part += 1;
                }
            }
        }
    }
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

    struct FragmentingWriter {
        bytes: Vec<u8>,
        maximum: usize,
        interrupt_once: bool,
    }

    struct ZeroWriter;

    impl<R: Read> Read for OneByteAtATime<R> {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let length = output.len().min(1);
            self.0.read(&mut output[..length])
        }
    }

    impl Write for FragmentingWriter {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            let written = self.maximum.min(input.len());
            self.bytes.extend_from_slice(&input[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, inputs: &[IoSlice<'_>]) -> io::Result<usize> {
            if self.interrupt_once {
                self.interrupt_once = false;
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let mut remaining = self.maximum;
            let mut written = 0;
            for input in inputs {
                let take = remaining.min(input.len());
                self.bytes.extend_from_slice(&input[..take]);
                remaining -= take;
                written += take;
                if remaining == 0 {
                    break;
                }
            }
            Ok(written)
        }
    }

    impl Write for ZeroWriter {
        fn write(&mut self, _input: &[u8]) -> io::Result<usize> {
            Ok(0)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, _inputs: &[IoSlice<'_>]) -> io::Result<usize> {
            Ok(0)
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
    fn borrowed_reader_reuses_capacity_across_frames() {
        let first = Frame::new(Opcode::Get, b"large".to_vec(), vec![7; 4096]);
        let second = Frame::new(Opcode::Set, b"small".to_vec(), Vec::new());
        let mut bytes = encode(first.as_ref()).unwrap();
        bytes.extend_from_slice(&encode(second.as_ref()).unwrap());
        let mut reader = OneByteAtATime(Cursor::new(bytes));
        let mut buffer = FrameBuffer::new();
        {
            let decoded = buffer.read(&mut reader).unwrap().expect("first");
            assert_eq!(decoded, first.as_ref());
        }
        let capacity = buffer.bytes.capacity();
        {
            let decoded = buffer.read(&mut reader).unwrap().expect("second");
            assert_eq!(decoded, second.as_ref());
        }
        assert_eq!(buffer.bytes.capacity(), capacity);
        assert!(buffer.read(&mut reader).unwrap().is_none());
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

    #[test]
    fn vectored_v1_output_matches_contiguous_encoding_under_partial_writes() {
        let frame = Frame::new(
            Opcode::Set,
            b"key".to_vec(),
            vec![7; VECTORED_BODY_THRESHOLD],
        );
        let expected = encode(frame.as_ref()).unwrap();
        let mut writer = FragmentingWriter {
            bytes: Vec::new(),
            maximum: 1_023,
            interrupt_once: true,
        };
        write_frame(&mut writer, &frame).unwrap();
        assert_eq!(writer.bytes, expected);
    }

    #[test]
    fn vectored_v2_output_matches_contiguous_encoding() {
        let frame = punah_protocol::v2::Frame {
            opcode: punah_protocol::v2::Opcode::ArtifactGet,
            status: punah_protocol::v2::Status::Hit,
            flags: 1,
            request_id: 42,
            ttl_seconds: 9,
            key: b"artifact".to_vec(),
            value: vec![5; VECTORED_BODY_THRESHOLD],
        };
        let expected = punah_protocol::v2::encode(frame.as_ref()).unwrap();
        let mut writer = FragmentingWriter {
            bytes: Vec::new(),
            maximum: 997,
            interrupt_once: false,
        };
        write_versioned_frame(&mut writer, &VersionedFrame::V2(frame)).unwrap();
        assert_eq!(writer.bytes, expected);
    }

    #[test]
    fn vectored_output_reports_write_zero() {
        let frame = Frame::new(Opcode::Ping, Vec::new(), Vec::new());
        assert!(matches!(
            write_frame(&mut ZeroWriter, &frame),
            Err(ConnectionError::Io(ref error)) if error.kind() == io::ErrorKind::WriteZero
        ));
    }

    #[test]
    fn invalid_frame_is_rejected_before_writing() {
        let frame = Frame::new(
            Opcode::Get,
            vec![0; punah_protocol::MAX_KEY_LEN + 1],
            Vec::new(),
        );
        let mut writer = FragmentingWriter {
            bytes: Vec::new(),
            maximum: usize::MAX,
            interrupt_once: false,
        };
        assert!(matches!(
            write_frame(&mut writer, &frame),
            Err(ConnectionError::Encode(_))
        ));
        assert!(writer.bytes.is_empty());
    }
}
