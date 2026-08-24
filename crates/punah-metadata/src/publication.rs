use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::checksum::crc32c;
use crate::{
    DecodeError, EncodeError, JOURNAL_COMMIT_MARKER_OFFSET, JournalEntry,
    MANIFEST_COMMIT_MARKER_OFFSET, ManifestCheckpoint, MetadataPointer,
    SUPERBLOCK_COMMIT_MARKER_OFFSET, SUPERBLOCK_SIZE, Superblock, commit_journal_entry,
    commit_manifest, commit_superblock, decode_journal_entry, decode_manifest, decode_superblock,
    encode_uncommitted_journal_entry, encode_uncommitted_manifest, encode_uncommitted_superblock,
};

pub const SUPERBLOCK_REGION_SIZE: u64 = (SUPERBLOCK_SIZE as u64) * 2;
const MAX_METADATA_OBJECT_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum PublicationError {
    Io(io::Error),
    Encode(EncodeError),
    Decode(DecodeError),
    /// Both complete root slots are in the canonical never-published state.
    Uninitialized,
    /// Root bytes exist, but neither root and all of its references validate.
    NoRecoverableRoot,
    InvalidPointer,
    GenerationMismatch,
    GenerationOverflow,
}

impl From<io::Error> for PublicationError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<EncodeError> for PublicationError {
    fn from(value: EncodeError) -> Self {
        Self::Encode(value)
    }
}
impl From<DecodeError> for PublicationError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

/// Minimal positional durable-I/O contract used by metadata publication.
pub trait MetadataIo {
    /// Returns the current addressable length.
    ///
    /// # Errors
    /// Returns an I/O error when device metadata cannot be read.
    fn size(&mut self) -> io::Result<u64>;
    /// Reads exactly `bytes.len()` bytes from `offset`.
    ///
    /// # Errors
    /// Returns an I/O error for a short or failed read.
    fn read_exact_at(&mut self, offset: u64, bytes: &mut [u8]) -> io::Result<()>;
    /// Writes all bytes beginning at `offset`.
    ///
    /// # Errors
    /// Returns an I/O error for a failed write.
    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()>;
    /// Makes all preceding writes durable before a later publication write.
    ///
    /// # Errors
    /// Returns an I/O error when durability cannot be established.
    fn flush(&mut self) -> io::Result<()>;
}

pub struct FileMetadataIo {
    file: File,
}

impl FileMetadataIo {
    /// Opens or creates a metadata file and durably reserves its two root slots.
    ///
    /// # Errors
    /// Returns an I/O error when the file cannot be opened or initialized.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if file.metadata()?.len() < SUPERBLOCK_REGION_SIZE {
            file.set_len(SUPERBLOCK_REGION_SIZE)?;
            file.sync_data()?;
        }
        Ok(Self { file })
    }

    /// Opens an existing metadata file read-only without extending or otherwise
    /// mutating it. Recovery/inspection works; publication through the returned
    /// handle fails with the operating system's read-only-file error.
    ///
    /// # Errors
    /// Returns an I/O error when the existing file cannot be opened.
    pub fn open_existing(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self { file })
    }
}

impl MetadataIo for FileMetadataIo {
    fn size(&mut self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
    fn read_exact_at(&mut self, offset: u64, bytes: &mut [u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(bytes)
    }
    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredRoot {
    pub slot: usize,
    pub superblock: Superblock,
    pub checkpoint: ManifestCheckpoint,
    pub journal_tail: Option<JournalEntry>,
}

pub struct MetadataPublisher<I> {
    io: I,
}

impl<I: MetadataIo> MetadataPublisher<I> {
    #[must_use]
    pub fn new(io: I) -> Self {
        Self { io }
    }
    #[must_use]
    pub fn into_inner(self) -> I {
        self.io
    }

    /// Recovers the newest root whose complete referenced objects validate.
    ///
    /// # Errors
    /// Returns `Uninitialized` only when both complete root slots are zero, and
    /// `NoRecoverableRoot` when nonzero metadata exists but neither root and its
    /// references validate.
    pub fn recover(&mut self) -> Result<RecoveredRoot, PublicationError> {
        let mut candidates = Vec::new();
        let mut all_slots_zero = true;
        for slot in 0..2 {
            let mut bytes = [0; SUPERBLOCK_SIZE];
            match self.io.read_exact_at(slot_offset(slot), &mut bytes) {
                Ok(()) => {
                    all_slots_zero &= bytes.iter().all(|byte| *byte == 0);
                    if let Ok(superblock) = decode_superblock(&bytes) {
                        candidates.push((slot, superblock));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                    all_slots_zero = false;
                }
                Err(error) => return Err(PublicationError::Io(error)),
            }
        }
        candidates.sort_by(|left, right| {
            right
                .1
                .generation
                .cmp(&left.1.generation)
                .then(left.0.cmp(&right.0))
        });
        for (slot, superblock) in candidates {
            match self.validate_root(slot, superblock) {
                Ok(root) => return Ok(root),
                Err(PublicationError::Io(error))
                    if error.kind() != io::ErrorKind::UnexpectedEof =>
                {
                    return Err(PublicationError::Io(error));
                }
                Err(_) => {}
            }
        }
        if all_slots_zero {
            Err(PublicationError::Uninitialized)
        } else {
            Err(PublicationError::NoRecoverableRoot)
        }
    }

    /// Appends and commits an immutable journal object without publishing a root.
    ///
    /// # Errors
    /// Returns an encoding or I/O error if the object cannot be made durable.
    pub fn append_journal(
        &mut self,
        entry: &JournalEntry,
    ) -> Result<MetadataPointer, PublicationError> {
        let bytes = encode_uncommitted_journal_entry(entry)?;
        self.append_committed(bytes, JOURNAL_COMMIT_MARKER_OFFSET, commit_journal_entry)
    }

    /// Publishes a complete checkpoint, alternating the root slot.
    ///
    /// # Errors
    /// Returns an error for invalid generations, references, encoding, or durable I/O.
    pub fn publish_checkpoint(
        &mut self,
        checkpoint: &ManifestCheckpoint,
        journal_tail: Option<MetadataPointer>,
    ) -> Result<RecoveredRoot, PublicationError> {
        let previous = match self.recover() {
            Ok(root) => Some(root),
            Err(PublicationError::Uninitialized) => None,
            Err(error) => return Err(error),
        };
        if let Some(root) = &previous {
            let expected = root
                .superblock
                .generation
                .checked_add(1)
                .ok_or(PublicationError::GenerationOverflow)?;
            if checkpoint.generation != expected {
                return Err(PublicationError::GenerationMismatch);
            }
        }
        if let Some(pointer) = journal_tail {
            self.read_journal(pointer)?;
        }
        let manifest = encode_uncommitted_manifest(checkpoint)?;
        let checkpoint_pointer =
            self.append_committed(manifest, MANIFEST_COMMIT_MARKER_OFFSET, commit_manifest)?;
        let slot = previous.as_ref().map_or(0, |root| 1 - root.slot);
        let superblock = Superblock {
            generation: checkpoint.generation,
            checkpoint: checkpoint_pointer,
            journal_tail: journal_tail.unwrap_or_default(),
        };
        let mut bytes = encode_uncommitted_superblock(&superblock);
        self.io.write_all_at(slot_offset(slot), &bytes)?;
        self.io.flush()?;
        commit_superblock(&mut bytes)?;
        self.io.write_all_at(
            slot_offset(slot) + SUPERBLOCK_COMMIT_MARKER_OFFSET as u64,
            &bytes[SUPERBLOCK_COMMIT_MARKER_OFFSET..SUPERBLOCK_COMMIT_MARKER_OFFSET + 4],
        )?;
        self.io.flush()?;
        self.validate_root(slot, superblock)
    }

    fn append_committed(
        &mut self,
        mut bytes: Vec<u8>,
        marker_offset: usize,
        commit: fn(&mut [u8]) -> Result<(), DecodeError>,
    ) -> Result<MetadataPointer, PublicationError> {
        let offset = align_8(self.io.size()?.max(SUPERBLOCK_REGION_SIZE))?;
        self.io.write_all_at(offset, &bytes)?;
        self.io.flush()?;
        commit(&mut bytes)?;
        self.io.write_all_at(
            offset + marker_offset as u64,
            &bytes[marker_offset..marker_offset + 4],
        )?;
        self.io.flush()?;
        Ok(MetadataPointer {
            offset,
            length: bytes.len() as u64,
            checksum: crc32c(&[&bytes]),
        })
    }

    fn validate_root(
        &mut self,
        slot: usize,
        superblock: Superblock,
    ) -> Result<RecoveredRoot, PublicationError> {
        let checkpoint = self.read_manifest(superblock.checkpoint)?;
        if checkpoint.generation != superblock.generation {
            return Err(PublicationError::GenerationMismatch);
        }
        let journal_tail = if is_zero_pointer(superblock.journal_tail) {
            None
        } else {
            let entry = self.read_journal(superblock.journal_tail)?;
            if entry.generation != superblock.generation {
                return Err(PublicationError::GenerationMismatch);
            }
            Some(entry)
        };
        Ok(RecoveredRoot {
            slot,
            superblock,
            checkpoint,
            journal_tail,
        })
    }
    fn read_manifest(
        &mut self,
        pointer: MetadataPointer,
    ) -> Result<ManifestCheckpoint, PublicationError> {
        Ok(decode_manifest(&self.read_object(pointer)?)?)
    }
    fn read_journal(&mut self, pointer: MetadataPointer) -> Result<JournalEntry, PublicationError> {
        let bytes = self.read_object(pointer)?;
        let (entry, consumed) = decode_journal_entry(&bytes)?;
        if consumed != bytes.len() {
            return Err(PublicationError::InvalidPointer);
        }
        Ok(entry)
    }
    fn read_object(&mut self, pointer: MetadataPointer) -> Result<Vec<u8>, PublicationError> {
        if pointer.offset < SUPERBLOCK_REGION_SIZE
            || pointer.length == 0
            || pointer.length > MAX_METADATA_OBJECT_SIZE
        {
            return Err(PublicationError::InvalidPointer);
        }
        let length =
            usize::try_from(pointer.length).map_err(|_| PublicationError::InvalidPointer)?;
        let mut bytes = vec![0; length];
        self.io.read_exact_at(pointer.offset, &mut bytes)?;
        if crc32c(&[&bytes]) != pointer.checksum {
            return Err(PublicationError::InvalidPointer);
        }
        Ok(bytes)
    }
}

fn is_zero_pointer(pointer: MetadataPointer) -> bool {
    pointer == MetadataPointer::default()
}
fn slot_offset(slot: usize) -> u64 {
    slot as u64 * SUPERBLOCK_SIZE as u64
}
fn align_8(value: u64) -> Result<u64, PublicationError> {
    value
        .checked_add(7)
        .map(|v| v / 8 * 8)
        .ok_or(PublicationError::InvalidPointer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SegmentDescriptor;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Clone)]
    struct MemoryIo {
        volatile: Vec<u8>,
        durable: Vec<u8>,
        fail_at: Option<usize>,
        calls: usize,
    }
    impl MemoryIo {
        fn new() -> Self {
            Self {
                volatile: vec![0; usize::try_from(SUPERBLOCK_REGION_SIZE).expect("small constant")],
                durable: vec![0; usize::try_from(SUPERBLOCK_REGION_SIZE).expect("small constant")],
                fail_at: None,
                calls: 0,
            }
        }
        fn crash(mut self) -> Self {
            self.volatile.clone_from(&self.durable);
            self.fail_at = None;
            self.calls = 0;
            self
        }
        fn fail_after(mut self, call: usize) -> Self {
            self.fail_at = Some(call);
            self.calls = 0;
            self
        }
        fn step(&mut self) -> io::Result<()> {
            if self.fail_at == Some(self.calls) {
                return Err(io::Error::other("injected crash"));
            }
            self.calls += 1;
            Ok(())
        }
    }
    impl MetadataIo for MemoryIo {
        fn size(&mut self) -> io::Result<u64> {
            Ok(self.volatile.len() as u64)
        }
        fn read_exact_at(&mut self, offset: u64, bytes: &mut [u8]) -> io::Result<()> {
            let start = usize::try_from(offset).map_err(|_| io::Error::other("offset overflow"))?;
            let end = start
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("overflow"))?;
            let source = self
                .volatile
                .get(start..end)
                .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
            bytes.copy_from_slice(source);
            Ok(())
        }
        fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
            self.step()?;
            let start = usize::try_from(offset).map_err(|_| io::Error::other("offset overflow"))?;
            let end = start + bytes.len();
            self.volatile.resize(self.volatile.len().max(end), 0);
            self.volatile[start..end].copy_from_slice(bytes);
            Ok(())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.step()?;
            self.durable.clone_from(&self.volatile);
            Ok(())
        }
    }
    fn checkpoint(generation: u64) -> ManifestCheckpoint {
        ManifestCheckpoint {
            generation,
            active_segments: vec![SegmentDescriptor {
                id: u32::try_from(generation).expect("test generation"),
                generation: 1,
                start_offset: 65_536,
                logical_length: 100,
                sealed_sequence: generation,
            }],
        }
    }

    #[test]
    fn blank_roots_are_distinct_from_corrupt_roots() {
        let mut blank = MetadataPublisher::new(MemoryIo::new());
        assert!(matches!(
            blank.recover(),
            Err(PublicationError::Uninitialized)
        ));

        let mut io = MemoryIo::new();
        io.volatile[0] = 1;
        io.durable[0] = 1;
        let mut corrupt = MetadataPublisher::new(io);
        assert!(matches!(
            corrupt.recover(),
            Err(PublicationError::NoRecoverableRoot)
        ));
    }

    #[test]
    fn corrupt_roots_cannot_be_overwritten_as_a_fresh_publication() {
        let mut publisher = MetadataPublisher::new(MemoryIo::new());
        publisher
            .publish_checkpoint(&checkpoint(1), None)
            .expect("one");
        publisher
            .publish_checkpoint(&checkpoint(2), None)
            .expect("two");
        let mut io = publisher.into_inner();
        for slot in 0..2 {
            let offset = slot * SUPERBLOCK_SIZE;
            io.volatile[offset] ^= 1;
            io.durable[offset] ^= 1;
        }
        let before = io.volatile.clone();
        let mut corrupt = MetadataPublisher::new(io);
        assert!(matches!(
            corrupt.recover(),
            Err(PublicationError::NoRecoverableRoot)
        ));
        assert!(matches!(
            corrupt.publish_checkpoint(&checkpoint(3), None),
            Err(PublicationError::NoRecoverableRoot)
        ));
        assert_eq!(corrupt.into_inner().volatile, before);
    }

    #[test]
    fn every_publication_boundary_recovers_old_or_new_root() {
        let mut initial = MetadataPublisher::new(MemoryIo::new());
        initial
            .publish_checkpoint(&checkpoint(1), None)
            .expect("initial");
        let stable = initial.into_inner().crash();
        for failure in 0..8 {
            let mut publisher = MetadataPublisher::new(stable.clone().fail_after(failure));
            let _ = publisher.publish_checkpoint(&checkpoint(2), None);
            let mut recovered = MetadataPublisher::new(publisher.into_inner().crash());
            let generation = recovered
                .recover()
                .expect("old or new root")
                .superblock
                .generation;
            assert!(generation == 1 || generation == 2, "failure {failure}");
        }
    }

    #[test]
    fn newer_root_with_corrupt_checkpoint_falls_back() {
        let mut publisher = MetadataPublisher::new(MemoryIo::new());
        publisher
            .publish_checkpoint(&checkpoint(1), None)
            .expect("one");
        let newest = publisher
            .publish_checkpoint(&checkpoint(2), None)
            .expect("two");
        let mut io = publisher.into_inner();
        let offset = usize::try_from(newest.superblock.checkpoint.offset).expect("test offset");
        io.volatile[offset] ^= 1;
        io.durable.clone_from(&io.volatile);
        assert_eq!(
            MetadataPublisher::new(io)
                .recover()
                .expect("fallback")
                .superblock
                .generation,
            1
        );
    }

    #[test]
    fn root_rejects_a_journal_tail_from_another_generation() {
        let mut publisher = MetadataPublisher::new(MemoryIo::new());
        let tail = publisher
            .append_journal(&JournalEntry {
                generation: 2,
                operation: crate::JournalOperation::Retire {
                    segments: vec![crate::SegmentRef {
                        id: 9,
                        generation: 1,
                    }],
                },
            })
            .expect("journal");
        assert!(matches!(
            publisher.publish_checkpoint(&checkpoint(1), Some(tail)),
            Err(PublicationError::GenerationMismatch)
        ));
    }

    #[test]
    fn filesystem_reopen_recovers_checkpoint_and_journal() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("punah-metadata-{}-{nonce}", std::process::id()));
        let journal = JournalEntry {
            generation: 1,
            operation: crate::JournalOperation::Retire {
                segments: vec![crate::SegmentRef {
                    id: 9,
                    generation: 1,
                }],
            },
        };
        {
            let mut publisher = MetadataPublisher::new(FileMetadataIo::open(&path).expect("open"));
            let tail = publisher.append_journal(&journal).expect("journal");
            publisher
                .publish_checkpoint(&checkpoint(1), Some(tail))
                .expect("publish");
        }
        let mut reopened = MetadataPublisher::new(FileMetadataIo::open(&path).expect("reopen"));
        let root = reopened.recover().expect("recover");
        assert_eq!(root.checkpoint, checkpoint(1));
        assert_eq!(root.journal_tail, Some(journal));
        std::fs::remove_file(path).expect("cleanup");
    }
}
