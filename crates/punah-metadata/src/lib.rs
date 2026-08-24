#![forbid(unsafe_code)]

mod checksum;
mod journal;
mod manifest;
mod publication;
mod superblock;

#[cfg(test)]
mod golden_tests;

pub use journal::{
    JOURNAL_COMMIT_MARKER_OFFSET, JournalEntry, JournalOperation, SegmentRef, commit_journal_entry,
    decode_journal_entry, encode_journal_entry, encode_uncommitted_journal_entry,
};
pub use manifest::{
    MANIFEST_COMMIT_MARKER_OFFSET, ManifestCheckpoint, SegmentDescriptor, commit_manifest,
    decode_manifest, encode_manifest, encode_uncommitted_manifest,
};
pub use publication::{
    FileMetadataIo, MetadataIo, MetadataPublisher, PublicationError, RecoveredRoot,
    SUPERBLOCK_REGION_SIZE,
};
pub use superblock::{
    MetadataPointer, SUPERBLOCK_COMMIT_MARKER_OFFSET, SUPERBLOCK_SIZE, Superblock,
    commit_superblock, decode_superblock, encode_superblock, encode_uncommitted_superblock,
    newest_valid_superblock,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EncodeError {
    TooManySegments(usize),
    LengthOverflow,
    InvalidOperationShape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    Incomplete,
    BadMagic,
    UnsupportedVersion(u16),
    InvalidHeaderLength(u16),
    InvalidTotalLength(u32),
    InvalidCommitMarker,
    ChecksumMismatch,
    UnknownOperation(u8),
    InvalidOperationShape,
    LengthOverflow,
}
