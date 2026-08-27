use std::fs::OpenOptions;
use std::path::PathBuf;

use punah_metadata::{FileMetadataIo, JournalEntry, ManifestCheckpoint, MetadataPublisher};

use crate::StoreError;

/// Immutable filesystem work for one compaction authority transition.
///
/// The Store owner captures the active prefix and replacement membership
/// before moving this value to a helper thread. The active segment remains
/// append-only while this task runs, so syncing its file makes the captured
/// prefix durable without borrowing mutable Store state.
pub(super) struct PublicationTask {
    pub(super) active_path: PathBuf,
    pub(super) metadata_path: PathBuf,
    pub(super) checkpoint: ManifestCheckpoint,
    pub(super) journal: JournalEntry,
}

impl PublicationTask {
    pub(super) fn execute(self) -> Result<(), StoreError> {
        let active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.active_path)?;
        active.sync_all()?;

        let io = FileMetadataIo::open(&self.metadata_path)?;
        let mut publisher = MetadataPublisher::new(io);
        let pointer = publisher.append_journal(&self.journal)?;
        publisher.publish_checkpoint(&self.checkpoint, Some(pointer))?;
        Ok(())
    }
}
