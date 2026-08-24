#![forbid(unsafe_code)]

//! Explicit, fail-closed inspection and recovery operations for Punah stores.

use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use punah_metadata::{
    FileMetadataIo, MetadataPublisher, PublicationError, RecoveredRoot, SegmentDescriptor,
};
use punah_store::{Store, StoreConfig, StoreError};

/// Classification of the two durable metadata root slots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataStatus {
    Missing,
    Uninitialized,
    Healthy {
        slot: usize,
        generation: u64,
        segments: Vec<SegmentDescriptor>,
        has_journal_tail: bool,
    },
    Corrupt,
}

/// One deterministically named segment found in the store directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentFile {
    pub generation: u32,
    pub path: PathBuf,
    pub physical_length: u64,
}

/// Read-only classification of metadata and segment membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Inspection {
    pub directory: PathBuf,
    pub metadata: MetadataStatus,
    pub referenced: Vec<SegmentFile>,
    /// Files not referenced by a healthy authoritative root.
    pub orphans: Vec<SegmentFile>,
    /// Files that cannot be classified without an explicit import or repair.
    pub unmanaged: Vec<SegmentFile>,
    /// Manifest references whose segment filename is absent.
    pub missing_references: Vec<u32>,
}

#[derive(Debug)]
pub enum AdminError {
    Io(io::Error),
    Metadata(PublicationError),
    Store(StoreError),
    State(&'static str),
    ClockBeforeEpoch,
}

impl fmt::Display for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for AdminError {}

impl From<io::Error> for AdminError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<PublicationError> for AdminError {
    fn from(value: PublicationError) -> Self {
        Self::Metadata(value)
    }
}

impl From<StoreError> for AdminError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

/// Result of explicitly publishing authority over unmanaged segments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImportReport {
    pub versions: usize,
    pub sealed_segments: usize,
}

/// Recoverable move of files that are not referenced by a healthy root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineReport {
    pub quarantine_directory: Option<PathBuf>,
    pub moved: Vec<PathBuf>,
}

/// Reads root and directory state without creating, extending, or repairing it.
///
/// # Errors
/// Returns an I/O error when directory entries or existing metadata cannot be read.
pub fn inspect(directory: impl AsRef<Path>) -> Result<Inspection, AdminError> {
    let directory = directory.as_ref().to_path_buf();
    let segments = segment_files(&directory)?;
    let metadata_path = directory.join("metadata.punah");
    if !metadata_path.exists() {
        return Ok(Inspection {
            directory,
            metadata: MetadataStatus::Missing,
            referenced: Vec::new(),
            orphans: Vec::new(),
            unmanaged: segments,
            missing_references: Vec::new(),
        });
    }
    let mut publisher = MetadataPublisher::new(FileMetadataIo::open_existing(&metadata_path)?);
    let root = match publisher.recover() {
        Ok(root) => root,
        Err(PublicationError::Uninitialized) => {
            return Ok(Inspection {
                directory,
                metadata: MetadataStatus::Uninitialized,
                referenced: Vec::new(),
                orphans: Vec::new(),
                unmanaged: segments,
                missing_references: Vec::new(),
            });
        }
        Err(PublicationError::NoRecoverableRoot) => {
            return Ok(Inspection {
                directory,
                metadata: MetadataStatus::Corrupt,
                referenced: Vec::new(),
                orphans: Vec::new(),
                unmanaged: segments,
                missing_references: Vec::new(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    Ok(classify_healthy(directory, root, segments))
}

/// Validates unmanaged segments and publishes their first authoritative root.
///
/// # Errors
/// Returns if the directory is empty, already initialized, corrupt, or contains
/// an invalid committed segment prefix.
pub fn import_unmanaged(config: StoreConfig) -> Result<ImportReport, AdminError> {
    let store = Store::import_unmanaged_segments(config)?;
    Ok(ImportReport {
        versions: store.version_snapshots().len(),
        sealed_segments: store.sealed_segments().len(),
    })
}

/// Moves healthy-root orphans into a timestamped directory on the same filesystem.
/// No file is deleted. Corrupt and uninitialized stores are refused because their
/// segment membership is not authoritative.
///
/// # Errors
/// Returns for non-authoritative metadata or a failed filesystem operation.
pub fn quarantine_orphans(directory: impl AsRef<Path>) -> Result<QuarantineReport, AdminError> {
    let directory = directory.as_ref();
    let inspection = inspect(directory)?;
    if !matches!(inspection.metadata, MetadataStatus::Healthy { .. }) {
        return Err(AdminError::State(
            "orphan quarantine requires a healthy authoritative root",
        ));
    }
    if inspection.orphans.is_empty() {
        return Ok(QuarantineReport {
            quarantine_directory: None,
            moved: Vec::new(),
        });
    }
    let quarantine = unique_quarantine_directory(directory, "orphans")?;
    fs::create_dir_all(&quarantine)?;
    sync_directory(&directory.join("quarantine"))?;
    let mut moved = Vec::with_capacity(inspection.orphans.len());
    for orphan in inspection.orphans {
        let file_name = orphan
            .path
            .file_name()
            .ok_or(AdminError::State("segment has no filename"))?;
        let target = quarantine.join(file_name);
        if let Err(error) = fs::rename(&orphan.path, &target) {
            rollback_moves(&moved, directory);
            return Err(error.into());
        }
        moved.push(target);
    }
    sync_directory(&quarantine)?;
    sync_directory(directory)?;
    Ok(QuarantineReport {
        quarantine_directory: Some(quarantine),
        moved,
    })
}

/// Explicitly repairs corrupt roots by quarantining the old metadata file and
/// importing the validated segment set. The old metadata is never deleted.
///
/// # Errors
/// Returns unless inspection classifies the roots as corrupt, or if quarantine,
/// validation, or new-root publication fails.
pub fn repair_corrupt_root(config: &StoreConfig) -> Result<ImportReport, AdminError> {
    let inspection = inspect(&config.directory)?;
    if inspection.metadata != MetadataStatus::Corrupt {
        return Err(AdminError::State("repair requires corrupt metadata roots"));
    }
    let quarantine = unique_quarantine_directory(&config.directory, "metadata")?;
    fs::create_dir_all(&quarantine)?;
    sync_directory(&config.directory.join("quarantine"))?;
    let metadata_path = config.directory.join("metadata.punah");
    let quarantined_metadata = quarantine.join("metadata.punah");
    fs::rename(&metadata_path, &quarantined_metadata)?;
    sync_directory(&quarantine)?;
    sync_directory(&config.directory)?;
    match import_unmanaged(config.clone()) {
        Ok(report) => Ok(report),
        Err(error) => {
            if !metadata_path.exists() {
                let _ = fs::rename(&quarantined_metadata, &metadata_path);
                let _ = sync_directory(&config.directory);
            }
            Err(error)
        }
    }
}

fn classify_healthy(
    directory: PathBuf,
    root: RecoveredRoot,
    segments: Vec<SegmentFile>,
) -> Inspection {
    let referenced_generations: HashSet<_> = root
        .checkpoint
        .active_segments
        .iter()
        .map(|descriptor| descriptor.generation)
        .collect();
    let present_generations: HashSet<_> =
        segments.iter().map(|segment| segment.generation).collect();
    let (referenced, orphans): (Vec<_>, Vec<_>) = segments
        .into_iter()
        .partition(|segment| referenced_generations.contains(&segment.generation));
    let mut missing_references: Vec<_> = referenced_generations
        .difference(&present_generations)
        .copied()
        .collect();
    missing_references.sort_unstable();
    Inspection {
        directory,
        metadata: MetadataStatus::Healthy {
            slot: root.slot,
            generation: root.superblock.generation,
            segments: root.checkpoint.active_segments,
            has_journal_tail: root.journal_tail.is_some(),
        },
        referenced,
        orphans,
        unmanaged: Vec::new(),
        missing_references,
    }
}

fn segment_files(directory: &Path) -> io::Result<Vec<SegmentFile>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let Some(generation) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("segment-"))
            .and_then(|name| name.strip_suffix(".punah"))
            .and_then(|generation| generation.parse().ok())
        else {
            continue;
        };
        files.push(SegmentFile {
            generation,
            physical_length: entry.metadata()?.len(),
            path,
        });
    }
    files.sort_unstable_by_key(|file| file.generation);
    Ok(files)
}

fn unique_quarantine_directory(directory: &Path, kind: &str) -> Result<PathBuf, AdminError> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AdminError::ClockBeforeEpoch)?
        .as_nanos();
    Ok(directory
        .join("quarantine")
        .join(format!("{kind}-{timestamp}-{}", std::process::id())))
}

fn rollback_moves(moved: &[PathBuf], directory: &Path) {
    for target in moved.iter().rev() {
        if let Some(name) = target.file_name() {
            let _ = fs::rename(target, directory.join(name));
        }
    }
    let _ = sync_directory(directory);
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    fs::File::open(directory)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    use punah_types::{Durability, Namespace};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "punah-admin-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("temp directory");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn config(directory: &Path) -> StoreConfig {
        StoreConfig {
            directory: directory.to_path_buf(),
            namespace: Namespace::new("default").expect("namespace"),
            segment_capacity: 4096,
            durability: Durability::LocalDurable,
        }
    }

    fn populated(directory: &Path) -> StoreConfig {
        let config = config(directory);
        let mut store = Store::open(config.clone()).expect("open");
        store
            .set(b"key", b"value".to_vec(), None)
            .expect("populate");
        drop(store);
        config
    }

    #[test]
    fn inspection_and_quarantine_move_only_healthy_root_orphans() {
        let directory = TempDir::new();
        let config = populated(&directory.0);
        let original = directory.0.join("segment-0000000000.punah");
        let orphan = directory.0.join("segment-0000000999.punah");
        fs::copy(&original, &orphan).expect("create orphan");

        let before = inspect(&directory.0).expect("inspect");
        assert!(matches!(before.metadata, MetadataStatus::Healthy { .. }));
        assert_eq!(before.referenced.len(), 1);
        assert_eq!(before.orphans.len(), 1);
        assert!(before.unmanaged.is_empty());

        let report = quarantine_orphans(&directory.0).expect("quarantine");
        assert_eq!(report.moved.len(), 1);
        assert!(!orphan.exists());
        assert!(report.moved[0].exists());
        assert!(inspect(&directory.0).unwrap().orphans.is_empty());
        assert_eq!(
            Store::open(config).unwrap().get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn import_is_explicit_and_inspection_does_not_create_metadata() {
        let directory = TempDir::new();
        let config = populated(&directory.0);
        let metadata = directory.0.join("metadata.punah");
        fs::remove_file(&metadata).expect("remove metadata");

        let inspection = inspect(&directory.0).expect("inspect unmanaged");
        assert_eq!(inspection.metadata, MetadataStatus::Missing);
        assert_eq!(inspection.unmanaged.len(), 1);
        assert!(!metadata.exists());
        assert_eq!(import_unmanaged(config.clone()).unwrap().versions, 1);
        assert!(metadata.exists());
        assert_eq!(
            Store::open(config).unwrap().get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn corrupt_root_repair_quarantines_old_metadata_before_import() {
        let directory = TempDir::new();
        let config = populated(&directory.0);
        let metadata = directory.0.join("metadata.punah");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&metadata)
            .expect("open metadata");
        file.write_all(&vec![
            0xff;
            usize::try_from(punah_metadata::SUPERBLOCK_REGION_SIZE)
                .expect("root region")
        ])
        .expect("corrupt roots");
        file.sync_data().expect("persist corruption");
        drop(file);
        assert_eq!(
            inspect(&directory.0).unwrap().metadata,
            MetadataStatus::Corrupt
        );

        let report = repair_corrupt_root(&config).expect("explicit repair");
        assert_eq!(report.versions, 1);
        assert!(matches!(
            inspect(&directory.0).unwrap().metadata,
            MetadataStatus::Healthy { .. }
        ));
        let quarantine = directory.0.join("quarantine");
        assert!(
            fs::read_dir(quarantine)
                .expect("quarantine root")
                .any(|entry| entry.expect("entry").path().join("metadata.punah").exists())
        );
        assert_eq!(
            Store::open(config).unwrap().get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }
}
