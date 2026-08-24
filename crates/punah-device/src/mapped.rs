//! Bounds-checked, owned reads from mapped segment files.
//!
//! This is the only module in `punah-device` allowed to establish an OS memory
//! mapping. It never creates or returns a Rust reference into file-backed
//! memory, because the active file may change through its writer handle.

#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::{MmapOptions, MmapRaw};

/// A read-only mapping whose address remains valid for its complete lifetime.
pub struct MappedSegment {
    mapping: MmapRaw,
}

/// A location cannot be represented by or contained within a mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappedReadError {
    OffsetOverflow,
    OutOfBounds,
}

impl MappedSegment {
    /// Maps the current complete length of `path` as read-only memory.
    ///
    /// # Errors
    ///
    /// Returns an OS error when the file cannot be opened or mapped, including
    /// when it is empty.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        // MmapRaw deliberately does not implement Deref and therefore cannot
        // create a long-lived Rust reference while the active file changes.
        let mapping = MmapOptions::new().map_raw_read_only(&file)?;
        Ok(Self { mapping })
    }

    /// Copies a checked mapped range into a new owned buffer.
    ///
    /// # Errors
    ///
    /// Rejects locations that overflow `usize` or extend beyond the file.
    pub fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>, MappedReadError> {
        let length = usize::try_from(length).map_err(|_| MappedReadError::OffsetOverflow)?;
        self.checked_start(offset, length)?;
        let mut output = vec![0; length];
        self.read_into(offset, &mut output)?;
        Ok(output)
    }

    /// Copies a checked mapped range into caller-owned memory.
    ///
    /// # Errors
    ///
    /// Rejects an offset that overflows or places any output byte beyond the
    /// original mapped length.
    pub fn read_into(&self, offset: u64, output: &mut [u8]) -> Result<(), MappedReadError> {
        let start = self.checked_start(offset, output.len())?;
        // SAFETY: the source range is checked against the mapping established
        // at construction, and `output` is valid for exactly `output.len()`
        // writable bytes. The mapping pointer is never exposed, so safe callers
        // cannot make the regions overlap. Crucially, this performs a raw copy
        // and never constructs `&[u8]` over file-backed memory that an active
        // writer may change. A racing write can produce a torn owned snapshot,
        // which record checksums reject; Punah itself serializes shard reads and
        // writes. Installed segment files are never truncated.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.mapping.as_ptr().add(start),
                output.as_mut_ptr(),
                output.len(),
            );
        }
        Ok(())
    }

    fn checked_start(&self, offset: u64, length: usize) -> Result<usize, MappedReadError> {
        let start = usize::try_from(offset).map_err(|_| MappedReadError::OffsetOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(MappedReadError::OffsetOverflow)?;
        if end > self.mapping.len() {
            return Err(MappedReadError::OutOfBounds);
        }
        Ok(start)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.mapping.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mapping.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn mapping_observes_writes_and_checks_every_range() {
        let path = std::env::temp_dir().join(format!(
            "punah-mapped-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = File::create(&path).expect("create");
        file.set_len(32).expect("size");
        let mapped = MappedSegment::open(&path).expect("map");
        file.seek(SeekFrom::Start(7)).expect("seek");
        file.write_all(b"punah").expect("write");
        file.sync_data().expect("sync");
        assert_eq!(mapped.read(7, 5), Ok(b"punah".to_vec()));
        assert!(matches!(
            mapped.read(0, u64::MAX),
            Err(MappedReadError::OutOfBounds | MappedReadError::OffsetOverflow)
        ));
        assert_eq!(mapped.read(31, 2), Err(MappedReadError::OutOfBounds));
        assert_eq!(
            mapped.read(u64::MAX, 2),
            Err(MappedReadError::OffsetOverflow)
        );
        drop(mapped);
        drop(file);
        std::fs::remove_file(path).expect("remove");
    }
}
