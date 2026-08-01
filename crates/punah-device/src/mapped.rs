//! Bounds-checked, read-only views of segment files.
//!
//! This is the only module in `punah-device` allowed to establish an OS memory
//! mapping. Callers receive slices whose lifetime is tied to the mapping and
//! cannot construct pointers or access bytes outside the mapped file.

#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::{Mmap, MmapOptions};

/// A read-only mapping whose address remains valid for its complete lifetime.
pub struct MappedSegment {
    mapping: Mmap,
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
        // SAFETY: the returned mapping owns its virtual-memory lifetime and is
        // never exposed mutably. Punah sizes segment files before mapping and
        // never truncates an installed segment. Writers use a separate file
        // handle and publish locations only after their commit marker is
        // complete. External file mutation remains outside this API's contract
        // and is detected by the record checksums at the decoding boundary.
        let mapping = unsafe { MmapOptions::new().map(&file)? };
        Ok(Self { mapping })
    }

    /// Returns a checked range tied to this mapping's lifetime.
    ///
    /// # Errors
    ///
    /// Rejects locations that overflow `usize` or extend beyond the file.
    pub fn read(&self, offset: u64, length: u32) -> Result<&[u8], MappedReadError> {
        let start = usize::try_from(offset).map_err(|_| MappedReadError::OffsetOverflow)?;
        let length = usize::try_from(length).map_err(|_| MappedReadError::OffsetOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(MappedReadError::OffsetOverflow)?;
        self.mapping
            .get(start..end)
            .ok_or(MappedReadError::OutOfBounds)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.mapping.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mapping.is_empty()
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
        assert_eq!(mapped.read(7, 5), Ok(&b"punah"[..]));
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
