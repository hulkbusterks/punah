use std::path::PathBuf;

use crate::{ConfigField, ValidationError, ValidationKind};

/// Durable append-only storage layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageConfig {
    /// Directory containing segment files.
    pub directory: PathBuf,
    /// Maximum byte length of one segment.
    pub segment_bytes: u64,
    /// Record alignment required by the selected storage backend.
    pub alignment_bytes: u64,
    /// Acknowledgement guarantee for the portable server path.
    pub durability: StorageDurability,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StorageDurability {
    Buffered,
    #[default]
    LocalDurable,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./punah-data"),
            segment_bytes: 1 << 30,
            alignment_bytes: 8,
            durability: StorageDurability::LocalDurable,
        }
    }
}

impl StorageConfig {
    pub(crate) fn validate(&self, errors: &mut Vec<ValidationError>) {
        if self.directory.as_os_str().is_empty() {
            errors.push(ValidationError::new(
                ConfigField::StorageDirectory,
                ValidationKind::Empty,
            ));
        }
        if self.segment_bytes == 0 {
            errors.push(ValidationError::new(
                ConfigField::SegmentBytes,
                ValidationKind::Zero,
            ));
        }
        if self.alignment_bytes == 0 {
            errors.push(ValidationError::new(
                ConfigField::AlignmentBytes,
                ValidationKind::Zero,
            ));
        } else if !self.alignment_bytes.is_power_of_two() {
            errors.push(ValidationError::new(
                ConfigField::AlignmentBytes,
                ValidationKind::NotPowerOfTwo,
            ));
        }
    }
}
