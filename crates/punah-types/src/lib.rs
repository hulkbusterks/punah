#![forbid(unsafe_code)]

use std::time::{Duration, SystemTime};

pub type ContentHash = [u8; 32];
pub type OperationHash = [u8; 32];

/// Collision-resistant identity of one exact, canonical operation descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct OperationId([u8; 32]);

impl OperationId {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        use std::fmt::Write as _;

        let mut output = String::with_capacity(64);
        for byte in self.0 {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
        }
        output
    }
}

impl From<[u8; 32]> for OperationId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl From<OperationId> for [u8; 32] {
    fn from(value: OperationId) -> Self {
        value.0
    }
}

/// Collision-resistant identity for immutable artifact bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ArtifactId([u8; 32]);

impl ArtifactId {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        use std::fmt::Write as _;

        let mut output = String::with_capacity(64);
        for byte in self.0 {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
        }
        output
    }
}

impl From<[u8; 32]> for ArtifactId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl From<ArtifactId> for [u8; 32] {
    fn from(value: ArtifactId) -> Self {
        value.0
    }
}

/// Fast, non-unique value used only to choose an owning shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ShardHash(u64);

impl ShardHash {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SegmentId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RecordLocation {
    pub segment_id: SegmentId,
    pub generation: u32,
    pub offset: u64,
    pub stored_len: u32,
    pub logical_len: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    Memory,
    Buffered,
    LocalDurable,
    Replicated { acknowledgements: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryState {
    Live,
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Namespace(Vec<u8>);

impl Namespace {
    /// Creates a non-empty namespace.
    ///
    /// # Errors
    ///
    /// Returns [`NamespaceError::Empty`] when `value` is empty.
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, NamespaceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NamespaceError::Empty);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceError {
    Empty,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactEntry {
    pub namespace: Namespace,
    pub content_hash: ContentHash,
    pub operation_hash: OperationHash,
    pub sequence: u64,
    pub location: RecordLocation,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
    pub state: EntryState,
}

impl ArtifactEntry {
    #[must_use]
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expiry| expiry <= now)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentStats {
    pub total_bytes: u64,
    pub live_bytes: u64,
    pub dead_bytes: u64,
    pub expired_bytes: u64,
}

impl SegmentStats {
    #[must_use]
    pub fn reclaimable_bytes(self) -> u64 {
        self.dead_bytes.saturating_add(self.expired_bytes)
    }

    /// Returns the live-data ratio in basis points, where 10,000 is 100%.
    #[must_use]
    pub fn live_ratio_basis_points(self) -> u16 {
        if self.total_bytes == 0 {
            return 10_000;
        }
        let scaled = self.live_bytes.saturating_mul(10_000) / self.total_bytes;
        u16::try_from(scaled.min(10_000)).unwrap_or(10_000)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeasePolicy {
    pub duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_namespace_is_rejected() {
        assert_eq!(Namespace::new([]), Err(NamespaceError::Empty));
    }

    #[test]
    fn zero_sized_segment_has_full_live_ratio() {
        assert_eq!(SegmentStats::default().live_ratio_basis_points(), 10_000);
    }
}
