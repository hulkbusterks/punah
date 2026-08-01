use std::collections::HashMap;
use std::time::SystemTime;

use punah_types::{ArtifactEntry, ContentHash, Namespace, RecordLocation, SegmentId, SegmentStats};

/// Stable identity of one logical object in the reference index.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexIdentity {
    pub namespace: Namespace,
    pub content_hash: ContentHash,
}

/// Identifies the bytes which established a tombstone watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TombstoneSource {
    /// A durable tombstone record at this location.
    Record(RecordLocation),
    /// An in-memory expiry derived from the formerly live record.
    ExpiredArtifact(RecordLocation),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TombstoneMetadata {
    pub sequence: u64,
    pub source: TombstoneSource,
}

/// Complete version state retained for recovery and compaction fencing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexVersion {
    Live(ArtifactEntry),
    Tombstone(TombstoneMetadata),
}

impl IndexVersion {
    #[must_use]
    pub fn sequence(&self) -> u64 {
        match self {
            Self::Live(entry) => entry.sequence,
            Self::Tombstone(tombstone) => tombstone.sequence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSnapshotEntry {
    pub identity: IndexIdentity,
    pub version: IndexVersion,
}

#[derive(Default)]
pub struct ReferenceIndex {
    versions: HashMap<IndexIdentity, IndexVersion>,
    segment_stats: HashMap<SegmentId, SegmentStats>,
}

impl ReferenceIndex {
    fn key(namespace: &Namespace, content_hash: &ContentHash) -> IndexIdentity {
        IndexIdentity {
            namespace: namespace.clone(),
            content_hash: *content_hash,
        }
    }

    #[must_use]
    pub fn get(
        &self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        now: SystemTime,
    ) -> Option<&ArtifactEntry> {
        match self.versions.get(&Self::key(namespace, content_hash)) {
            Some(IndexVersion::Live(entry)) if !entry.is_expired_at(now) => Some(entry),
            _ => None,
        }
    }

    pub fn insert(&mut self, entry: ArtifactEntry) -> Option<ArtifactEntry> {
        match self.versions.insert(
            Self::key(&entry.namespace, &entry.content_hash),
            IndexVersion::Live(entry),
        ) {
            Some(IndexVersion::Live(previous)) => Some(previous),
            _ => None,
        }
    }

    /// Inserts `entry` only when it is newer than every known version,
    /// including a tombstone watermark.
    pub fn insert_latest(&mut self, entry: ArtifactEntry) -> bool {
        let key = Self::key(&entry.namespace, &entry.content_hash);
        if self
            .versions
            .get(&key)
            .is_some_and(|version| version.sequence() >= entry.sequence)
        {
            return false;
        }
        self.versions.insert(key, IndexVersion::Live(entry));
        true
    }

    /// Applies a durable tombstone only when it is newer than known state.
    pub fn apply_tombstone(
        &mut self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        sequence: u64,
        location: RecordLocation,
    ) -> bool {
        self.apply_tombstone_from(
            namespace,
            content_hash,
            TombstoneMetadata {
                sequence,
                source: TombstoneSource::Record(location),
            },
        )
    }

    /// Applies an expiry watermark tied to the record from which it arose.
    pub fn apply_expiry(
        &mut self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        sequence: u64,
        expired_location: RecordLocation,
    ) -> bool {
        self.apply_tombstone_from(
            namespace,
            content_hash,
            TombstoneMetadata {
                sequence,
                source: TombstoneSource::ExpiredArtifact(expired_location),
            },
        )
    }

    fn apply_tombstone_from(
        &mut self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        tombstone: TombstoneMetadata,
    ) -> bool {
        let key = Self::key(namespace, content_hash);
        if self
            .versions
            .get(&key)
            .is_some_and(|current| current.sequence() >= tombstone.sequence)
        {
            return false;
        }
        self.versions
            .insert(key, IndexVersion::Tombstone(tombstone));
        true
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.versions
            .values()
            .filter(|version| matches!(version, IndexVersion::Live(_)))
            .count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn version(&self, namespace: &Namespace, content_hash: &ContentHash) -> Option<u64> {
        self.version_metadata(namespace, content_hash)
            .map(IndexVersion::sequence)
    }

    #[must_use]
    pub fn version_metadata(
        &self,
        namespace: &Namespace,
        content_hash: &ContentHash,
    ) -> Option<&IndexVersion> {
        self.versions.get(&Self::key(namespace, content_hash))
    }

    /// Returns an owned, immutable view suitable for planning compaction.
    #[must_use]
    pub fn snapshot(&self) -> Vec<IndexSnapshotEntry> {
        let mut snapshot: Vec<_> = self
            .versions
            .iter()
            .map(|(identity, version)| IndexSnapshotEntry {
                identity: identity.clone(),
                version: version.clone(),
            })
            .collect();
        snapshot.sort_unstable_by(|left, right| {
            left.identity
                .namespace
                .as_bytes()
                .cmp(right.identity.namespace.as_bytes())
                .then_with(|| left.identity.content_hash.cmp(&right.identity.content_hash))
        });
        snapshot
    }

    /// Redirects a live entry only if both its version and exact physical
    /// location still match the record copied by compaction.
    pub fn redirect_if_current(
        &mut self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        sequence: u64,
        old: RecordLocation,
        new: RecordLocation,
    ) -> bool {
        let Some(IndexVersion::Live(entry)) =
            self.versions.get_mut(&Self::key(namespace, content_hash))
        else {
            return false;
        };
        if entry.sequence != sequence || entry.location != old {
            return false;
        }
        entry.location = new;
        true
    }

    #[must_use]
    pub fn segment_stats(&self, segment: SegmentId) -> SegmentStats {
        self.segment_stats
            .get(&segment)
            .copied()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
