use std::collections::VecDeque;
use std::time::SystemTime;

use punah_identity::shard_hash_parts;
use punah_types::{ArtifactEntry, ContentHash, Namespace, RecordLocation};

use crate::ReferenceIndex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardedIndexError {
    ShardCountNotPowerOfTwo,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExpiryReport {
    /// Expiry records inspected. This is bounded by the caller's budget.
    pub examined: usize,
    /// Live entries removed from the index.
    pub expired: usize,
    /// Locations which may now be reclaimed by compaction.
    pub reclaimed_locations: Vec<RecordLocation>,
    /// Sum of the expired records' stored lengths.
    pub reclaimed_bytes: u64,
}

#[derive(Clone, Debug)]
struct ExpiryRecord {
    namespace: Namespace,
    content_hash: ContentHash,
    sequence: u64,
    expires_at: SystemTime,
    location: RecordLocation,
}

#[derive(Default)]
struct Shard {
    index: ReferenceIndex,
    expiries: VecDeque<ExpiryRecord>,
}

/// A deterministic, power-of-two partitioning of the reference index.
///
/// The type deliberately does not synchronize internally: the intended data
/// plane assigns each shard to one worker/core. Callers needing shared access
/// can put individual owners behind their runtime's message-passing boundary.
/// Cleanup is explicit and accepts the caller's clock and work budget, so no
/// background thread or global clock is required.
pub struct ShardedIndex {
    shards: Vec<Shard>,
    expiry_cursor: usize,
}

impl ShardedIndex {
    /// Creates an index with `shard_count` power-of-two shards.
    ///
    /// # Errors
    ///
    /// Returns [`ShardedIndexError::ShardCountNotPowerOfTwo`] for zero or a
    /// non-power-of-two count.
    pub fn new(shard_count: usize) -> Result<Self, ShardedIndexError> {
        if !shard_count.is_power_of_two() {
            return Err(ShardedIndexError::ShardCountNotPowerOfTwo);
        }
        Ok(Self {
            shards: (0..shard_count).map(|_| Shard::default()).collect(),
            expiry_cursor: 0,
        })
    }

    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns the stable shard for an artifact identity using XXH3-64.
    /// Full identity comparison still happens inside the owning shard.
    #[must_use]
    pub fn shard_for(&self, namespace: &Namespace, content_hash: &ContentHash) -> usize {
        let hash = shard_hash_parts([namespace.as_bytes(), content_hash]);
        let mask = u64::try_from(self.shards.len() - 1).unwrap_or(u64::MAX);
        usize::try_from(hash.get() & mask).unwrap_or(0)
    }

    #[must_use]
    pub fn get(
        &self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        now: SystemTime,
    ) -> Option<&ArtifactEntry> {
        self.shards[self.shard_for(namespace, content_hash)]
            .index
            .get(namespace, content_hash, now)
    }

    /// Inserts only a newer version and schedules its optional expiry.
    pub fn insert_latest(&mut self, entry: ArtifactEntry) -> bool {
        let shard_index = self.shard_for(&entry.namespace, &entry.content_hash);
        let expiry = entry.expires_at.map(|expires_at| ExpiryRecord {
            namespace: entry.namespace.clone(),
            content_hash: entry.content_hash,
            sequence: entry.sequence,
            expires_at,
            location: entry.location,
        });
        let shard = &mut self.shards[shard_index];
        if !shard.index.insert_latest(entry) {
            return false;
        }
        if let Some(expiry) = expiry {
            insert_expiry_sorted(&mut shard.expiries, expiry);
        }
        true
    }

    /// Applies a versioned tombstone to the owning shard.
    pub fn tombstone(
        &mut self,
        namespace: &Namespace,
        content_hash: &ContentHash,
        sequence: u64,
        location: RecordLocation,
    ) -> bool {
        let shard = self.shard_for(namespace, content_hash);
        self.shards[shard]
            .index
            .apply_tombstone(namespace, content_hash, sequence, location)
    }

    /// Removes up to `budget` due expiry records, returning reclaimable data.
    /// Superseded expiry records consume budget but never remove newer values.
    pub fn expire_at(&mut self, now: SystemTime, budget: usize) -> ExpiryReport {
        let mut report = ExpiryReport::default();
        if budget == 0 {
            return report;
        }
        let shard_count = self.shards.len();
        let mut empty_shards = 0;
        while report.examined < budget && empty_shards < shard_count {
            let shard_index = self.expiry_cursor;
            self.expiry_cursor = (self.expiry_cursor + 1) & (shard_count - 1);
            let shard = &mut self.shards[shard_index];
            let Some(expiry) = shard.expiries.front() else {
                empty_shards += 1;
                continue;
            };
            if expiry.expires_at > now {
                empty_shards += 1;
                continue;
            }
            empty_shards = 0;
            let Some(expiry) = shard.expiries.pop_front() else {
                continue;
            };
            report.examined += 1;
            if shard.index.version(&expiry.namespace, &expiry.content_hash) != Some(expiry.sequence)
            {
                continue;
            }
            // Expiry is a logical deletion. Advancing the version prevents an
            // old artifact replay from resurrecting the expired value.
            let deletion_sequence = expiry.sequence.saturating_add(1);
            if shard.index.apply_expiry(
                &expiry.namespace,
                &expiry.content_hash,
                deletion_sequence,
                expiry.location,
            ) {
                report.expired += 1;
                report.reclaimed_bytes += u64::from(expiry.location.stored_len);
                report.reclaimed_locations.push(expiry.location);
            }
        }
        report
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.index.len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.index.is_empty())
    }
}

fn insert_expiry_sorted(expiries: &mut VecDeque<ExpiryRecord>, expiry: ExpiryRecord) {
    let position = expiries
        .iter()
        .position(|current| current.expires_at > expiry.expires_at)
        .unwrap_or(expiries.len());
    expiries.insert(position, expiry);
}

#[cfg(test)]
mod tests {
    use super::*;
    use punah_types::{EntryState, OperationHash, SegmentId};
    use std::time::Duration;

    fn namespace(value: &str) -> Namespace {
        Namespace::new(value).expect("namespace")
    }

    fn entry(namespace: Namespace, hash: ContentHash, sequence: u64, expiry: u64) -> ArtifactEntry {
        ArtifactEntry {
            namespace,
            content_hash: hash,
            operation_hash: OperationHash::default(),
            sequence,
            location: RecordLocation {
                segment_id: SegmentId(1),
                generation: 0,
                offset: sequence,
                stored_len: 12,
                logical_len: 8,
            },
            created_at: SystemTime::UNIX_EPOCH,
            expires_at: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(expiry)),
            state: EntryState::Live,
        }
    }

    #[test]
    fn invalid_shard_counts_are_rejected() {
        assert!(matches!(
            ShardedIndex::new(0),
            Err(ShardedIndexError::ShardCountNotPowerOfTwo)
        ));
        assert!(matches!(
            ShardedIndex::new(3),
            Err(ShardedIndexError::ShardCountNotPowerOfTwo)
        ));
        assert!(ShardedIndex::new(8).is_ok());
    }

    #[test]
    fn routing_is_stable_and_uses_key_bytes() {
        let index = ShardedIndex::new(16).expect("index");
        let ns = namespace("tenant");
        assert_eq!(
            index.shard_for(&ns, &[7; 32]),
            index.shard_for(&ns, &[7; 32])
        );
        let routes: std::collections::HashSet<_> = (0_u8..32)
            .map(|byte| {
                let mut hash = [0; 32];
                hash[0] = byte;
                index.shard_for(&ns, &hash)
            })
            .collect();
        assert!(routes.len() > 1);
    }

    #[test]
    fn expiry_obeys_budget_and_returns_locations() {
        let mut index = ShardedIndex::new(1).expect("index");
        index.insert_latest(entry(namespace("a"), [1; 32], 1, 2));
        index.insert_latest(entry(namespace("a"), [2; 32], 2, 2));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(3);
        let first = index.expire_at(now, 1);
        assert_eq!(
            (first.examined, first.expired, first.reclaimed_bytes),
            (1, 1, 12)
        );
        assert_eq!(index.len(), 1);
        assert_eq!(index.expire_at(now, 1).expired, 1);
        assert!(index.is_empty());
    }

    #[test]
    fn superseded_expiry_cannot_delete_or_resurrect_newer_entry() {
        let mut index = ShardedIndex::new(2).expect("index");
        let ns = namespace("a");
        index.insert_latest(entry(ns.clone(), [1; 32], 10, 2));
        index.insert_latest(entry(ns.clone(), [1; 32], 20, 20));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(3);
        assert_eq!(index.expire_at(now, 1).expired, 0);
        assert_eq!(
            index.get(&ns, &[1; 32], now).map(|entry| entry.sequence),
            Some(20)
        );

        let later = SystemTime::UNIX_EPOCH + Duration::from_secs(21);
        assert_eq!(index.expire_at(later, 1).expired, 1);
        assert!(!index.insert_latest(entry(ns.clone(), [1; 32], 20, 30)));
        assert!(index.get(&ns, &[1; 32], later).is_none());
    }
}
