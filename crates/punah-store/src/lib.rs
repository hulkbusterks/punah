#![forbid(unsafe_code)]

//! Synchronous reference store built over Punah's polling storage primitives.
//!
//! Exact key equality uses the original durable key bytes. Immutable payloads
//! receive a BLAKE3-256 artifact identity; routing hashes are not identities.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use punah_compaction::{
    CompactionPlan, PlanningError, PlanningLimits, SegmentCandidate, plan_compaction,
};
use punah_device::{FileDevice, MappedReadError, MappedSegment};
use punah_engine::{AppendCompletion, AppendError, SegmentWriter};
use punah_events::{
    ArtifactLocationSnapshot, EventPublisher, LifecycleFact, LifecycleSnapshot, RemovalReason,
};
use punah_identity::artifact_id;
use punah_metadata::{
    FileMetadataIo, JournalEntry, JournalOperation, ManifestCheckpoint, MetadataPublisher,
    PublicationError, SegmentDescriptor, SegmentRef,
};
use punah_record::{
    DecodeError, DecodedRecordRef, EncodeError, RECORD_HEADER_LEN, Record, RecordType,
    committed_record_len, decode, decode_ref, encoded_len, encoded_len_for_fields,
};
use punah_types::{
    ArtifactId, ContentHash, Durability, EntryState, Namespace, RecordLocation, SegmentId,
    SegmentStats,
};

const QUEUE_CAPACITY: usize = 64;
const MAX_COHORT_RECORDS: usize = 64;
const MAX_COHORT_BYTES: u64 = 1024 * 1024;
type Relocation = (Vec<u8>, u64, RecordLocation, RecordLocation);
type CopyResult = (usize, u64, Vec<Relocation>);

struct PreparedCompaction {
    generation: u32,
    path: PathBuf,
    committed_len: u64,
    sealed_sequence: u64,
    mapping: Option<MappedSegment>,
    copied_records: usize,
    copied_bytes: u64,
    relocations: Vec<Relocation>,
}

/// Configuration for a [`Store`].
#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub directory: PathBuf,
    pub namespace: Namespace,
    pub segment_capacity: u64,
    pub durability: Durability,
}

/// Errors returned by the synchronous reference store.
#[derive(Debug)]
pub enum StoreError {
    Io(io::Error),
    Decode { path: PathBuf, source: DecodeError },
    Encode(EncodeError),
    Append(AppendError),
    InvalidCapacity,
    ClockBeforeEpoch,
    SequenceOverflow,
    SegmentOverflow,
    Metadata(PublicationError),
    InvalidCompactionPlan,
    StorePoisoned,
    MissingSegment(u32),
    InvalidLocation(MappedReadError),
    LocationMismatch,
    InvalidManifest,
    BatchAborted,
    ExplicitImportRequired,
    NoSegmentsToImport,
    AlreadyInitialized,
}

impl From<io::Error> for StoreError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<PublicationError> for StoreError {
    fn from(value: PublicationError) -> Self {
        Self::Metadata(value)
    }
}

#[derive(Clone)]
struct Version {
    artifact_id: ArtifactId,
    sequence: u64,
    location: RecordLocation,
    expires_at_unix_ns: Option<u64>,
    state: EntryState,
}

struct RecoveredStoreState {
    versions: HashMap<Vec<u8>, Version>,
    segments: Vec<SealedSegment>,
    sequence: u64,
    active_boundary: u64,
    active_start_offset: u64,
    active_sequence: u64,
    active_segment_id: SegmentId,
    mappings: HashMap<u32, MappedSegment>,
}

#[derive(Clone, Debug)]
struct RecoveryCandidate {
    descriptor: SegmentDescriptor,
    path: PathBuf,
    active: bool,
    strict_prefix: bool,
    scan_suffix: bool,
}

/// Immutable description of the latest durable version of one exact key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionSnapshot {
    pub key: Vec<u8>,
    pub sequence: u64,
    pub location: RecordLocation,
    pub expires_at_unix_ns: Option<u64>,
    pub state: EntryState,
    pub artifact_id: ArtifactId,
}

/// A segment that will no longer receive appends and can be considered by compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedSegment {
    pub segment_id: SegmentId,
    pub generation: u32,
    pub start_offset: u64,
    pub committed_len: u64,
    pub sealed_sequence: u64,
}

/// Authoritative byte accounting for one immutable sealed segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedSegmentMetrics {
    pub segment: SealedSegment,
    pub stats: SegmentStats,
}

/// Durable single-process reference key/value store.
pub struct Store {
    config: StoreConfig,
    writer: SegmentWriter<FileDevice>,
    segment_id: SegmentId,
    generation: u32,
    active_start_offset: u64,
    active_sequence: u64,
    sequence: u64,
    versions: HashMap<Vec<u8>, Version>,
    mapped_segments: HashMap<u32, MappedSegment>,
    sealed_segments: Vec<SealedSegment>,
    metadata_generation: u64,
    next_generation: u32,
    poisoned: bool,
    events: Option<EventPublisher>,
    io_telemetry: StoreIoTelemetry,
}

/// Outcome of one exclusive, synchronous compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionReport {
    pub input_segments: usize,
    pub copied_records: usize,
    pub copied_bytes: u64,
    pub removed_segments: usize,
}

/// Result of one bounded expiration-cleanup pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CleanupReport {
    /// Expired values removed from memory and superseded by durable tombstones.
    pub tombstoned: usize,
    /// Whether the pass stopped at its caller-provided work budget.
    pub budget_exhausted: bool,
}

/// Monotonic foreground append telemetry from one Store owner.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreIoTelemetry {
    pub singleton_appends: u64,
    pub cohort_appends: u64,
    pub appended_records: u64,
    pub encoded_bytes: u64,
    /// Exact for the current writer protocol: `LocalDurable` uses two barriers
    /// per successful singleton/cohort append; Memory and Buffered use none.
    pub durability_flushes: u64,
    pub rotations: u64,
    pub max_records_per_append: u64,
}

/// One independently visible mutation eligible for ordered cohort commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreMutation {
    Set {
        key: Vec<u8>,
        payload: Vec<u8>,
        ttl: Option<Duration>,
    },
    Delete {
        key: Vec<u8>,
    },
}

/// Type-preserving result of one [`StoreMutation`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationValue {
    Set,
    Delete(bool),
}

#[derive(Clone, Copy)]
enum Publication {
    Set(ArtifactId),
    Delete,
}

impl Store {
    /// Opens or creates a store from an authoritative manifest.
    ///
    /// A missing or blank manifest is accepted only for a genuinely empty
    /// directory. Existing segment files require
    /// [`Store::import_unmanaged_segments`], keeping recovery policy explicit.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, filesystem failures, or
    /// corruption within committed records.
    pub fn open(config: StoreConfig) -> Result<Self, StoreError> {
        Self::open_internal(config, false)
    }

    /// Explicitly adopts deterministic segment files from an uninitialized
    /// directory and publishes the first authoritative manifest.
    ///
    /// This is a migration/repair operation, not normal recovery. It refuses to
    /// replace a healthy or corrupt nonblank root.
    ///
    /// # Errors
    /// Returns when no segments exist, metadata is already authoritative or
    /// corrupt/nonblank, a segment is invalid, or publication fails.
    pub fn import_unmanaged_segments(config: StoreConfig) -> Result<Self, StoreError> {
        Self::open_internal(config, true)
    }

    #[allow(clippy::too_many_lines)] // Keeps one fail-closed recovery/import state transition.
    fn open_internal(config: StoreConfig, import_unmanaged: bool) -> Result<Self, StoreError> {
        if config.segment_capacity < 128 {
            return Err(StoreError::InvalidCapacity);
        }
        fs::create_dir_all(&config.directory)?;
        let discovered_segments = segment_files(&config.directory)?;
        let discovered_max_generation = discovered_segments
            .iter()
            .map(|(generation, _)| *generation)
            .max();
        let metadata_path = config.directory.join("metadata.punah");
        let metadata_existed = metadata_path.exists();
        let recovered_root = if metadata_existed {
            let metadata_io = FileMetadataIo::open_existing(&metadata_path)?;
            let mut publisher = MetadataPublisher::new(metadata_io);
            match publisher.recover() {
                Ok(root) => Some(root),
                Err(PublicationError::Uninitialized) => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        if import_unmanaged {
            if recovered_root.is_some() {
                return Err(StoreError::AlreadyInitialized);
            }
            if discovered_segments.is_empty() {
                return Err(StoreError::NoSegmentsToImport);
            }
        } else if recovered_root.is_none() && !discovered_segments.is_empty() {
            return Err(StoreError::ExplicitImportRequired);
        }
        let segments = if let Some(root) = &recovered_root {
            recovery_candidates(&config.directory, &root.checkpoint.active_segments)?
        } else {
            let mut files = discovered_segments;
            files.sort_unstable_by_key(|(generation, _)| *generation);
            discovered_candidates(files)
        };

        let RecoveredStoreState {
            versions,
            segments: recovered_segments,
            sequence,
            active_boundary: last_boundary,
            active_start_offset,
            active_sequence,
            active_segment_id,
            mappings: mut mapped_segments,
        } = recover_mapped_segments(&segments)?;

        let generation = segments
            .last()
            .map_or(0, |candidate| candidate.descriptor.generation);
        let segment_id = segments.last().map_or(SegmentId(generation), |candidate| {
            SegmentId(candidate.descriptor.id)
        });
        debug_assert_eq!(segment_id, active_segment_id);
        let path = segment_path(&config.directory, generation);
        let device = if path.exists() {
            FileDevice::open(&path, config.segment_capacity, QUEUE_CAPACITY)?
        } else {
            let device = FileDevice::create(&path, config.segment_capacity, QUEUE_CAPACITY)?;
            device.sync_all()?;
            sync_directory(&config.directory)?;
            device
        };
        let writer = SegmentWriter::new_at(
            device,
            segment_id,
            generation,
            config.segment_capacity,
            last_boundary,
        );
        if let std::collections::hash_map::Entry::Vacant(entry) = mapped_segments.entry(generation)
        {
            entry.insert(MappedSegment::open(&path)?);
        }
        let next_generation = segments
            .iter()
            .map(|candidate| candidate.descriptor.generation)
            .max()
            .into_iter()
            .chain(discovered_max_generation)
            .max()
            .unwrap_or(generation)
            .checked_add(1)
            .ok_or(StoreError::SegmentOverflow)?;
        let mut store = Self {
            config,
            writer,
            segment_id,
            generation,
            active_start_offset,
            active_sequence,
            sequence,
            versions,
            mapped_segments,
            sealed_segments: recovered_segments,
            metadata_generation: recovered_root
                .as_ref()
                .map_or(0, |root| root.superblock.generation),
            next_generation,
            poisoned: false,
            events: None,
            io_telemetry: StoreIoTelemetry::default(),
        };
        if recovered_root.is_none() {
            store.publish_manifest()?;
        }
        Ok(store)
    }

    /// Attaches a best-effort observer stream. It never participates in authority.
    pub fn set_event_publisher(&mut self, publisher: EventPublisher) {
        self.events = Some(publisher);
    }

    /// Returns monotonic foreground append counters for this Store owner.
    #[must_use]
    pub const fn io_telemetry(&self) -> StoreIoTelemetry {
        self.io_telemetry
    }

    /// Durably stores a value, optionally expiring it after `ttl`.
    ///
    /// # Errors
    /// Returns an error if time cannot be represented or persistence fails.
    pub fn set(
        &mut self,
        key: &[u8],
        payload: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), StoreError> {
        self.ensure_writable()?;
        let (record, publication) = self.prepare_mutation(StoreMutation::Set {
            key: key.to_vec(),
            payload,
            ttl,
        })?;
        let completion = self.append(record)?;
        let value = self.apply_publication(&completion, publication);
        debug_assert_eq!(value, MutationValue::Set);
        Ok(())
    }

    /// Reads the latest live payload from its authoritative mapped segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock, mapped location, or durable record is
    /// invalid. Corruption is never converted into a cache miss.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        if self.poisoned {
            return Err(StoreError::StorePoisoned);
        }
        let now = unix_ns(SystemTime::now())?;
        let Some(version) = self
            .versions
            .get(key)
            .filter(|value| is_live_at(value, now))
        else {
            return Ok(None);
        };
        let mapping = self
            .mapped_segments
            .get(&version.location.generation)
            .ok_or(StoreError::MissingSegment(version.location.generation))?;
        let path = segment_path(&self.config.directory, version.location.generation);
        let bytes = mapping
            .read(
                version.location.offset,
                u64::from(version.location.stored_len),
            )
            .map_err(StoreError::InvalidLocation)?;
        decode_version_payload(bytes, &path, key, version).map(Some)
    }

    /// Persists a tombstone and removes the value.
    ///
    /// # Errors
    /// Returns an error if time cannot be represented or persistence fails.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        self.ensure_writable()?;
        let (record, publication) =
            self.prepare_mutation(StoreMutation::Delete { key: key.to_vec() })?;
        let completion = self.append(record)?;
        let MutationValue::Delete(existed) = self.apply_publication(&completion, publication)
        else {
            unreachable!("one delete produces one delete result");
        };
        Ok(existed)
    }

    /// Persists independent mutations in input order using bounded commit
    /// cohorts. Grouping changes only I/O scheduling: every mutation remains
    /// independently visible, and repeated operations on one key retain their
    /// input order.
    ///
    /// # Errors
    /// Returns before publication when deterministic preparation fails. An I/O
    /// failure may leave an unacknowledged committed prefix and fail-stops the
    /// store until reopen, matching singleton append semantics.
    pub fn mutate_batch(
        &mut self,
        mutations: Vec<StoreMutation>,
    ) -> Result<Vec<MutationValue>, StoreError> {
        self.ensure_writable()?;
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        for mutation in &mutations {
            self.validate_mutation(mutation)?;
        }
        if mutations.len() == 1 {
            let mutation = mutations
                .into_iter()
                .next()
                .ok_or(StoreError::BatchAborted)?;
            let (record, publication) = self.prepare_validated_mutation(mutation)?;
            let completion = self.append(record)?;
            return Ok(vec![self.apply_publication(&completion, publication)]);
        }

        let mut records = Vec::with_capacity(mutations.len());
        let mut publications = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let (record, publication) = self.prepare_validated_mutation(mutation)?;
            records.push(record);
            publications.push(publication);
        }
        let completions = self.append_records(records)?;
        debug_assert_eq!(completions.len(), publications.len());

        let mut values = Vec::with_capacity(completions.len());
        for (completion, publication) in completions.into_iter().zip(publications) {
            values.push(self.apply_publication(&completion, publication));
        }
        Ok(values)
    }

    /// Checks record-format representability and segment admission without
    /// copying payload bytes or consuming a storage sequence.
    ///
    /// Runtime batching uses this to keep one malformed request from aborting
    /// unrelated requests that happen to share an I/O cohort.
    ///
    /// # Errors
    /// Returns when the Store is failed, a variable field cannot be represented
    /// by record v1, or the encoded mutation cannot fit an empty segment.
    pub fn validate_mutation(&self, mutation: &StoreMutation) -> Result<(), StoreError> {
        self.ensure_writable()?;
        let (key_len, payload_len) = match mutation {
            StoreMutation::Set { key, payload, .. } => (key.len(), payload.len()),
            StoreMutation::Delete { key } => (key.len(), 0),
        };
        let length = encoded_len_for_fields(
            self.config.namespace.as_bytes().len(),
            key_len,
            0,
            payload_len,
        )
        .map_err(StoreError::Encode)?;
        let length = u64::try_from(length).map_err(|_| StoreError::SegmentOverflow)?;
        if length > self.config.segment_capacity {
            return Err(StoreError::Append(AppendError::SegmentFull));
        }
        Ok(())
    }

    /// Durably tombstones at most `budget` expired values.
    ///
    /// Cleanup is explicit and bounded so callers can schedule it without an
    /// ambient clock or an unbounded latency spike. Physical segment-space
    /// reclamation remains the compactor's responsibility.
    ///
    /// # Errors
    /// Returns an error if the clock or persistence operation fails. Entries
    /// successfully tombstoned before an error remain cleaned.
    pub fn cleanup_expired(&mut self, budget: usize) -> Result<CleanupReport, StoreError> {
        self.ensure_writable()?;
        let now = unix_ns(SystemTime::now())?;
        if budget == 0 {
            return Ok(CleanupReport {
                tombstoned: 0,
                budget_exhausted: self.versions.values().any(|value| {
                    value.state == EntryState::Live
                        && value.expires_at_unix_ns.is_some_and(|expiry| expiry <= now)
                }),
            });
        }
        let expired: Vec<_> = self
            .versions
            .iter()
            .filter_map(|(key, value)| {
                (value.state == EntryState::Live)
                    .then_some(value)
                    .and_then(|value| {
                        value
                            .expires_at_unix_ns
                            .is_some_and(|expiry| expiry <= now)
                            .then_some(key.clone())
                    })
            })
            .take(budget.saturating_add(1))
            .collect();
        let budget_exhausted = expired.len() > budget;
        let mut tombstoned = 0;
        for key in expired.into_iter().take(budget) {
            let record = self.record(
                RecordType::Tombstone,
                key.clone(),
                [0; 32],
                now,
                None,
                Vec::new(),
            )?;
            let completion = self.append(record)?;
            self.publish_tombstone(key, &completion, RemovalReason::Expired);
            tombstoned += 1;
        }
        Ok(CleanupReport {
            tombstoned,
            budget_exhausted,
        })
    }

    /// Returns stable copies of the latest durable versions, including tombstones.
    #[must_use]
    pub fn version_snapshots(&self) -> Vec<VersionSnapshot> {
        let mut snapshots: Vec<_> = self
            .versions
            .iter()
            .map(|(key, version)| VersionSnapshot {
                key: key.clone(),
                sequence: version.sequence,
                location: version.location,
                expires_at_unix_ns: version.expires_at_unix_ns,
                state: version.state,
                artifact_id: version.artifact_id,
            })
            .collect();
        snapshots.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        snapshots
    }

    /// Returns the authoritative location view paired with the event watermark.
    /// Calling this on the store owner thread supplies the resynchronization
    /// barrier expected by [`punah_events::Subscription::resynced_through`].
    #[must_use]
    pub fn lifecycle_snapshot(&self) -> LifecycleSnapshot {
        let through_cursor = self
            .events
            .as_ref()
            .map_or(0, EventPublisher::current_cursor);
        let artifacts = self
            .version_snapshots()
            .into_iter()
            .map(|version| ArtifactLocationSnapshot {
                key: version.key,
                namespace: self.config.namespace.clone(),
                artifact_id: version.artifact_id,
                location: version.location,
                storage_sequence: version.sequence,
                state: version.state,
            })
            .collect();
        LifecycleSnapshot {
            through_cursor,
            artifacts,
        }
    }

    /// Returns segments that are sealed against further appends.
    #[must_use]
    pub fn sealed_segments(&self) -> &[SealedSegment] {
        &self.sealed_segments
    }

    /// Computes compaction accounting from the current exact-key index.
    ///
    /// Tombstones and expired latest values remain live until a newer durable
    /// record supersedes them. This deliberately reports `expired_bytes = 0`:
    /// [`Store::cleanup_expired`] first writes a tombstone, after which the old
    /// value naturally becomes dead and reclaimable.
    #[must_use]
    pub fn sealed_segment_metrics(&self) -> Vec<SealedSegmentMetrics> {
        let mut metrics: Vec<_> = self
            .sealed_segments
            .iter()
            .copied()
            .map(|segment| SealedSegmentMetrics {
                segment,
                stats: SegmentStats {
                    total_bytes: segment.committed_len,
                    ..SegmentStats::default()
                },
            })
            .collect();
        let positions: HashMap<_, _> = metrics
            .iter()
            .enumerate()
            .map(|(index, metrics)| {
                (
                    (metrics.segment.segment_id, metrics.segment.generation),
                    index,
                )
            })
            .collect();
        for version in self.versions.values() {
            let Some(index) =
                positions.get(&(version.location.segment_id, version.location.generation))
            else {
                continue;
            };
            metrics[*index].stats.live_bytes = metrics[*index]
                .stats
                .live_bytes
                .saturating_add(u64::from(version.location.stored_len));
        }
        for metrics in &mut metrics {
            debug_assert!(metrics.stats.live_bytes <= metrics.stats.total_bytes);
            metrics.stats.dead_bytes = metrics
                .stats
                .total_bytes
                .saturating_sub(metrics.stats.live_bytes);
        }
        metrics
    }

    /// Selects one bounded compaction batch from authoritative Store state.
    ///
    /// Output headroom is clamped to one physical segment because the current
    /// compactor writes one replacement segment per plan.
    ///
    /// # Errors
    /// Returns a deterministic planning error when no bytes are reclaimable,
    /// headroom is insufficient, or accounting overflows.
    pub fn plan_compaction(
        &self,
        mut limits: PlanningLimits,
    ) -> Result<CompactionPlan, PlanningError> {
        limits.available_bytes = limits.available_bytes.min(self.config.segment_capacity);
        let candidates: Vec<_> = self
            .sealed_segment_metrics()
            .into_iter()
            .map(|metrics| SegmentCandidate {
                segment: SegmentRef {
                    id: metrics.segment.segment_id.0,
                    generation: metrics.segment.generation,
                },
                stats: metrics.stats,
                sealed: true,
                active: false,
            })
            .collect();
        plan_compaction(&candidates, limits)
    }

    /// Rewrites the latest versions located in the plan's sealed inputs, then
    /// atomically publishes the replacement membership before deleting inputs.
    /// Exclusive `&mut self` is the initial compaction publication barrier.
    ///
    /// # Errors
    /// Returns an error for an invalid plan, corrupt input, insufficient output
    /// capacity, failed durable I/O, or failed metadata publication.
    pub fn compact(&mut self, plan: &CompactionPlan) -> Result<CompactionReport, StoreError> {
        self.ensure_writable()?;
        if plan.input_segments.is_empty() {
            return Err(StoreError::InvalidCompactionPlan);
        }
        if plan.required_output_bytes > self.config.segment_capacity {
            return Err(StoreError::InvalidCompactionPlan);
        }
        let inputs = plan.input_segments.clone();
        if inputs.iter().any(|input| {
            !self.sealed_segments.iter().any(|sealed| {
                sealed.segment_id.0 == input.id && sealed.generation == input.generation
            })
        }) {
            return Err(StoreError::InvalidCompactionPlan);
        }

        let inputs_are_all_dead = !self.versions.values().any(|version| {
            inputs.iter().any(|input| {
                version.location.segment_id.0 == input.id
                    && version.location.generation == input.generation
            })
        });
        let mut prepared = if inputs_are_all_dead {
            None
        } else {
            Some(self.prepare_compaction(&inputs)?)
        };
        // The replacement checkpoint also makes the current active prefix a
        // strict recovery fence. Buffered records must reach stable storage
        // before that metadata can become authoritative.
        self.writer.device().sync_all()?;

        self.sealed_segments.retain(|sealed| {
            !inputs.iter().any(|input| {
                input.id == sealed.segment_id.0 && input.generation == sealed.generation
            })
        });
        if let Some(prepared_output) = prepared.as_mut()
            && let Some(output_mapping) = prepared_output.mapping.take()
        {
            self.sealed_segments.push(SealedSegment {
                segment_id: SegmentId(prepared_output.generation),
                generation: prepared_output.generation,
                start_offset: 0,
                committed_len: prepared_output.committed_len,
                sealed_sequence: prepared_output.sealed_sequence,
            });
            self.mapped_segments
                .insert(prepared_output.generation, output_mapping);
        }
        let output_generation = prepared.as_ref().map_or(0, |prepared| prepared.generation);
        let output_len = prepared
            .as_ref()
            .map_or(0, |prepared| prepared.committed_len);
        if let Err(error) = self.publish_replacement(&inputs, output_generation, output_len) {
            // A failed flush/write can be observationally ambiguous: the new
            // root may already be durable. Only reopen may resolve authority.
            self.poisoned = true;
            return Err(error);
        }
        let (copied_records, copied_bytes, output_path) = if let Some(prepared) = prepared {
            self.apply_relocations(prepared.relocations);
            (
                prepared.copied_records,
                prepared.copied_bytes,
                (prepared.committed_len == 0).then_some(prepared.path),
            )
        } else {
            (0, 0, None)
        };
        let removed_segments = self.retire_compaction_files(&inputs, output_path.as_deref())?;
        Ok(CompactionReport {
            input_segments: inputs.len(),
            copied_records,
            copied_bytes,
            removed_segments,
        })
    }

    fn prepare_compaction(
        &mut self,
        inputs: &[SegmentRef],
    ) -> Result<PreparedCompaction, StoreError> {
        let generation = self.allocate_generation()?;
        let path = segment_path(&self.config.directory, generation);
        let device = FileDevice::create(&path, self.config.segment_capacity, QUEUE_CAPACITY)?;
        let mut output = SegmentWriter::new(
            device,
            SegmentId(generation),
            generation,
            self.config.segment_capacity,
        );
        let (copied_records, copied_bytes, relocations) = self.copy_inputs(inputs, &mut output)?;
        let sealed_sequence = relocations
            .iter()
            .map(|(_, sequence, _, _)| *sequence)
            .max()
            .unwrap_or(0);
        let committed_len = output.reserved_boundary();
        let device = output.into_device();
        device.sync_all()?;
        sync_directory(&self.config.directory)?;
        drop(device);
        let mapping = (committed_len > 0)
            .then(|| MappedSegment::open(&path))
            .transpose()?;
        Ok(PreparedCompaction {
            generation,
            path,
            committed_len,
            sealed_sequence,
            mapping,
            copied_records,
            copied_bytes,
            relocations,
        })
    }

    fn apply_relocations(&mut self, relocations: Vec<Relocation>) {
        for (key, sequence, old, new) in relocations {
            if let Some(version) = self.versions.get_mut(&key)
                && version.sequence == sequence
                && version.location == old
            {
                version.location = new;
                let artifact_id = version.artifact_id;
                self.publish_event(LifecycleFact::ArtifactRelocated {
                    key,
                    namespace: self.config.namespace.clone(),
                    artifact_id,
                    old_location: old,
                    new_location: new,
                    storage_sequence: sequence,
                });
            }
        }
    }

    fn retire_compaction_files(
        &mut self,
        inputs: &[SegmentRef],
        empty_output_path: Option<&Path>,
    ) -> Result<usize, StoreError> {
        for input in inputs {
            self.mapped_segments.remove(&input.generation);
        }
        let mut removed_segments = 0;
        for input in inputs {
            match fs::remove_file(segment_path(&self.config.directory, input.generation)) {
                Ok(()) => removed_segments += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(output_path) = empty_output_path {
            match fs::remove_file(output_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        sync_directory(&self.config.directory)?;
        Ok(removed_segments)
    }

    fn copy_inputs(
        &self,
        inputs: &[SegmentRef],
        output: &mut SegmentWriter<FileDevice>,
    ) -> Result<CopyResult, StoreError> {
        let mut result = (0, 0_u64, Vec::new());
        for input in inputs {
            let sealed = self
                .sealed_segments
                .iter()
                .find(|sealed| {
                    sealed.segment_id.0 == input.id && sealed.generation == input.generation
                })
                .ok_or(StoreError::InvalidCompactionPlan)?;
            let mapping = self
                .mapped_segments
                .get(&input.generation)
                .ok_or(StoreError::MissingSegment(input.generation))?;
            let copied = self.copy_input_records(*input, sealed, mapping, output)?;
            result.0 += copied.0;
            result.1 = result
                .1
                .checked_add(copied.1)
                .ok_or(StoreError::SegmentOverflow)?;
            result.2.extend(copied.2);
        }
        Ok(result)
    }

    fn copy_input_records(
        &self,
        input: SegmentRef,
        sealed: &SealedSegment,
        mapping: &MappedSegment,
        output: &mut SegmentWriter<FileDevice>,
    ) -> Result<CopyResult, StoreError> {
        let path = segment_path(&self.config.directory, input.generation);
        let mut result = (0, 0_u64, Vec::new());
        let mut cursor = 0_u64;
        let end = sealed
            .start_offset
            .checked_add(sealed.committed_len)
            .ok_or(StoreError::SegmentOverflow)?;
        let mut record_bytes = Vec::new();
        while cursor < sealed.committed_len {
            let offset = sealed
                .start_offset
                .checked_add(cursor)
                .ok_or(StoreError::SegmentOverflow)?;
            let header = read_mapped_header(mapping, offset)?;
            copy_mapped_record(mapping, offset, end, &header, &mut record_bytes, &path)?;
            let decoded = decode(&record_bytes).map_err(|source| StoreError::Decode {
                path: path.clone(),
                source,
            })?;
            let old_location = RecordLocation {
                segment_id: SegmentId(input.id),
                generation: input.generation,
                offset,
                stored_len: u32::try_from(decoded.encoded_len)
                    .map_err(|_| StoreError::SegmentOverflow)?,
                logical_len: u32::try_from(decoded.record.payload.len())
                    .map_err(|_| StoreError::SegmentOverflow)?,
            };
            if self.current_at(&decoded.record.key, decoded.record.sequence, old_location) {
                let key = decoded.record.key.clone();
                let sequence = decoded.record.sequence;
                output
                    .try_append(decoded.record, Durability::LocalDurable)
                    .map_err(|rejected| StoreError::Append(rejected.error))?;
                let completion = poll_append(output)?;
                result.0 += 1;
                result.1 = result
                    .1
                    .checked_add(u64::from(completion.location.stored_len))
                    .ok_or(StoreError::SegmentOverflow)?;
                result
                    .2
                    .push((key, sequence, old_location, completion.location));
            }
            cursor = cursor
                .checked_add(
                    u64::try_from(decoded.encoded_len).map_err(|_| StoreError::SegmentOverflow)?,
                )
                .ok_or(StoreError::SegmentOverflow)?;
        }
        Ok(result)
    }

    fn current_at(&self, key: &[u8], sequence: u64, location: RecordLocation) -> bool {
        self.versions
            .get(key)
            .is_some_and(|version| version.sequence == sequence && version.location == location)
    }

    fn publish_tombstone(
        &mut self,
        key: Vec<u8>,
        completion: &AppendCompletion,
        reason: RemovalReason,
    ) {
        let removed = self
            .versions
            .get(&key)
            .filter(|version| version.state == EntryState::Live)
            .map(|version| version.artifact_id);
        self.versions.insert(
            key.clone(),
            Version {
                artifact_id: ArtifactId::new([0; 32]),
                sequence: completion.record.sequence,
                location: completion.location,
                expires_at_unix_ns: None,
                state: EntryState::Tombstone,
            },
        );
        if let Some(artifact_id) = removed {
            self.publish_event(LifecycleFact::ArtifactRemoved {
                key,
                namespace: self.config.namespace.clone(),
                artifact_id,
                storage_sequence: completion.record.sequence,
                reason,
            });
        }
    }

    fn apply_publication(
        &mut self,
        completion: &AppendCompletion,
        publication: Publication,
    ) -> MutationValue {
        let key = completion.record.key.clone();
        match publication {
            Publication::Set(artifact_id) => {
                self.versions.insert(
                    key.clone(),
                    Version {
                        artifact_id,
                        sequence: completion.record.sequence,
                        location: completion.location,
                        expires_at_unix_ns: completion.record.expires_at_unix_ns,
                        state: EntryState::Live,
                    },
                );
                self.publish_event(LifecycleFact::ArtifactStored {
                    key,
                    namespace: self.config.namespace.clone(),
                    artifact_id,
                    location: completion.location,
                    storage_sequence: completion.record.sequence,
                });
                MutationValue::Set
            }
            Publication::Delete => {
                let existed = self
                    .versions
                    .get(&key)
                    .is_some_and(|value| is_live_at(value, completion.record.created_at_unix_ns));
                self.publish_tombstone(key, completion, RemovalReason::Deleted);
                MutationValue::Delete(existed)
            }
        }
    }

    fn publish_event(&self, fact: LifecycleFact) {
        if let Some(events) = &self.events {
            events.publish(fact);
        }
    }

    fn record(
        &mut self,
        record_type: RecordType,
        key: Vec<u8>,
        hash: ContentHash,
        created: u64,
        expires: Option<u64>,
        payload: Vec<u8>,
    ) -> Result<Record, StoreError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        Ok(Record {
            record_type,
            flags: 0,
            namespace: self.config.namespace.clone(),
            key,
            sequence: self.sequence,
            created_at_unix_ns: created,
            expires_at_unix_ns: expires,
            operation_hash: [0; 32],
            content_hash: hash,
            dependencies: Vec::new(),
            payload,
        })
    }

    fn prepare_mutation(
        &mut self,
        mutation: StoreMutation,
    ) -> Result<(Record, Publication), StoreError> {
        self.validate_mutation(&mutation)?;
        self.prepare_validated_mutation(mutation)
    }

    fn prepare_validated_mutation(
        &mut self,
        mutation: StoreMutation,
    ) -> Result<(Record, Publication), StoreError> {
        match mutation {
            StoreMutation::Set { key, payload, ttl } => {
                let created = unix_ns(SystemTime::now())?;
                let expires = ttl.and_then(|duration| {
                    u64::try_from(duration.as_nanos())
                        .ok()
                        .and_then(|delta| created.checked_add(delta))
                });
                let artifact_id = artifact_id(&payload);
                let record = self.record(
                    RecordType::Artifact,
                    key,
                    artifact_id.into(),
                    created,
                    expires,
                    payload,
                )?;
                Ok((record, Publication::Set(artifact_id)))
            }
            StoreMutation::Delete { key } => {
                let record = self.record(
                    RecordType::Tombstone,
                    key,
                    [0; 32],
                    unix_ns(SystemTime::now())?,
                    None,
                    Vec::new(),
                )?;
                Ok((record, Publication::Delete))
            }
        }
    }

    fn append(&mut self, record: Record) -> Result<AppendCompletion, StoreError> {
        let encoded_len = encoded_len(&record).map_err(StoreError::Encode)?;
        let encoded_len = u64::try_from(encoded_len).map_err(|_| StoreError::SegmentOverflow)?;
        if encoded_len > self.config.segment_capacity {
            return Err(StoreError::Append(AppendError::SegmentFull));
        }
        self.append_prepared_single(record, encoded_len)
    }

    fn append_records(
        &mut self,
        records: Vec<Record>,
    ) -> Result<Vec<AppendCompletion>, StoreError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut pending = VecDeque::with_capacity(records.len());
        for record in records {
            let encoded_len = encoded_len(&record).map_err(StoreError::Encode)?;
            let encoded_len =
                u64::try_from(encoded_len).map_err(|_| StoreError::SegmentOverflow)?;
            if encoded_len > self.config.segment_capacity {
                return Err(StoreError::Append(AppendError::SegmentFull));
            }
            pending.push_back((record, encoded_len));
        }

        if pending.len() == 1 {
            let (record, encoded_len) = pending
                .pop_front()
                .expect("one prepared record remains available");
            return self
                .append_prepared_single(record, encoded_len)
                .map(|completion| vec![completion]);
        }

        let mut completions = Vec::with_capacity(pending.len());
        while let Some((_, first_len)) = pending.front() {
            if *first_len > self.writer.remaining_capacity() {
                if let Err(error) = self.rotate() {
                    if !completions.is_empty() {
                        self.poisoned = true;
                    }
                    return Err(error);
                }
                continue;
            }

            let mut cohort = Vec::with_capacity(MAX_COHORT_RECORDS.min(pending.len()));
            let mut cohort_bytes = 0_u64;
            let mut segment_bytes = self.writer.remaining_capacity();
            while cohort.len() < MAX_COHORT_RECORDS {
                let Some((_, next_len)) = pending.front() else {
                    break;
                };
                if *next_len > segment_bytes {
                    break;
                }
                if !cohort.is_empty()
                    && cohort_bytes
                        .checked_add(*next_len)
                        .is_none_or(|bytes| bytes > MAX_COHORT_BYTES)
                {
                    break;
                }
                let (record, encoded_len) =
                    pending.pop_front().expect("front record remains available");
                cohort_bytes = cohort_bytes
                    .checked_add(encoded_len)
                    .ok_or(StoreError::SegmentOverflow)?;
                segment_bytes -= encoded_len;
                cohort.push(record);
            }
            debug_assert!(!cohort.is_empty());

            match self.writer.append_cohort(cohort, self.config.durability) {
                Ok(mut committed) => {
                    self.record_append_telemetry(
                        u64::try_from(committed.len()).unwrap_or(u64::MAX),
                        cohort_bytes,
                        true,
                    );
                    for completion in &committed {
                        self.active_sequence = self.active_sequence.max(completion.record.sequence);
                    }
                    completions.append(&mut committed);
                }
                Err(rejected) => {
                    if !completions.is_empty()
                        || matches!(
                            rejected.error,
                            AppendError::Submission(_)
                                | AppendError::Io(_)
                                | AppendError::UnexpectedCompletion
                                | AppendError::WriterFailed
                        )
                    {
                        self.poisoned = true;
                    }
                    return Err(StoreError::Append(rejected.error));
                }
            }
        }
        Ok(completions)
    }

    fn append_prepared_single(
        &mut self,
        record: Record,
        encoded_len: u64,
    ) -> Result<AppendCompletion, StoreError> {
        if encoded_len > self.writer.remaining_capacity() {
            self.rotate()?;
        }
        self.writer
            .try_append(record, self.config.durability)
            .map_err(|rejected| StoreError::Append(rejected.error))?;
        loop {
            if let Some(result) = self.writer.poll() {
                let completion = result.map_err(|error| {
                    self.poisoned = true;
                    StoreError::Append(error)
                })?;
                self.active_sequence = self.active_sequence.max(completion.record.sequence);
                self.record_append_telemetry(1, encoded_len, false);
                return Ok(completion);
            }
            std::thread::yield_now();
        }
    }

    fn rotate(&mut self) -> Result<(), StoreError> {
        // Rotation turns the old active prefix into a strict sealed range.
        // Promote Buffered/Memory bytes before publishing that range.
        self.writer.device().sync_all()?;
        let committed_len = self
            .writer
            .reserved_boundary()
            .checked_sub(self.active_start_offset)
            .ok_or(StoreError::InvalidManifest)?;
        let sealed = SealedSegment {
            segment_id: self.segment_id,
            generation: self.generation,
            start_offset: self.active_start_offset,
            committed_len,
            sealed_sequence: self.active_sequence,
        };
        let generation = self.allocate_generation()?;
        let segment_id = SegmentId(generation);
        let path = segment_path(&self.config.directory, generation);
        let device = FileDevice::create(&path, self.config.segment_capacity, QUEUE_CAPACITY)?;
        device.sync_all()?;
        sync_directory(&self.config.directory)?;
        let mapping = MappedSegment::open(&path)?;
        let writer =
            SegmentWriter::new(device, segment_id, generation, self.config.segment_capacity);

        self.sealed_segments.push(sealed);
        self.generation = generation;
        self.segment_id = segment_id;
        self.writer = writer;
        self.active_start_offset = 0;
        self.active_sequence = 0;
        self.mapped_segments.insert(generation, mapping);
        if let Err(error) = self.publish_manifest() {
            self.poisoned = true;
            return Err(error);
        }
        self.io_telemetry.rotations = self.io_telemetry.rotations.saturating_add(1);
        Ok(())
    }

    fn record_append_telemetry(&mut self, records: u64, bytes: u64, cohort: bool) {
        if cohort {
            self.io_telemetry.cohort_appends = self.io_telemetry.cohort_appends.saturating_add(1);
        } else {
            self.io_telemetry.singleton_appends =
                self.io_telemetry.singleton_appends.saturating_add(1);
        }
        self.io_telemetry.appended_records =
            self.io_telemetry.appended_records.saturating_add(records);
        self.io_telemetry.encoded_bytes = self.io_telemetry.encoded_bytes.saturating_add(bytes);
        self.io_telemetry.max_records_per_append =
            self.io_telemetry.max_records_per_append.max(records);
        if matches!(self.config.durability, Durability::LocalDurable) {
            self.io_telemetry.durability_flushes =
                self.io_telemetry.durability_flushes.saturating_add(2);
        }
    }

    fn ensure_writable(&self) -> Result<(), StoreError> {
        if self.poisoned {
            Err(StoreError::StorePoisoned)
        } else {
            Ok(())
        }
    }

    fn allocate_generation(&mut self) -> Result<u32, StoreError> {
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .ok_or(StoreError::SegmentOverflow)?;
        Ok(generation)
    }

    fn publish_manifest(&mut self) -> Result<(), StoreError> {
        self.metadata_generation = self
            .metadata_generation
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let mut active_segments: Vec<_> = self
            .sealed_segments
            .iter()
            .map(|segment| SegmentDescriptor {
                id: segment.segment_id.0,
                generation: segment.generation,
                start_offset: segment.start_offset,
                logical_length: segment.committed_len,
                sealed_sequence: segment.sealed_sequence,
            })
            .collect();
        let active_length = self
            .writer
            .reserved_boundary()
            .checked_sub(self.active_start_offset)
            .ok_or(StoreError::InvalidManifest)?;
        active_segments.push(SegmentDescriptor {
            id: self.segment_id.0,
            generation: self.generation,
            start_offset: self.active_start_offset,
            logical_length: active_length,
            sealed_sequence: 0,
        });
        let checkpoint = ManifestCheckpoint {
            generation: self.metadata_generation,
            active_segments,
        };
        let metadata_path = self.config.directory.join("metadata.punah");
        let metadata_existed = metadata_path.exists();
        let io = FileMetadataIo::open(&metadata_path)?;
        if !metadata_existed {
            sync_directory(&self.config.directory)?;
        }
        MetadataPublisher::new(io).publish_checkpoint(&checkpoint, None)?;
        Ok(())
    }

    fn publish_replacement(
        &mut self,
        inputs: &[SegmentRef],
        output_generation: u32,
        output_len: u64,
    ) -> Result<(), StoreError> {
        self.metadata_generation = self
            .metadata_generation
            .checked_add(1)
            .ok_or(StoreError::SequenceOverflow)?;
        let mut active_segments: Vec<_> = self
            .sealed_segments
            .iter()
            .map(|segment| SegmentDescriptor {
                id: segment.segment_id.0,
                generation: segment.generation,
                start_offset: segment.start_offset,
                logical_length: segment.committed_len,
                sealed_sequence: segment.sealed_sequence,
            })
            .collect();
        let active_length = self
            .writer
            .reserved_boundary()
            .checked_sub(self.active_start_offset)
            .ok_or(StoreError::InvalidManifest)?;
        active_segments.push(SegmentDescriptor {
            id: self.segment_id.0,
            generation: self.generation,
            start_offset: self.active_start_offset,
            logical_length: active_length,
            sealed_sequence: 0,
        });
        let checkpoint = ManifestCheckpoint {
            generation: self.metadata_generation,
            active_segments,
        };
        let io = FileMetadataIo::open(self.config.directory.join("metadata.punah"))?;
        let mut publisher = MetadataPublisher::new(io);
        let operation = if output_len == 0 {
            JournalOperation::Retire {
                segments: inputs.to_vec(),
            }
        } else {
            JournalOperation::Replace {
                old: inputs.to_vec(),
                new: vec![SegmentRef {
                    id: output_generation,
                    generation: output_generation,
                }],
            }
        };
        let pointer = publisher.append_journal(&JournalEntry {
            generation: self.metadata_generation,
            operation,
        })?;
        publisher.publish_checkpoint(&checkpoint, Some(pointer))?;
        Ok(())
    }
}

fn replay(
    candidate: &RecoveryCandidate,
    mapping: &MappedSegment,
    versions: &mut HashMap<Vec<u8>, Version>,
) -> Result<(u64, u64), StoreError> {
    let start = candidate.descriptor.start_offset;
    let strict_end = candidate
        .descriptor
        .start_offset
        .checked_add(candidate.descriptor.logical_length)
        .ok_or(StoreError::InvalidManifest)?;
    let mapping_len = u64::try_from(mapping.len()).map_err(|_| StoreError::InvalidManifest)?;
    if start > mapping_len || strict_end > mapping_len {
        return Err(StoreError::InvalidManifest);
    }

    let mut cursor = start;
    let mut sequence = 0_u64;
    let mut record_bytes = Vec::new();
    let header_len = u64::try_from(RECORD_HEADER_LEN).map_err(|_| StoreError::SegmentOverflow)?;
    if candidate.strict_prefix {
        while cursor < strict_end {
            if strict_end.saturating_sub(cursor) < header_len {
                return Err(StoreError::Decode {
                    path: candidate.path.clone(),
                    source: DecodeError::Incomplete,
                });
            }
            let header = read_mapped_header(mapping, cursor)?;
            copy_mapped_record(
                mapping,
                cursor,
                strict_end,
                &header,
                &mut record_bytes,
                &candidate.path,
            )?;
            let decoded = decode_ref(&record_bytes).map_err(|source| StoreError::Decode {
                path: candidate.path.clone(),
                source,
            })?;
            let encoded_len = decoded.encoded_len;
            apply_replayed(candidate, cursor, decoded, versions, &mut sequence)?;
            cursor = cursor
                .checked_add(u64::try_from(encoded_len).map_err(|_| StoreError::SegmentOverflow)?)
                .ok_or(StoreError::SegmentOverflow)?;
        }
    }

    if candidate.scan_suffix {
        cursor = strict_end.max(cursor);
        while mapping_len.saturating_sub(cursor) >= header_len {
            let header = read_mapped_header(mapping, cursor)?;
            if header.iter().all(|byte| *byte == 0) {
                break;
            }
            let copied = copy_mapped_record(
                mapping,
                cursor,
                mapping_len,
                &header,
                &mut record_bytes,
                &candidate.path,
            );
            if matches!(
                copied,
                Err(StoreError::Decode {
                    source: DecodeError::Incomplete | DecodeError::InvalidCommitMarker,
                    ..
                })
            ) {
                break;
            }
            copied?;
            let decoded = match decode_ref(&record_bytes) {
                Ok(value) => value,
                Err(DecodeError::Incomplete | DecodeError::InvalidCommitMarker) => break,
                Err(source) => {
                    return Err(StoreError::Decode {
                        path: candidate.path.clone(),
                        source,
                    });
                }
            };
            let encoded_len = decoded.encoded_len;
            apply_replayed(candidate, cursor, decoded, versions, &mut sequence)?;
            cursor = cursor
                .checked_add(u64::try_from(encoded_len).map_err(|_| StoreError::SegmentOverflow)?)
                .ok_or(StoreError::SegmentOverflow)?;
        }
    }

    Ok((cursor, sequence))
}

fn read_mapped_header(
    mapping: &MappedSegment,
    offset: u64,
) -> Result<[u8; RECORD_HEADER_LEN], StoreError> {
    let mut header = [0_u8; RECORD_HEADER_LEN];
    mapping
        .read_into(offset, &mut header)
        .map_err(StoreError::InvalidLocation)?;
    Ok(header)
}

fn copy_mapped_record(
    mapping: &MappedSegment,
    offset: u64,
    authoritative_end: u64,
    header: &[u8; RECORD_HEADER_LEN],
    output: &mut Vec<u8>,
    path: &Path,
) -> Result<(), StoreError> {
    let length = committed_record_len(header).map_err(|source| StoreError::Decode {
        path: path.to_path_buf(),
        source,
    })?;
    let length_u64 = u64::try_from(length).map_err(|_| StoreError::SegmentOverflow)?;
    let record_end = offset
        .checked_add(length_u64)
        .ok_or(StoreError::SegmentOverflow)?;
    if record_end > authoritative_end {
        return Err(StoreError::Decode {
            path: path.to_path_buf(),
            source: DecodeError::Incomplete,
        });
    }
    output.resize(length, 0);
    output[..RECORD_HEADER_LEN].copy_from_slice(header);
    if length > RECORD_HEADER_LEN {
        let body_offset = offset
            .checked_add(u64::try_from(RECORD_HEADER_LEN).map_err(|_| StoreError::SegmentOverflow)?)
            .ok_or(StoreError::SegmentOverflow)?;
        mapping
            .read_into(body_offset, &mut output[RECORD_HEADER_LEN..])
            .map_err(StoreError::InvalidLocation)?;
    }
    Ok(())
}

fn apply_replayed(
    candidate: &RecoveryCandidate,
    cursor: u64,
    decoded: DecodedRecordRef<'_>,
    versions: &mut HashMap<Vec<u8>, Version>,
    sequence: &mut u64,
) -> Result<(), StoreError> {
    let record_sequence = decoded.record.sequence;
    *sequence = (*sequence).max(record_sequence);
    let stored_len = u32::try_from(decoded.encoded_len).map_err(|_| StoreError::SegmentOverflow)?;
    let logical_len =
        u32::try_from(decoded.record.payload.len()).map_err(|_| StoreError::SegmentOverflow)?;
    let state = match decoded.record.record_type {
        RecordType::Artifact => EntryState::Live,
        RecordType::Tombstone => EntryState::Tombstone,
    };
    let version = Version {
        artifact_id: ArtifactId::new(decoded.record.content_hash),
        sequence: record_sequence,
        location: RecordLocation {
            segment_id: SegmentId(candidate.descriptor.id),
            generation: candidate.descriptor.generation,
            offset: cursor,
            stored_len,
            logical_len,
        },
        expires_at_unix_ns: decoded.record.expires_at_unix_ns,
        state,
    };
    let should_publish = versions
        .get(decoded.record.key)
        .is_none_or(|current| record_sequence >= current.sequence);
    if should_publish {
        versions.insert(decoded.record.key.to_vec(), version);
    }
    Ok(())
}

fn recover_mapped_segments(
    segments: &[RecoveryCandidate],
) -> Result<RecoveredStoreState, StoreError> {
    let mut state = RecoveredStoreState {
        versions: HashMap::new(),
        segments: Vec::with_capacity(segments.len()),
        sequence: 0,
        active_boundary: 0,
        active_start_offset: 0,
        active_sequence: 0,
        active_segment_id: SegmentId(0),
        mappings: HashMap::with_capacity(segments.len()),
    };
    for candidate in segments {
        let mapping = MappedSegment::open(&candidate.path)?;
        let (boundary, sequence) = replay(candidate, &mapping, &mut state.versions)?;
        if candidate.strict_prefix
            && !candidate.active
            && sequence != candidate.descriptor.sealed_sequence
        {
            return Err(StoreError::InvalidManifest);
        }
        state.sequence = state.sequence.max(sequence);
        if candidate.active {
            state.active_boundary = boundary;
            state.active_start_offset = candidate.descriptor.start_offset;
            state.active_sequence = sequence;
            state.active_segment_id = SegmentId(candidate.descriptor.id);
        } else {
            state.segments.push(SealedSegment {
                segment_id: SegmentId(candidate.descriptor.id),
                generation: candidate.descriptor.generation,
                start_offset: candidate.descriptor.start_offset,
                committed_len: boundary
                    .checked_sub(candidate.descriptor.start_offset)
                    .ok_or(StoreError::InvalidManifest)?,
                sealed_sequence: if candidate.strict_prefix {
                    candidate.descriptor.sealed_sequence
                } else {
                    sequence
                },
            });
        }
        state
            .mappings
            .insert(candidate.descriptor.generation, mapping);
    }
    Ok(state)
}

fn decode_version_payload(
    mut bytes: Vec<u8>,
    path: &Path,
    key: &[u8],
    version: &Version,
) -> Result<Vec<u8>, StoreError> {
    let (payload_start, payload_len) = {
        let decoded = decode_ref(&bytes).map_err(|source| StoreError::Decode {
            path: path.to_path_buf(),
            source,
        })?;
        if decoded.encoded_len != bytes.len()
            || decoded.record.key != key
            || decoded.record.sequence != version.sequence
            || decoded.record.payload.len()
                != usize::try_from(version.location.logical_len)
                    .map_err(|_| StoreError::LocationMismatch)?
        {
            return Err(StoreError::LocationMismatch);
        }
        let dependency_bytes = decoded
            .record
            .dependencies()
            .len()
            .checked_mul(32)
            .ok_or(StoreError::LocationMismatch)?;
        let payload_start = RECORD_HEADER_LEN
            .checked_add(decoded.record.namespace.len())
            .and_then(|offset| offset.checked_add(decoded.record.key.len()))
            .and_then(|offset| offset.checked_add(dependency_bytes))
            .ok_or(StoreError::LocationMismatch)?;
        (payload_start, decoded.record.payload.len())
    };
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or(StoreError::LocationMismatch)?;
    bytes.copy_within(payload_start..payload_end, 0);
    bytes.truncate(payload_len);
    Ok(bytes)
}

fn is_live_at(version: &Version, now: u64) -> bool {
    version.state == EntryState::Live
        && version.expires_at_unix_ns.is_none_or(|expiry| expiry > now)
}

fn recovery_candidates(
    directory: &Path,
    descriptors: &[SegmentDescriptor],
) -> Result<Vec<RecoveryCandidate>, StoreError> {
    if descriptors.is_empty() {
        return Err(StoreError::InvalidManifest);
    }
    let mut ids = HashSet::with_capacity(descriptors.len());
    let mut generations = HashSet::with_capacity(descriptors.len());
    let last = descriptors.len() - 1;
    descriptors
        .iter()
        .enumerate()
        .map(|(index, descriptor)| {
            if !ids.insert(descriptor.id) || !generations.insert(descriptor.generation) {
                return Err(StoreError::InvalidManifest);
            }
            if index == last && descriptor.sealed_sequence != 0 {
                return Err(StoreError::InvalidManifest);
            }
            Ok(RecoveryCandidate {
                descriptor: *descriptor,
                path: segment_path(directory, descriptor.generation),
                active: index == last,
                strict_prefix: true,
                scan_suffix: index == last,
            })
        })
        .collect()
}

fn discovered_candidates(files: Vec<(u32, PathBuf)>) -> Vec<RecoveryCandidate> {
    let last = files.len().checked_sub(1);
    files
        .into_iter()
        .enumerate()
        .map(|(index, (generation, path))| RecoveryCandidate {
            descriptor: SegmentDescriptor {
                id: generation,
                generation,
                start_offset: 0,
                logical_length: 0,
                sealed_sequence: 0,
            },
            path,
            active: Some(index) == last,
            strict_prefix: false,
            scan_suffix: true,
        })
        .collect()
}

fn segment_files(directory: &Path) -> io::Result<Vec<(u32, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(value) = name
            .strip_prefix("segment-")
            .and_then(|value| value.strip_suffix(".punah"))
            .and_then(|value| value.parse().ok())
        {
            files.push((value, path));
        }
    }
    Ok(files)
}

fn segment_path(directory: &Path, generation: u32) -> PathBuf {
    directory.join(format!("segment-{generation:010}.punah"))
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    fs::File::open(directory)?.sync_all()
}

fn poll_append(writer: &mut SegmentWriter<FileDevice>) -> Result<AppendCompletion, StoreError> {
    loop {
        if let Some(result) = writer.poll() {
            return result.map_err(StoreError::Append);
        }
        std::thread::yield_now();
    }
}

fn unix_ns(time: SystemTime) -> Result<u64, StoreError> {
    let nanos = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::ClockBeforeEpoch)?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| StoreError::ClockBeforeEpoch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use punah_events::{EventBus, Poll};
    use punah_record::encode;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "punah-store-{}-{}",
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
    fn config(path: &Path, capacity: u64) -> StoreConfig {
        StoreConfig {
            directory: path.to_path_buf(),
            namespace: Namespace::new("default").expect("namespace"),
            segment_capacity: capacity,
            durability: Durability::LocalDurable,
        }
    }

    fn recovery_record(key: &[u8], sequence: u64) -> Record {
        Record {
            record_type: RecordType::Artifact,
            flags: 0,
            namespace: Namespace::new("default").expect("namespace"),
            key: key.to_vec(),
            sequence,
            created_at_unix_ns: sequence,
            expires_at_unix_ns: None,
            operation_hash: [0; 32],
            content_hash: [u8::try_from(sequence).unwrap_or(u8::MAX); 32],
            dependencies: Vec::new(),
            payload: vec![u8::try_from(sequence).unwrap_or(u8::MAX); 16],
        }
    }

    fn candidate(length: u64, active: bool, scan_suffix: bool) -> RecoveryCandidate {
        RecoveryCandidate {
            descriptor: SegmentDescriptor {
                id: 17,
                generation: 3,
                start_offset: 0,
                logical_length: length,
                sealed_sequence: u64::from(!active),
            },
            path: PathBuf::from("recovery-test.punah"),
            active,
            strict_prefix: true,
            scan_suffix,
        }
    }

    fn map_bytes(bytes: &[u8]) -> (TempDir, MappedSegment) {
        let directory = TempDir::new();
        let path = directory.0.join("mapped-test.punah");
        fs::write(&path, bytes).expect("write mapped fixture");
        let mapping = MappedSegment::open(path).expect("map fixture");
        (directory, mapping)
    }

    #[test]
    fn recovery_obeys_sealed_boundary_and_active_suffix() {
        let first = encode(&recovery_record(b"first", 1)).expect("first");
        let second = encode(&recovery_record(b"second", 2)).expect("second");
        let first_len = u64::try_from(first.len()).expect("length");
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);
        bytes.resize(bytes.len() + 4096, 0);
        let (_directory, mapping) = map_bytes(&bytes);

        let mut sealed_versions = HashMap::new();
        let (sealed_boundary, _) = replay(
            &candidate(first_len, false, false),
            &mapping,
            &mut sealed_versions,
        )
        .expect("sealed replay");
        assert_eq!(sealed_boundary, first_len);
        assert!(sealed_versions.contains_key(b"first".as_slice()));
        assert!(!sealed_versions.contains_key(b"second".as_slice()));

        let mut active_versions = HashMap::new();
        let (active_boundary, sequence) = replay(
            &candidate(first_len, true, true),
            &mapping,
            &mut active_versions,
        )
        .expect("active replay");
        assert_eq!(
            active_boundary,
            u64::try_from(first.len() + second.len()).unwrap()
        );
        assert_eq!(sequence, 2);
        assert_eq!(active_versions.len(), 2);
        assert_eq!(
            active_versions[b"second".as_slice()].location.segment_id,
            SegmentId(17)
        );
    }

    #[test]
    fn strict_manifest_boundary_rejects_partial_record() {
        let bytes = encode(&recovery_record(b"partial", 1)).expect("record");
        let (_directory, mapping) = map_bytes(&bytes);
        let mut versions = HashMap::new();
        assert!(matches!(
            replay(
                &candidate(u64::try_from(bytes.len() - 8).unwrap(), false, false),
                &mapping,
                &mut versions,
            ),
            Err(StoreError::Decode {
                source: DecodeError::Incomplete,
                ..
            })
        ));
    }

    #[test]
    fn active_suffix_reports_nonzero_corrupt_header() {
        let first = encode(&recovery_record(b"first", 1)).expect("record");
        let first_len = u64::try_from(first.len()).expect("length");
        let mut bytes = first;
        bytes.extend_from_slice(&[1; RECORD_HEADER_LEN]);
        let (_directory, mapping) = map_bytes(&bytes);
        let mut versions = HashMap::new();
        assert!(matches!(
            replay(&candidate(first_len, true, true), &mapping, &mut versions,),
            Err(StoreError::Decode {
                source: DecodeError::BadMagic,
                ..
            })
        ));
    }

    #[test]
    fn manifest_rejects_duplicate_segment_identity() {
        let descriptor = SegmentDescriptor {
            id: 7,
            generation: 9,
            start_offset: 0,
            logical_length: 0,
            sealed_sequence: 0,
        };
        assert!(matches!(
            recovery_candidates(Path::new("."), &[descriptor, descriptor]),
            Err(StoreError::InvalidManifest)
        ));
    }

    #[test]
    fn manifest_rejects_a_nonzero_sequence_on_the_active_descriptor() {
        let descriptor = SegmentDescriptor {
            id: 7,
            generation: 9,
            start_offset: 0,
            logical_length: 0,
            sealed_sequence: 1,
        };
        assert!(matches!(
            recovery_candidates(Path::new("."), &[descriptor]),
            Err(StoreError::InvalidManifest)
        ));
    }

    #[test]
    fn recovery_rejects_a_sealed_sequence_fence_that_does_not_match_contents() {
        let dir = TempDir::new();
        let sealed_path = segment_path(&dir.0, 3);
        let active_path = segment_path(&dir.0, 4);
        let encoded = encode(&recovery_record(b"sealed", 2)).expect("encode sealed record");
        fs::write(&sealed_path, &encoded).expect("write sealed fixture");
        fs::write(&active_path, vec![0; RECORD_HEADER_LEN]).expect("write active fixture");
        let candidates = vec![
            RecoveryCandidate {
                descriptor: SegmentDescriptor {
                    id: 3,
                    generation: 3,
                    start_offset: 0,
                    logical_length: u64::try_from(encoded.len()).expect("record length"),
                    sealed_sequence: 1,
                },
                path: sealed_path,
                active: false,
                strict_prefix: true,
                scan_suffix: false,
            },
            RecoveryCandidate {
                descriptor: SegmentDescriptor {
                    id: 4,
                    generation: 4,
                    start_offset: 0,
                    logical_length: 0,
                    sealed_sequence: 0,
                },
                path: active_path,
                active: true,
                strict_prefix: true,
                scan_suffix: true,
            },
        ];
        assert!(matches!(
            recover_mapped_segments(&candidates),
            Err(StoreError::InvalidManifest)
        ));
    }

    #[test]
    fn nonzero_active_start_is_preserved_when_recovery_rotates_it() {
        let dir = TempDir::new();
        let capacity = 512_u64;
        let start_offset = 128_u64;
        let generation = 3_u32;
        let record = recovery_record(b"before", 7);
        let encoded = encode(&record).expect("encode fixture");
        let encoded_len = u64::try_from(encoded.len()).expect("record length");
        let segment_path = segment_path(&dir.0, generation);
        let mut segment = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&segment_path)
            .expect("create fixture segment");
        segment.set_len(capacity).expect("size fixture segment");
        segment
            .seek(SeekFrom::Start(start_offset))
            .expect("seek fixture record");
        segment.write_all(&encoded).expect("write fixture record");
        segment.sync_all().expect("persist fixture segment");
        drop(segment);

        let checkpoint = ManifestCheckpoint {
            generation: 1,
            active_segments: vec![SegmentDescriptor {
                id: 17,
                generation,
                start_offset,
                logical_length: encoded_len,
                sealed_sequence: 0,
            }],
        };
        MetadataPublisher::new(
            FileMetadataIo::open(dir.0.join("metadata.punah")).expect("open metadata"),
        )
        .publish_checkpoint(&checkpoint, None)
        .expect("publish fixture manifest");

        let cfg = config(&dir.0, capacity);
        let mut store = Store::open(cfg.clone()).expect("open fixture");
        assert_eq!(store.get(b"before").unwrap(), Some(vec![7; 16]));
        store
            .set(b"after", vec![8; 128], None)
            .expect("append after recovery");
        assert_eq!(
            store.sealed_segments(),
            &[SealedSegment {
                segment_id: SegmentId(17),
                generation,
                start_offset,
                committed_len: encoded_len,
                sealed_sequence: 7,
            }]
        );
        drop(store);

        let recovered_root = MetadataPublisher::new(
            FileMetadataIo::open(dir.0.join("metadata.punah")).expect("reopen metadata"),
        )
        .recover()
        .expect("recover manifest");
        assert_eq!(
            recovered_root.checkpoint.active_segments[0],
            SegmentDescriptor {
                id: 17,
                generation,
                start_offset,
                logical_length: encoded_len,
                sealed_sequence: 7,
            }
        );

        let reopened = Store::open(cfg).expect("reopen store");
        assert_eq!(reopened.get(b"before").unwrap(), Some(vec![7; 16]));
        assert_eq!(reopened.get(b"after").unwrap(), Some(vec![8; 128]));
        assert_eq!(reopened.sealed_segments()[0].start_offset, start_offset);
        assert_eq!(reopened.sealed_segments()[0].committed_len, encoded_len);
        assert_eq!(reopened.sealed_segments()[0].sealed_sequence, 7);
    }

    #[test]
    fn corrupt_metadata_cannot_fall_back_to_segment_discovery() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        let mut store = Store::open(cfg.clone()).expect("open");
        store
            .set(b"authoritative", b"value".to_vec(), None)
            .expect("set");
        drop(store);

        let metadata_path = dir.0.join("metadata.punah");
        let mut metadata = fs::OpenOptions::new()
            .write(true)
            .open(&metadata_path)
            .expect("open metadata");
        metadata.seek(SeekFrom::Start(0)).expect("seek roots");
        metadata
            .write_all(&vec![
                0xff;
                usize::try_from(punah_metadata::SUPERBLOCK_REGION_SIZE)
                    .expect("root region fits")
            ])
            .expect("corrupt roots");
        metadata.sync_data().expect("persist corruption");
        drop(metadata);
        let before = fs::read(&metadata_path).expect("snapshot corrupt metadata");

        assert!(matches!(
            Store::open(cfg),
            Err(StoreError::Metadata(PublicationError::NoRecoverableRoot))
        ));
        assert_eq!(
            fs::read(metadata_path).expect("metadata remains unchanged"),
            before
        );
    }

    #[test]
    fn unmanaged_segments_require_explicit_import_and_then_reopen_normally() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        let mut store = Store::open(cfg.clone()).expect("open");
        store
            .set(b"legacy", b"payload".to_vec(), None)
            .expect("set");
        drop(store);

        let metadata_path = dir.0.join("metadata.punah");
        fs::remove_file(&metadata_path).expect("simulate legacy directory");
        assert!(matches!(
            Store::open(cfg.clone()),
            Err(StoreError::ExplicitImportRequired)
        ));
        assert!(!metadata_path.exists(), "normal open must remain read-only");

        let imported = Store::import_unmanaged_segments(cfg.clone()).expect("explicit import");
        assert_eq!(imported.get(b"legacy").unwrap(), Some(b"payload".to_vec()));
        drop(imported);
        assert!(metadata_path.exists());
        assert_eq!(
            Store::open(cfg.clone()).unwrap().get(b"legacy").unwrap(),
            Some(b"payload".to_vec())
        );
        assert!(matches!(
            Store::import_unmanaged_segments(cfg),
            Err(StoreError::AlreadyInitialized)
        ));
    }

    #[test]
    fn explicit_import_rejects_an_empty_directory() {
        let dir = TempDir::new();
        assert!(matches!(
            Store::import_unmanaged_segments(config(&dir.0, 4096)),
            Err(StoreError::NoSegmentsToImport)
        ));
        assert!(!dir.0.join("metadata.punah").exists());
    }

    #[test]
    fn set_get_and_overwrite() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.set(b"key", b"one".to_vec(), None).expect("set");
        store.set(b"key", b"two".to_vec(), None).expect("overwrite");
        assert_eq!(store.get(b"key").unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn mutation_cohort_preserves_repeated_key_order_across_restart() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        let mut store = Store::open(cfg.clone()).expect("open");
        let values = store
            .mutate_batch(vec![
                StoreMutation::Set {
                    key: b"key".to_vec(),
                    payload: b"one".to_vec(),
                    ttl: None,
                },
                StoreMutation::Delete {
                    key: b"key".to_vec(),
                },
                StoreMutation::Set {
                    key: b"key".to_vec(),
                    payload: b"two".to_vec(),
                    ttl: None,
                },
            ])
            .expect("cohort");
        assert_eq!(
            values,
            vec![
                MutationValue::Set,
                MutationValue::Delete(true),
                MutationValue::Set
            ]
        );
        assert_eq!(store.get(b"key").unwrap(), Some(b"two".to_vec()));
        drop(store);

        let reopened = Store::open(cfg).expect("reopen");
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"two".to_vec()));
        assert_eq!(reopened.version_snapshots()[0].sequence, 3);
    }

    #[test]
    fn io_telemetry_reports_actual_singleton_and_cohort_durability_groups() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.set(b"single", b"one".to_vec(), None).expect("single");
        store
            .mutate_batch(vec![
                StoreMutation::Set {
                    key: b"a".to_vec(),
                    payload: b"two".to_vec(),
                    ttl: None,
                },
                StoreMutation::Set {
                    key: b"b".to_vec(),
                    payload: b"three".to_vec(),
                    ttl: None,
                },
                StoreMutation::Delete { key: b"a".to_vec() },
            ])
            .expect("cohort");
        let telemetry = store.io_telemetry();
        assert_eq!(telemetry.singleton_appends, 1);
        assert_eq!(telemetry.cohort_appends, 1);
        assert_eq!(telemetry.appended_records, 4);
        assert_eq!(telemetry.durability_flushes, 4);
        assert_eq!(telemetry.max_records_per_append, 3);
        assert!(telemetry.encoded_bytes >= 4 * 128);
    }

    #[test]
    fn mutation_cohort_splits_cleanly_at_segment_boundaries() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 512);
        let mutations: Vec<_> = (0_u8..8)
            .map(|index| StoreMutation::Set {
                key: format!("key-{index}").into_bytes(),
                payload: vec![index; 80],
                ttl: None,
            })
            .collect();
        let mut store = Store::open(cfg.clone()).expect("open");
        assert!(
            store
                .mutate_batch(mutations)
                .expect("rotating cohort")
                .iter()
                .all(|value| *value == MutationValue::Set)
        );
        assert!(!store.sealed_segments().is_empty());
        drop(store);

        let reopened = Store::open(cfg).expect("reopen");
        for index in 0_u8..8 {
            assert_eq!(
                reopened.get(format!("key-{index}").as_bytes()).unwrap(),
                Some(vec![index; 80])
            );
        }
    }

    #[test]
    fn rotate_failure_after_a_committed_batch_prefix_fail_stops_the_store() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 512)).expect("open");
        fs::remove_dir_all(&dir.0).expect("remove directory to fail the later rotation");
        let mutations = (0_u8..3)
            .map(|index| StoreMutation::Set {
                key: format!("key-{index}").into_bytes(),
                payload: vec![index; 80],
                ttl: None,
            })
            .collect();

        assert!(matches!(
            store.mutate_batch(mutations),
            Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
        assert!(store.poisoned);
        assert!(matches!(
            store.get(b"key-0"),
            Err(StoreError::StorePoisoned)
        ));
        assert!(matches!(
            store.set(b"later", b"rejected".to_vec(), None),
            Err(StoreError::StorePoisoned)
        ));
    }

    #[test]
    fn oversized_record_fails_without_rotating_forever() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 512)).expect("open");
        assert!(matches!(
            store.set(b"large", vec![1; 1024], None),
            Err(StoreError::Append(AppendError::SegmentFull))
        ));
        assert!(store.sealed_segments().is_empty());
        store
            .set(b"small", b"still writable".to_vec(), None)
            .expect("small append");
        assert_eq!(
            store.get(b"small").unwrap(),
            Some(b"still writable".to_vec())
        );
    }

    #[test]
    fn mapped_get_reports_corruption_instead_of_a_miss() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.set(b"key", b"value".to_vec(), None).expect("set");
        let location = store.version_snapshots()[0].location;
        let payload_offset = location.offset + 128 + 7 + 3;
        let path = segment_path(&dir.0, location.generation);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open writer");
        file.seek(SeekFrom::Start(payload_offset)).expect("seek");
        file.write_all(&[0xff]).expect("corrupt payload");
        file.sync_data().expect("publish corruption");

        assert!(matches!(
            store.get(b"key"),
            Err(StoreError::Decode {
                source: DecodeError::RecordChecksumMismatch,
                ..
            })
        ));
    }

    #[test]
    fn arbitrary_binary_keys_use_exact_equality() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.set(&[0, 1, 0], b"left".to_vec(), None).expect("set");
        store.set(&[0, 1], b"right".to_vec(), None).expect("set");
        assert_eq!(store.get(&[0, 1, 0]).unwrap(), Some(b"left".to_vec()));
        assert_eq!(store.get(&[0, 1]).unwrap(), Some(b"right".to_vec()));
        assert_eq!(store.get(&[0, 1, 0, 0]).unwrap(), None);
    }

    #[test]
    fn delete_survives_restart() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            store.set(b"key", b"value".to_vec(), None).expect("set");
            assert!(store.delete(b"key").expect("delete"));
        }
        let reopened = Store::open(cfg).expect("reopen");
        assert_eq!(reopened.get(b"key").unwrap(), None);
        let snapshots = reopened.version_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].key, b"key");
        assert_eq!(snapshots[0].sequence, 2);
        assert_eq!(snapshots[0].state, EntryState::Tombstone);
        assert!(snapshots[0].location.stored_len >= 128);
    }

    #[test]
    fn ttl_hides_expired_values() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store
            .set(b"key", b"value".to_vec(), Some(Duration::ZERO))
            .expect("set");
        assert_eq!(store.get(b"key").unwrap(), None);
    }

    #[test]
    fn bounded_cleanup_tombstones_expired_values_across_restart() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        let mut store = Store::open(cfg.clone()).expect("open");
        for key in [b"one".as_slice(), b"two".as_slice()] {
            store
                .set(key, b"value".to_vec(), Some(Duration::ZERO))
                .expect("set");
        }
        assert_eq!(
            store.cleanup_expired(1).expect("first cleanup"),
            CleanupReport {
                tombstoned: 1,
                budget_exhausted: true,
            }
        );
        assert_eq!(
            store.cleanup_expired(1).expect("second cleanup"),
            CleanupReport {
                tombstoned: 1,
                budget_exhausted: false,
            }
        );
        drop(store);
        let reopened = Store::open(cfg).expect("reopen");
        assert_eq!(reopened.get(b"one").unwrap(), None);
        assert_eq!(reopened.get(b"two").unwrap(), None);
    }

    #[test]
    fn forced_rotation_recovers_all_segments_and_reopens_tail() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 320);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            for index in 0..5 {
                store
                    .set(format!("key-{index}").as_bytes(), vec![index; 40], None)
                    .expect("set");
            }
        }
        let mut store = Store::open(cfg.clone()).expect("reopen");
        let snapshots = store.version_snapshots();
        assert_eq!(snapshots.len(), 5);
        assert_eq!(
            snapshots
                .iter()
                .map(|item| item.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert!(snapshots.windows(2).all(|pair| {
            pair[0].location.generation <= pair[1].location.generation
                && (pair[0].location.generation != pair[1].location.generation
                    || pair[0].location.offset < pair[1].location.offset)
        }));
        assert!(!store.sealed_segments().is_empty());
        assert!(
            store
                .sealed_segments()
                .iter()
                .all(|segment| segment.committed_len > 0)
        );
        for index in 0..5 {
            assert_eq!(
                store.get(format!("key-{index}").as_bytes()).unwrap(),
                Some(vec![index; 40])
            );
        }
        store
            .set(b"after", b"restart".to_vec(), None)
            .expect("append");
        assert_eq!(
            Store::open(cfg)
                .expect("second reopen")
                .get(b"after")
                .unwrap(),
            Some(b"restart".to_vec())
        );
    }

    #[test]
    fn artifact_identity_describes_payload_not_lookup_key() {
        assert_eq!(artifact_id(b"payload"), artifact_id(b"payload"));
        assert_ne!(artifact_id(b"payload"), artifact_id(b"other"));
    }

    #[test]
    fn compaction_reclaims_stale_versions_and_preserves_tombstones_after_restart() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 320);
        let mut store = Store::open(cfg.clone()).expect("open");
        store.set(b"key", b"old".to_vec(), None).expect("old");
        store.set(b"key", b"new".to_vec(), None).expect("new");
        store
            .set(b"gone", b"value".to_vec(), None)
            .expect("gone value");
        store.delete(b"gone").expect("delete");
        store
            .set(b"filler", vec![7; 40], None)
            .expect("force tombstone segment sealed");

        let inputs: Vec<_> = store
            .sealed_segments()
            .iter()
            .map(|segment| SegmentRef {
                id: segment.segment_id.0,
                generation: segment.generation,
            })
            .collect();
        assert!(inputs.len() >= 2);
        let plan = CompactionPlan {
            input_segments: inputs.clone(),
            input_bytes: 0,
            reclaimable_bytes: 0,
            required_output_bytes: 0,
            required_headroom_bytes: 0,
        };
        let report = store.compact(&plan).expect("compact");
        assert_eq!(report.input_segments, inputs.len());
        assert_eq!(report.copied_records, 2);
        assert_eq!(report.removed_segments, inputs.len());
        assert_eq!(store.get(b"key").unwrap(), Some(b"new".to_vec()));
        assert_eq!(store.get(b"gone").unwrap(), None);
        drop(store);

        let reopened = Store::open(cfg).expect("reopen");
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"new".to_vec()));
        assert_eq!(reopened.get(b"gone").unwrap(), None);
        assert_eq!(
            reopened
                .version_snapshots()
                .into_iter()
                .find(|snapshot| snapshot.key == b"gone")
                .expect("tombstone")
                .state,
            EntryState::Tombstone
        );
        for input in inputs {
            assert!(!segment_path(&dir.0, input.generation).exists());
        }
    }

    #[test]
    fn sealed_metrics_count_only_superseded_records_as_reclaimable() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 320)).expect("open");
        store.set(b"key", b"old".to_vec(), None).expect("old");
        store.set(b"key", b"new".to_vec(), None).expect("new");
        store
            .set(b"expired", b"still fenced".to_vec(), Some(Duration::ZERO))
            .expect("expired");
        store
            .set(b"filler", vec![7; 40], None)
            .expect("seal prior records");

        let metrics = store.sealed_segment_metrics();
        assert!(!metrics.is_empty());
        assert!(metrics.iter().all(|item| {
            item.stats.total_bytes == item.stats.live_bytes + item.stats.dead_bytes
                && item.stats.expired_bytes == 0
        }));
        assert!(metrics.iter().any(|item| item.stats.dead_bytes > 0));

        let latest = store
            .version_snapshots()
            .into_iter()
            .find(|snapshot| snapshot.key == b"expired")
            .expect("latest expired record");
        let owner = metrics
            .iter()
            .find(|item| {
                item.segment.segment_id == latest.location.segment_id
                    && item.segment.generation == latest.location.generation
            })
            .expect("expired record sealed");
        assert!(owner.stats.live_bytes >= u64::from(latest.location.stored_len));

        let plan = store
            .plan_compaction(PlanningLimits {
                available_bytes: u64::MAX,
                emergency_reserve_bytes: 0,
                max_input_bytes: 320,
                output_safety_margin_bytes: 0,
                minimum_reclaim_ratio_bps: 0,
                minimum_input_segments: 1,
            })
            .expect("plan stale record");
        assert!(plan.input_bytes <= 320);
        assert!(plan.reclaimable_bytes > 0);
        assert!(plan.required_output_bytes <= 320);
    }

    #[test]
    fn all_dead_compaction_publishes_retirement_without_an_empty_output_file() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 256)).expect("open");
        store.set(b"key", b"old".to_vec(), None).expect("old");
        store.set(b"key", b"new".to_vec(), None).expect("new");
        let next_generation = store.next_generation;
        let plan = store
            .plan_compaction(PlanningLimits {
                available_bytes: 256,
                emergency_reserve_bytes: 0,
                max_input_bytes: 256,
                output_safety_margin_bytes: 0,
                minimum_reclaim_ratio_bps: 0,
                minimum_input_segments: 1,
            })
            .expect("all-dead plan");
        assert_eq!(plan.required_output_bytes, 0);

        let report = store.compact(&plan).expect("retire");
        assert_eq!(report.copied_records, 0);
        assert_eq!(report.copied_bytes, 0);
        assert_eq!(store.next_generation, next_generation);
        assert!(!segment_path(&dir.0, next_generation).exists());
        assert_eq!(store.get(b"key").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn manifest_ignores_orphan_segment_files() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            store.set(b"real", b"value".to_vec(), None).expect("set");
        }
        fs::copy(segment_path(&dir.0, 0), segment_path(&dir.0, 999)).expect("create crash orphan");
        let reopened = Store::open(cfg).expect("reopen from manifest");
        assert_eq!(reopened.get(b"real").unwrap(), Some(b"value".to_vec()));
        assert!(
            reopened
                .version_snapshots()
                .iter()
                .all(|snapshot| snapshot.location.generation != 999)
        );
    }

    #[test]
    fn rotation_output_durable_before_root_is_ignored_and_never_reused() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 4096);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            store.set(b"real", b"value".to_vec(), None).expect("set");
        }
        let orphan_generation = 1_u32;
        let orphan_path = segment_path(&dir.0, orphan_generation);
        let orphan = FileDevice::create(&orphan_path, cfg.segment_capacity, QUEUE_CAPACITY)
            .expect("create would-be rotation output");
        orphan.sync_all().expect("persist orphan");
        drop(orphan);
        sync_directory(&dir.0).expect("persist orphan filename");

        let mut reopened = Store::open(cfg).expect("recover old root");
        assert_eq!(reopened.get(b"real").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.generation, 0);
        assert_eq!(reopened.next_generation, 2);
        assert!(orphan_path.exists());
        reopened
            .set(b"later", b"not clobbered".to_vec(), None)
            .expect("continue after orphan");
    }

    #[test]
    fn compaction_output_before_root_is_an_orphan_and_old_inputs_remain_authoritative() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 320);
        let mut store = Store::open(cfg.clone()).expect("open");
        store.set(b"key", b"old".to_vec(), None).expect("old");
        store.set(b"key", b"new".to_vec(), None).expect("new");
        store
            .set(b"filler", vec![7; 40], None)
            .expect("seal inputs");
        let inputs: Vec<_> = store
            .sealed_segments()
            .iter()
            .map(|segment| SegmentRef {
                id: segment.segment_id.0,
                generation: segment.generation,
            })
            .collect();
        let prepared = store.prepare_compaction(&inputs).expect("durable output");
        assert!(prepared.path.exists());
        let orphan_generation = prepared.generation;
        drop(prepared);
        drop(store);

        let reopened = Store::open(cfg).expect("old root remains authoritative");
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"new".to_vec()));
        assert!(
            reopened
                .sealed_segments()
                .iter()
                .all(|segment| segment.generation != orphan_generation)
        );
        assert!(segment_path(&dir.0, orphan_generation).exists());
    }

    #[test]
    fn compaction_root_before_input_deletion_recovers_only_replacement_membership() {
        let dir = TempDir::new();
        let cfg = config(&dir.0, 320);
        let mut store = Store::open(cfg.clone()).expect("open");
        store.set(b"key", b"old".to_vec(), None).expect("old");
        store.set(b"key", b"new".to_vec(), None).expect("new");
        store
            .set(b"filler", vec![7; 40], None)
            .expect("seal inputs");
        let inputs: Vec<_> = store
            .sealed_segments()
            .iter()
            .map(|segment| SegmentRef {
                id: segment.segment_id.0,
                generation: segment.generation,
            })
            .collect();
        let mut prepared = store.prepare_compaction(&inputs).expect("durable output");
        store.writer.device().sync_all().expect("active prefix");
        store.sealed_segments.retain(|sealed| {
            !inputs.iter().any(|input| {
                input.id == sealed.segment_id.0 && input.generation == sealed.generation
            })
        });
        let mapping = prepared.mapping.take().expect("nonempty output");
        store.sealed_segments.push(SealedSegment {
            segment_id: SegmentId(prepared.generation),
            generation: prepared.generation,
            start_offset: 0,
            committed_len: prepared.committed_len,
            sealed_sequence: prepared.sealed_sequence,
        });
        store.mapped_segments.insert(prepared.generation, mapping);
        store
            .publish_replacement(&inputs, prepared.generation, prepared.committed_len)
            .expect("publish new authority");
        store.apply_relocations(prepared.relocations);
        for input in &inputs {
            assert!(segment_path(&dir.0, input.generation).exists());
        }
        let output_generation = prepared.generation;
        drop(store);

        let reopened = Store::open(cfg).expect("recover replacement root");
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"new".to_vec()));
        assert!(
            reopened
                .sealed_segments()
                .iter()
                .any(|segment| segment.generation == output_generation)
        );
        assert!(inputs.iter().all(|input| {
            reopened
                .sealed_segments()
                .iter()
                .all(|segment| segment.generation != input.generation)
        }));
    }

    #[test]
    fn poisoned_store_rejects_every_mutating_entry_point() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.poisoned = true;
        assert!(matches!(
            store.set(b"key", b"value".to_vec(), None),
            Err(StoreError::StorePoisoned)
        ));
        assert!(matches!(
            store.delete(b"key"),
            Err(StoreError::StorePoisoned)
        ));
        assert!(matches!(
            store.cleanup_expired(1),
            Err(StoreError::StorePoisoned)
        ));
        let empty = CompactionPlan {
            input_segments: Vec::new(),
            input_bytes: 0,
            reclaimable_bytes: 0,
            required_output_bytes: 0,
            required_headroom_bytes: 0,
        };
        assert!(matches!(
            store.compact(&empty),
            Err(StoreError::StorePoisoned)
        ));
    }

    #[test]
    fn committed_mutations_publish_facts_and_snapshot_resynchronizes() {
        let dir = TempDir::new();
        let bus = EventBus::new(4);
        let mut observer = bus.subscribe();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.set_event_publisher(bus.publisher());

        store.set(b"key", b"value".to_vec(), None).expect("set");
        let stored = match observer.try_next() {
            Poll::Event(event) => event,
            other => panic!("expected stored event, got {other:?}"),
        };
        assert!(matches!(
            stored.fact,
            LifecycleFact::ArtifactStored { ref key, storage_sequence: 1, .. }
                if key == b"key"
        ));

        assert!(store.delete(b"key").expect("delete"));
        assert!(matches!(
            observer.try_next(),
            Poll::Event(event)
                if matches!(event.fact, LifecycleFact::ArtifactRemoved {
                    reason: RemovalReason::Deleted,
                    storage_sequence: 2,
                    ..
                })
        ));

        let snapshot = store.lifecycle_snapshot();
        assert_eq!(snapshot.through_cursor, 2);
        assert_eq!(snapshot.artifacts.len(), 1);
        assert_eq!(snapshot.artifacts[0].state, EntryState::Tombstone);
    }
}
