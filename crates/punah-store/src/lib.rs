#![forbid(unsafe_code)]

//! Synchronous reference store built over Punah's polling storage primitives.
//!
//! Exact key equality uses the original durable key bytes. Immutable payloads
//! receive a BLAKE3-256 artifact identity; routing hashes are not identities.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use punah_compaction::CompactionPlan;
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
use punah_record::{DecodeError, EncodeError, Record, RecordType, decode, encode_uncommitted};
use punah_types::{
    ArtifactId, ContentHash, Durability, EntryState, Namespace, RecordLocation, SegmentId,
};

const QUEUE_CAPACITY: usize = 4;
type Relocation = (Vec<u8>, u64, RecordLocation, RecordLocation);
type CopyResult = (usize, u64, Vec<Relocation>);

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
    pub committed_len: u64,
}

/// Durable single-process reference key/value store.
pub struct Store {
    config: StoreConfig,
    writer: SegmentWriter<FileDevice>,
    generation: u32,
    sequence: u64,
    versions: HashMap<Vec<u8>, Version>,
    mapped_segments: HashMap<u32, MappedSegment>,
    sealed_segments: Vec<SealedSegment>,
    metadata_generation: u64,
    next_generation: u32,
    poisoned: bool,
    events: Option<EventPublisher>,
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

impl Store {
    /// Opens or creates a store and replays all deterministically named segment files.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, filesystem failures, or
    /// corruption within committed records.
    pub fn open(config: StoreConfig) -> Result<Self, StoreError> {
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
        let mut publisher = MetadataPublisher::new(FileMetadataIo::open(&metadata_path)?);
        let recovered_root = match publisher.recover() {
            Ok(root) => Some(root),
            Err(PublicationError::NoRecoverableRoot) => None,
            Err(error) => return Err(error.into()),
        };
        let segments = if let Some(root) = &recovered_root {
            root.checkpoint
                .active_segments
                .iter()
                .map(|descriptor| {
                    (
                        descriptor.generation,
                        segment_path(&config.directory, descriptor.generation),
                    )
                })
                .collect()
        } else {
            let mut files = discovered_segments;
            files.sort_unstable_by_key(|(generation, _)| *generation);
            files
        };

        let mut versions = HashMap::new();
        let mut recovered_segments = Vec::new();
        let mut sequence = 0_u64;
        let mut last_boundary = 0_u64;
        for (generation, path) in &segments {
            let bytes = fs::read(path)?;
            let (boundary, recovered_sequence) = replay(path, *generation, &bytes, &mut versions)?;
            recovered_segments.push((*generation, boundary));
            sequence = sequence.max(recovered_sequence);
            if Some(*generation) == segments.last().map(|item| item.0) {
                last_boundary = boundary;
            }
        }

        let generation = segments.last().map_or(0, |item| item.0);
        let path = segment_path(&config.directory, generation);
        let device = if path.exists() {
            FileDevice::open(&path, config.segment_capacity, QUEUE_CAPACITY)?
        } else {
            FileDevice::create(&path, config.segment_capacity, QUEUE_CAPACITY)?
        };
        let writer = SegmentWriter::new_at(
            device,
            SegmentId(generation),
            generation,
            config.segment_capacity,
            last_boundary,
        );
        let mapped_segments = map_segments(&segments, generation, &path)?;
        let next_generation = segments
            .iter()
            .map(|(candidate, _)| *candidate)
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
            generation,
            sequence,
            versions,
            mapped_segments,
            sealed_segments: recovered_segments
                .into_iter()
                .filter(|(candidate, _)| *candidate != generation)
                .map(|(generation, committed_len)| SealedSegment {
                    segment_id: SegmentId(generation),
                    generation,
                    committed_len,
                })
                .collect(),
            metadata_generation: recovered_root
                .as_ref()
                .map_or(0, |root| root.superblock.generation),
            next_generation,
            poisoned: false,
            events: None,
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
        let created = unix_ns(SystemTime::now())?;
        let expires = ttl.and_then(|duration| {
            u64::try_from(duration.as_nanos())
                .ok()
                .and_then(|delta| created.checked_add(delta))
        });
        let artifact_id = artifact_id(&payload);
        let content_hash = artifact_id.into();
        let record = self.record(
            RecordType::Artifact,
            key.to_vec(),
            content_hash,
            created,
            expires,
            payload,
        )?;
        let completion = self.append(record)?;
        self.versions.insert(
            key.to_vec(),
            Version {
                artifact_id,
                sequence: completion.record.sequence,
                location: completion.location,
                expires_at_unix_ns: completion.record.expires_at_unix_ns,
                state: EntryState::Live,
            },
        );
        self.publish_event(LifecycleFact::ArtifactStored {
            key: key.to_vec(),
            namespace: self.config.namespace.clone(),
            artifact_id,
            location: completion.location,
            storage_sequence: completion.record.sequence,
        });
        Ok(())
    }

    /// Reads the latest live payload from its authoritative mapped segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock, mapped location, or durable record is
    /// invalid. Corruption is never converted into a cache miss.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
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
        let bytes = mapping
            .read(version.location.offset, version.location.stored_len)
            .map_err(StoreError::InvalidLocation)?;
        let decoded = decode(bytes).map_err(|source| StoreError::Decode {
            path: segment_path(&self.config.directory, version.location.generation),
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
        Ok(Some(decoded.record.payload))
    }

    /// Persists a tombstone and removes the value.
    ///
    /// # Errors
    /// Returns an error if time cannot be represented or persistence fails.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, StoreError> {
        self.ensure_writable()?;
        let now = unix_ns(SystemTime::now())?;
        let existed = self
            .versions
            .get(key)
            .is_some_and(|value| is_live_at(value, now));
        let record = self.record(
            RecordType::Tombstone,
            key.to_vec(),
            [0; 32],
            unix_ns(SystemTime::now())?,
            None,
            Vec::new(),
        )?;
        let completion = self.append(record)?;
        self.publish_tombstone(key.to_vec(), &completion, RemovalReason::Deleted);
        Ok(existed)
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

        let output_generation = self.allocate_generation()?;
        let output_path = segment_path(&self.config.directory, output_generation);
        let device =
            FileDevice::create(&output_path, self.config.segment_capacity, QUEUE_CAPACITY)?;
        let mut output = SegmentWriter::new(
            device,
            SegmentId(output_generation),
            output_generation,
            self.config.segment_capacity,
        );
        let (copied_records, copied_bytes, relocated) = self.copy_inputs(&inputs, &mut output)?;
        let output_len = output.reserved_boundary();
        drop(output);
        let output_mapping = (output_len > 0)
            .then(|| MappedSegment::open(&output_path))
            .transpose()?;

        self.sealed_segments.retain(|sealed| {
            !inputs.iter().any(|input| {
                input.id == sealed.segment_id.0 && input.generation == sealed.generation
            })
        });
        if let Some(output_mapping) = output_mapping {
            self.sealed_segments.push(SealedSegment {
                segment_id: SegmentId(output_generation),
                generation: output_generation,
                committed_len: output_len,
            });
            self.mapped_segments
                .insert(output_generation, output_mapping);
        }
        if let Err(error) = self.publish_replacement(&inputs, output_generation, output_len) {
            // A failed flush/write can be observationally ambiguous: the new
            // root may already be durable. Only reopen may resolve authority.
            self.poisoned = true;
            return Err(error);
        }
        for (key, sequence, old, new) in relocated {
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
        for input in &inputs {
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
        if output_len == 0 {
            match fs::remove_file(output_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        sync_directory(&self.config.directory)?;
        Ok(CompactionReport {
            input_segments: plan.input_segments.len(),
            copied_records,
            copied_bytes,
            removed_segments,
        })
    }

    fn copy_inputs(
        &self,
        inputs: &[SegmentRef],
        output: &mut SegmentWriter<FileDevice>,
    ) -> Result<CopyResult, StoreError> {
        let mut result = (0, 0_u64, Vec::new());
        for input in inputs {
            let path = segment_path(&self.config.directory, input.generation);
            let bytes = fs::read(&path)?;
            let mut cursor = 0_usize;
            while cursor < bytes.len() && !bytes[cursor..].iter().all(|byte| *byte == 0) {
                let decoded = match decode(&bytes[cursor..]) {
                    Ok(decoded) => decoded,
                    Err(DecodeError::Incomplete | DecodeError::InvalidCommitMarker) => break,
                    Err(source) => return Err(StoreError::Decode { path, source }),
                };
                let old_location = RecordLocation {
                    segment_id: SegmentId(input.id),
                    generation: input.generation,
                    offset: u64::try_from(cursor).map_err(|_| StoreError::SegmentOverflow)?,
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
                    .checked_add(decoded.encoded_len)
                    .ok_or(StoreError::SegmentOverflow)?;
            }
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

    fn append(&mut self, mut record: Record) -> Result<AppendCompletion, StoreError> {
        let encoded_len = encode_uncommitted(&record)
            .map_err(StoreError::Encode)?
            .len();
        if u64::try_from(encoded_len).map_or(true, |length| length > self.config.segment_capacity) {
            return Err(StoreError::Append(AppendError::SegmentFull));
        }
        loop {
            match self.writer.try_append(record, self.config.durability) {
                Ok(_) => loop {
                    if let Some(result) = self.writer.poll() {
                        return result.map_err(StoreError::Append);
                    }
                    std::thread::yield_now();
                },
                Err(rejected) if rejected.error == AppendError::SegmentFull => {
                    record = *rejected.record;
                    self.rotate()?;
                }
                Err(rejected) => return Err(StoreError::Append(rejected.error)),
            }
        }
    }

    fn rotate(&mut self) -> Result<(), StoreError> {
        self.sealed_segments.push(SealedSegment {
            segment_id: SegmentId(self.generation),
            generation: self.generation,
            committed_len: self.writer.reserved_boundary(),
        });
        self.generation = self.allocate_generation()?;
        let device = FileDevice::create(
            segment_path(&self.config.directory, self.generation),
            self.config.segment_capacity,
            QUEUE_CAPACITY,
        )?;
        let mapping = MappedSegment::open(segment_path(&self.config.directory, self.generation))?;
        self.writer = SegmentWriter::new(
            device,
            SegmentId(self.generation),
            self.generation,
            self.config.segment_capacity,
        );
        self.mapped_segments.insert(self.generation, mapping);
        if let Err(error) = self.publish_manifest() {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
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
                start_offset: 0,
                logical_length: segment.committed_len,
                sealed_sequence: self.sequence,
            })
            .collect();
        active_segments.push(SegmentDescriptor {
            id: self.generation,
            generation: self.generation,
            start_offset: 0,
            logical_length: self.writer.reserved_boundary(),
            sealed_sequence: 0,
        });
        let checkpoint = ManifestCheckpoint {
            generation: self.metadata_generation,
            active_segments,
        };
        let io = FileMetadataIo::open(self.config.directory.join("metadata.punah"))?;
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
                start_offset: 0,
                logical_length: segment.committed_len,
                sealed_sequence: self.sequence,
            })
            .collect();
        active_segments.push(SegmentDescriptor {
            id: self.generation,
            generation: self.generation,
            start_offset: 0,
            logical_length: self.writer.reserved_boundary(),
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
    path: &Path,
    generation: u32,
    bytes: &[u8],
    versions: &mut HashMap<Vec<u8>, Version>,
) -> Result<(u64, u64), StoreError> {
    let mut cursor = 0_usize;
    let mut sequence = 0_u64;
    while cursor < bytes.len() {
        let tail = &bytes[cursor..];
        if tail.iter().all(|byte| *byte == 0) {
            break;
        }
        let decoded = match decode(tail) {
            Ok(value) => value,
            Err(DecodeError::Incomplete | DecodeError::InvalidCommitMarker) => break,
            Err(source) => {
                return Err(StoreError::Decode {
                    path: path.to_path_buf(),
                    source,
                });
            }
        };
        let record_sequence = decoded.record.sequence;
        sequence = sequence.max(record_sequence);
        let stored_len =
            u32::try_from(decoded.encoded_len).map_err(|_| StoreError::SegmentOverflow)?;
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
                segment_id: SegmentId(generation),
                generation,
                offset: u64::try_from(cursor).map_err(|_| StoreError::SegmentOverflow)?,
                stored_len,
                logical_len,
            },
            expires_at_unix_ns: decoded.record.expires_at_unix_ns,
            state,
        };
        let should_publish = versions
            .get(&decoded.record.key)
            .is_none_or(|current| record_sequence >= current.sequence);
        if should_publish {
            versions.insert(decoded.record.key, version);
        }
        cursor = cursor
            .checked_add(decoded.encoded_len)
            .ok_or(StoreError::SegmentOverflow)?;
    }
    Ok((
        u64::try_from(cursor).map_err(|_| StoreError::SegmentOverflow)?,
        sequence,
    ))
}

fn is_live_at(version: &Version, now: u64) -> bool {
    version.state == EntryState::Live
        && version.expires_at_unix_ns.is_none_or(|expiry| expiry > now)
}

fn map_segments(
    segments: &[(u32, PathBuf)],
    active_generation: u32,
    active_path: &Path,
) -> io::Result<HashMap<u32, MappedSegment>> {
    let mut mappings = HashMap::new();
    for (generation, path) in segments {
        mappings.insert(*generation, MappedSegment::open(path)?);
    }
    if let std::collections::hash_map::Entry::Vacant(entry) = mappings.entry(active_generation) {
        entry.insert(MappedSegment::open(active_path)?);
    }
    Ok(mappings)
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

    #[test]
    fn set_get_and_overwrite() {
        let dir = TempDir::new();
        let mut store = Store::open(config(&dir.0, 4096)).expect("open");
        store.set(b"key", b"one".to_vec(), None).expect("set");
        store.set(b"key", b"two".to_vec(), None).expect("overwrite");
        assert_eq!(store.get(b"key").unwrap(), Some(b"two".to_vec()));
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
