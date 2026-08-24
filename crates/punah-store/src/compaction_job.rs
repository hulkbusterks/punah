//! Resumable copy state for one immutable-segment compaction plan.

use std::path::PathBuf;

use punah_compaction::CompactionPlan;
use punah_device::{FileDevice, MappedSegment};
use punah_engine::SegmentWriter;
use punah_metadata::SegmentRef;
use punah_record::{committed_record_len, decode};
use punah_types::{Durability, RecordLocation, SegmentId};

use super::{
    CompactionReport, PreparedCompaction, QUEUE_CAPACITY, Relocation, Store, StoreError,
    copy_mapped_record, read_mapped_header, segment_path, sync_directory,
};

/// Maximum work admitted by one background copy slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionSliceBudget {
    pub records: usize,
    pub bytes: u64,
}

impl CompactionSliceBudget {
    /// The portable owner-thread default: one writer cohort and at most 1 MiB
    /// of input examination before foreground work can preempt.
    pub const DEFAULT: Self = Self {
        records: 64,
        bytes: 1024 * 1024,
    };

    fn validate(self) -> Result<Self, StoreError> {
        if self.records == 0 || self.bytes == 0 {
            Err(StoreError::InvalidCompactionPlan)
        } else {
            Ok(self)
        }
    }
}

/// Result of advancing one compaction state machine once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionProgress {
    Copying {
        examined_records: usize,
        examined_bytes: u64,
        copied_records: usize,
        copied_bytes: u64,
    },
    /// Output bytes and their directory entry are durable; authority still
    /// names the old inputs until a later publication step succeeds.
    ReadyToPublish,
    Complete(CompactionReport),
}

/// Opaque, single-owner state for a resumable compaction.
///
/// Sealed inputs are immutable. Foreground overwrites may make an already
/// copied record stale between slices; publication's exact relocation compare
/// deliberately ignores that stale copy.
pub struct CompactionJob {
    inputs: Vec<SegmentRef>,
    output: Option<SegmentWriter<FileDevice>>,
    output_generation: Option<u32>,
    output_path: Option<PathBuf>,
    input_index: usize,
    input_cursor: u64,
    copied_records: usize,
    copied_bytes: u64,
    relocations: Vec<Relocation>,
    copy_complete: bool,
    output_prepared: bool,
    prepared: Option<PreparedCompaction>,
    terminal: bool,
}

impl CompactionJob {
    #[cfg(test)]
    pub(crate) const fn output_generation(&self) -> Option<u32> {
        self.output_generation
    }

    #[cfg(test)]
    pub(crate) fn take_prepared(&mut self) -> Option<PreparedCompaction> {
        self.prepared.take()
    }
}

impl Drop for CompactionJob {
    fn drop(&mut self) {
        // Graceful cancellation occurs before authority publication, so its
        // private output is safe to remove. A process crash cannot run Drop;
        // recovery still treats that durable file as an orphan.
        drop(self.output.take());
        let path = self.output_path.take().or_else(|| {
            self.prepared.take().map(|prepared| {
                drop(prepared.mapping);
                prepared.path
            })
        });
        if let Some(path) = path {
            let directory = path.parent().map(PathBuf::from);
            let _ = std::fs::remove_file(path);
            if let Some(directory) = directory {
                let _ = sync_directory(&directory);
            }
        }
    }
}

impl Store {
    /// Validates and begins one resumable compaction job.
    ///
    /// At most one job should be advanced for a Store at a time. Starting a
    /// second job is fail-safe but may leave the first output as an ignored
    /// orphan if their inputs overlap.
    ///
    /// # Errors
    /// Returns an error for an invalid plan or output-file creation failure.
    pub fn begin_compaction(&mut self, plan: &CompactionPlan) -> Result<CompactionJob, StoreError> {
        self.ensure_writable()?;
        if plan.input_segments.is_empty()
            || plan.required_output_bytes > self.config.segment_capacity
            || plan.input_segments.iter().any(|input| {
                !self.sealed_segments.iter().any(|sealed| {
                    sealed.segment_id.0 == input.id && sealed.generation == input.generation
                })
            })
        {
            return Err(StoreError::InvalidCompactionPlan);
        }

        let inputs = plan.input_segments.clone();
        let has_live_input = self.versions.values().any(|version| {
            inputs.iter().any(|input| {
                version.location.segment_id.0 == input.id
                    && version.location.generation == input.generation
            })
        });
        let (output, output_generation, output_path) = if has_live_input {
            let generation = self.allocate_generation()?;
            let path = segment_path(&self.config.directory, generation);
            let device = FileDevice::create(&path, self.config.segment_capacity, QUEUE_CAPACITY)?;
            (
                Some(SegmentWriter::new(
                    device,
                    SegmentId(generation),
                    generation,
                    self.config.segment_capacity,
                )),
                Some(generation),
                Some(path),
            )
        } else {
            (None, None, None)
        };

        Ok(CompactionJob {
            inputs,
            output,
            output_generation,
            output_path,
            input_index: 0,
            input_cursor: 0,
            copied_records: 0,
            copied_bytes: 0,
            relocations: Vec::new(),
            copy_complete: !has_live_input,
            output_prepared: false,
            prepared: None,
            terminal: false,
        })
    }

    /// Advances at most one bounded input-copy slice or publishes a completed
    /// replacement. Publication remains one synchronous durability barrier.
    ///
    /// # Errors
    /// Returns for invalid state, corrupt input, output I/O, or publication
    /// failure. A failed job is terminal and must be discarded.
    pub fn advance_compaction(
        &mut self,
        job: &mut CompactionJob,
        budget: CompactionSliceBudget,
    ) -> Result<CompactionProgress, StoreError> {
        self.ensure_writable()?;
        let budget = budget.validate()?;
        if job.terminal {
            return Err(StoreError::InvalidCompactionPlan);
        }
        if job.copy_complete {
            if !job.output_prepared {
                let result = self.prepare_compaction_output(job);
                if result.is_err() {
                    job.terminal = true;
                    return result.map(|()| CompactionProgress::ReadyToPublish);
                }
                job.output_prepared = true;
                return Ok(CompactionProgress::ReadyToPublish);
            }
            job.terminal = true;
            return self
                .finish_compaction_job(job)
                .map(CompactionProgress::Complete);
        }

        let result = self.copy_compaction_slice(job, budget);
        if result.is_err() {
            job.terminal = true;
        }
        result
    }

    #[allow(clippy::too_many_lines)] // One linear scan keeps cursor admission and decode atomic.
    fn copy_compaction_slice(
        &self,
        job: &mut CompactionJob,
        budget: CompactionSliceBudget,
    ) -> Result<CompactionProgress, StoreError> {
        let mut examined_records = 0_usize;
        let mut examined_bytes = 0_u64;
        let mut pending = Vec::new();
        let mut pending_metadata = Vec::new();
        let mut record_bytes = Vec::new();

        while job.input_index < job.inputs.len() && examined_records < budget.records {
            let input = job.inputs[job.input_index];
            let sealed = self
                .sealed_segments
                .iter()
                .find(|sealed| {
                    sealed.segment_id.0 == input.id && sealed.generation == input.generation
                })
                .ok_or(StoreError::InvalidCompactionPlan)?;
            if job.input_cursor == sealed.committed_len {
                job.input_index += 1;
                job.input_cursor = 0;
                continue;
            }
            if job.input_cursor > sealed.committed_len {
                return Err(StoreError::InvalidCompactionPlan);
            }

            let mapping = self
                .mapped_segments
                .get(&input.generation)
                .ok_or(StoreError::MissingSegment(input.generation))?;
            let offset = sealed
                .start_offset
                .checked_add(job.input_cursor)
                .ok_or(StoreError::SegmentOverflow)?;
            let end = sealed
                .start_offset
                .checked_add(sealed.committed_len)
                .ok_or(StoreError::SegmentOverflow)?;
            let path = segment_path(&self.config.directory, input.generation);
            let header = read_mapped_header(mapping, offset)?;
            let encoded_len =
                committed_record_len(&header).map_err(|source| StoreError::Decode {
                    path: path.clone(),
                    source,
                })?;
            let encoded_len_u64 =
                u64::try_from(encoded_len).map_err(|_| StoreError::SegmentOverflow)?;
            if examined_records > 0
                && examined_bytes
                    .checked_add(encoded_len_u64)
                    .is_none_or(|total| total > budget.bytes)
            {
                break;
            }
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
                pending_metadata.push((
                    decoded.record.key.clone(),
                    decoded.record.sequence,
                    old_location,
                ));
                pending.push(decoded.record);
            }
            job.input_cursor = job
                .input_cursor
                .checked_add(encoded_len_u64)
                .ok_or(StoreError::SegmentOverflow)?;
            examined_records += 1;
            examined_bytes = examined_bytes
                .checked_add(encoded_len_u64)
                .ok_or(StoreError::SegmentOverflow)?;
        }

        let slice_copied_records = pending.len();
        let mut slice_copied_bytes = 0_u64;
        if !pending.is_empty() {
            let output = job
                .output
                .as_mut()
                .ok_or(StoreError::InvalidCompactionPlan)?;
            let completions = output
                .append_cohort(pending, Durability::LocalDurable)
                .map_err(|rejected| StoreError::Append(rejected.error))?;
            if completions.len() != pending_metadata.len() {
                return Err(StoreError::InvalidCompactionPlan);
            }
            for (completion, (key, sequence, old_location)) in
                completions.into_iter().zip(pending_metadata)
            {
                slice_copied_bytes = slice_copied_bytes
                    .checked_add(u64::from(completion.location.stored_len))
                    .ok_or(StoreError::SegmentOverflow)?;
                job.relocations
                    .push((key, sequence, old_location, completion.location));
            }
            job.copied_records = job
                .copied_records
                .checked_add(slice_copied_records)
                .ok_or(StoreError::SegmentOverflow)?;
            job.copied_bytes = job
                .copied_bytes
                .checked_add(slice_copied_bytes)
                .ok_or(StoreError::SegmentOverflow)?;
        }

        while job.input_index < job.inputs.len() {
            let input = job.inputs[job.input_index];
            let sealed = self
                .sealed_segments
                .iter()
                .find(|sealed| {
                    sealed.segment_id.0 == input.id && sealed.generation == input.generation
                })
                .ok_or(StoreError::InvalidCompactionPlan)?;
            if job.input_cursor != sealed.committed_len {
                break;
            }
            job.input_index += 1;
            job.input_cursor = 0;
        }
        job.copy_complete = job.input_index == job.inputs.len();
        Ok(CompactionProgress::Copying {
            examined_records,
            examined_bytes,
            copied_records: slice_copied_records,
            copied_bytes: slice_copied_bytes,
        })
    }

    fn prepare_compaction_output(&self, job: &mut CompactionJob) -> Result<(), StoreError> {
        job.prepared = if let Some(output) = job.output.take() {
            let generation = job
                .output_generation
                .ok_or(StoreError::InvalidCompactionPlan)?;
            let path = job
                .output_path
                .clone()
                .ok_or(StoreError::InvalidCompactionPlan)?;
            let sealed_sequence = job
                .relocations
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
            job.output_path = None;
            Some(PreparedCompaction {
                generation,
                path,
                committed_len,
                sealed_sequence,
                mapping,
                copied_records: job.copied_records,
                copied_bytes: job.copied_bytes,
                relocations: std::mem::take(&mut job.relocations),
            })
        } else {
            None
        };
        Ok(())
    }

    fn finish_compaction_job(
        &mut self,
        job: &mut CompactionJob,
    ) -> Result<CompactionReport, StoreError> {
        self.finish_prepared_compaction(&job.inputs, job.prepared.take())
    }
}
