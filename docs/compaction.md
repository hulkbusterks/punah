# Compaction

This document defines Punah's crash-safe segment compaction contract. It is a
durable-format and correctness specification, not merely a file-cleanup
algorithm. The first implementation targets the portable single-node store,
while keeping its publication mechanism usable in a reserved SPDK metadata
region.

## Current implementation status

The portable store now has a synchronous/offline executor. `Store::compact`
accepts a planner-produced batch and holds an exclusive mutable borrow for the
entire operation; this is the initial publication barrier. It accepts only
sealed inputs, copies a record only when its sequence and complete old location
still match the latest exact-key version, and retains latest tombstones.

Replacement records use `LocalDurable` even when foreground durability is
weaker. Punah then appends a durable `Replace` or `Retire` record, publishes a
complete checkpoint through the alternating superblock root, redirects exact
matching in-memory locations, and only then unlinks sources and syncs the
directory. An ambiguous metadata publication failure poisons the open store;
reopening is required to determine which valid root became durable.

Once a root exists, its manifest membership is authoritative. Filename scans
only bootstrap an absent manifest. Unreferenced filenames affect the next
segment ID but are never made visible, preventing both orphan resurrection and
ID reuse. Filesystem tests cover stale versions, tombstones, rotation, restart,
reclamation, and flushed-but-unpublished orphans. Multi-output batches, reader
epochs, online shard barriers, and bounded orphan quarantine remain later work.

## Chosen metadata model

Punah uses a hybrid metadata model:

```text
dual-slot superblock -> manifest checkpoint -> append-only topology journal
                                              -> active segment set
```

The first implementation may write a complete manifest checkpoint for every
topology change and leave the journal empty. The on-disk formats nevertheless
reserve the journal pointer and generation fields now. A later implementation
can append topology records between checkpoints without changing the root
publication protocol.

The superblock is two fixed, independently checksummed slots. A slot contains
only bounded root metadata: format version, monotonically increasing manifest
generation, checkpoint location and length, journal tail, checksum, and commit
marker. It does not contain the complete segment list.

A manifest checkpoint is immutable and contains:

- its generation and parent generation;
- the complete ordered active-segment set;
- each segment's identity, generation, logical committed boundary, and state;
- the next segment identifier and next record sequence;
- the topology-journal range folded into the checkpoint;
- a checksum and commit marker.

Topology-journal records are immutable, checksummed, generation-linked records
such as `Create`, `Seal`, `Replace`, and `Retire`. A `Replace` names the exact
input segment set and exact replacement set. Journal replay must form one
unbroken chain from the checkpoint generation; forks, gaps, or references to
unknown segments are corruption, not choices made by recovery.

## Correctness invariants

These invariants are mandatory across normal execution, process termination,
power loss at any write boundary, recovery, and compaction retries.

1. **One authoritative generation.** Recovery selects the highest-generation
   committed superblock slot whose referenced checkpoint and journal chain
   fully validate. An invalid newer slot is ignored in favor of the older valid
   slot.
2. **Immutable published objects.** A checkpoint, journal record, or sealed
   segment is never modified after publication. New generations replace them
   by reference.
3. **Copy before publish.** Every replacement segment is complete, committed,
   and durable at the requested local durability boundary before metadata can
   make it authoritative.
4. **Publish before retire.** Source segments remain readable until a durable
   metadata generation names their replacements. Physical deletion is a later
   reclamation step.
5. **No resurrection.** For every `(namespace, content_hash)`, recovery and
   online publication preserve the highest accepted sequence, including a
   tombstone sequence watermark.
6. **Conditional relocation.** A copied artifact changes the in-memory index
   only if the index still refers to the exact old `RecordLocation` and the
   same sequence. A concurrent newer write or tombstone always wins.
7. **Stable identities.** Compaction preserves record identity and semantic
   metadata: namespace, hashes, sequence, timestamps, expiry, dependencies,
   flags, record type, and payload. Only `RecordLocation` changes.
8. **Committed records only.** An uncommitted, truncated, or corrupt source
   record is never copied. Corruption before a source segment's manifest
   boundary fails compaction and recovery.
9. **Closed input set.** Only sealed segments may be compacted. The active
   writer is never an input.
10. **Exact replacement.** A metadata generation either retains every source
    segment or replaces the complete declared input set. Partial topology
    publication is invalid.
11. **Reader safety.** Retired segments are not unmapped, closed out from under
    a read, or deleted until every reader that could hold their locations has
    exited its read epoch (a reference count is sufficient initially).
12. **Idempotence.** Repeating recovery, orphan discovery, or a compaction
    request after a crash produces the same logical key state. It may create
    new orphan output, but cannot change visible values without a committed
    metadata generation.
13. **Monotonic allocation.** Segment IDs, metadata generations, and record
    sequences are never reused, including after a failed or orphaned attempt.
14. **Durability is not inferred.** `LocalDurable` publication requires segment
    data, checkpoint/journal data, and the final superblock commit to cross the
    device persistence boundary in that order.

## Tombstones and expiry

The first compactor conservatively copies the latest tombstone watermark for a
key whenever that watermark is represented in a selected source segment. This
may retain small tombstone records longer than theoretically necessary, but it
prevents an older artifact in an unselected segment from being resurrected.

Expired artifacts are logically dead only after the normal cleanup path has
written their tombstone. Compaction must not invent a tombstone using its own
wall-clock observation: doing so would mix expiry policy, sequence allocation,
and relocation. It may skip an expired artifact only when a newer durable
tombstone is already part of the authoritative state.

Tombstone elision is a later optimization. It requires a durable per-namespace
sequence floor proving that no active segment, replica, delayed completion, or
recovery source can introduce an older sequence. Until such a floor exists,
retaining the latest tombstone is the safe rule.

## Sequence fencing

Compaction is optimistic and does not stop foreground writes for its scan and
copy phases.

For each source record selected as live, the compactor captures:

```text
(namespace, content_hash, sequence, old_location)
```

It copies the record unchanged and records the resulting `new_location`. Before
publishing the topology generation it asks the shard owner to validate that:

```text
current.sequence == captured.sequence
and current.location == old_location
```

After metadata publication, it applies
`ReferenceIndex::redirect_if_current(old_location, new_location)`. The current
API already prevents a stale location from replacing a newer one; the
implementation should strengthen its signature or companion check to make the
sequence condition explicit rather than relying on location uniqueness.

If validation fails, the copied record is harmless dead data in the replacement
segment. It must not be indexed. This wastes bounded space but avoids pausing
writes or mutating an immutable replacement. A replacement segment may contain
such stale copies and remains valid.

There is one short publication barrier per affected index shard:

1. finish and durably seal replacement segments;
2. obtain shard publication barriers and validate captured entries;
3. publish the new metadata generation;
4. redirect only still-current entries;
5. release barriers and retire source segments.

Foreground writes may continue during scanning and copying. They are paused
only at the owning shard while topology publication and matching redirects are
made indivisible from that shard's point of view. If metadata publication
fails, no redirects occur and old locations remain authoritative.

## State machine

```text
Idle
  -> Selecting
  -> Copying
  -> SealingOutputs
  -> ValidatingFence
  -> PersistingMetadata
  -> PublishingRoot
  -> RedirectingIndex
  -> RetiringInputs
  -> Reclaiming
  -> Idle
```

- **Selecting:** choose sealed inputs using reclaimable bytes, live ratio,
  available output headroom, and a bounded work budget. Persist nothing.
- **Copying:** scan only committed source prefixes and append eligible records
  to newly allocated replacement segments.
- **SealingOutputs:** finish record commit pipelines, store committed
  boundaries, and flush replacement segments.
- **ValidatingFence:** acquire affected shard barriers and discard relocation
  intents that no longer match current sequence and location.
- **PersistingMetadata:** write and flush a new immutable checkpoint, or journal
  `Replace` record, naming old and replacement segments.
- **PublishingRoot:** write the inactive superblock slot body, flush it, write
  its commit marker, and flush again. This is the linearization point.
- **RedirectingIndex:** install validated new locations. This is reconstruction
  of state already made authoritative on disk; a crash before it is harmless.
- **RetiringInputs:** mark old handles unavailable to new readers and advance a
  retirement epoch.
- **Reclaiming:** after readers drain, remove source files or release their raw
  device extents. Directory metadata must be flushed when filesystem deletion
  durability matters.

Any error before `PublishingRoot` abandons the attempt and keeps the old
generation. Any error after it is resolved by recovery from the new generation.
The compactor never attempts an in-place rollback.

## Crash matrix

| Crash point | Durable state after restart | Required recovery action |
|---|---|---|
| Before output allocation | Old generation only | None |
| During output record body/commit | Old generation plus incomplete orphan | Ignore and later remove orphan |
| After output commit, before output flush | Old generation; output durability unknown | Ignore orphan |
| After output flush, before metadata write | Old generation plus valid orphan | Ignore and later remove orphan |
| During checkpoint/journal body | Old generation plus invalid metadata object | Reject by checksum/commit marker |
| After metadata object flush, before superblock body | Old root, valid unreachable metadata/output | Treat as orphans |
| During inactive superblock body | Old slot remains authoritative | Reject torn new slot |
| After body flush, before slot commit marker | Old slot remains authoritative | Reject uncommitted new slot |
| During commit-marker write | Highest fully validating slot wins | Never guess from generation alone |
| After marker flush, before index redirects | New generation | Replay new active set and rebuild index |
| During index redirects | New generation | Discard memory state and rebuild index |
| After input retirement, before reader drain | New generation; old files still present | Recreate retirement set and wait |
| During deletion of old files | New generation; some retired files remain | Delete remaining unreferenced files |
| After deletion, before directory flush | New generation; filenames may reappear | Repeat idempotent orphan cleanup |
| Both superblock slots invalid | No automatically trusted root | Enter repair/read-only mode; scan is diagnostic, not silent authority |

Recovery must validate the entire referenced object graph before selecting a
slot. A valid superblock pointing to a missing, corrupt, or generation-mismatched
checkpoint is not a valid root.

## Recovery and orphan cleanup

Normal startup performs these steps:

1. read both superblock slots;
2. validate checksums, commit markers, formats, and referenced checkpoints;
3. choose the highest valid generation;
4. replay the linked topology journal through its declared tail;
5. open exactly the resulting active segment set;
6. validate each declared committed boundary and rebuild key/index state in
   sequence order;
7. set next IDs and sequence from the maximum of manifest allocation counters
   and all discoverable filenames, so an orphan's ID is never reused;
8. classify every unreferenced segment/checkpoint/journal object as an orphan;
9. move orphans to a quarantine namespace, or delete them only after the root
   is fully recovered and no open reader references them.

Unknown files are never incorporated merely because their generation number is
large. This prevents a flushed-but-unpublished compaction output from becoming
visible. Cleanup is bounded and retryable. Operators should be able to inspect
quarantined objects before deletion in diagnostic mode.

Self-describing segment headers and a full-device scan remain a repair tool. A
repair operation must produce and explicitly publish a new checkpoint; it must
not happen silently during ordinary startup when both root slots are lost.

## Disk-pressure behavior

Compaction temporarily needs both source and replacement bytes. It must reserve
output headroom before selecting inputs and must not assume that deleting inputs
mid-operation is safe.

Default policy:

- never delete an authoritative segment to create compaction space;
- reject or defer compaction if worst-case output plus metadata reserve does not
  fit;
- retain a fixed emergency reserve for tombstones, metadata publication, and
  orderly sealing; ordinary artifact writes cannot consume it;
- apply backpressure before the filesystem or device reaches zero free space;
- continue reads while writes are backpressured;
- permit cleanup tombstones from the emergency reserve;
- expose `NoCompactionHeadroom` separately from device I/O failure;
- do not automatically evict live artifacts in the first implementation.

Selection should estimate output as live bytes plus record/alignment overhead,
then include a conservative safety margin. If estimates prove wrong, abandon
before publication; the old generation remains authoritative and output is an
orphan. Incremental compaction should bound input bytes so one attempt cannot
consume the whole reserve.

The background convenience policy requires at least 75% reclaimable bytes in
each selected segment and at least two inputs whenever live records require a
replacement. A single all-dead segment remains immediately eligible because it
is retired without creating output. Both thresholds are explicit planner
inputs: an operator may lower them for sparse filesystems or disk pressure, but
one-for-one partial rewrites are not the portable default for fixed-size
arenas.

Whether an explicitly configured cache namespace may evict live artifacts under
pressure is a product-policy decision, not part of the base compactor.

## Implementation status

The portable reference milestone is implemented:

- `punah-metadata` provides dual-root, manifest, and journal codecs plus the
  ordered filesystem publisher.
- Store indexes own locations rather than payloads. Once a root exists,
  manifest membership and sealed boundaries are authoritative; orphan
  filenames are not replayed.
- Rotation seals the former active segment, opens and maps its replacement, and
  publishes a new manifest generation.
- `punah-compaction` supplies a pure bounded planner with headroom checks.
- Store compaction revalidates exact current locations, copies each retained
  record once, makes replacement bytes durable, publishes the new manifest,
  installs replacement mappings, and only then removes retired inputs.
- Recovery, tombstone fencing, stale-copy rejection, manifest-orphan handling,
  restart, and low-headroom behavior have deterministic tests.

The reference compactor is intentionally synchronous under one exclusive Store
borrow. GET copies its payload before returning, so no mapping borrow or reader
lease can outlive that borrow; removal is quiescent without a separate epoch
system. Background or zero-copy reads will require reference-counted read leases
and deferred reclamation before they are allowed to overlap compaction.

Remaining production work is policy and acceleration rather than the original
location-backed correctness skeleton:

1. split synchronous compaction into resumable copy slices so its currently
   observed pause budget can become a hard preemption bound;
2. design a version-2 predecessor-linked topology journal if checkpoint
   benchmarks show full manifests are materially expensive; journal v1 is an
   audit tail and cannot reconstruct descriptor fences;
3. extend the volatile-versus-durable `CrashDevice` through full Store rotation
   and compaction state transitions (writer cohort states are exhausted only
   under the current atomic-write, whole-image-Flush model);
4. measure reclaimed bytes, write amplification, pause time, temporary space,
   recovery cost, and foreground latency under churn;
5. integrate the checked-in aligned extent-v2 body/footer primitive with the
   future direct-device backend and validate its Flush/FUA behavior on hardware.

Implemented maintenance now includes authoritative per-segment live/dead byte
accounting, an opt-in shard-local scheduler that runs only after a mutation
threshold and an observed-empty ingress, hard input/output byte limits, and
per-shard pause/reclamation/failure counters, including peak input and copied
bytes. Latest expired values and tombstones remain live until bounded cleanup
writes a superseding tombstone. The synchronous compactor always finishes an
accepted plan, so `pause_budget` is explicitly telemetry/a target rather than a
false cancellation guarantee.

Operator recovery is also explicit. `punah-admin inspect` is read-only,
`import` validates unmanaged committed prefixes before publishing authority,
`repair` preserves corrupt metadata in quarantine before import, and
`quarantine-orphans` only moves files classified as unreferenced by a healthy
root. No command deletes quarantined data.

## Decisions requiring user input

The correctness mechanism itself does not need another choice. Conservative
defaults can carry the first implementation. These policies materially change
observable product behavior and should be reviewed before they are stabilized:

1. **Disk-full policy:** remain read-only/reject writes (recommended default),
   or allow configured cache namespaces to evict live unpinned artifacts.
2. **Emergency reserve:** choose an operational default as a percentage and a
   minimum byte count after benchmarks reveal typical segment/metadata sizes.
3. **Quarantine retention:** choose the operator-defined retention interval;
   the implemented command only moves files and never deletes them.
4. **Publication latency policy:** briefly barrier all affected shards for one
   atomic multi-shard generation, or publish independent per-shard generations.
   The former is simpler and is recommended until measurements justify the
   latter.

Compression, record re-encoding, semantic deduplication, replica-aware
tombstone elision, and live-data eviction are explicitly outside the first
compaction milestone. They each alter identity, fencing, or durability policy
and must not enter as incidental optimizations.
