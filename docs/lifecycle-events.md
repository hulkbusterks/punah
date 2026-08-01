# Lifecycle facts and observer recovery

`punah-events` is the seam between Punah's authoritative local state and
replaceable routers, advisors, metrics collectors, or future replication
controllers. It does not schedule work and its output is not a second source of
truth.

## Authority ordering

The store publishes `ArtifactStored` only after the record append has completed
and its in-memory authoritative version has changed. It publishes
`ArtifactRemoved` only after a durable tombstone replaces a live version.
`ArtifactRelocated` follows durable replacement-manifest publication and the
fenced location update. Failed mutations therefore produce no success fact.

Each event has two distinct numbers:

- `storage_sequence` identifies the durable logical version. Compaction keeps it
  unchanged while moving the bytes.
- `cursor` orders observations from one `EventBus`. Relocation receives a new
  cursor even though its storage sequence is unchanged.

The cursor is process-local observer state, not durable storage authority.

## Backpressure and gaps

Every subscriber has an independent bounded queue. Publication uses only
non-blocking lock acquisition and non-blocking sends. A busy registry, full
queue, disconnected consumer, or slow policy component cannot delay a storage
acknowledgement.

This deliberately permits gaps. `Subscription::try_next` returns `Poll::Lagged`
when its queue overflowed or its observed cursor falls behind the published
watermark. A lagged consumer must discard any derived location view and ask the
store owner for `Store::lifecycle_snapshot()`.

After installing the snapshot, the consumer calls `resynced_through` with its
watermark. Events newer than that watermark are retained, covering the race
between taking a snapshot and installing it. Duplicates and reconnects should
still be treated as normal by external consumers.

## Snapshot contents

The snapshot contains the exact lookup key, BLAKE3 artifact identity, durable
storage sequence, location, and live/tombstone state. It is intentionally a
fact view: it contains no placement preference, eviction score, workflow
priority, or compute decision.
