# Changes from the Redgo Go prototype

## Reference parity milestone

Punah's first Rust milestone preserves the useful Redgo v1 wire layout while
replacing the prototype's coupled server/storage path with separate protocol,
network, service, and store crates. A runnable `punah-server` composes them.

Unlike the Go branch, restart recovery replays every ordered segment, deletes
are durable tombstones, rotation reopens the committed tail, response writes
are complete writes, accepted sockets have an explicit blocking mode, and
active connections are accounted through RAII. Expired values can be removed
with a bounded explicit pass that durably prevents resurrection. Physical
space reclamation is now available through explicit bounded compaction, which
copies only still-current records and publishes authoritative manifest changes.

This document preserves valuable context from the original Redgo implementation.
It records deliberate changes so that later optimization does not erase the
reasoning behind them.

## Product boundary

The Go prototype was evolving toward a general Redis-like distributed key-value
store. The Rust project is narrower: it caches immutable agent artifacts and
reusable inference context. General-purpose database features are accepted only
when this workload requires them.

## Payload ownership

The Go `ShardedMap` originally stored values, then changed to
`RecordLocation`. Rust retains that indirection. Index entries own compact
metadata while append-only segments own payload bytes. This keeps index growth
related to object count rather than payload volume and supports mmap, ordinary
files, and SPDK without changing the logical engine.

Punah maps every preallocated segment, but its safe API exposes bytes only
inside a checked closure. GET makes one owned payload copy so synchronous
compaction cannot retire a mapping during a response. Large network responses
then use scatter/gather to avoid building another payload-sized frame.

## Log and arena

The Go plan contained separate WAL, Bitcask, and mmap arena concepts at
different stages. Punah initially uses one self-describing append-only log as
both recovery log and value store. This avoids writing every artifact twice.
A separate WAL will be introduced only if multi-record transactions require it.

## Physical versus logical size

The Go mmap arena preallocated files and treated physical file size as the
current append offset after restart. These are distinct in Rust: capacity is
physical, while the committed logical boundary comes from validated records or
segment metadata. Preallocation cannot make a new segment appear full.

Recovery no longer scales with unused capacity: it examines one zero header at
the active boundary instead of scanning the remaining file. Sealed manifest
lengths are strict. A nonzero corrupt metadata root fails closed rather than
falling back to filename discovery.

## Recoverability

The draft Go arena stored value length, checksum, and payload, but not the key.
That made rebuilding the in-memory index impossible from arena contents alone.
Rust records include namespace, content identity, operation identity, sequence,
expiry, dependencies, and payload so recovery has no hidden external metadata
dependency.

## Deletes and reclamation

The Go delete path removed only the in-memory entry. Rust models deletion as a
durable tombstone. Expired, superseded, and tombstoned records contribute dead
bytes; compaction copies only locations that are still current, then retires and
eventually reclaims old segments.

## Sharding

The Go shard calculation used a bit mask and therefore silently required a
power-of-two shard count. Rust validates that constraint explicitly. One worker
owns each configured shard and Store; submission uses a per-shard shutdown gate,
not a global data or admission mutex.

## Networking and I/O

Go goroutines and kernel TCP were appropriate for the prototype. The portable
Rust path already uses bounded TCP batches, shard-owned workers, polling device
requests, and explicit backpressure. Local-durable workers form zero-wait,
bounded mutation cohorts so many records share two flush barriers. The Linux
target replaces portable file/socket adapters with pinned NIC and NVMe queues
without changing ownership or durability semantics.

## Memory safety

The Go runtime reduced many lifetime concerns but conflicted with off-heap and
kernel-bypass goals. Rust makes ownership explicit. Unsafe operations are not
spread through the engine: the current `MmapRaw` conversion is isolated behind
a checked closure API, and future FFI, DMA, and ring modules must provide the
same kind of documented ownership boundary.

## Performance claims

The Go README included throughput and latency targets before reproducible
measurements existed. Rust documentation separates targets from results and
prioritizes scaling curves, tail latency, recovery cost, and agent-level work
saved.

The current measured history—including single-encode admission, mmap recovery,
durable group commit, per-shard ingress, and response scatter/gather—is kept in
[the optimization log](optimization-log-2026-08.md) so future accelerator work
does not accidentally restore a Redgo-era bottleneck.
