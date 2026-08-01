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
space reclamation is intentionally deferred to compaction.

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
power-of-two shard count. Rust will either enforce that constraint explicitly
or use a mapping algorithm whose requirements are represented by its type. The
production target is per-core shard ownership rather than a globally shared
map protected by many locks.

## Networking and I/O

Go goroutines and kernel TCP were appropriate for the prototype. The target
Rust data plane uses pinned polling workers with owned NIC and NVMe queues.
Portable polling backends preserve this execution model during development.
Conventional async Rust remains suitable for the control plane, not the
SPDK/DPDK hot path.

## Memory safety

The Go runtime reduced many lifetime concerns but conflicted with off-heap and
kernel-bypass goals. Rust makes ownership explicit. Unsafe operations are not
spread through the engine; they are confined to future FFI, DMA, mmap, and ring
modules with safe ownership-transfer APIs above them.

## Performance claims

The Go README included throughput and latency targets before reproducible
measurements existed. Rust documentation separates targets from results and
prioritizes scaling curves, tail latency, recovery cost, and agent-level work
saved.
