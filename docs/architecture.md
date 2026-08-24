# Architecture

## Product boundary

Punah stores immutable artifacts produced or consumed by distributed agents.
It prevents duplicate work through exact operation identities and leases. A
semantic result may be offered as a candidate, but is never treated as an exact
cache hit.

On an exact cache miss, an agent attempts to acquire a bounded lease for the
operation identity. One holder performs the work while peers observe the lease
deadline instead of repeating it. Tokens fence expired owners: after another
agent reacquires a lease, the former owner cannot renew or release it.

## Data model

An artifact has a content hash, an operation hash, namespace, timestamps,
optional expiry, dependencies, and a payload. The in-memory index stores
metadata and a `RecordLocation`; the payload remains in an append-only segment.

The append-only segment is both the durable log and value store. A second WAL
is not required until Punah supports multi-record atomic transactions.

The portable store maps every segment read-only and resolves index locations
against those mappings. Mapping a segment reserves virtual address space; it
does not pin the complete file in physical memory. The operating-system page
cache admits hot pages and evicts cold pages under pressure. Punah does not
inline small payloads in the authoritative index, so payload volume cannot
silently become unbounded heap usage.

A future small-object cache, if measurements justify one, is a bounded and
discardable acceleration layer. It never changes the durable representation:
every payload remains authoritative in its segment and every index entry
remains location-based.

## Data plane

The target data plane is polling and shared-nothing:

```text
NIC RX queue -> owning core -> index shard -> NVMe queue pair -> NIC TX queue
```

Each worker owns its queues and shard group. Cross-core routing uses bounded,
batched queues. Backpressure is explicit. Request-path allocation is avoided.

The portable runtime already preserves the ownership shape: one thread owns
each Store, single submissions lock only that shard's ingress while checking
shutdown, and independent shards do not contend on a global submission mutex.
The worker remains deliberately serial for authorize/store/release ordering.

The portable TCP path batches only complete frames already buffered on a
connection and never delays a lone request to fill a batch. Same-owner commands
execute in request order; independent owners may run concurrently; responses
are restored to connection order. Pipeline depth is selected by the client so
interactive latency and bulk throughput can make different tradeoffs.
For `LocalDurable` mutations, an owner also drains only messages that are already
queued into a bounded storage cohort; it never sleeps to fill one. GET remains
an ordering barrier.

Tokio or another conventional async runtime may be used by the control plane,
but does not own the SPDK/DPDK polling loops.

## Portable-first backends

Storage and transport expose polling contracts. Implementations progress from
deterministic memory, to ordinary OS facilities, and finally SPDK/DPDK. The
engine must not depend on a particular backend.

## Segment lifecycle

```text
Active -> Sealed -> Compacting -> Retired -> Reclaimable
```

Only an active segment accepts appends. Compaction copies live records from
sealed segments. The current reference compactor runs synchronously under the
Store's exclusive mutable ownership, and GET returns an owned payload, so no
reader can retain a mapping during reclamation. A future background or direct
mmap-to-network path must add read leases and defer retirement until they drain.

The reference engine maps the pre-sized active file through a read-only handle
while its exclusive owner writes through a separate handle. A location becomes
visible only after its commit completes. Compaction maps replacement output
before publishing relocated locations, then drops input mappings before file
removal. The mmap creation is isolated in `punah-device::MappedSegment`; its
`MmapRaw` API copies checked ranges directly into owned memory and never exposes
a raw pointer or file-backed Rust reference.

## Cleanup

- Expiry removes stale index entries and accounts their record bytes as dead.
- Compaction selects sealed segments by dead-byte ratio and available space.
- Reclamation unmaps and removes retired segments when no reader can use them.
- Lease expiry removes abandoned computations.
- Buffer and queue capacities are bounded.
- Namespace quotas are required before multi-tenant deployment; they are not
  yet implemented in the portable milestone.

Deleting a live index entry does not immediately discard its sequence
watermark. The watermark prevents an older artifact discovered during recovery,
replication, or delayed completion from resurrecting deleted data. A watermark
may be removed only when retention rules prove that no older record can arrive.

## Control plane

The control plane owns configuration, metrics, health, membership, lifecycle,
and administration. It communicates with the data plane through bounded
messages and never reaches into device-owned state directly.
