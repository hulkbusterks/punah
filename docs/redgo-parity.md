# Redgo parity definition

Parity is measured against code present on Redgo's `dev-kritik` branch, not
against roadmap claims in its README. That branch does not currently compile as
an executable: its server constructor call is stale, engine tests target the
old value representation, and persistence recovery cannot rebuild keys.

## Parity target

Punah reaches functional parity when a single node can:

- serve the compatible v1 protocol over TCP
- execute `PING`, `GET`, `SET`, and `DELETE`
- enforce connection, frame, key, and value limits
- apply TTL on reads and through budgeted cleanup
- append and retrieve persistent values
- persist deletion as tombstones
- restart and rebuild its index from all segments
- rotate full segments
- acknowledge according to configured durability
- stop accepting connections and drain active workers cleanly

## Current mapping

| Capability | Redgo `dev-kritik` | Punah |
|---|---|---|
| 16-byte binary protocol | Working library | Compatible codec implemented |
| TCP fragmentation/deadlines | Working reader | Implemented and loopback tested |
| GET/SET/DELETE/PING dispatch | Partial; executable cannot construct server | Implemented with stable acknowledgements and error frames |
| Sharded TTL index | Partial | Implemented with version fencing |
| Persistent append/read | Partial mmap arena | Polling memory/file devices, location index, and mmap reads implemented |
| Recovery | Cannot rebuild index | Manifest-ordered strict recovery and active-tail discovery implemented |
| Durable deletion | Missing | Implemented and verified across a TCP-driven restart |
| Durability modes | Configuration not honored per write | Explicit state machine implemented |
| Segment rotation | Present but broken after reopen | Implemented with ordered replay and tail reopen tests |
| Physical reclamation | Planned only | Bounded planner and explicit crash-safe compaction implemented |
| Typed configuration | YAML/env implementation | Typed programmatic configuration implemented |

The end-to-end parity test drives PING, SET, GET, and DELETE through TCP,
restarts the node twice against the same segment directory, verifies the value
survives the first restart, and verifies the tombstone survives the second.
Configured request and connection limits are enforced before storage work.

TTL lookup and bounded durable expiration cleanup are implemented. Cleanup
removes index visibility and writes tombstones; an explicit compaction plan can
then reclaim dead segment bytes. An opt-in, bounded shard-local background
scheduler is implemented with pause/reclamation telemetry; production disk-full
and hard compaction-preemption policy remain future work.

## Explicit non-goals for parity

These existed only as empty files, comments, or plans in the Go branch:

- QUIC
- SPDK and DPDK
- io_uring and direct I/O
- hint files
- functional WAL separate from the arena
- Prometheus metrics
- production structured logging
- complete client and benchmark tools
- distributed clustering

They must not be described as existing Redgo functionality. This list defines
what parity did not require, not Punah's present feature set: Punah has since
implemented compaction, benchmark tooling, and a bounded ordinary-file
`io_uring` adapter independently. Direct I/O, SPDK, DPDK, and the other listed
items remain roadmap work or explicit non-goals as documented elsewhere.

## Go behavior deliberately not copied

- TCP workers failed to decrement active connections or complete the shutdown
  wait group.
- listener acceptance could block cancellation indefinitely.
- response writes assumed one `Write` call consumed the full buffer.
- preallocated file length was confused with logical append position.
- only the newest existing arena file was mapped after restart.
- arena records omitted keys, preventing index reconstruction.
- delete changed only memory and could not survive restart.
- configured synchronous writes did not perform a per-operation durability
  action.
- global mutable hash seed could make maps interfere.
- the configured maximum value size was stored but not enforced.

Punah preserves protocol compatibility where useful, but correctness takes
priority over bug-for-bug compatibility.
