# Punah

*Punah* comes from Sanskrit `पुनः` (*punaḥ*): again, anew, once more.

Punah is a Linux-first, userspace distributed cache for immutable agent
artifacts and reusable inference context. Its data plane is designed to grow
into a polling, shared-nothing architecture backed by DPDK and SPDK, while
remaining testable on ordinary development machines.

It is a Rust redesign of [Redgo](https://github.com/hulkbusterks/redgo), the
original Go prototype that established its sharded, mmap-backed direction.

The portable single-node reference path is runnable and tested end to end.
Performance claims must still be backed by reproducible benchmarks; SPDK and
DPDK backends remain future Linux work.

## Initial scope

- Content-addressed, immutable artifacts
- Exact operation-result reuse
- Expiration, tombstones, and bounded leases
- Append-only, self-describing storage segments
- Location-only indexes with mmap-backed payload reads
- Crash recovery and background compaction
- Portable reference backends before SPDK/DPDK backends

Punah is not an inference server, vector database, agent framework, or general
Redis replacement.

## Workspace

- `punah-types`: stable domain and storage types
- `punah-device`: polling-oriented block-device interface
- `punah-engine`: index and storage-engine contracts
- `punah-operation`: canonical operation descriptors and exact operation IDs
- `punah-identity`: BLAKE3 artifact identity and XXH3 shard routing
- `punah-checksum`: portable hardware-dispatched CRC32-C corruption checks
- `punah-cluster`: immutable membership epochs and deterministic node ownership
- `punah-events`: loss-aware, non-authoritative lifecycle observations
- `punah-metadata`: crash-safe superblock, checkpoint, and journal publication
- `punah-compaction`: deterministic bounded compaction planning
- `punah-admin`: read-only inspection and explicit import/repair/quarantine tools
- `punah-protocol`: Redgo-compatible v1 and native Punah v2 wire codecs
- `punah-network`: bounded, stoppable TCP transport
- `punah-record`: versioned durable record codec
- `punah-store`: durable multi-segment reference store
- `punah-service`: transport-independent command dispatch
- `punah-server`: runnable node assembly and executable
- `punah-bench`: CSV scaling and recovery benchmark harness
- `punah-sim`: deterministic swarm reuse, ownership, and failover simulator
- `docs`: architectural and correctness specifications

## Development

The repository pins its tested Rust toolchain. Run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --all-features --no-deps --locked
```

Generate the deterministic 1-to-64-agent reuse curve with:

```sh
cargo run --release -p punah-sim
```

Run a local node with an optional listen address and data directory:

```sh
cargo run -p punah-server -- 127.0.0.1:7379 ./punah-data
```

Inspect a store without modifying it, or explicitly adopt legacy segments:

```sh
cargo run -p punah-admin -- inspect ./punah-data/shard-0000
cargo run -p punah-admin -- import ./punah-data/shard-0000 1073741824 default
```

`repair` is accepted only for nonzero corrupt roots and preserves the old
metadata under `quarantine/`; `quarantine-orphans` moves only files proven
unreferenced by a healthy root. Neither command deletes data.

The macOS reference implementation is for correctness and scaling curves.
SPDK/DPDK validation requires a suitable Linux host with direct device access.

## Current portable measurements

Release-mode Apple M4 development measurements now capture the important
scaling changes, not a single headline number:

| Change | Before | After | Result |
|---|---:|---:|---:|
| Open 100k × 1 KiB records in a 1 GiB segment | 402.7 ms median | 22.89 ms median | 17.6x faster |
| Local-durable TCP SET, 1 KiB, 16 clients/pipeline 16 | 394 ops/s | 2,986 ops/s | 7.6x throughput |
| Writer LocalDurable, 64 B, singleton→mean 62.5 records/cohort | 126 ops/s | 7,945 ops/s | 62.8x via flush amortization |
| Buffered shared node, 64 B, concurrency 16, 1→2 shards | 322.6k ops/s | 439.6k ops/s | sublinear scaling, then plateau |
| 32 KiB verified GET, contiguous→vectored output | 126.4k ops/s | 164.2k ops/s | +29.9%; won 5/5 paired trials |
| Partial-live compaction owner stall | ~104–107 ms whole compaction | 7.86–10.01 ms max step | copy and authority I/O are off the owner |

These are regression anchors, not Linux or kernel-bypass claims. The recovery
path is consistent with flat cost across preallocated capacity,
local durability amortizes two flushes across bounded cohorts, and runtime
admission is per-shard rather than globally locked. A controlled five-trial
server A/B validates the 32 KiB scatter/gather crossover; smaller bodies retain
the contiguous path. Background compaction yields between bounded copy slices,
then syncs the captured active prefix and publishes metadata on a helper thread.
Foreground appends may continue beyond that fence; a rare rotation rendezvous
with the publication before changing segment topology.

The complete decisions, trial values, caveats, and commands are preserved in
the [August 2026 optimization log](docs/optimization-log-2026-08.md). The
[initial benchmark baseline](docs/benchmark-baseline.md) and
[preliminary Redgo comparison](docs/redgo-performance-comparison.md) remain as
historical checkpoints rather than descriptions of the current hot path.

## Design documentation

- [Architecture](docs/architecture.md)
- [Mapped segment safety](docs/mapped-segment-safety.md)
- [Record format](docs/record-format.md)
- [Durability](docs/durability.md)
- [Metadata format](docs/metadata-format.md)
- [Compaction](docs/compaction.md)
- [Hashing and identity](docs/hashing.md)
- [Canonical operations](docs/operations.md)
- [Native protocol v2](docs/protocol-v2.md)
- [Lifecycle events and resynchronization](docs/lifecycle-events.md)
- [Swarm reuse simulation](docs/swarm-simulation.md)
- [Cluster membership authority](docs/cluster-authority.md)
- [Safety policy](docs/safety.md)
- [Verification and fuzzing](docs/verification.md)
- [Platform support and acceleration](docs/platforms.md)
- [Component and policy boundaries](docs/component-boundaries.md)
- [Code organization](docs/code-organization.md)
- [Changes from the Redgo Go prototype](docs/migration-from-redgo-go.md)
- [Redgo parity definition](docs/redgo-parity.md)
- [Benchmark policy](docs/benchmarks.md)
- [Current benchmark baseline](docs/benchmark-baseline.md)
- [August 2026 optimization log](docs/optimization-log-2026-08.md)
- [Preliminary Redgo performance comparison](docs/redgo-performance-comparison.md)

## License

Punah is available under the MIT License.
