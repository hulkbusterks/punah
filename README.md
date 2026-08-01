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

Install a stable Rust toolchain and run:

```sh
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Generate the deterministic 1-to-64-agent reuse curve with:

```sh
cargo run --release -p punah-sim
```

Run a local node with an optional listen address and data directory:

```sh
cargo run -p punah-server -- 127.0.0.1:7379 ./punah-data
```

The macOS reference implementation is for correctness and scaling curves.
SPDK/DPDK validation requires a suitable Linux host with direct device access.

## Current portable baseline

An initial release-mode smoke run on an Apple M4 MacBook Air demonstrates that
the benchmark harness and durable recovery path work. These are regression
anchors, **not competitive claims**: concurrency currently means independent
stores, the run is a single sample, and the intentionally tiny 64 KiB segments
make manifest publication and rotation unusually frequent.

| Workload | Payload | Workers | Throughput | Write p50 | Write p99 | Segments |
|---|---:|---:|---:|---:|---:|---:|
| Buffered write | 64 B | 1 | 12,099 ops/s | 10.9 µs | 31.8 µs | 5 |
| Buffered write | 64 B | 4 | 15,260 ops/s | 11.5 µs | 1.78 ms | 8 |
| Buffered write | 1 KiB | 1 | 2,976 ops/s | 27.7 µs | 15.8 ms | 20 |
| Buffered write | 1 KiB | 4 | 3,815 ops/s | 44.0 µs | 28.1 ms | 24 |
| Recovery | 64 B | 1 | 2.88 ms total | — | — | 5 |
| Recovery | 1 KiB | 1 | 13.0 ms total | — | — | 19 |

The exact command, raw CSV, environment, and interpretation are preserved in
[the current benchmark baseline](docs/benchmark-baseline.md). The next benchmark
milestone adds longer statistical runs, CPU/RSS/device measurements, TCP load,
durable-mode sweeps, and equivalent Redis comparisons.

A newer three-trial shared-node mixed run now reaches median aggregate rates of
approximately 253k ops/s for 64-byte payloads on one client, 313k ops/s with
four clients on one shard, and 155k ops/s for 1 KiB payloads with four clients
across four shards. See the baseline document for the complete matrix and why
these remain development measurements rather than comparative claims.

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
- [Platform support and acceleration](docs/platforms.md)
- [Component and policy boundaries](docs/component-boundaries.md)
- [Code organization](docs/code-organization.md)
- [Changes from the Redgo Go prototype](docs/migration-from-redgo-go.md)
- [Redgo parity definition](docs/redgo-parity.md)
- [Benchmark policy](docs/benchmarks.md)
- [Current benchmark baseline](docs/benchmark-baseline.md)
- [Preliminary Redgo performance comparison](docs/redgo-performance-comparison.md)

## License

Punah is available under the MIT License.
