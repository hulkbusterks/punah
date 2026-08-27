# Benchmark policy

Punah reports measured results, hardware configuration, build flags, workload,
and reproducible commands. Targets are not presented as achieved performance.

Primary scaling studies include:

- throughput versus worker count
- latency versus queue depth and batch size
- lookup cost versus entry count
- memory overhead versus object count
- recovery time versus committed bytes and record count
- compaction cost versus live-data ratio
- foreground P50/P95/P99/P99.9 while background compaction reports actual
  input, copied, reclaimable bytes, failures, and pause-budget overruns
- cross-core and cross-NUMA routing ratio
- agent-level tool calls, model tokens, bytes, and wall time saved

Every optimization should first state its expected curve: constant with
preallocated capacity, linear with committed bytes, sublinear with shard count,
or amortized with cohort size. A benchmark is useful when it can disprove that
shape, even if the absolute number comes from development hardware.

Report P50, P95, P99, and P99.9 latency with throughput. Separate exact hits,
misses, writes, small artifacts, and large artifacts.

The M4 development environment establishes correctness and comparative scaling
curves. Kernel-bypass numbers require Linux hardware with disclosed NIC, NVMe,
PCIe topology, NUMA placement, hugepage configuration, and core affinity.

Backends should be compared progressively: memory, ordinary file/TCP, optional
io_uring, SPDK, then DPDK plus SPDK.

For network changes, hold the client binary fixed when isolating server work.
The shared `tcp_compare` client uses Punah's own encoder/decoder; rebuilding it
can change allocations or copies on both sides and obscure the server effect.
For recovery, sweep segment capacity independently from record count so a
whole-preallocation scan cannot hide inside a committed-byte curve. Report at
least three trials and retain the individual observations, not only the median.

Local-durability group commit has a dedicated writer-layer harness:

```sh
cargo run --release --locked -p punah-bench --bin cohort_compare -- \
  500 64,1024 2,4,8,16,32,64 5 64
```

It creates a fresh sparse file per row, alternates case order, and reports actual
records per append, accepted writes/bytes, Flush count, QueueFull retries, and
P50/P95/P99/P99.9 group latency. Rows fail validation unless LocalDurable used
two accepted writes per record and two Flush operations per append call. Group
latency is not individual request latency; this harness isolates the writer and
does not measure Runtime draining, TCP, rotation, or metadata publication.

The matching Runtime/Store harness validates that automatic owner-thread
draining reaches the writer rather than merely measuring a manually assembled
cohort:

```sh
cargo run --release --locked -p punah-bench --bin runtime_cohort_compare -- \
  200 256 2,4,8,16,32,64 3
```

It reconciles submitted operations with authoritative Store counters for
records, encoded bytes, actual append groups, Flushes, and rotations. The
background-maintenance A/B harness likewise reports foreground latency and
actual compaction input/copied/reclaimable bytes, failures, and pause overruns:

```sh
cargo run --release --locked -p punah-bench --bin compaction_churn -- \
  160 256 8 16384 3 16 65536 7500 25 8 2
```

The final two arguments model one never-overwritten persistent key every eight
writes and require two partial inputs per replacement. CSV includes the
configured and achieved reclaim ratios, foreground encoded bytes, logical
write amplification, maximum per-cycle input/output, retained logical and
allocated segment bytes, verified hot-cache reopen time, and actual maintenance
steps, copy slices, and durable-output preparations. `max_pause_ns` is the
largest individual owner-thread step, not the full wall time of a multi-slice
compaction or its off-owner publication task. While that task is in flight, an
idle shard waits up to 1 ms for a foreground request before polling completion;
this bounds idle polling without intentionally delaying queued work.

Response assembly has an in-process A/B harness that keeps one executable and
client implementation, changes only `ServerConfig` between forced contiguous
and vectored output, alternates order, preloads outside timing, validates every
response byte, and uses a fresh Store per case:

```sh
cargo run --release --locked -p punah-bench --bin tcp_output_compare -- \
  100000 16384,32768,65536 16 16 1024 5
```

The optional final selector is `both`, `contiguous`, or `vectored`. It exists so
one response path can be measured as one OS process without adding unsafe
platform-counter FFI to the benchmark crate. On Linux, build once and collect
CPU plus fault counters for each path independently:

```sh
cargo build --release --locked -p punah-bench --bin tcp_output_compare
perf stat -r 5 -e task-clock,cycles,instructions,minor-faults,major-faults -- \
  target/release/tcp_output_compare 100000 32768 16 16 1024 1 contiguous
perf stat -r 5 -e task-clock,cycles,instructions,minor-faults,major-faults -- \
  target/release/tcp_output_compare 100000 32768 16 16 1024 1 vectored
```

The CSV elapsed time excludes node startup and preload; external process
counters necessarily include them. Compare counter deltas only between matched
fresh cases, alternate which mode runs first, retain every observation, and do
not present the counters as hot-phase-only values.

Hosted CI compiles benchmark code but does not gate throughput. Development
results and decisions are recorded in the
[August 2026 optimization log](optimization-log-2026-08.md); production claims
require disclosed Linux topology and dedicated runs.
