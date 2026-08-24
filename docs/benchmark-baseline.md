# Portable benchmark baseline

This document preserves Punah's first measured release-mode baseline. It proves
that the harness produces machine-readable scaling and recovery observations;
it is not a performance claim or an SPDK/DPDK projection.

## Environment

- Recorded: 2026-08-01
- Machine: Apple MacBook Air, Apple M4, 10 cores (4 performance, 6 efficiency)
- Memory: 16 GB
- Architecture: arm64
- Operating system: Darwin kernel 25.5.0
- Rust: 1.97.1, LLVM 22.1.6
- Build profile: `release`
- Durability: `Buffered`
- Segment capacity: 65,536 bytes
- Samples per point: one

The benchmark's worker dimension currently creates independent stores in
independent directories. It measures portable horizontal scaling without a
shared lock or shared device; it must not be described as one-node concurrent
request throughput.

The 64 KiB segment capacity is intentionally small enough to exercise rotation
and recovery with only 1,000 records. It exaggerates metadata publication and
filesystem flush overhead compared with production-sized segments.

## Reproduction

```sh
cargo run --release -p punah-bench --bin punah-bench -- \
  --mode all \
  --operations 1000 \
  --payload-sizes 64,1024 \
  --concurrency 1,2,4 \
  --segment-capacities 65536
```

## Raw CSV

```csv
benchmark,profile,durability,operations,payload_bytes,concurrency,segment_capacity,segments,elapsed_ns,throughput_ops_per_second,latency_p50_ns,latency_p99_ns
write,release,buffered,1000,64,1,65536,5,82650416,12099.153,10875,31833
write,release,buffered,1000,64,2,65536,6,75612958,13225.246,11125,60042
write,release,buffered,1000,64,4,65536,8,65532917,15259.507,11458,1782208
write,release,buffered,1000,1024,1,65536,20,336018459,2976.027,27708,15830250
write,release,buffered,1000,1024,2,65536,22,312771334,3197.224,32708,28757208
write,release,buffered,1000,1024,4,65536,24,262136084,3814.812,43959,28066125
recovery,release,buffered,1000,64,1,65536,5,2877125,347569.188,0,0
recovery,release,buffered,1000,1024,1,65536,19,12987416,76997.611,0,0
```

Recovery throughput is derived from records divided by reopen time. Recovery
rows do not report operation latency, so their p50 and p99 fields are zero.

## Initial observations

- Increasing independent workers raises aggregate throughput, but the present
  sample is too small and noisy to characterize a scaling law.
- Larger payloads create more segments and increase total recovery time.
- The high 1 KiB write p99 values coincide with frequent segment rotation and
  crash-safe manifest publication. Production-sized segment sweeps are needed
  to separate steady-state append cost from rotation cost.
- Results need warmup, repeated trials, confidence intervals, CPU/RSS/device
  measurements, mixed read/write workloads, and shared-node execution before
  they can guide public performance comparisons.

## Shared-node mixed baseline

After introducing shard-owned workers, a second release-mode run measured one
shared Punah runtime. Every point uses the deterministic default mix—60% GET,
25% SET, 10% overwrite, and 5% DELETE—with 500 warmup operations and three
measured trials. Reported values below are medians selected by aggregate
throughput; latency values come from the same trial.

```sh
cargo run --release -p punah-bench --bin punah-bench -- \
  --mode shared \
  --operations 5000 \
  --payload-sizes 64,1024 \
  --concurrency 1,4 \
  --shards 1,4 \
  --segment-capacities 1048576 \
  --trials 3 \
  --warmup 500 \
  --seed 42
```

| Payload | Clients | Shards | Median throughput | Aggregate p50 | Aggregate p99 |
|---:|---:|---:|---:|---:|---:|
| 64 B | 1 | 1 | 252,531 ops/s | 3.38 µs | 8.71 µs |
| 64 B | 1 | 4 | 256,522 ops/s | 3.50 µs | 8.63 µs |
| 64 B | 4 | 1 | 312,634 ops/s | 11.7 µs | 30.0 µs |
| 64 B | 4 | 4 | 316,513 ops/s | 10.2 µs | 38.9 µs |
| 1 KiB | 1 | 1 | 86,970 ops/s | 3.75 µs | 10.5 µs |
| 1 KiB | 1 | 4 | 213,041 ops/s | 4.17 µs | 11.3 µs |
| 1 KiB | 4 | 1 | 113,523 ops/s | 9.54 µs | 24.2 µs |
| 1 KiB | 4 | 4 | 155,345 ops/s | 10.9 µs | 38.7 µs |

No measured request encountered bounded-queue backpressure in this matrix. The
1 KiB results show useful shard scaling; the 64-byte workload is sufficiently
small that scheduling, channel, and filesystem overhead dominate and extra
shards provide little median benefit at four clients.

These results include request submission, queueing, shard dispatch, exact-key
lookup, record encoding, BLAKE3 identity, CRC32-C, and the configured buffered
file path. They exclude TCP and use only 5,000 measured operations per point.
Future baselines must use longer runs, confidence intervals rather than three
sample medians, CPU/RSS/device counters, durable mode, network clients, and a
matched comparison target.
