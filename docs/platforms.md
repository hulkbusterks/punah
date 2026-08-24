# Platform support and acceleration

Punah separates portable semantics from accelerated device adapters. Linux is
the production-performance target, not the minimum environment required to use
or develop the system.

## Support tiers

| Tier | Platforms | Purpose | Backends |
|---|---|---|---|
| Portable correctness | Linux and macOS | Local agent swarms, development, CI, recovery and format validation | Standard files and TCP |
| Production performance | Linux | Shared inference nodes and distributed deployments | io_uring, then optional SPDK, DPDK, RDMA/GDS integrations |
| Future best effort | Windows | Client development and portable-node evaluation | Standard files and TCP |

The portable backend is a real implementation, not a mock. It must retain the
same record, identity, durability, recovery, manifest, compaction, and protocol
semantics as accelerated backends. Performance and exact persistence boundaries
remain backend-specific and must be documented and benchmarked separately.

## Dependency boundary

Domain crates do not contain operating-system conditionals or processor
intrinsics. Portable maintained libraries may dispatch to hardware CRC, SIMD,
or other instructions internally while preserving a scalar fallback.

Linux-only facilities live in leaf adapter crates:

```text
domain and codecs
       |
polling device and transport traits
       |
       +-- portable file/socket adapters
       +-- Linux io_uring adapter
       +-- Linux SPDK block adapter
       +-- Linux DPDK/RDMA transport adapter
```

An optimized adapter may strengthen performance but may not weaken commit,
ownership, fencing, checksum, or recovery invariants. `unsafe` is denied by
default. The mapped-file module and Linux `io_uring` SQE submission module are
the two documented local exceptions; future FFI crates must opt in locally,
document every safety contract, and expose ownership-based safe APIs to the
rest of Punah.

## Optimization order

1. Establish shared-node workload curves with the portable backend.
2. Remove algorithmic allocation, serialization, and queue bottlenecks.
3. Measure the checked-in bounded ordinary-file `io_uring` backend against
   `FileDevice`; it intentionally has no direct-I/O or registered-buffer claims.
4. Measure whether kernel networking is limiting the intended workload.
5. Add SPDK for direct NVMe access when storage measurements justify it.
6. Add DPDK or RDMA only when network measurements justify their operational
   and maintenance cost.

This order keeps local development useful and prevents specialized hardware
from hiding incorrect lifecycle or concurrency semantics.

The Linux-only `UringDevice` now implements the same owned-buffer `BlockDevice`
contract with bounded admission, request-ID/CQE correlation, asynchronous
read/write, and `Fsync(DATASYNC)`. The matching writer benchmark is:

```sh
cargo run --release --locked -p punah-bench --bin uring_compare -- \
  30000 1024 64 64 5
```

Run it on disclosed Linux kernel/filesystem/hardware. The binary returns an
explicit unsupported error on other platforms; this repository records no
`io_uring` performance number from macOS.

## Product boundary

Punah is a computation-aware reuse and cache layer. It may use a filesystem,
object store, or enterprise data platform as a durable capacity tier; it does
not attempt to replace their general namespaces, snapshots, erasure coding,
data protection, or enterprise administration.

The portable node should support useful local swarms. The distributed Linux
deployment should target agent artifacts, operation results, context objects,
and optional inference KV blocks close to compute.
