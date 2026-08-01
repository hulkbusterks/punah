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
- cross-core and cross-NUMA routing ratio
- agent-level tool calls, model tokens, bytes, and wall time saved

Report P50, P95, P99, and P99.9 latency with throughput. Separate exact hits,
misses, writes, small artifacts, and large artifacts.

The M4 development environment establishes correctness and comparative scaling
curves. Kernel-bypass numbers require Linux hardware with disclosed NIC, NVMe,
PCIe topology, NUMA placement, hugepage configuration, and core affinity.

Backends should be compared progressively: memory, ordinary file/TCP, optional
io_uring, SPDK, then DPDK plus SPDK.
