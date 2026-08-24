# Preliminary Redgo versus Punah comparison

Date: 2026-08-01. Host: Apple M4 MacBook Air, macOS. Both servers and the common
client were compiled in release mode and communicated over localhost using the
same Redgo v1 frame bytes. These are development measurements, not Linux
SPDK/DPDK projections.

## Compatibility repair and fairness

The `dev-kritik` branch no longer builds as a complete server: its engine tests
still pass `[]byte` values after `ShardedMap.Set` changed to accept
`RecordLocation`, and `cmd/server` calls the old two-argument TCP constructor.
The comparison checkout was temporary and received only these adapter changes:

- create the existing mmap `Storage` and pass it to `NewTCPServer`;
- disable benchmark-time logging.

No Go engine, storage, hashing, protocol, or synchronization behavior was
optimized. These temporary changes are not part of the Punah repository.

Redgo acknowledges an mmap copy without `msync`. Punah therefore ran with the
new explicit `Buffered` acknowledgement mode. Punah remains `LocalDurable` by
default; its durable SET numbers are intentionally not compared with Redgo's
weaker acknowledgement.

The common `tcp_compare` client uses persistent TCP connections, TCP_NODELAY,
1,024 preloaded keys, exact request/response sequencing per connection, and
identical CRC32-C v1 frames. Each table cell is the median throughput of three
30,000-operation trials. Mixed means 80% GET and 20% SET.

## Median throughput

| Payload | Clients | Workload | Redgo ops/s | Punah ops/s | Redgo/Punah |
|---:|---:|---|---:|---:|---:|
| 64 B | 1 | GET | 66,094 | 49,204 | 1.34x |
| 64 B | 1 | SET | 65,545 | 44,009 | 1.49x |
| 64 B | 1 | Mixed | 66,071 | 46,918 | 1.41x |
| 64 B | 4 | GET | 135,098 | 106,558 | 1.27x |
| 64 B | 4 | SET | 137,926 | 94,573 | 1.46x |
| 64 B | 4 | Mixed | 136,268 | 105,213 | 1.30x |
| 64 B | 16 | GET | 175,868 | 152,062 | 1.16x |
| 64 B | 16 | SET | 179,510 | 142,599 | 1.26x |
| 64 B | 16 | Mixed | 186,390 | 149,493 | 1.25x |
| 1 KiB | 1 | GET | 64,434 | 48,006 | 1.34x |
| 1 KiB | 1 | SET | 63,692 | 41,680 | 1.53x |
| 1 KiB | 1 | Mixed | 64,100 | 45,485 | 1.41x |
| 1 KiB | 4 | GET | 133,299 | 104,906 | 1.27x |
| 1 KiB | 4 | SET | 130,020 | 96,094 | 1.35x |
| 1 KiB | 4 | Mixed | 135,007 | 104,168 | 1.30x |
| 1 KiB | 16 | GET | 176,913 | 153,448 | 1.15x |
| 1 KiB | 16 | SET | 175,966 | 140,667 | 1.25x |
| 1 KiB | 16 | Mixed | 180,451 | 150,259 | 1.20x |

In this initial measurement, Redgo won the request/response throughput test. Its connection
goroutine directly takes the selected map/arena locks. Punah performs an extra
bounded-channel submission, allocation/copy, wakeup, and reply for every
request so that one thread exclusively owns each durable shard. That ownership
model was intended for batching and polling, but this measured revision did not
yet batch and therefore paid the cost without receiving the benefit. Later
sections record the first batching follow-up.

## Sustained churn and tail behavior

One fresh-server run used 200,000 mixed operations, 1 KiB values, and 16
connections:

| Metric | Redgo | Punah buffered |
|---|---:|---:|
| Throughput | 171,040 ops/s | 154,106 ops/s |
| p50 | 84.9 us | 95.5 us |
| p99 | 225 us | 223 us |
| p99.9 | 487 us | 334 us |
| Server RSS after run | 56,352 KiB | 7,936 KiB |
| Traced GC cycles during run | 136 | none (non-GC runtime) |

This is where the reason for the Rust port becomes visible. Redgo's mmap arena
keeps values outside the Go heap, and its live Go heap remained only a few MiB,
but protocol/key/response allocations still triggered frequent small garbage
collections. Punah used roughly one seventh of the measured RSS and had a 31%
lower p99.9 in this run, despite lower median throughput.

## Topology defect found by measurement

Punah initially inherited Redgo's default of 256 shards. In Redgo those are 256
mutex-protected maps; in Punah each shard owns an OS thread and store. The first
run consequently created 256 threads and used about 81 MiB RSS. The unused
`worker_threads` configuration suggested a separation the runtime did not
implement.

The configuration now exposes the real topology: one owner thread per shard.
Its portable default is the largest power of two no greater than available host
parallelism (eight on this host). Idle RSS fell to about 6 MiB and post-churn
RSS to about 8 MiB. Linux polling deployments can later pin the same owners.

## Interpretation and next optimization

Rust has not produced a headline throughput jump yet. It has already produced
substantially lower resident memory and better extreme-tail behavior under
churn, while also implementing stronger recovery, tombstone, fencing, and
identity semantics.

The next measured optimization target at that point was the TCP-to-owner boundary:

1. submit batches rather than one request per channel wakeup;
2. pass owned frame buffers through without cloning key/value fields again;
3. return completion batches or pipeline request IDs instead of synchronously
   waiting after every submission;
4. measure queue delay separately from storage and protocol time.

That work is useful for io_uring, QUIC, DPDK, and SPDK alike. Removing the owner
boundary merely to win the portable microbenchmark would discard the target
shared-nothing design.

## Batched owner submission follow-up

The first optimization pass now implements connection-local v1 pipelining and
shard-local batch submission:

- the TCP reader groups only complete frames already present in its 64 KiB
  buffer, up to 32 at a time, so it never waits to fill a batch;
- validation remains transport-independent;
- one runtime message and reply channel carries all commands for one owner;
- commands for the same key retain request order because they share an owner;
- responses are restored to connection order;
- mixed v1/v2 batches use the conservative sequential path until v2 lease
  operations receive their own typed batch semantics.

For 100,000 1 KiB mixed operations, three-trial median throughput was:

| Clients | Pipeline | Throughput | Representative p50 | Representative p99 |
|---:|---:|---:|---:|---:|
| 1 | 1 | 44.6k ops/s | 22 us | 31 us |
| 1 | 4 | 97.2k ops/s | 35 us | 50 us |
| 1 | 16 | 178k ops/s | 59 us | 95 us |
| 1 | 32 | 254k ops/s | 71 us | 132 us |
| 4 | 1 | 106k ops/s | 34 us | 79 us |
| 4 | 4 | 189k ops/s | 64 us | 133 us |
| 4 | 16 | 324k ops/s | 134 us | 241 us |
| 16 | 1 | 143k ops/s | 98 us | 299 us |
| 16 | 4 | 243k ops/s | 195 us | 530 us |
| 16 | 16 | 416k ops/s | 327 us | 2.76 ms |
| 16 | 32 | 478k ops/s | 769 us | 1.97 ms |

The throughput recovery is substantial, but queueing latency rises with the
number of in-flight operations. Pipeline depth is therefore a client workload
choice, not a server-wide promise. Interactive agent requests should use a
small depth; bulk artifact transfer and prefetch can use larger windows.

## Location-backed mmap follow-up

Punah no longer retains a heap-owned payload in every live index entry. Every
payload remains authoritative in its append-only segment and GET resolves the
indexed `RecordLocation` through a bounds-checked read-only mapping.

An Apple M4 development run used 100,000 unique keys with 1 KiB payloads,
16 clients, pipeline depth 16, and buffered durability:

| Phase | Throughput | Server RSS |
|---|---:|---:|
| SET 100,000 unique values | 270,904 ops/s | 28 MiB |
| GET across all 100,000 values | 470,176 ops/s | 141 MiB |

The post-SET RSS is the important heap-scaling signal: approximately 98 MiB of
logical payload does not remain duplicated in the index. The post-GET RSS
includes mapped file pages made resident by touching the complete dataset.
Those pages are managed and reclaimable by the operating system; RSS alone
does not distinguish them from private heap, so it must not be interpreted as
an application retention figure.

This run is a development measurement, not an mmap-versus-Redgo comparison.
Its purpose is to verify the expected scaling shape and ensure that restoring
Redgo's location-only index did not destroy GET throughput.

## Reproducing the client side

Build and invoke the shared load generator with:

```console
cargo build --release -p punah-bench --bin tcp_compare
target/release/tcp_compare 127.0.0.1:7379 mixed 30000 1024 16 1
```

Pipeline depth and key-space size are optional and respectively default to one
and 1,024:

```console
target/release/tcp_compare 127.0.0.1:7379 set 100000 1024 16 16 100000
```

Output is CSV containing elapsed time, throughput, p50, p99, and p99.9.

## Current status

This document intentionally preserves the original comparison and its
follow-ups. It is not the current hot-path description: Punah now also has
single-encode admission, capacity-flat mmap recovery, durable storage cohorts,
per-shard ingress gates, and size-thresholded response scatter/gather. Their
trial values and design reasons are in the
[August 2026 optimization log](optimization-log-2026-08.md).
