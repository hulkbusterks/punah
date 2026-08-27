# Portable optimization log — August 2026

This log records why the portable path changed, not just its latest numbers.
All measurements below are development data from an Apple M4 MacBook Air on
macOS, built in release mode. They are useful for regression direction and
scaling shape; they are not Linux, io_uring, SPDK, or DPDK claims.

## Decisions and measured effects

### Admit, then encode exactly once

Store first measures `encoded_len`, rejects a record larger than the complete
segment, and rotates only when the record can fit a fresh segment. The writer
checks state, queue admission, arithmetic, and capacity before calculating
checksums and copying the accepted record once. Rotation therefore no longer
performs a throwaway full encoding.

For buffered TCP SET, 1 KiB values, 16 clients, pipeline 16, and a 100,000-key
space, the three-trial median moved from 278,040 to 286,520 ops/s, about 3%.
Compaction already encoded each retained record once and was left unchanged.

### Map capacity; scan only committed records

The first location-backed recovery implementation read each complete
preallocated file and tested its whole zero tail. Store recovery now maps a
segment once, copies and validates one 128-byte header, bounds the declared
record against its authoritative fence, and copies only that record into a
reused owned buffer. No Rust reference is created over file-backed bytes.

After that final safety shape, 100,000 records of 1 KiB in a 1 GiB segment
reopened in 23.131/22.840/22.890 ms. Against the original 724.3/402.7/335.4 ms
trials, the medians are 22.890 ms and 402.7 ms: a 17.6x reduction. At 10,000
records, the three-trial medians were 2.308 ms for 64 MiB capacity and 2.200 ms
for 1 GiB. This is consistent with the intended capacity-flat shape on a warm
same-process filesystem cache; it is not a cold-start claim.

The payload remains mmap-authoritative for every size. A future small-object
cache, if profiling justifies it, will be bounded and discardable rather than a
second durable representation.

### Cohort local durability

The portable writer can reserve a bounded cohort, submit multiple bodies,
share one durability barrier, submit multiple markers, share the final barrier,
and publish in reservation order. Store cohorts are capped at 64 records and
about 1 MiB. Runtime workers add zero intentional wait and drain only mutations
already queued; GET is an ordering barrier and oversized values run alone.

A matched 30,000-operation TCP SET run with 1 KiB values, 16 clients, pipeline
16, and `LocalDurable` acknowledgement rose from 394.32 to 2,986.47 ops/s,
about 7.6x. This is the expected gain from amortizing two filesystem flushes;
it is not a multi-record atomicity promise. That was one end-to-end matched
point, so the writer mechanism is also measured independently below.

`cohort_compare` uses the real `SegmentWriter<FileDevice>`, a new sparse file
per row, and telemetry that rejects a row unless writes equal `2 × records` and
flushes equal `2 × append calls`. With 500 operations, queue depth 64, and five
trials, median throughput scaled as follows:

| Requested cohort | Mean actual records | Flushes / 500 records | 64 B | 1 KiB |
|---:|---:|---:|---:|---:|
| singleton | 1.0 | 1,000 | 126 ops/s | 128 ops/s |
| 2 | 2.0 | 500 | 257 ops/s | 253 ops/s |
| 4 | 4.0 | 250 | 499 ops/s | 506 ops/s |
| 8 | 7.94 | 126 | 1,010 ops/s | 1,001 ops/s |
| 16 | 15.63 | 64 | 1,962 ops/s | 1,969 ops/s |
| 32 | 31.25 | 32 | 3,970 ops/s | 3,970 ops/s |
| 64 | 62.5 | 16 | 7,945 ops/s | 7,689 ops/s |

The 64-byte endpoint is 62.8x the singleton rate. The near-linear curve is
flush amortization, not a claim of 63x more CPU capacity: median group latency
remained near 8 ms while more records shared each pair of barriers. This
writer-layer curve excludes Runtime draining, TCP, rotation, and metadata.

The subsequent Runtime/Store curve uses owner-thread telemetry rather than
inferring batches from submitted requests. With 200 operations, 256-byte values,
LocalDurable files, no rotation, and three trials, every row reconciled 200
records, 83,200 encoded bytes, and exactly two flushes per actual append group.
Median throughput was 127.1 ops/s for singleton, then 252.9, 502.7, 1,006.1,
1,932.3, 3,640.2, and 6,079.1 ops/s for requested groups 2–64. The batch-64
median is 47.8x singleton (the individual trials ranged from 47.1x to 51.2x).
Group p50 remained close to one 8 ms durability pair; these are group latencies,
not synthetic per-record latency values. This includes Runtime dispatch, Store
record preparation, durable append, and index publication, but not TCP.

### Per-shard ingress instead of a global gate

The runtime's former global submission mutex existed only to order acceptance
against shutdown. It is now one small gate per shard: a submit rechecks the
accepting flag under its owner's gate, and shutdown closes acceptance before
placing each sentinel. Same-shard order is preserved while unrelated shards no
longer serialize at admission.

A 50,000-operation shared-node sweep with concurrency 16 and 1,024 seeded hot
keys (SET also creates `new-*` keys) produced these three-trial medians:

| Payload | 1 shard | 2 shards | 4 shards | 8 shards |
|---:|---:|---:|---:|---:|
| 64 B | 322,593 | 439,575 | 408,497 | 417,161 ops/s |
| 1 KiB | 358,132 | 400,807 | 407,866 | 401,043 ops/s |

The shape is sublinear and saturates near two to four owners on this machine;
adding shards is no longer blocked by a global lock, but it cannot create more
CPU, cache bandwidth, or socket throughput. This is a post-change scaling curve,
not a matched global-gate A/B, so it does not quantify the mutex removal alone.

### Scatter/gather only where it wins

GET must currently copy mmap bytes into an owned payload so compaction cannot
retire a segment during network output. The response path can still avoid the
second payload-sized copy by checksumming a fixed header and using vectored
writes. A fixed pre-change client binary isolated the server-side change.

The old three-trial comparison was noisy and its client did not prove every GET
was a seeded hit. The checked-in client now validates opcode, key, length, and
every payload byte with a reusable borrowed `FrameBuffer`. A new integrated A/B
goes further: the same executable starts fresh in-process nodes, changes only
`ServerConfig` between forced contiguous and vectored responses, preloads
outside timing, alternates order, and validates every response.

At 100,000 verified GETs, 16 clients, pipeline 16, 1,024 hot keys, and five
trials, the throughput observations were:

| Payload | Contiguous trials (ops/s) | Vectored trials (ops/s) | Median result |
|---:|---|---|---:|
| 16 KiB | 213869, 222697, 235344, 169872, 94842 | 211055, 241123, 212504, 235050, 191876 | 212.5k vs 213.9k; -0.6% |
| 32 KiB | 145945, 106562, 126384, 127413, 62093 | 166455, 164159, 144794, 169885, 94436 | 164.2k vs 126.4k; +29.9% |
| 64 KiB | 65824, 78157, 82256, 59060, 80477 | 85202, 76226, 95430, 62771, 84968 | 85.0k vs 78.2k; +8.7% |

The environment remained noisy, but vectors won all five paired 32 KiB trials
and four of five at 64 KiB; 16 KiB was effectively neutral at the median. The
32 KiB automatic threshold is therefore retained with direct evidence rather
than a hypothesis. True mmap-to-socket borrowing remains gated on read leases
and deferred segment retirement.

A later external-counter sanity run isolated one mode per process on the Apple
M4. Each process ran three fresh 100,000-GET, 32 KiB cases with the same 16
clients, pipeline 16, and 1,024-key fixture. Contiguous throughput observations
were 165.4k, 147.4k, and 151.6k ops/s; vectored observations were 175.0k,
186.6k, and 174.6k ops/s, a +15.5% median delta. Whole-process macOS accounting
reported 99.87 billion versus 95.51 billion instructions (-4.4%), 40.01 billion
versus 37.63 billion cycles (-6.0%), 8,057 versus 7,837 page reclaims, and zero
major page faults for both. Those counters include startup, preload, and
teardown and the two processes were not interleaved, so they support the
direction but do not replace the paired hot-interval A/B or the pending Linux
`perf stat` run.

### First background-compaction churn smoke test

The checked-in `compaction_churn` binary now alternates disabled/background
case order and reports real Runtime maintenance counters alongside foreground
SET latency. A small Apple M4 development run used 200 LocalDurable overwrites,
256-byte values, eight hot keys, 16 KiB segments, a 16-mutation check interval,
one-segment input budget, and three trials. It was a harness validation, not a
production tuning run.

Disabled throughput medians were 121.0, 123.8, and 121.8 ops/s. Background
medians were 112.1, 111.6, and 111.2 ops/s. Every background trial performed
four all-dead retirements, reporting 65,280 input/reclaimable bytes and zero
copied bytes. Median SET latency stayed near 8.0 ms, while p99 rose from roughly
26–27 ms to 40 ms. All twelve compactions exceeded the provisional 25 ms pause
target; max observed pauses were 32.2, 37.8, and 34.3 ms.

That shape is useful despite its tiny scale: the byte accounting is internally
consistent, and it proves the current synchronous publication step cannot claim
a hard pause bound. The next implementation step is resumable copy/publication
work or a dedicated all-dead retirement path; the current scheduler exposes the
overrun instead of hiding it.

The all-dead path was then changed to publish retirement directly: it no longer
allocates a generation or creates, syncs, and removes an empty replacement.
The matched rerun still produced background medians of 110.3, 111.1, and 113.1
ops/s versus disabled 119.3, 118.2, and 119.3 ops/s. Background p99 improved to
about 36 ms and max pauses to 32.9, 28.9, and 31.0 ms, but all twelve cycles
still exceeded 25 ms. The remaining cost is dominated by mandatory active-file,
metadata, and directory durability barriers. Reducing manifest bytes with an
incremental journal would not remove those barriers, so journal v2 remains
measurement-gated rather than being added as speculative format complexity.

### Reclaim yield and input aggregation

A partial-live sweep showed that byte bounds alone still allowed wasteful
maintenance. With 160 LocalDurable writes, 256-byte values, 64 cyclic hot keys,
16 KiB segments, and checks every 16 mutations, a 50% minimum reclaimed the
same 32,640 bytes as 75% but copied 9,792 rather than 3,264 bytes. Median
background throughput was 97.8 versus 107.9 ops/s, logical write amplification
was 1.15x versus 1.05x, and maximum pauses were about 155 versus 90 ms. The
disabled medians for those alternated runs were 120.1 and 120.7 ops/s.

Waiting for 100% dead input eliminated copying and reached 115.5 ops/s in that
fully cyclic workload, but it performed no compaction when one persistent cold
record was inserted every eight writes. It retained all four 16 KiB files, so
all-dead-only is not a safe general default for mixed hot/cold segments.

The background convenience policy now uses a configurable 75% per-segment
minimum and requires at least two inputs for partial replacement; a single
all-dead input bypasses the count and uses zero-output retirement. In the mixed
workload, with a 64 KiB input budget, the final three-trial run performed one
two-input compaction per trial. It reclaimed 28,560 committed bytes, copied
4,160, reported 1.0636x logical write amplification, and reduced retained
logical and allocated segment capacity from 65,536 to 49,152 bytes. Background
throughput median was 111.4 versus 120.2 ops/s disabled, and p99 stayed near 27
ms. The one publication still paused the owner for roughly 104–107 ms, proving
that record-copy slicing alone would not satisfy the 25 ms target; durable file,
root, and directory publication must be redesigned or moved behind a stronger
availability mechanism. Verified hot-cache reopen remained below 0.5 ms in
these final trials.

Compaction was then split into an explicit Store state machine: bounded input
copy, durable-output readiness, and authority publication. The default copy
slice scans at most 64 records/1 MiB and appends its live subset as one durable
cohort. Runtime gives queued foreground requests priority between slices. An
exact relocation-fence test overwrites a record after its copy slice and proves
that publication cannot redirect the newer foreground version.

The matched 75%-reclaim mixed rerun produced two copy slices, one output
preparation, and one publication step per completed compaction. Background
throughput observations were 116.05, 116.95, and 117.56 ops/s versus disabled
120.48, 119.32, and 121.24 ops/s; medians were 116.95 and 120.48 (-2.9%). Max
individual owner-step pauses were 28.17, 28.17, and 27.96 ms, down from the
earlier 104–107 ms whole-compaction pauses. Exactly one of four steps per trial
exceeded 25 ms. Copy work is therefore preemptible and no longer the
tail-latency driver; synchronous active-file/root/directory publication is the
measured remaining floor.

### Captured-prefix asynchronous publication

The publication floor was removed from the normal Store-owner path without
letting topology race metadata. At durable-output readiness, Store captures the
active segment's committed prefix, replacement membership, next metadata
generation, and layout epoch. A named helper thread syncs the active file and
publishes the journal/root from that immutable snapshot. Foreground reads and
append-only writes continue; recovery treats later committed bytes as the
active suffix. Rotation is deliberately the rendezvous point and joins the
publisher before sealing or publishing another layout. Any helper, join,
install, or epoch failure fail-stops the Store.

The same command and 75%-reclaim mixed fixture produced disabled throughputs of
120.33, 120.30, and 119.95 ops/s and background throughputs of 114.87, 115.20,
and 113.81 ops/s (medians 120.30 and 114.87, -4.5%). Each background trial still
reclaimed 28,560 bytes while copying 4,160. Maximum owner-step pauses were
10.01, 9.04, and 7.86 ms, down from 28.17, 28.17, and 27.96 ms; none exceeded
the 25 ms budget. Normal SET fsync latency still dominates the roughly 27 ms
P99, so this result is an owner-availability improvement, not a claim that all
request latency is below 10 ms. A 1 ms idle wait between publication polls keeps
each cycle to eight observed maintenance steps instead of busy-spinning.

## Reproduction shapes

The shared-node sweep is available directly from the checked-in harness:

```sh
cargo run --release --locked -p punah-bench --bin punah-bench -- \
  --mode shared --operations 50000 --payload-sizes 64,1024 \
  --concurrency 16 --shards 1,2,4,8 --trials 3
```

The writer-local durability curve is:

```sh
cargo run --release --locked -p punah-bench --bin cohort_compare -- \
  500 64,1024 2,4,8,16,32,64 5 64
```

The Runtime/Store cohort curve with actual owner telemetry is:

```sh
cargo run --release --locked -p punah-bench --bin runtime_cohort_compare -- \
  200 256 2,4,8,16,32,64 3
```

The compaction-under-churn A/B harness is:

```sh
cargo run --release --locked -p punah-bench --bin compaction_churn -- \
  160 256 8 16384 3 16 65536 7500 25 8 2
```

The post-safety recovery measurements are:

```sh
cargo run --release --locked -p punah-bench --bin punah-bench -- \
  --mode recovery --operations 100000 --payload-sizes 1024 \
  --segment-capacities 1073741824 --trials 3
```

The TCP comparison client accepts:

```sh
target/release/tcp_compare ADDRESS get|set|mixed OPS PAYLOAD CLIENTS PIPELINE KEY_SPACE PRELOAD_KEYS
```

The controlled response-output crossover is:

```sh
cargo run --release --locked -p punah-bench --bin tcp_output_compare -- \
  100000 16384,32768,65536 16 16 1024 5
```

For server-side response comparisons, keep the client binary fixed; rebuilding
it after a protocol/network optimization can change client allocations and hide
or exaggerate the server effect. Restore the same pristine prepared snapshot for
every trial, alternate A/B order, record client and server binary hashes, report
durability, and take at least five trials.

## Next measured work

Runtime auto-drain now has an end-to-end Store-backed cohort curve with actual
record, byte, group, and Flush telemetry, and compaction now has hot-only plus
mixed hot/cold reclaim-yield curves. The response and Linux storage A/B runners
now accept a single-mode/backend selector so external `perf stat` runs can
capture CPU and page-fault deltas without unsafe counter FFI. Those counters are
still awaiting a disclosed Linux run. Linux work should run the checked-in
ordinary-file versus `io_uring` baseline before specialized SPDK or DPDK
hardware is allowed to obscure algorithmic costs.
