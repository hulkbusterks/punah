# Durability

Punah exposes durability rather than implying it.

```rust
pub enum Durability {
    Memory,
    Buffered,
    LocalDurable,
    Replicated { acknowledgements: u8 },
}
```

- `Memory`: loss is permitted when the process stops.
- `Buffered`: accepted by the local storage pipeline; power loss may lose it.
- `LocalDurable`: the record and commit state crossed the configured local
  device persistence boundary before acknowledgement.
- `Replicated`: local durability plus the requested number of durable replica
  acknowledgements.

The SPDK backend must document the exact use of flush/FUA and the guarantees of
supported devices. An I/O completion alone must not be described as durable
unless the device contract makes it so.

The core recovery property is: every acknowledged `LocalDurable` operation is
recovered, and Punah never serves a payload that fails structural or content
validation. Recovery may also contain a valid durable suffix whose successful
acknowledgement was interrupted.

Record encoding initially produces an uncommitted buffer. The storage pipeline
must persist the record body before establishing its commit marker; only a
committed record may be published in the index. Device write completions return
ownership of submitted buffers so DMA-capable backends cannot recycle memory
while hardware may still reference it.

For one record, the reference segment writer expresses the protocol as an
explicit polling state machine:

```text
write record -> [flush record] -> write commit -> [flush commit] -> publish
```

Bracketed flushes are required by `LocalDurable` and omitted by `Memory` and
`Buffered`. Replication remains a higher-level stage and is rejected by the
single-node writer rather than silently weakening its requested guarantee.

The same protocol admits a bounded group commit without changing visibility:

```text
N body writes -> wait for all -> [one flush]
N commit-marker writes -> wait for all -> [one flush]
publish N locations in reservation order
```

Completions may arrive out of order and are correlated by device request ID;
publication still follows reservation order. `QueueFull` before acceptance is
retryable backpressure. Any failure after a device accepts I/O fail-stops the
writer because it may have created an ambiguous gap. A cohort is not a
multi-record transaction: after a crash, recovery may expose a safe committed
prefix rather than all or none of the cohort.

In particular, power loss immediately after the final Flush takes effect can
leave the whole cohort durable while the caller observes an I/O failure. That
is normal acknowledgement ambiguity, not evidence that the failed call was
atomic or safe to retry without recovery.

The portable Store bounds a cohort by 64 records and approximately 1 MiB of
encoded bytes. A value larger than that byte budget is admitted only as a
singleton. The runtime adds no intentional batching delay: a `LocalDurable`
owner drains only mutation messages already queued, stopping at 64 messages,
64 mutations, or the byte budget. Reads remain ordering barriers. Singleton
calls retain their direct fast path.

The writer tracks a *reserved boundary* as soon as a device accepts a record
write. This is intentionally distinct from the recovered committed boundary.
After a pipeline failure, the writer is failed and its segment must be recovered
or sealed; subsequent records cannot be appended behind an uncommitted gap.

Record version 1 commits with a four-byte marker at offset 124. That is correct
for the portable file backend but is not assumed to satisfy raw SPDK alignment.
An SPDK adapter needs a capability-specific aligned commit design—most likely a
record-v2 marker block or cohort footer—before direct-device support is claimed.
