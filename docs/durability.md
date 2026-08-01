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

The core recovery property is: after interruption at any write boundary, Punah
recovers a valid prefix of acknowledged durable operations and never serves a
payload that fails structural or content validation.

Record encoding initially produces an uncommitted buffer. The storage pipeline
must persist the record body before establishing its commit marker; only a
committed record may be published in the index. Device write completions return
ownership of submitted buffers so DMA-capable backends cannot recycle memory
while hardware may still reference it.

The reference segment writer expresses the protocol as an explicit polling
state machine:

```text
write record -> [flush record] -> write commit -> [flush commit] -> publish
```

Bracketed flushes are required by `LocalDurable` and omitted by `Memory` and
`Buffered`. Replication remains a higher-level stage and is rejected by the
single-node writer rather than silently weakening its requested guarantee.

The writer tracks a *reserved boundary* as soon as a device accepts a record
write. This is intentionally distinct from the recovered committed boundary.
After a pipeline failure, the writer is failed and its segment must be recovered
or sealed; subsequent records cannot be appended behind an uncommitted gap.
