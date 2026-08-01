# Mapped segment safety

Punah's durable index contains metadata and `RecordLocation` values, never
payload copies. `punah-device::MappedSegment` is the sole unsafe boundary used
to establish a read-only operating-system mapping. All consumers use its safe,
bounds-checked `read(offset, length)` method.

## Invariants

- Segment files are sized before they are mapped and are never truncated while
  a mapping is installed.
- A mapping is read-only. Mutation is performed by the segment's exclusive
  writer through a separate file handle.
- The store publishes a location only after the record and commit marker have
  completed the configured durability sequence.
- Checked addition and integer conversion reject overflowing ranges.
- Record decoding verifies the commit marker, header checksum, record checksum,
  encoded length, key, sequence, and logical payload length before returning a
  GET result.
- Compaction maps replacement output before installing its locations. Input
  mappings are dropped before their files are removed.
- Mapping or decoding failures are storage errors, not cache misses.

External mutation of a Punah segment while the process owns it violates the
storage contract. Checksums still detect ordinary corruption, but no process
may truncate an installed mapping because operating systems can signal a fatal
mapping fault when a truncated page is accessed.

## Small-object caching

There is deliberately no inline-payload threshold. All values share one
durable representation. If profiling later shows that mapping lookups or page
faults dominate small hot reads, Punah may add a bounded cache keyed by durable
identity and location. Such a cache must be optional, capacity-limited, and
safe to discard without changing correctness or recovery.
