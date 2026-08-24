# Mapped segment safety

Punah's durable index contains metadata and `RecordLocation` values, never
payload copies. `punah-device::MappedSegment` is the one current unsafe boundary:
it owns a `memmap2::MmapRaw` and exposes only bounds-checked copies through
`read` and `read_into`.

`MmapRaw` is intentional. The active segment is preallocated, mapped for reads,
and updated through the owning writer's separate file handle. A dereferencing
`Mmap` could create a Rust slice while the file changes. `MmapRaw` does not
implement `Deref`; the module performs a raw pointer copy directly into owned
memory and never constructs a Rust reference over mapped file bytes.

## Invariants

- Segment files are sized before mapping and are never truncated while their
  mapping is installed.
- Exactly one shard worker owns an active segment and serializes its reads and
  writes. A racing external write could produce a torn owned snapshot, which
  the record checksums reject.
- External mutation of files owned by a running Punah process is outside the
  storage contract.
- Checked conversion and addition reject ranges that overflow or extend beyond
  the mapped file before any destination allocation occurs.
- A location is published only after its body and commit marker complete the
  configured durability sequence.
- Record decoding verifies the marker, lengths, identity fields, and both
  checksums before a GET or recovery scan accepts bytes.
- Compaction maps a replacement before installing its locations, removes input
  mappings before deleting their files, and currently runs under the Store's
  exclusive mutable ownership.
- Mapping or decoding failures are storage errors, not cache misses.

The unsafe block states these invariants next to the raw copy. No pointer or
file-backed reference crosses the module boundary. Miri is useful for
the safe codecs and state machines, but it is not a verifier for operating-system
memory mappings; the mapping boundary is covered by focused filesystem tests,
strict ownership, and sanitizer runs when those are automated.

## Recovery and response copies

Recovery maps each segment once. It copies one 128-byte header, validates the
declared aligned length against the authoritative fence, then copies only that
record into a reusable owned buffer. Sealed segments scan exactly their
manifest-published range; the active segment validates that strict prefix and
then discovers only a contiguous committed suffix. An all-zero header marks
unused preallocated capacity, so memory is O(max record) and startup does not
scan the whole preallocated tail.

A GET copies its complete stored record into one owned `Vec`, validates it,
then compacts the payload to the front of the same allocation. That ownership
is what lets the Store finish the read before compaction can retire a segment.
For response bodies of at least 32 KiB, the TCP path writes header, key, and that
existing payload with vectored I/O, avoiding construction of one contiguous
response frame. Returning a direct mmap borrow to the socket
would require an `Arc`-backed read lease plus deferred unmapping and deletion;
it is a separate lifecycle feature, not a safe local substitution.

## Small-object caching

There is deliberately no inline-payload threshold in the authoritative index.
All values share one durable representation. If profiling later shows page
faults dominate small hot reads, Punah may add a bounded cache keyed by durable
identity and location. It must be optional, capacity-limited, and safe to drop
without changing correctness or recovery.
