# Record format

The durable format is versioned before the first implementation. Integer byte
order is little-endian unless a later version explicitly states otherwise.

## Segment container

The portable version-1 segment is a preallocated, headerless record region.
Segment identity, generation, start offset, authoritative logical length, and
sealed sequence live in the crash-safe manifest rather than in an unimplemented
per-file superblock. Records are self-describing, so an explicitly selected
legacy-import path can discover a contiguous committed prefix when metadata is
pristine.

Physical capacity is never the logical write position. A sealed manifest entry
is scanned strictly through `start_offset + logical_length`. The last active
entry validates its published prefix, whose length may lag ordinary appends,
then scans a contiguous committed suffix. The first all-zero 128-byte header,
an incomplete record, or an uncommitted record ends that active suffix. Nonzero
corruption before an accepted boundary is an error.

## Record version 1

The initial record uses a 128-byte fixed header followed by variable data:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic `PUNAH001` |
| 8 | 2 | format version |
| 10 | 1 | record type |
| 11 | 1 | flags |
| 12 | 2 | header length |
| 14 | 2 | original key length |
| 16 | 2 | namespace length |
| 18 | 2 | dependency count |
| 20 | 4 | payload length |
| 24 | 8 | sequence number |
| 32 | 8 | creation time, Unix nanoseconds |
| 40 | 8 | expiry time, Unix nanoseconds; zero means none |
| 48 | 32 | operation hash |
| 80 | 32 | content hash |
| 112 | 4 | header CRC32-C |
| 116 | 4 | complete-record CRC32-C |
| 120 | 4 | aligned total record length |
| 124 | 4 | commit marker |

Variable data is namespace bytes, original key bytes, dependency hashes,
payload bytes, then zero padding to the configured alignment. The original key
is retained because a routing hash is not an equality proof. Version 1 currently
uses eight-byte alignment in the portable engine. Its four-byte commit marker is
not suitable for every raw device; a direct SPDK backend may require a new
aligned commit representation while retaining the same logical fields.

Header CRC covers bytes `0..112`. Record CRC covers the fixed header except the
record-CRC and commit fields, plus the complete variable region and padding.
This allows the commit marker to be established after record data is durable.

The original prototype used the provisional `REDGO001` identity. It changed to
`PUNAH001` before any compatibility promise or persistent deployment existed,
so no backward-compatibility reader is provided.

The initially developed Punah record also treated a derived key hash as key
equality. Before release, the reserved key-length field and original key bytes
were added to version 1. No deployed Punah format predates this correction.
Artifact records use BLAKE3-256 of the immutable payload as content identity;
tombstones carry a zero content identity and name their target by exact key.

## Commit protocol

1. Reserve aligned space in the active segment.
2. Write the record with an uncommitted state.
3. Persist data according to the selected durability level.
4. Establish and persist the commit indication.
5. Publish the location in the in-memory index.
6. Acknowledge the caller.

Recovery accepts only structurally complete, committed records with valid
checksums. It replays manifest order while sequence fencing ensures newer
artifacts and tombstones supersede stale locations.

## Aligned extent/footer version 2 primitive

The record crate now contains a direct-device codec primitive without switching
the portable segment format. An aligned extent has two separately submitted
objects:

1. an alignment-sized/padded body beginning with `PUNAXB02`, containing dense
   committed v1 logical records;
2. one alignment-sized footer beginning with `PUNAXC02`, repeating the body
   length, used length, record count, sequence endpoints, and body CRC32-C.

Alignment must be a power of two from 512 bytes through 64 KiB. Both headers
have independent CRC32-C values, body/footer fields must match exactly, and all
padding is canonical zero. The intended direct-device sequence is `N aligned
body writes -> wait -> Flush -> one aligned footer write -> wait -> Flush ->
ordered publication`. Missing, torn, mismatched, or corrupt footers authorize no
records. A valid final footer may still become durable while its acknowledgement
is lost, matching the existing ambiguous-final-Flush rule.

This packs small records inside an aligned body rather than spending a direct-
I/O block per key, and removes record v1's four-byte in-place marker write from
the direct path. It does not make a cohort a multi-record transaction: after
recovery validates the extent, records are published individually in sequence.
The current `FileDevice` Store still writes record v1. Activating extent v2
requires a segment-version discriminator, a device capability contract for
alignment/Flush/FUA, recovery integration, and Linux direct-I/O benchmarks.

## Compaction correctness

Before copying a candidate record, the compactor verifies that the index still
points to the candidate's exact `(segment, offset, length, generation)`. Index
redirection uses compare-and-replace semantics so a concurrent newer version
cannot be overwritten by compaction.
