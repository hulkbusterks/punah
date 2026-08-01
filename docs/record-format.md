# Record format

The durable format is versioned before the first implementation. Integer byte
order is little-endian unless a later version explicitly states otherwise.

## Segment

A segment begins with redundant superblock information containing at least:

- magic and format version
- segment identifier and generation
- device/block alignment
- logical committed boundary
- creation sequence and checksum

The physical file or namespace capacity is not the logical write position.
Preallocation must never make an empty segment appear full during recovery.

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
is retained because a routing hash is not an equality proof. Version 1 currently uses eight-byte
alignment in the portable engine; device backends may require a larger segment
write alignment without changing logical record parsing.

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
checksums. It rebuilds the index in sequence-number order. Newer artifacts and
tombstones supersede older locations.

## Compaction correctness

Before copying a candidate record, the compactor verifies that the index still
points to the candidate's exact `(segment, offset, length, generation)`. Index
redirection uses compare-and-replace semantics so a concurrent newer version
cannot be overwritten by compaction.
