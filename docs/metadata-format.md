# Durable metadata format

Punah uses a small dual-slot superblock as the stable root of an immutable
manifest checkpoint and an append-only topology journal. This document defines
the version-1 codec and its portable publication API. Compaction selection and
file deletion remain outside this layer.

All integers are unsigned little-endian. Reserved bytes are written as zero.
Every object has a CRC32-C checksum and a four-byte commit marker
`0xC01117ED`. The checksum excludes both the checksum field and commit marker.
An encoder first emits a zero marker. A publisher writes and flushes that body,
then writes and flushes the marker. Readers never accept a missing, partial, or
incorrect marker.

## Dual-slot superblock

Each slot is exactly 4096 bytes. Implementations place two slots at stable,
independently writable locations. Recovery validates both and chooses the valid
slot with the greatest generation; equal generations deterministically prefer
slot A. Two complete all-zero slots mean the metadata area is pristine and may
bootstrap legacy segment discovery. Any nonzero state with no recoverable root
is corruption and fails closed; it is never silently treated as a fresh store.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `PUNASB01` |
| 8 | 2 | version, `1` |
| 10 | 2 | header length, `64` |
| 12 | 4 | reserved |
| 16 | 8 | superblock generation |
| 24 | 8 | checkpoint offset |
| 32 | 8 | checkpoint length |
| 40 | 4 | checkpoint CRC32-C |
| 44 | 8 | journal-tail offset |
| 52 | 8 | journal-tail length |
| 60 | 4 | journal-tail CRC32-C |
| 64 | 4024 | reserved |
| 4088 | 4 | slot CRC32-C over bytes `0..4088` |
| 4092 | 4 | commit marker |

A zero journal-tail pointer denotes no journal entries after the checkpoint.
Pointer checksums let the publication layer validate referenced objects before
accepting a newer root.

## Manifest checkpoint

A checkpoint is immutable and contains the complete ordered active segment
set for one metadata generation. Its size is `64 + segment_count * 32`.

Header fields are: magic `PUNAMF01` at 0, version and header length at 8 and
10, generation at 16, segment count at 24, total length at 28, checksum at 56,
and commit marker at 60. The checksum covers bytes `0..56` and all descriptors
from byte 64 onward.

Each 32-byte segment descriptor contains:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | segment ID |
| 4 | 4 | segment generation |
| 8 | 8 | physical start offset |
| 16 | 8 | logical length |
| 24 | 8 | highest sealed record sequence |

The `(segment ID, segment generation)` pair prevents a stale reference from
silently naming reused storage.

## Topology journal

Journal records are append-only and eight-byte aligned. Version 1 supports:

- `Replace`: atomically supersede one or more old segment references with one
  or more new references.
- `Retire`: declare one or more already-superseded references eligible for
  physical reclamation. Retirement is distinct from replacement so a crash
  cannot make old bytes disappear before the replacement is authoritative.

The 48-byte header stores magic `PUNATJ01`, version, header length, operation
kind at byte 12 (`1` Replace, `2` Retire), old/reference count at 14, new count
at 16, generation at 24, total length at 32, checksum at 40, and commit marker
at 44. Segment references follow the header as `(u32 ID, u32 generation)`.
Replace requires non-empty old and new lists. Retire requires a non-empty old
list and a zero new count.

## Publication API and invariants

`MetadataPublisher` operates over the small positional `MetadataIo` interface:
size, exact-offset reads, exact-offset writes, and a durability flush. The
filesystem implementation uses explicit seeks and `sync_data`; the same API can
be implemented by a raw-device backend without importing filesystem semantics.
The first 8192 bytes are reserved for slots A and B. Immutable checkpoint and
journal objects are appended after that region at eight-byte-aligned offsets.

Recovery does not select a root merely because its slot checksum is valid. It
reads the entire referenced checkpoint, verifies the pointer checksum, validates
the checkpoint codec and commit marker, and verifies that its generation equals
the root generation. A nonzero journal-tail pointer is validated the same way.
If any referenced object is invalid, recovery tries the older valid slot. If
both roots or their referenced objects are invalid, Store open returns
`NoRecoverableRoot` before scanning segment filenames or rewriting metadata.

Manifest descriptors are authoritative. Sealed entries are read only through
their exact published `start_offset` and `logical_length`; valid-looking bytes
beyond that boundary are ignored. The final active segment's published length
is a strict prefix because ordinary appends do not republish the manifest. Its
contiguous committed suffix is therefore discovered and checked separately.

`publish_checkpoint` alternates slots and requires consecutive generations once
a root exists. `append_journal` durably commits an immutable record but does not
make it authoritative by itself; a subsequent root publication must reference
it. The zero pointer is represented only by three zero fields.

Before publishing a checkpoint, Store makes every segment boundary newly made
strict by that checkpoint durable. Rotation syncs the old active file before
sealing it. Compaction syncs both its replacement and the current active prefix.
New segment files are synced and their directory entry is synced before a root
may reference the filename.

The implemented portable publication path preserves this order, and every
future backend must do the same:

1. Write and make durable every replacement segment.
2. Write and make durable an immutable checkpoint or journal record body.
3. Write and make durable that object's commit marker.
4. Write the inactive superblock body pointing to validated metadata.
5. Make the body durable, then write and make durable its commit marker.
6. Only after the new root is recoverable may retired storage be reclaimed.

Checksums detect corruption; they do not provide cryptographic authenticity.
Generation comparisons are ordinary integer comparisons. Publication rejects
generation overflow rather than wrapping it.

There is one unavoidable version-1 ambiguity: deletion of `metadata.punah`, or
destruction that restores both slots to exact zeros, is indistinguishable from
a genuinely new metadata file. Normal Store open therefore accepts that state
only when no segment files exist. If segments remain it returns
`ExplicitImportRequired` without creating metadata. `punah-admin import` is the
explicit authority-changing operation; `punah-admin repair` first quarantines
nonzero corrupt metadata and then runs the same validated import. Neither path
silently adopts bytes during normal recovery.

A version-1 journal is an audit tail for the root that points to it. Recovery
requires its generation to equal the superblock generation. It is not a replay
chain: it has no predecessor pointer and its segment references do not contain
the descriptor fences required to reconstruct rotation. Incremental checkpoints
therefore require a version-2 metadata record and cannot be retrofitted by
loosening version-1 recovery.
