# Hashing and identity

Punah gives each byte transformation one narrow job. Calling all of these
values a “hash” hides important differences in their guarantees.

| Job | Representation | Algorithm or comparison | Collision policy |
| --- | --- | --- | --- |
| Immutable artifact identity | `ArtifactId` (256 bits) | BLAKE3-256 | The digest is the identity |
| Shard routing | `ShardHash` (64 bits) | XXH3-64 | Collisions are harmless; compare identity/key at the shard |
| Arbitrary user-key equality | Original key bytes | Exact byte comparison | No hash substitutes for equality |
| Record corruption detection | Checksum field | CRC32-C | Detect accidental damaged/torn bytes |
| Redgo-compatible wire integrity | Checksum field | CRC32-C | Detect accidental frame corruption |

## Domain boundaries

`artifact_id(bytes)` identifies immutable content. Changing one byte should
produce a different identity, and the complete 256-bit result is persisted.
Punah does not truncate it because the record format already reserves 32 bytes.

`shard_hash(bytes)` and `shard_hash_parts(parts)` only route a lookup to an
owner. The multi-part form length-prefixes every component, so component
boundaries remain meaningful. Routing must never be treated as proof that two
keys are equal.

CRC32-C is not an identity. It answers whether stored or transported bytes were
probably damaged, and its format is kept separate from BLAKE3 and XXH3.

All wire, record, and metadata checksums go through `punah-checksum`. Its
maintained portable dependency selects hardware acceleration where supported
and falls back to software elsewhere. Punah contains no macOS-, Linux-, ARM-,
or x86-specific checksum path. The shared one-shot, multipart, and streaming
APIs preserve the exact CRC32-C bytes in existing wire and durable formats.

## Reference-store migration boundary

The store migration is explicit: records persist the complete original key,
recovery and the in-memory map compare those bytes, and artifact records carry
BLAKE3-256 of their immutable payload. Tombstones identify their target through
the durable key and use a zero content identity because they contain no
artifact. XXH3 may accelerate lookup and select shards, but compaction compares
original keys and never trusts `ShardHash` as equality proof.

## Dependency policy

Punah uses the maintained `blake3`, `xxhash-rust`, and `crc32c`
implementations. These algorithms are intentionally not handwritten inside the
project. Known-vector tests pin their externally specified byte-level behavior.
