#![forbid(unsafe_code)]

//! Explicit identity and routing primitives.
//!
//! Artifact identity and shard routing have different correctness properties:
//! BLAKE3-256 identifies immutable bytes, while XXH3-64 only chooses where to
//! look. Neither function is a storage-corruption checksum.

use punah_types::{ArtifactId, ShardHash};
use xxhash_rust::xxh3::Xxh3;

/// Computes the durable identity of immutable artifact bytes with BLAKE3-256.
#[must_use]
pub fn artifact_id(bytes: &[u8]) -> ArtifactId {
    ArtifactId::from(*blake3::hash(bytes).as_bytes())
}

/// Computes a stable XXH3-64 routing hash for one byte string.
///
/// Collisions are expected to be harmless: the owning shard must still compare
/// the complete logical key or collision-resistant artifact identity.
#[must_use]
pub fn shard_hash(bytes: &[u8]) -> ShardHash {
    ShardHash::new(xxhash_rust::xxh3::xxh3_64(bytes))
}

/// Computes a stable routing hash for multiple framed byte strings.
///
/// Length-prefixing makes component boundaries unambiguous: `a, bc` cannot be
/// confused with `ab, c`.
#[must_use]
pub fn shard_hash_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> ShardHash {
    let mut hasher = Xxh3::new();
    for part in parts {
        hasher.update(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(part);
    }
    ShardHash::new(hasher.digest())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blake3_official_vectors_are_stable() {
        assert_eq!(
            artifact_id(b"").to_hex(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        assert_eq!(
            artifact_id(b"abc").to_hex(),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
    }

    #[test]
    fn xxh3_upstream_vectors_are_stable() {
        assert_eq!(shard_hash(b"").get(), 0x2d06_8005_38d3_94c2);
        assert_eq!(shard_hash(b"abc").get(), 0x78af_5f94_892f_3950);
    }

    #[test]
    fn part_boundaries_affect_routing() {
        assert_ne!(
            shard_hash_parts([b"a".as_slice(), b"bc".as_slice()]),
            shard_hash_parts([b"ab".as_slice(), b"c".as_slice()])
        );
    }
}
