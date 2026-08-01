#![forbid(unsafe_code)]
//! Portable CRC32-C checksums for Punah's durable and wire formats.
//!
//! The implementation dependency selects hardware acceleration where the
//! target supports it and retains a software fallback. Callers do not contain
//! platform-specific code.

/// Computes the CRC32-C (Castagnoli) checksum of one byte slice.
#[must_use]
pub fn checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Computes the checksum of several slices as if they were concatenated.
#[must_use]
pub fn checksum_parts(parts: &[&[u8]]) -> u32 {
    let mut checksum = Checksum::new();
    for part in parts {
        checksum.update(part);
    }
    checksum.finish()
}

/// Incremental CRC32-C state for records assembled from disjoint regions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Checksum(u32);

impl Checksum {
    /// Creates an empty CRC32-C stream.
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Incorporates the next bytes in logical concatenation order.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0 = crc32c::crc32c_append(self.0, bytes);
    }

    /// Returns the checksum accumulated so far.
    #[must_use]
    pub const fn finish(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Checksum, checksum, checksum_parts};

    #[test]
    fn matches_castagnoli_check_value() {
        assert_eq!(checksum(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn empty_input_has_zero_checksum() {
        assert_eq!(checksum(b""), 0);
        assert_eq!(checksum_parts(&[]), 0);
    }

    #[test]
    fn parts_and_streaming_are_logical_concatenation() {
        let expected = checksum(b"123456789");
        assert_eq!(checksum_parts(&[b"1234", b"", b"56789"]), expected);

        let mut stream = Checksum::new();
        stream.update(b"1234");
        stream.update(b"");
        stream.update(b"56789");
        assert_eq!(stream.finish(), expected);
    }
}
