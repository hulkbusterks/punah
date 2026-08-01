use crate::{ConfigField, ValidationError, ValidationKind};

/// Protocol and server resource limits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LimitsConfig {
    /// Maximum concurrently accepted TCP connections.
    pub max_connections: usize,
    /// Maximum complete wire frame, including its fixed header.
    pub max_frame_bytes: u64,
    /// Maximum artifact key length.
    pub max_key_bytes: u64,
    /// Maximum artifact value length.
    pub max_value_bytes: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        const MIB: u64 = 1 << 20;
        Self {
            max_connections: 10_000,
            max_frame_bytes: 17 * MIB,
            max_key_bytes: 64 * 1_024 - 1,
            max_value_bytes: 16 * MIB,
        }
    }
}

impl LimitsConfig {
    pub(crate) fn validate(&self, errors: &mut Vec<ValidationError>) {
        const FRAME_HEADER_BYTES: u64 = 16;

        for (field, value) in [
            (ConfigField::MaxConnections, self.max_connections as u64),
            (ConfigField::MaxFrameBytes, self.max_frame_bytes),
            (ConfigField::MaxKeyBytes, self.max_key_bytes),
            (ConfigField::MaxValueBytes, self.max_value_bytes),
        ] {
            if value == 0 {
                errors.push(ValidationError::new(field, ValidationKind::Zero));
            }
        }

        // Punah's current wire format stores key length in u16 and value length
        // in u32. Reject configurations that the encoder cannot represent.
        if self.max_key_bytes > u64::from(u16::MAX) {
            errors.push(ValidationError::new(
                ConfigField::MaxKeyBytes,
                ValidationKind::AboveProtocolMaximum,
            ));
        }
        if self.max_value_bytes > u64::from(u32::MAX) {
            errors.push(ValidationError::new(
                ConfigField::MaxValueBytes,
                ValidationKind::AboveProtocolMaximum,
            ));
        }

        let payload_bytes = self.max_key_bytes.checked_add(self.max_value_bytes);
        let required = payload_bytes.and_then(|bytes| bytes.checked_add(FRAME_HEADER_BYTES));
        if required.is_none_or(|bytes| bytes > self.max_frame_bytes) {
            errors.push(ValidationError::new(
                ConfigField::MaxFrameBytes,
                ValidationKind::SumExceeds {
                    other: ConfigField::MaxFrameBytes,
                },
            ));
        }
    }
}
