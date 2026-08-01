//! Dependency-free, programmatic configuration for Punah.
//!
//! Redgo loaded equivalent settings from YAML. Punah first establishes parity
//! at the typed configuration boundary: applications construct [`Config`],
//! optionally override defaults, and call [`Config::validate`] before starting
//! services. Parsing and environment overlays belong in a future adapter crate.

mod engine;
mod error;
mod limits;
mod storage;
mod tcp;

pub use engine::EngineConfig;
pub use error::{ConfigField, ValidationError, ValidationErrors, ValidationKind};
pub use limits::LimitsConfig;
pub use storage::{StorageConfig, StorageDurability};
pub use tcp::TcpConfig;

/// Complete Punah service configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Config {
    /// TCP listener and socket behavior.
    pub tcp: TcpConfig,
    /// In-memory engine topology and queue sizing.
    pub engine: EngineConfig,
    /// Durable segment layout.
    pub storage: StorageConfig,
    /// Protocol and resource bounds.
    pub limits: LimitsConfig,
}

impl Config {
    /// Validates all sections and cross-section invariants.
    ///
    /// Every discovered problem is returned so an operator can fix a complete
    /// configuration in one pass.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErrors`] when one or more fields violate their local
    /// bounds or a frame cannot fit in the configured segment size.
    pub fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = Vec::new();
        self.tcp.validate(&mut errors);
        self.engine.validate(&mut errors);
        self.storage.validate(&mut errors);
        self.limits.validate(&mut errors);

        if self.limits.max_frame_bytes > self.storage.segment_bytes {
            errors.push(ValidationError::new(
                ConfigField::MaxFrameBytes,
                ValidationKind::Exceeds {
                    other: ConfigField::SegmentBytes,
                },
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors::new(errors))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Config::default().validate().expect("defaults must start");
    }

    #[test]
    fn validation_collects_errors_across_sections() {
        let mut config = Config::default();
        config.tcp.listen_address.clear();
        config.engine.shard_count = 3;
        config.storage.segment_bytes = 0;
        config.limits.max_connections = 0;

        let errors = config.validate().expect_err("configuration is invalid");
        assert!(
            errors
                .iter()
                .any(|error| error.field == ConfigField::ListenAddress)
        );
        assert!(
            errors
                .iter()
                .any(|error| error.field == ConfigField::ShardCount)
        );
        assert!(
            errors
                .iter()
                .any(|error| error.field == ConfigField::SegmentBytes)
        );
        assert!(
            errors
                .iter()
                .any(|error| error.field == ConfigField::MaxConnections)
        );
    }

    #[test]
    fn frame_must_fit_in_segment() {
        let mut config = Config::default();
        config.storage.segment_bytes = config.limits.max_frame_bytes - 1;
        let errors = config.validate().expect_err("frame cannot fit");
        assert!(errors.iter().any(|error| {
            error.field == ConfigField::MaxFrameBytes
                && error.kind
                    == ValidationKind::Exceeds {
                        other: ConfigField::SegmentBytes,
                    }
        }));
    }

    #[test]
    fn zero_timeouts_and_non_power_of_two_alignment_are_rejected() {
        let mut config = Config::default();
        config.tcp.read_timeout = std::time::Duration::ZERO;
        config.storage.alignment_bytes = 3;

        let errors = config.validate().expect_err("configuration is invalid");
        assert!(errors.iter().any(|error| {
            *error
                == ValidationError {
                    field: ConfigField::ReadTimeout,
                    kind: ValidationKind::Zero,
                }
        }));
        assert!(errors.iter().any(|error| {
            *error
                == ValidationError {
                    field: ConfigField::AlignmentBytes,
                    kind: ValidationKind::NotPowerOfTwo,
                }
        }));
    }

    #[test]
    fn protocol_and_composed_frame_limits_are_checked() {
        let mut protocol = Config::default();
        protocol.limits.max_key_bytes = u64::from(u16::MAX) + 1;
        let errors = protocol
            .validate()
            .expect_err("key length cannot be encoded");
        assert!(errors.iter().any(|error| {
            error.field == ConfigField::MaxKeyBytes
                && error.kind == ValidationKind::AboveProtocolMaximum
        }));

        let mut composed = Config::default();
        composed.limits.max_frame_bytes = 100;
        composed.limits.max_key_bytes = 50;
        composed.limits.max_value_bytes = 50;
        let errors = composed.validate().expect_err("header must also fit");
        assert!(errors.iter().any(|error| {
            error.field == ConfigField::MaxFrameBytes
                && matches!(error.kind, ValidationKind::SumExceeds { .. })
        }));
    }
}
