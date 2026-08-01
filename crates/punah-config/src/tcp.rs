use std::time::Duration;

use crate::{ConfigField, ValidationError, ValidationKind};

/// TCP listener and established-connection behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpConfig {
    /// Socket address passed to the server listener.
    pub listen_address: String,
    /// Maximum time without a successful inbound read.
    pub read_timeout: Duration,
    /// Maximum time allowed for an outbound write.
    pub write_timeout: Duration,
    /// Maximum lifetime of an otherwise inactive connection.
    pub idle_timeout: Duration,
    /// Enables `TCP_NODELAY` on accepted connections.
    pub no_delay: bool,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:7379".into(),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_mins(5),
            no_delay: true,
        }
    }
}

impl TcpConfig {
    pub(crate) fn validate(&self, errors: &mut Vec<ValidationError>) {
        if self.listen_address.trim().is_empty() {
            errors.push(ValidationError::new(
                ConfigField::ListenAddress,
                ValidationKind::Empty,
            ));
        }
        for (field, timeout) in [
            (ConfigField::ReadTimeout, self.read_timeout),
            (ConfigField::WriteTimeout, self.write_timeout),
            (ConfigField::IdleTimeout, self.idle_timeout),
        ] {
            if timeout.is_zero() {
                errors.push(ValidationError::new(field, ValidationKind::Zero));
            }
        }
    }
}
