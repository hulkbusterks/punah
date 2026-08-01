use core::fmt;

/// A stable identifier for a configuration field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigField {
    ListenAddress,
    MaxConnections,
    ReadTimeout,
    WriteTimeout,
    IdleTimeout,
    ShardCount,
    QueueCapacity,
    StorageDirectory,
    SegmentBytes,
    AlignmentBytes,
    MaxFrameBytes,
    MaxKeyBytes,
    MaxValueBytes,
}

impl fmt::Display for ConfigField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ListenAddress => "tcp.listen_address",
            Self::MaxConnections => "limits.max_connections",
            Self::ReadTimeout => "tcp.read_timeout",
            Self::WriteTimeout => "tcp.write_timeout",
            Self::IdleTimeout => "tcp.idle_timeout",
            Self::ShardCount => "engine.shard_count",
            Self::QueueCapacity => "engine.queue_capacity",
            Self::StorageDirectory => "storage.directory",
            Self::SegmentBytes => "storage.segment_bytes",
            Self::AlignmentBytes => "storage.alignment_bytes",
            Self::MaxFrameBytes => "limits.max_frame_bytes",
            Self::MaxKeyBytes => "limits.max_key_bytes",
            Self::MaxValueBytes => "limits.max_value_bytes",
        })
    }
}

/// Machine-readable reason a field is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationKind {
    Empty,
    Zero,
    NotPowerOfTwo,
    AboveProtocolMaximum,
    Exceeds { other: ConfigField },
    SumExceeds { other: ConfigField },
}

/// One configuration validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidationError {
    pub field: ConfigField,
    pub kind: ValidationKind,
}

impl ValidationError {
    pub(crate) const fn new(field: ConfigField, kind: ValidationKind) -> Self {
        Self { field, kind }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ", self.field)?;
        match self.kind {
            ValidationKind::Empty => formatter.write_str("must not be empty"),
            ValidationKind::Zero => formatter.write_str("must be greater than zero"),
            ValidationKind::NotPowerOfTwo => formatter.write_str("must be a power of two"),
            ValidationKind::AboveProtocolMaximum => {
                formatter.write_str("exceeds the wire format maximum")
            }
            ValidationKind::Exceeds { other } => write!(formatter, "must not exceed {other}"),
            ValidationKind::SumExceeds { other } => {
                write!(formatter, "does not fit within {other} with related fields")
            }
        }
    }
}

/// All failures found during one validation pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors(Vec<ValidationError>);

impl ValidationErrors {
    pub(crate) const fn new(errors: Vec<ValidationError>) -> Self {
        Self(errors)
    }

    /// Returns the individual validation failures.
    #[must_use]
    pub fn as_slice(&self) -> &[ValidationError] {
        &self.0
    }

    /// Iterates over individual validation failures.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ValidationError> {
        self.0.iter()
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid configuration ({} error(s))",
            self.0.len()
        )?;
        for error in &self.0 {
            write!(formatter, ": {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}
