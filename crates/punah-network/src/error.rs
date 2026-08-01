use std::{error::Error, fmt, io};

use punah_protocol::{DecodeError, EncodeError};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    Decode(DecodeError),
    Encode(EncodeError),
    Handler(BoxError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "network I/O failed: {error}"),
            Self::Decode(error) => write!(formatter, "invalid request frame: {error}"),
            Self::Encode(error) => write!(formatter, "response encoding failed: {error}"),
            Self::Handler(error) => write!(formatter, "request handler failed: {error}"),
        }
    }
}

impl Error for ConnectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Handler(error) => Some(error.as_ref()),
        }
    }
}

impl From<io::Error> for ConnectionError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<DecodeError> for ConnectionError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<EncodeError> for ConnectionError {
    fn from(error: EncodeError) -> Self {
        Self::Encode(error)
    }
}
