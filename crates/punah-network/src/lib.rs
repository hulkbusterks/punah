#![forbid(unsafe_code)]
//! Blocking TCP transport for Punah's framed protocol.

mod connection;
mod error;
mod server;

pub use connection::{
    VersionedFrame, read_frame, read_versioned_frame, write_frame, write_versioned_frame,
};
pub use error::{BoxError, ConnectionError};
pub use server::{RequestHandler, Server, ServerConfig, StopHandle, VersionedRequestHandler};
