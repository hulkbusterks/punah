#![forbid(unsafe_code)]
//! Framing primitives for Punah's binary TCP protocol.

mod error;
mod frame;
mod opcode;
pub mod v2;

pub use error::{DecodeError, EncodeError};
pub use frame::{
    Frame, FrameRef, HEADER_LEN, MAX_FRAME_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, VERSION, decode,
    encode, encode_into,
};
pub use opcode::Opcode;
