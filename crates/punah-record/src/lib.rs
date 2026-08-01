#![forbid(unsafe_code)]

mod codec;

pub use codec::{
    COMMIT_MARKER_OFFSET, DecodeError, DecodedRecord, EncodeError, Record, RecordType,
    commit_in_place, commit_marker_bytes, decode, encode, encode_uncommitted,
};
