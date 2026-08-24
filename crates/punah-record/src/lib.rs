#![forbid(unsafe_code)]

mod aligned;
mod codec;

pub use aligned::{
    AlignedDecodeError, AlignedEncodeError, AlignedExtent, DecodedAlignedExtent,
    decode_aligned_extent, encode_aligned_extent,
};

pub use codec::{
    COMMIT_MARKER_OFFSET, DecodeError, DecodedRecord, DecodedRecordRef, EncodeError,
    RECORD_HEADER_LEN, Record, RecordRef, RecordType, commit_in_place, commit_marker_bytes,
    committed_record_len, decode, decode_ref, encode, encode_uncommitted, encoded_len,
    encoded_len_for_fields,
};
