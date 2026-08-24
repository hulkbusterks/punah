#![forbid(unsafe_code)]

mod index;
mod lease;
mod recovery;
mod segment;
mod sharded;

pub use index::{
    IndexIdentity, IndexSnapshotEntry, IndexVersion, ReferenceIndex, TombstoneMetadata,
    TombstoneSource,
};
pub use lease::{AcquireResult, Lease, LeaseError, LeaseTable, LeaseToken};
pub use recovery::{RecoveryError, RecoveryReport, recover_segment};
pub use segment::{
    AppendCompletion, AppendError, AppendRejected, AppendTicket, CohortRejected, SegmentWriter,
};
pub use sharded::{ExpiryReport, ShardedIndex, ShardedIndexError};
