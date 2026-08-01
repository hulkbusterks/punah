//! Pure v2 request validation and runtime-backed execution.

use std::time::{Duration, SystemTime};

use punah_engine::LeaseToken;
use punah_identity::artifact_id;
use punah_protocol::v2::{Frame, FrameRef, Opcode, Status};
use punah_runtime::{LeaseAcquire, LeasePolicy, RuntimeError};
use punah_types::{ArtifactId, OperationId};

use crate::RuntimeService;

const ARTIFACT_DOMAIN: u8 = 0x01;
const RESULT_PREFIX_LEN: usize = 48;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V2ValidationError {
    ResponseStatus,
    Flags,
    KeyLength,
    ValueLength,
    Ttl,
    IdentityMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V2Command<'a> {
    ArtifactGet(ArtifactId),
    ArtifactPut {
        id: ArtifactId,
        bytes: &'a [u8],
    },
    OperationResolve(OperationId),
    LeaseAcquire {
        operation: OperationId,
        duration: Duration,
    },
    LeaseRenew {
        operation: OperationId,
        token: LeaseToken,
        duration: Duration,
    },
    LeaseRelease {
        operation: OperationId,
        token: LeaseToken,
    },
    ResultPublish {
        operation: OperationId,
        token: LeaseToken,
        result: ArtifactId,
        bytes: &'a [u8],
        ttl: Option<Duration>,
    },
}

/// Validates v2 semantics without reading a clock or mutating state.
///
/// # Errors
/// Returns a precise shape or identity error before any runtime operation.
#[allow(clippy::match_same_arms)]
pub fn validate_v2(frame: FrameRef<'_>) -> Result<V2Command<'_>, V2ValidationError> {
    if frame.status != Status::None {
        return Err(V2ValidationError::ResponseStatus);
    }
    if frame.flags != 0 {
        return Err(V2ValidationError::Flags);
    }
    let id = exact_32(frame.key).ok_or(V2ValidationError::KeyLength)?;
    match frame.opcode {
        Opcode::ArtifactGet if frame.value.is_empty() && frame.ttl_seconds == 0 => {
            Ok(V2Command::ArtifactGet(ArtifactId::new(id)))
        }
        Opcode::ArtifactGet => Err(empty_value_or_ttl(frame.value)),
        Opcode::ArtifactPut if frame.ttl_seconds == 0 => {
            let claimed = ArtifactId::new(id);
            if artifact_id(frame.value) != claimed {
                return Err(V2ValidationError::IdentityMismatch);
            }
            Ok(V2Command::ArtifactPut {
                id: claimed,
                bytes: frame.value,
            })
        }
        Opcode::ArtifactPut => Err(V2ValidationError::Ttl),
        Opcode::OperationResolve if frame.value.is_empty() && frame.ttl_seconds == 0 => {
            Ok(V2Command::OperationResolve(OperationId::new(id)))
        }
        Opcode::OperationResolve => Err(empty_value_or_ttl(frame.value)),
        Opcode::LeaseAcquire if frame.value.is_empty() && frame.ttl_seconds != 0 => {
            Ok(V2Command::LeaseAcquire {
                operation: OperationId::new(id),
                duration: Duration::from_secs(u64::from(frame.ttl_seconds)),
            })
        }
        Opcode::LeaseAcquire => Err(empty_value_or_ttl(frame.value)),
        Opcode::LeaseRenew if frame.value.len() == 16 && frame.ttl_seconds != 0 => {
            Ok(V2Command::LeaseRenew {
                operation: OperationId::new(id),
                token: token(frame.value),
                duration: Duration::from_secs(u64::from(frame.ttl_seconds)),
            })
        }
        Opcode::LeaseRenew => Err(token_or_ttl(frame.value)),
        Opcode::LeaseRelease if frame.value.len() == 16 && frame.ttl_seconds == 0 => {
            Ok(V2Command::LeaseRelease {
                operation: OperationId::new(id),
                token: token(frame.value),
            })
        }
        Opcode::LeaseRelease => Err(token_or_ttl(frame.value)),
        Opcode::ResultPublish if frame.value.len() >= RESULT_PREFIX_LEN => {
            let token = token(&frame.value[..16]);
            let mut result_bytes = [0_u8; 32];
            result_bytes.copy_from_slice(&frame.value[16..48]);
            let result = ArtifactId::new(result_bytes);
            let bytes = &frame.value[48..];
            if artifact_id(bytes) != result {
                return Err(V2ValidationError::IdentityMismatch);
            }
            Ok(V2Command::ResultPublish {
                operation: OperationId::new(id),
                token,
                result,
                bytes,
                ttl: (frame.ttl_seconds != 0)
                    .then(|| Duration::from_secs(u64::from(frame.ttl_seconds))),
            })
        }
        Opcode::ResultPublish => Err(V2ValidationError::ValueLength),
    }
}

impl RuntimeService {
    /// Executes one v2 request using the caller-provided time for deterministic fencing.
    #[must_use]
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub fn handle_v2_at(&self, request: Frame, now: SystemTime) -> Frame {
        let command = match validate_v2(request.as_ref()) {
            Ok(command) => command,
            Err(error) => {
                return reply(
                    &request,
                    Status::Invalid,
                    Vec::new(),
                    format!("{error:?}").into_bytes(),
                    0,
                );
            }
        };
        let result = match command {
            V2Command::ArtifactGet(id) => {
                self.runtime
                    .get(&artifact_key(id))
                    .map(|value| match value {
                        Some(value) => {
                            reply(&request, Status::Hit, id.as_bytes().to_vec(), value, 0)
                        }
                        None => reply(
                            &request,
                            Status::Miss,
                            id.as_bytes().to_vec(),
                            Vec::new(),
                            0,
                        ),
                    })
            }
            V2Command::ArtifactPut { id, bytes } => self
                .runtime
                .set(&artifact_key(id), bytes.to_vec(), None)
                .map(|()| {
                    reply(
                        &request,
                        Status::Stored,
                        id.as_bytes().to_vec(),
                        Vec::new(),
                        0,
                    )
                }),
            V2Command::OperationResolve(operation) => self
                .runtime
                .get_result(operation.into())
                .map(|value| match value {
                    Some(value) => reply(
                        &request,
                        Status::Hit,
                        operation.as_bytes().to_vec(),
                        value,
                        0,
                    ),
                    None => reply(
                        &request,
                        Status::Miss,
                        operation.as_bytes().to_vec(),
                        Vec::new(),
                        0,
                    ),
                }),
            V2Command::LeaseAcquire {
                operation,
                duration,
            } => self
                .runtime
                .acquire_lease(operation.into(), now, LeasePolicy { duration })
                .map(|outcome| match outcome {
                    LeaseAcquire::Granted { token, .. } => reply(
                        &request,
                        Status::LeaseGranted,
                        operation.as_bytes().to_vec(),
                        token.0.to_vec(),
                        request.ttl_seconds,
                    ),
                    LeaseAcquire::Held { expires_at } => reply(
                        &request,
                        Status::LeaseHeld,
                        operation.as_bytes().to_vec(),
                        Vec::new(),
                        remaining_seconds(now, expires_at),
                    ),
                }),
            V2Command::LeaseRenew {
                operation,
                token,
                duration,
            } => self
                .runtime
                .renew_lease(operation.into(), token, now, LeasePolicy { duration })
                .map(|expires| {
                    reply(
                        &request,
                        Status::LeaseGranted,
                        operation.as_bytes().to_vec(),
                        token.0.to_vec(),
                        remaining_seconds(now, expires),
                    )
                }),
            V2Command::LeaseRelease { operation, token } => self
                .runtime
                .release_lease(operation.into(), token, now)
                .map(|()| {
                    reply(
                        &request,
                        Status::Deleted,
                        operation.as_bytes().to_vec(),
                        Vec::new(),
                        0,
                    )
                }),
            V2Command::ResultPublish {
                operation,
                token,
                bytes,
                ..
            } => self
                .runtime
                .publish_result(
                    operation.into(),
                    token,
                    now,
                    bytes.to_vec(),
                    ttl_from(request.ttl_seconds),
                )
                .map(|()| {
                    reply(
                        &request,
                        Status::Stored,
                        operation.as_bytes().to_vec(),
                        Vec::new(),
                        0,
                    )
                }),
        };
        match result {
            Ok(response) => response,
            Err(RuntimeError::Backpressure | RuntimeError::Shutdown) => {
                reply(&request, Status::Backpressure, Vec::new(), Vec::new(), 0)
            }
            Err(RuntimeError::Lease(_)) => {
                reply(&request, Status::Conflict, Vec::new(), Vec::new(), 0)
            }
            Err(RuntimeError::Store(_)) => {
                reply(&request, Status::Error, Vec::new(), Vec::new(), 0)
            }
        }
    }

    /// Convenience boundary for production callers; tests should prefer [`Self::handle_v2_at`].
    #[must_use]
    pub fn handle_v2(&self, request: Frame) -> Frame {
        self.handle_v2_at(request, SystemTime::now())
    }
}

fn exact_32(bytes: &[u8]) -> Option<[u8; 32]> {
    bytes.try_into().ok()
}
fn empty_value_or_ttl(value: &[u8]) -> V2ValidationError {
    if value.is_empty() {
        V2ValidationError::Ttl
    } else {
        V2ValidationError::ValueLength
    }
}
fn token_or_ttl(value: &[u8]) -> V2ValidationError {
    if value.len() == 16 {
        V2ValidationError::Ttl
    } else {
        V2ValidationError::ValueLength
    }
}
fn token(bytes: &[u8]) -> LeaseToken {
    LeaseToken(bytes.try_into().expect("validated 16-byte token"))
}
fn artifact_key(id: ArtifactId) -> [u8; 33] {
    let mut key = [0_u8; 33];
    key[0] = ARTIFACT_DOMAIN;
    key[1..].copy_from_slice(id.as_bytes());
    key
}
fn ttl_from(seconds: u32) -> Option<Duration> {
    (seconds != 0).then(|| Duration::from_secs(u64::from(seconds)))
}
fn remaining_seconds(now: SystemTime, deadline: SystemTime) -> u32 {
    let Ok(duration) = deadline.duration_since(now) else {
        return 0;
    };
    u32::try_from(
        duration
            .as_secs()
            .saturating_add(u64::from(duration.subsec_nanos() != 0)),
    )
    .unwrap_or(u32::MAX)
}
fn reply(request: &Frame, status: Status, key: Vec<u8>, value: Vec<u8>, ttl_seconds: u32) -> Frame {
    Frame {
        opcode: request.opcode,
        status,
        flags: 0,
        request_id: request.request_id,
        ttl_seconds,
        key,
        value,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use punah_protocol::v2::Frame;
    use punah_runtime::{Runtime, RuntimeConfig};
    use punah_types::{Durability, Namespace};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn frame(opcode: Opcode, key: Vec<u8>, value: Vec<u8>, ttl: u32) -> Frame {
        Frame {
            opcode,
            status: Status::None,
            flags: 0,
            request_id: 77,
            ttl_seconds: ttl,
            key,
            value,
        }
    }

    #[test]
    fn validator_rejects_response_metadata_and_wrong_fixed_lengths() {
        let mut request = frame(Opcode::ArtifactGet, vec![0; 31], Vec::new(), 0);
        assert_eq!(
            validate_v2(request.as_ref()),
            Err(V2ValidationError::KeyLength)
        );
        request.key.push(0);
        request.status = Status::Hit;
        assert_eq!(
            validate_v2(request.as_ref()),
            Err(V2ValidationError::ResponseStatus)
        );
    }

    #[test]
    fn artifact_and_result_claims_are_verified() {
        let bytes = b"answer";
        let id = artifact_id(bytes);
        assert!(matches!(
            validate_v2(
                frame(
                    Opcode::ArtifactPut,
                    id.as_bytes().to_vec(),
                    bytes.to_vec(),
                    0
                )
                .as_ref()
            ),
            Ok(V2Command::ArtifactPut { .. })
        ));
        let mut envelope = vec![1; 16];
        envelope.extend_from_slice(id.as_bytes());
        envelope.extend_from_slice(bytes);
        assert!(matches!(
            validate_v2(frame(Opcode::ResultPublish, vec![2; 32], envelope, 0).as_ref()),
            Ok(V2Command::ResultPublish { .. })
        ));
        assert_eq!(
            validate_v2(frame(Opcode::ArtifactPut, vec![0; 32], bytes.to_vec(), 0).as_ref()),
            Err(V2ValidationError::IdentityMismatch)
        );
    }

    #[test]
    fn runtime_flow_preserves_request_id_fencing_and_key_domains() {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("punah-v2-service-{}-{id}", std::process::id()));
        let runtime = Runtime::open(&RuntimeConfig {
            directory: directory.clone(),
            namespace: Namespace::new("v2-test").unwrap(),
            shard_count: 2,
            queue_capacity: 8,
            segment_capacity: 4096,
            durability: Durability::LocalDurable,
        })
        .unwrap();
        let service = RuntimeService::new(runtime.handle());
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let operation = vec![9; 32];

        let acquire = service.handle_v2_at(
            frame(Opcode::LeaseAcquire, operation.clone(), Vec::new(), 30),
            now,
        );
        assert_eq!(acquire.status, Status::LeaseGranted);
        assert_eq!(acquire.request_id, 77);
        assert_eq!(acquire.value.len(), 16);

        let result_bytes = b"operation result";
        let result_id = artifact_id(result_bytes);
        let mut envelope = acquire.value;
        envelope.extend_from_slice(result_id.as_bytes());
        envelope.extend_from_slice(result_bytes);
        assert_eq!(
            service
                .handle_v2_at(
                    frame(Opcode::ResultPublish, operation.clone(), envelope, 0),
                    now,
                )
                .status,
            Status::Stored
        );
        let resolved = service.handle_v2_at(
            frame(Opcode::OperationResolve, operation.clone(), Vec::new(), 0),
            now,
        );
        assert_eq!(resolved.status, Status::Hit);
        assert_eq!(resolved.value, result_bytes);

        // Equal 32-byte logical IDs cannot alias across the two physical domains.
        let artifact_bytes = b"artifact";
        let artifact = artifact_id(artifact_bytes);
        assert_eq!(
            service
                .handle_v2_at(
                    frame(
                        Opcode::ArtifactPut,
                        artifact.as_bytes().to_vec(),
                        artifact_bytes.to_vec(),
                        0,
                    ),
                    now,
                )
                .status,
            Status::Stored
        );
        assert_eq!(
            service
                .handle_v2_at(
                    frame(
                        Opcode::OperationResolve,
                        artifact.as_bytes().to_vec(),
                        Vec::new(),
                        0,
                    ),
                    now,
                )
                .status,
            Status::Miss
        );

        runtime.shutdown();
        fs::remove_dir_all(directory).unwrap();
    }
}
