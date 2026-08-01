#![forbid(unsafe_code)]
//! Transport-independent request handling for Punah's binary protocol.
//!
//! Shape validation and response construction are pure. [`StoreService`] is
//! the imperative boundary that serializes access to the reference store.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use punah_protocol::{Frame, HEADER_LEN, MAX_FRAME_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, Opcode};
use punah_runtime::{BatchCommand, BatchValue, RuntimeError, RuntimeHandle};
use punah_store::Store;

mod v2_service;
pub use v2_service::{V2ValidationError, validate_v2};

const INVALID_PING: &[u8] = b"PING requires empty key, value, flags, and TTL";
const INVALID_GET: &[u8] = b"GET requires a key and empty value, flags, and TTL";
const INVALID_SET: &[u8] = b"SET requires a key and zero flags";
const INVALID_DELETE: &[u8] = b"DELETE requires a key and empty value, flags, and TTL";
const RESPONSE_ONLY: &[u8] = b"response opcode is not a valid request";
const STORE_FAILURE: &[u8] = b"store failure";
const STORE_UNAVAILABLE: &[u8] = b"store unavailable";
const KEY_LIMIT: &[u8] = b"key exceeds configured limit";
const VALUE_LIMIT: &[u8] = b"value exceeds configured limit";
const FRAME_LIMIT: &[u8] = b"frame exceeds configured limit";

/// Per-request resource limits enforced after wire decoding and before storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    /// Maximum complete encoded frame size, including the fixed header.
    pub max_frame_bytes: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: MAX_KEY_LEN,
            max_value_bytes: MAX_VALUE_LEN,
            max_frame_bytes: MAX_FRAME_LEN,
        }
    }
}

/// Cloneable service adapter around the single-writer reference store.
#[derive(Clone)]
pub struct StoreService {
    store: Arc<Mutex<Store>>,
    limits: RequestLimits,
}

impl StoreService {
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            limits: RequestLimits::default(),
        }
    }

    #[must_use]
    pub const fn from_shared(store: Arc<Mutex<Store>>) -> Self {
        Self {
            store,
            limits: RequestLimits {
                max_key_bytes: MAX_KEY_LEN,
                max_value_bytes: MAX_VALUE_LEN,
                max_frame_bytes: MAX_FRAME_LEN,
            },
        }
    }

    #[must_use]
    pub const fn with_limits(store: Arc<Mutex<Store>>, limits: RequestLimits) -> Self {
        Self { store, limits }
    }

    #[must_use]
    pub fn shared_store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
    }

    /// Validates and executes one request, always producing a protocol frame.
    ///
    /// Invalid request shapes, poisoned storage state, and persistence errors
    /// become `ERROR` frames so an individual command does not tear down its
    /// connection.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // Matches the transport handler's owned-frame API.
    pub fn handle(&self, request: Frame) -> Frame {
        let command = match validate(&request, self.limits) {
            Ok(command) => command,
            Err(message) => return error(message),
        };
        if command == Command::Ping {
            return response(Opcode::Pong, Vec::new(), Vec::new());
        }
        let Ok(mut store) = self.store.lock() else {
            return error(STORE_UNAVAILABLE);
        };
        dispatch(command, &mut *store)
    }
}

/// Cloneable protocol adapter for the shard-owned production runtime.
#[derive(Clone)]
pub struct RuntimeService {
    runtime: RuntimeHandle,
    limits: RequestLimits,
}

impl RuntimeService {
    #[must_use]
    pub fn new(runtime: RuntimeHandle) -> Self {
        Self::with_limits(runtime, RequestLimits::default())
    }

    #[must_use]
    pub const fn with_limits(runtime: RuntimeHandle, limits: RequestLimits) -> Self {
        Self { runtime, limits }
    }

    /// Validates and dispatches one request to its owning shard.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn handle(&self, request: Frame) -> Frame {
        let command = match validate(&request, self.limits) {
            Ok(command) => command,
            Err(message) => return error(message),
        };
        if command == Command::Ping {
            return response(Opcode::Pong, Vec::new(), Vec::new());
        }
        let mut runtime = self.runtime.clone();
        dispatch(command, &mut runtime)
    }

    /// Validates and executes a group through shard-local runtime batches.
    /// Response order exactly matches request order.
    #[must_use]
    pub fn handle_batch(&self, requests: Vec<Frame>) -> Vec<Frame> {
        let mut responses: Vec<Option<Frame>> = (0..requests.len()).map(|_| None).collect();
        let mut commands = Vec::new();
        let mut metadata = Vec::new();
        for (index, request) in requests.into_iter().enumerate() {
            let command = match validate(&request, self.limits) {
                Ok(command) => command,
                Err(message) => {
                    responses[index] = Some(error(message));
                    continue;
                }
            };
            let key = request.key.clone();
            let (opcode, command) = match command {
                Command::Ping => {
                    responses[index] = Some(response(Opcode::Pong, Vec::new(), Vec::new()));
                    continue;
                }
                Command::Get { .. } => (Opcode::Get, BatchCommand::Get { key }),
                Command::Set { ttl, .. } => (
                    Opcode::Set,
                    BatchCommand::Set {
                        key,
                        value: request.value,
                        ttl: (ttl != 0).then(|| Duration::from_secs(u64::from(ttl))),
                    },
                ),
                Command::Delete { .. } => (Opcode::Delete, BatchCommand::Delete { key }),
            };
            metadata.push((index, opcode, request.key));
            commands.push(command);
        }
        match self.runtime.execute_batch(commands) {
            Ok(outcomes) => {
                for ((index, opcode, key), outcome) in metadata.into_iter().zip(outcomes) {
                    responses[index] = Some(batch_response(opcode, key, outcome));
                }
            }
            Err(_) => {
                for (index, _, _) in metadata {
                    responses[index] = Some(error(STORE_UNAVAILABLE));
                }
            }
        }
        responses
            .into_iter()
            .map(|response| response.unwrap_or_else(|| error(STORE_UNAVAILABLE)))
            .collect()
    }
}

fn batch_response(
    opcode: Opcode,
    key: Vec<u8>,
    outcome: Result<BatchValue, RuntimeError>,
) -> Frame {
    match outcome {
        Ok(BatchValue::Get(value)) if opcode == Opcode::Get => {
            response(opcode, key, value.unwrap_or_default())
        }
        Ok(BatchValue::Set) if opcode == Opcode::Set => response(opcode, key, Vec::new()),
        Ok(BatchValue::Delete(existed)) if opcode == Opcode::Delete => {
            response(opcode, key, vec![u8::from(existed)])
        }
        Ok(_) | Err(RuntimeError::Store(_) | RuntimeError::Lease(_)) => error(STORE_FAILURE),
        Err(RuntimeError::Backpressure | RuntimeError::Shutdown) => error(STORE_UNAVAILABLE),
    }
}

#[derive(Clone, Copy)]
enum ExecutionError {
    Failure,
    Unavailable,
}

trait Executor {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExecutionError>;
    fn set(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), ExecutionError>;
    fn delete(&mut self, key: &[u8]) -> Result<bool, ExecutionError>;
}

impl Executor for Store {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExecutionError> {
        Store::get(self, key).map_err(|_| ExecutionError::Failure)
    }

    fn set(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), ExecutionError> {
        Store::set(self, key, value, ttl).map_err(|_| ExecutionError::Failure)
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool, ExecutionError> {
        Store::delete(self, key).map_err(|_| ExecutionError::Failure)
    }
}

impl Executor for RuntimeHandle {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExecutionError> {
        RuntimeHandle::get(self, key).map_err(|error| runtime_error(&error))
    }

    fn set(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), ExecutionError> {
        RuntimeHandle::set(self, key, value, ttl).map_err(|error| runtime_error(&error))
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool, ExecutionError> {
        RuntimeHandle::delete(self, key).map_err(|error| runtime_error(&error))
    }
}

const fn runtime_error(error: &RuntimeError) -> ExecutionError {
    match error {
        RuntimeError::Store(_) | RuntimeError::Lease(_) => ExecutionError::Failure,
        RuntimeError::Backpressure | RuntimeError::Shutdown => ExecutionError::Unavailable,
    }
}

fn dispatch(command: Command<'_>, executor: &mut impl Executor) -> Frame {
    let result = match command {
        Command::Ping => return response(Opcode::Pong, Vec::new(), Vec::new()),
        Command::Get { key } => executor
            .get(key)
            .map(|value| response(Opcode::Get, key.to_vec(), value.unwrap_or_default())),
        Command::Set { key, value, ttl } => {
            let ttl = (ttl != 0).then(|| Duration::from_secs(u64::from(ttl)));
            executor
                .set(key, value.to_vec(), ttl)
                .map(|()| response(Opcode::Set, key.to_vec(), Vec::new()))
        }
        Command::Delete { key } => executor
            .delete(key)
            .map(|existed| response(Opcode::Delete, key.to_vec(), vec![u8::from(existed)])),
    };
    match result {
        Ok(frame) => frame,
        Err(ExecutionError::Failure) => error(STORE_FAILURE),
        Err(ExecutionError::Unavailable) => error(STORE_UNAVAILABLE),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command<'a> {
    Ping,
    Get {
        key: &'a [u8],
    },
    Set {
        key: &'a [u8],
        value: &'a [u8],
        ttl: u32,
    },
    Delete {
        key: &'a [u8],
    },
}

fn validate(frame: &Frame, limits: RequestLimits) -> Result<Command<'_>, &'static [u8]> {
    if frame.key.len() > limits.max_key_bytes {
        return Err(KEY_LIMIT);
    }
    if frame.value.len() > limits.max_value_bytes {
        return Err(VALUE_LIMIT);
    }
    let frame_len = HEADER_LEN
        .checked_add(frame.key.len())
        .and_then(|length| length.checked_add(frame.value.len()));
    if frame_len.is_none_or(|length| length > limits.max_frame_bytes) {
        return Err(FRAME_LIMIT);
    }
    match frame.opcode {
        Opcode::Ping if empty_metadata(frame) && frame.key.is_empty() && frame.value.is_empty() => {
            Ok(Command::Ping)
        }
        Opcode::Ping => Err(INVALID_PING),
        Opcode::Get if empty_metadata(frame) && !frame.key.is_empty() && frame.value.is_empty() => {
            Ok(Command::Get { key: &frame.key })
        }
        Opcode::Get => Err(INVALID_GET),
        Opcode::Set if frame.flags == 0 && !frame.key.is_empty() => Ok(Command::Set {
            key: &frame.key,
            value: &frame.value,
            ttl: frame.ttl,
        }),
        Opcode::Set => Err(INVALID_SET),
        Opcode::Delete
            if empty_metadata(frame) && !frame.key.is_empty() && frame.value.is_empty() =>
        {
            Ok(Command::Delete { key: &frame.key })
        }
        Opcode::Delete => Err(INVALID_DELETE),
        Opcode::Pong | Opcode::Error => Err(RESPONSE_ONLY),
    }
}

const fn empty_metadata(frame: &Frame) -> bool {
    frame.flags == 0 && frame.ttl == 0
}

fn response(opcode: Opcode, key: Vec<u8>, value: Vec<u8>) -> Frame {
    Frame::new(opcode, key, value)
}

fn error(message: &[u8]) -> Frame {
    response(Opcode::Error, Vec::new(), message.to_vec())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use punah_runtime::{Runtime, RuntimeConfig};
    use punah_store::StoreConfig;
    use punah_types::{Durability, Namespace};

    use super::*;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn service(segment_capacity: u64) -> (StoreService, PathBuf) {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("punah-service-{}-{id}", std::process::id()));
        let store = Store::open(StoreConfig {
            directory: directory.clone(),
            namespace: Namespace::new("test").unwrap(),
            segment_capacity,
            durability: Durability::LocalDurable,
        })
        .unwrap();
        (StoreService::new(store), directory)
    }

    fn request(opcode: Opcode, key: &[u8], value: &[u8]) -> Frame {
        Frame::new(opcode, key.to_vec(), value.to_vec())
    }

    fn runtime_service() -> (Runtime, RuntimeService, PathBuf) {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("punah-runtime-service-{}-{id}", std::process::id()));
        let runtime = Runtime::open(&RuntimeConfig {
            directory: directory.clone(),
            namespace: Namespace::new("test").unwrap(),
            shard_count: 4,
            queue_capacity: 8,
            segment_capacity: 4096,
            durability: Durability::LocalDurable,
        })
        .unwrap();
        let service = RuntimeService::new(runtime.handle());
        (runtime, service, directory)
    }

    #[test]
    fn ping_has_an_exact_empty_pong() {
        let (service, directory) = service(4096);
        assert_eq!(
            service.handle(request(Opcode::Ping, b"", b"")),
            request(Opcode::Pong, b"", b"")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn set_get_and_delete_have_stable_acknowledgements() {
        let (service, directory) = service(4096);
        assert_eq!(
            service.handle(request(Opcode::Set, b"answer", b"42")),
            request(Opcode::Set, b"answer", b"")
        );
        assert_eq!(
            service.handle(request(Opcode::Get, b"answer", b"")),
            request(Opcode::Get, b"answer", b"42")
        );
        assert_eq!(
            service.handle(request(Opcode::Delete, b"answer", b"")),
            request(Opcode::Delete, b"answer", &[1])
        );
        assert_eq!(
            service.handle(request(Opcode::Delete, b"answer", b"")),
            request(Opcode::Delete, b"answer", &[0])
        );
        assert_eq!(
            service.handle(request(Opcode::Get, b"answer", b"")),
            request(Opcode::Get, b"answer", b"")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn ttl_seconds_are_accepted_without_becoming_response_metadata() {
        let (service, directory) = service(4096);
        let mut set = request(Opcode::Set, b"short", b"lived");
        set.ttl = 2;
        assert_eq!(service.handle(set), request(Opcode::Set, b"short", b""));
        assert_eq!(
            service.handle(request(Opcode::Get, b"short", b"")),
            request(Opcode::Get, b"short", b"lived")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_malformed_and_response_only_requests() {
        let (service, directory) = service(4096);
        for malformed in [
            request(Opcode::Get, b"", b""),
            request(Opcode::Get, b"key", b"unexpected"),
            request(Opcode::Set, b"", b"value"),
            request(Opcode::Delete, b"key", b"unexpected"),
            request(Opcode::Pong, b"", b""),
            request(Opcode::Error, b"", b""),
        ] {
            assert_eq!(service.handle(malformed).opcode, Opcode::Error);
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn persistence_failure_is_an_error_frame() {
        let (service, directory) = service(128);
        let result = service.handle(request(Opcode::Set, b"key", b"cannot-fit"));
        assert_eq!(result, error(STORE_FAILURE));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn configured_limits_reject_each_excess_before_storage() {
        let (base, directory) = service(4096);
        let service = StoreService::with_limits(
            base.shared_store(),
            RequestLimits {
                max_key_bytes: 3,
                max_value_bytes: 4,
                max_frame_bytes: HEADER_LEN + 6,
            },
        );
        assert_eq!(
            service.handle(request(Opcode::Get, b"four", b"")),
            error(KEY_LIMIT)
        );
        assert_eq!(
            service.handle(request(Opcode::Set, b"key", b"value")),
            error(VALUE_LIMIT)
        );
        assert_eq!(
            service.handle(request(Opcode::Set, b"key", b"four")),
            error(FRAME_LIMIT)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn ping_does_not_lock_the_store() {
        let (service, directory) = service(4096);
        let shared = service.shared_store();
        let _ = std::panic::catch_unwind(|| {
            let _guard = shared.lock().unwrap();
            panic!("poison for test");
        });
        assert_eq!(
            service.handle(request(Opcode::Ping, b"", b"")),
            request(Opcode::Pong, b"", b"")
        );
        assert_eq!(
            service.handle(request(Opcode::Get, b"key", b"")),
            error(STORE_UNAVAILABLE)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn runtime_adapter_preserves_command_responses_and_ping_needs_no_worker() {
        let (runtime, service, directory) = runtime_service();
        assert_eq!(
            service.handle(request(Opcode::Set, b"answer", b"42")),
            request(Opcode::Set, b"answer", b"")
        );
        assert_eq!(
            service.handle(request(Opcode::Get, b"answer", b"")),
            request(Opcode::Get, b"answer", b"42")
        );
        assert_eq!(
            service.handle(request(Opcode::Delete, b"answer", b"")),
            request(Opcode::Delete, b"answer", &[1])
        );
        runtime.shutdown();
        assert_eq!(
            service.handle(request(Opcode::Get, b"answer", b"")),
            error(STORE_UNAVAILABLE)
        );
        assert_eq!(
            service.handle(request(Opcode::Ping, b"", b"")),
            request(Opcode::Pong, b"", b"")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn runtime_batch_preserves_same_key_order_and_invalid_slots() {
        let (runtime, service, directory) = runtime_service();
        let responses = service.handle_batch(vec![
            request(Opcode::Set, b"answer", b"42"),
            request(Opcode::Get, b"answer", b""),
            request(Opcode::Get, b"", b""),
            request(Opcode::Delete, b"answer", b""),
        ]);
        assert_eq!(responses[0], request(Opcode::Set, b"answer", b""));
        assert_eq!(responses[1], request(Opcode::Get, b"answer", b"42"));
        assert_eq!(responses[2].opcode, Opcode::Error);
        assert_eq!(responses[3], request(Opcode::Delete, b"answer", &[1]));
        runtime.shutdown();
        fs::remove_dir_all(directory).unwrap();
    }
}
