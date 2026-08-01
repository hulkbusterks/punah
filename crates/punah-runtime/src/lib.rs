#![forbid(unsafe_code)]

//! Portable shard-owned execution for Punah stores.
//!
//! Each exact key has one deterministic owner. A worker thread exclusively owns
//! that shard's [`Store`](punah_store::Store), so storage mutation needs neither
//! a shared store mutex nor ambient global state. Bounded request queues make
//! overload visible to callers.
//!
//! Computation leases are deliberately ephemeral coordination state. Restarting
//! a runtime invalidates every outstanding token; clients must reacquire. A
//! successfully published result is ordinary Store data and follows the
//! configured durability and restart guarantees.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use punah_engine::{AcquireResult, LeaseError, LeaseTable, LeaseToken};
use punah_identity::shard_hash;
use punah_store::{CleanupReport, Store, StoreConfig, StoreError};
use punah_types::{Durability, Namespace, OperationHash};

static NEXT_RUNTIME_EPOCH: AtomicU64 = AtomicU64::new(1);
const OPERATION_RESULT_DOMAIN: u8 = 0x02;

fn operation_result_key(operation: &OperationHash) -> [u8; 33] {
    let mut key = [0_u8; 33];
    key[0] = OPERATION_RESULT_DOMAIN;
    key[1..].copy_from_slice(operation);
    key
}

/// Caller-selected lifetime for a computation lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeasePolicy {
    pub duration: Duration,
}

/// Result of attempting to coalesce work for one exact operation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseAcquire {
    Granted {
        token: LeaseToken,
        expires_at: std::time::SystemTime,
    },
    Held {
        expires_at: std::time::SystemTime,
    },
}

/// Configuration shared by all owned shards.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub directory: PathBuf,
    pub namespace: Namespace,
    pub shard_count: usize,
    pub queue_capacity: usize,
    pub segment_capacity: u64,
    pub durability: Durability,
}

/// Failure to create a runtime.
#[derive(Debug)]
pub enum OpenError {
    InvalidShardCount,
    InvalidQueueCapacity,
    Store { shard: usize, source: StoreError },
    WorkerSpawn(std::io::Error),
}

/// Failure to accept or execute an operation.
#[derive(Debug)]
pub enum RuntimeError {
    /// The owning shard's bounded queue has no remaining capacity.
    Backpressure,
    /// Shutdown has begun or the owning worker exited.
    Shutdown,
    Store(StoreError),
    Lease(LeaseError),
}

/// One owned portable data operation suitable for shard batching.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchCommand {
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl BatchCommand {
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key } | Self::Set { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Type-preserving outcome of one [`BatchCommand`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchValue {
    Get(Option<Vec<u8>>),
    Set,
    Delete(bool),
}

enum Request {
    Batch {
        commands: Vec<(usize, BatchCommand)>,
        reply: SyncSender<Vec<(usize, Result<BatchValue, RuntimeError>)>>,
    },
    Get {
        key: Vec<u8>,
        reply: SyncSender<Result<Option<Vec<u8>>, RuntimeError>>,
    },
    GetResult {
        operation: OperationHash,
        reply: SyncSender<Result<Option<Vec<u8>>, RuntimeError>>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        reply: SyncSender<Result<(), RuntimeError>>,
    },
    Delete {
        key: Vec<u8>,
        reply: SyncSender<Result<bool, RuntimeError>>,
    },
    Cleanup {
        budget: usize,
        reply: SyncSender<Result<CleanupReport, RuntimeError>>,
    },
    LeaseAcquire {
        operation: OperationHash,
        now: std::time::SystemTime,
        policy: LeasePolicy,
        reply: SyncSender<Result<LeaseAcquire, RuntimeError>>,
    },
    LeaseRenew {
        operation: OperationHash,
        token: LeaseToken,
        now: std::time::SystemTime,
        policy: LeasePolicy,
        reply: SyncSender<Result<std::time::SystemTime, RuntimeError>>,
    },
    LeaseRelease {
        operation: OperationHash,
        token: LeaseToken,
        now: std::time::SystemTime,
        reply: SyncSender<Result<(), RuntimeError>>,
    },
    PublishResult {
        operation: OperationHash,
        token: LeaseToken,
        now: std::time::SystemTime,
        value: Vec<u8>,
        ttl: Option<Duration>,
        reply: SyncSender<Result<(), RuntimeError>>,
    },
    Shutdown,
    #[cfg(test)]
    Barrier {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

struct Shared {
    senders: Vec<SyncSender<Request>>,
    accepting: AtomicBool,
    submission_gate: Mutex<()>,
}

/// Cloneable submission side of a [`Runtime`].
#[derive(Clone)]
pub struct RuntimeHandle {
    shared: Arc<Shared>,
}

impl RuntimeHandle {
    /// Returns the stable owner index for an exact key.
    #[must_use]
    pub fn shard_for(&self, key: &[u8]) -> usize {
        route(key, self.shared.senders.len())
    }

    /// Executes owned commands in shard-local batches and restores input order.
    ///
    /// At most one queue message and reply channel are created per participating
    /// shard, rather than per operation. Commands accepted before another shard
    /// reports backpressure still complete and return their individual results.
    ///
    /// # Errors
    /// Returns shutdown when admission has stopped. Per-command backpressure and
    /// storage failures are returned in the result vector.
    pub fn execute_batch(
        &self,
        commands: Vec<BatchCommand>,
    ) -> Result<Vec<Result<BatchValue, RuntimeError>>, RuntimeError> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        let command_count = commands.len();
        let mut shards: Vec<Vec<(usize, BatchCommand)>> =
            (0..self.shared.senders.len()).map(|_| Vec::new()).collect();
        for (index, command) in commands.into_iter().enumerate() {
            shards[self.shard_for(command.key())].push((index, command));
        }

        let guard = self
            .shared
            .submission_gate
            .lock()
            .map_err(|_| RuntimeError::Shutdown)?;
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Shutdown);
        }
        let mut replies = Vec::new();
        let mut results: Vec<Option<Result<BatchValue, RuntimeError>>> =
            (0..command_count).map(|_| None).collect();
        for (shard, commands) in shards.into_iter().enumerate() {
            if commands.is_empty() {
                continue;
            }
            let indexes: Vec<_> = commands.iter().map(|(index, _)| *index).collect();
            let (reply, receiver) = mpsc::sync_channel(1);
            match self.shared.senders[shard].try_send(Request::Batch { commands, reply }) {
                Ok(()) => replies.push(receiver),
                Err(TrySendError::Full(_)) => {
                    for index in indexes {
                        results[index] = Some(Err(RuntimeError::Backpressure));
                    }
                }
                Err(TrySendError::Disconnected(_)) => {
                    for index in indexes {
                        results[index] = Some(Err(RuntimeError::Shutdown));
                    }
                }
            }
        }
        drop(guard);
        for receiver in replies {
            for (index, result) in receiver.recv().map_err(|_| RuntimeError::Shutdown)? {
                results[index] = Some(result);
            }
        }
        results
            .into_iter()
            .map(|result| result.ok_or(RuntimeError::Shutdown))
            .collect()
    }

    /// Reads a key, rejecting immediately when its owner queue is full.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or the owning store's failure.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            key,
            Request::Get {
                key: key.to_vec(),
                reply,
            },
        )?;
        receive(&result)
    }

    /// Stores exact key and value bytes, rejecting immediately on backpressure.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or the owning store's failure.
    pub fn set(
        &self,
        key: &[u8],
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            key,
            Request::Set {
                key: key.to_vec(),
                value,
                ttl,
                reply,
            },
        )?;
        receive(&result)
    }

    /// Durably tombstones a key, rejecting immediately on backpressure.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or the owning store's failure.
    pub fn delete(&self, key: &[u8]) -> Result<bool, RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            key,
            Request::Delete {
                key: key.to_vec(),
                reply,
            },
        )?;
        receive(&result)
    }

    /// Runs bounded expiry cleanup on every shard and combines the reports.
    ///
    /// # Errors
    /// Returns without hiding partial admission if a shard queue is full. Any
    /// already accepted cleanup requests still complete on their owners.
    pub fn cleanup_expired(&self, budget_per_shard: usize) -> Result<CleanupReport, RuntimeError> {
        let mut replies = Vec::with_capacity(self.shared.senders.len());
        let guard = self
            .shared
            .submission_gate
            .lock()
            .map_err(|_| RuntimeError::Shutdown)?;
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Shutdown);
        }
        for sender in &self.shared.senders {
            let (reply, result) = mpsc::sync_channel(1);
            try_send(
                sender,
                Request::Cleanup {
                    budget: budget_per_shard,
                    reply,
                },
            )?;
            replies.push(result);
        }
        drop(guard);
        let mut combined = CleanupReport::default();
        for reply in replies {
            let report = receive(&reply)?;
            combined.tombstoned = combined.tombstoned.saturating_add(report.tombstoned);
            combined.budget_exhausted |= report.budget_exhausted;
        }
        Ok(combined)
    }

    /// Acquires the single ephemeral computation lease for an operation.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or invalid lease policy failures.
    pub fn acquire_lease(
        &self,
        operation: OperationHash,
        now: std::time::SystemTime,
        policy: LeasePolicy,
    ) -> Result<LeaseAcquire, RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            &operation,
            Request::LeaseAcquire {
                operation,
                now,
                policy,
                reply,
            },
        )?;
        receive(&result)
    }

    /// Extends a live lease when `token` is still its fenced owner.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or a lease fencing failure.
    pub fn renew_lease(
        &self,
        operation: OperationHash,
        token: LeaseToken,
        now: std::time::SystemTime,
        policy: LeasePolicy,
    ) -> Result<std::time::SystemTime, RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            &operation,
            Request::LeaseRenew {
                operation,
                token,
                now,
                policy,
                reply,
            },
        )?;
        receive(&result)
    }

    /// Releases a live lease without publishing a result.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or a lease fencing failure.
    pub fn release_lease(
        &self,
        operation: OperationHash,
        token: LeaseToken,
        now: std::time::SystemTime,
    ) -> Result<(), RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            &operation,
            Request::LeaseRelease {
                operation,
                token,
                now,
                reply,
            },
        )?;
        receive(&result)
    }

    /// Persists a result under the exact 32-byte operation ID, then releases
    /// its lease. Authorization and storage execute on the same shard owner.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, storage failure, or a lease fencing
    /// failure. Storage failure retains the lease so the owner may retry.
    pub fn publish_result(
        &self,
        operation: OperationHash,
        token: LeaseToken,
        now: std::time::SystemTime,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(
            &operation,
            Request::PublishResult {
                operation,
                token,
                now,
                value,
                ttl,
                reply,
            },
        )?;
        receive(&result)
    }

    /// Reads a previously published result by exact operation identity.
    ///
    /// # Errors
    /// Returns backpressure, shutdown, or a storage failure.
    pub fn get_result(&self, operation: OperationHash) -> Result<Option<Vec<u8>>, RuntimeError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.submit(&operation, Request::GetResult { operation, reply })?;
        receive(&result)
    }

    fn submit(&self, key: &[u8], request: Request) -> Result<(), RuntimeError> {
        let _guard = self
            .shared
            .submission_gate
            .lock()
            .map_err(|_| RuntimeError::Shutdown)?;
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Shutdown);
        }
        try_send(&self.shared.senders[self.shard_for(key)], request)
    }
}

/// Owns worker lifetimes; dropping it drains accepted work and joins workers.
pub struct Runtime {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

impl Runtime {
    /// Opens all shard stores and starts their exclusive worker threads.
    ///
    /// # Errors
    /// Returns invalid configuration, shard storage, or worker-spawn failures.
    pub fn open(config: &RuntimeConfig) -> Result<Self, OpenError> {
        if config.shard_count == 0 || !config.shard_count.is_power_of_two() {
            return Err(OpenError::InvalidShardCount);
        }
        if config.queue_capacity == 0 {
            return Err(OpenError::InvalidQueueCapacity);
        }
        let mut stores = Vec::with_capacity(config.shard_count);
        for shard in 0..config.shard_count {
            let shard_config = StoreConfig {
                directory: config.directory.join(format!("shard-{shard:04}")),
                namespace: config.namespace.clone(),
                segment_capacity: config.segment_capacity,
                durability: config.durability,
            };
            stores.push(
                Store::open(shard_config).map_err(|source| OpenError::Store { shard, source })?,
            );
        }

        let mut senders = Vec::with_capacity(config.shard_count);
        let mut workers = Vec::with_capacity(config.shard_count);
        let runtime_epoch = NEXT_RUNTIME_EPOCH.fetch_add(1, Ordering::Relaxed);
        for (shard, store) in stores.into_iter().enumerate() {
            let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
            let namespace = config.namespace.clone();
            let worker = thread::Builder::new()
                .name(format!("punah-shard-{shard:04}"))
                .spawn(move || run_worker(store, &namespace, runtime_epoch, &receiver))
                .map_err(OpenError::WorkerSpawn)?;
            senders.push(sender);
            workers.push(worker);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                senders,
                accepting: AtomicBool::new(true),
                submission_gate: Mutex::new(()),
            }),
            workers,
        })
    }

    #[must_use]
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Stops admission, drains all previously accepted work, and joins workers.
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        if self.workers.is_empty() {
            return;
        }
        if let Ok(_guard) = self.shared.submission_gate.lock() {
            self.shared.accepting.store(false, Ordering::Release);
            for sender in &self.shared.senders {
                let _ = sender.send(Request::Shutdown);
            }
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn route(key: &[u8], shard_count: usize) -> usize {
    #[allow(clippy::cast_possible_truncation)]
    let hash = shard_hash(key).get() as usize;
    hash & (shard_count - 1)
}

fn try_send(sender: &SyncSender<Request>, request: Request) -> Result<(), RuntimeError> {
    sender.try_send(request).map_err(|error| match error {
        TrySendError::Full(_) => RuntimeError::Backpressure,
        TrySendError::Disconnected(_) => RuntimeError::Shutdown,
    })
}

fn receive<T>(receiver: &Receiver<Result<T, RuntimeError>>) -> Result<T, RuntimeError> {
    receiver.recv().map_err(|_| RuntimeError::Shutdown)?
}

#[allow(clippy::too_many_lines)] // Exhaustive owner dispatch keeps state transitions in one loop.
fn run_worker(
    mut store: Store,
    namespace: &Namespace,
    runtime_epoch: u64,
    receiver: &Receiver<Request>,
) {
    let mut leases = LeaseTable::default();
    let mut next_token = 1_u64;
    while let Ok(request) = receiver.recv() {
        match request {
            Request::Batch { commands, reply } => {
                execute_batch_request(&mut store, commands, &reply);
            }
            Request::Get { key, reply } => {
                let _ = reply.send(store.get(&key).map_err(RuntimeError::Store));
            }
            Request::GetResult { operation, reply } => {
                let _ = reply.send(
                    store
                        .get(&operation_result_key(&operation))
                        .map_err(RuntimeError::Store),
                );
            }
            Request::Set {
                key,
                value,
                ttl,
                reply,
            } => {
                let _ = reply.send(store.set(&key, value, ttl).map_err(RuntimeError::Store));
            }
            Request::Delete { key, reply } => {
                let _ = reply.send(store.delete(&key).map_err(RuntimeError::Store));
            }
            Request::Cleanup { budget, reply } => {
                let _ = reply.send(store.cleanup_expired(budget).map_err(RuntimeError::Store));
            }
            Request::LeaseAcquire {
                operation,
                now,
                policy,
                reply,
            } => {
                let token = lease_token(runtime_epoch, next_token);
                next_token = next_token.wrapping_add(1);
                let result = leases
                    .try_acquire(namespace.clone(), operation, token, now, policy.duration)
                    .map(|result| match result {
                        AcquireResult::Acquired(lease) => LeaseAcquire::Granted {
                            token: lease.token,
                            expires_at: lease.expires_at,
                        },
                        AcquireResult::HeldUntil(expires_at) => LeaseAcquire::Held { expires_at },
                    })
                    .map_err(RuntimeError::Lease);
                let _ = reply.send(result);
            }
            Request::LeaseRenew {
                operation,
                token,
                now,
                policy,
                reply,
            } => {
                let result = leases
                    .renew(namespace, &operation, token, now, policy.duration)
                    .map_err(RuntimeError::Lease);
                let _ = reply.send(result);
            }
            Request::LeaseRelease {
                operation,
                token,
                now,
                reply,
            } => {
                let result = leases
                    .authorize(namespace, &operation, token, now)
                    .and_then(|()| leases.release(namespace, &operation, token))
                    .map_err(RuntimeError::Lease);
                let _ = reply.send(result);
            }
            Request::PublishResult {
                operation,
                token,
                now,
                value,
                ttl,
                reply,
            } => {
                let result = leases
                    .authorize(namespace, &operation, token, now)
                    .map_err(RuntimeError::Lease)
                    .and_then(|()| {
                        store
                            .set(&operation_result_key(&operation), value, ttl)
                            .map_err(RuntimeError::Store)
                    })
                    .and_then(|()| {
                        leases
                            .release(namespace, &operation, token)
                            .map_err(RuntimeError::Lease)
                    });
                let _ = reply.send(result);
            }
            Request::Shutdown => break,
            #[cfg(test)]
            Request::Barrier { entered, release } => {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
    }
}

fn execute_batch_request(
    store: &mut Store,
    commands: Vec<(usize, BatchCommand)>,
    reply: &SyncSender<Vec<(usize, Result<BatchValue, RuntimeError>)>>,
) {
    let results = commands
        .into_iter()
        .map(|(index, command)| (index, execute_batch_command(store, command)))
        .collect();
    let _ = reply.send(results);
}

fn execute_batch_command(
    store: &mut Store,
    command: BatchCommand,
) -> Result<BatchValue, RuntimeError> {
    match command {
        BatchCommand::Get { key } => store
            .get(&key)
            .map(BatchValue::Get)
            .map_err(RuntimeError::Store),
        BatchCommand::Set { key, value, ttl } => store
            .set(&key, value, ttl)
            .map(|()| BatchValue::Set)
            .map_err(RuntimeError::Store),
        BatchCommand::Delete { key } => store
            .delete(&key)
            .map(BatchValue::Delete)
            .map_err(RuntimeError::Store),
    }
}

fn lease_token(runtime_epoch: u64, sequence: u64) -> LeaseToken {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&runtime_epoch.to_be_bytes());
    bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    LeaseToken(bytes)
}

#[cfg(test)]
mod tests;
