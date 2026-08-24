#![forbid(unsafe_code)]

//! Portable shard-owned execution for Punah stores.
//!
//! Each exact key has one deterministic owner. A worker thread exclusively owns
//! that shard's [`punah_store::Store`], so storage mutation needs neither
//! a shared store mutex nor ambient global state. Bounded request queues make
//! overload visible to callers.
//!
//! Computation leases are deliberately ephemeral coordination state. Restarting
//! a runtime invalidates every outstanding token; clients must reacquire. A
//! successfully published result is ordinary Store data and follows the
//! configured durability and restart guarantees.

mod maintenance;

pub use maintenance::{BackgroundCompaction, MaintenanceSnapshot};

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use punah_engine::{AcquireResult, LeaseError, LeaseTable, LeaseToken};
use punah_identity::shard_hash;
use punah_store::{
    CleanupReport, MutationValue, Store, StoreConfig, StoreError, StoreIoTelemetry, StoreMutation,
};
use punah_types::{Durability, Namespace, OperationHash};

use crate::maintenance::{MaintenanceCounters, MaintenanceScheduler};

static NEXT_RUNTIME_EPOCH: AtomicU64 = AtomicU64::new(1);
const OPERATION_RESULT_DOMAIN: u8 = 0x02;
const MAX_DRAINED_BATCH_MESSAGES: usize = 64;
const MAX_DRAINED_MUTATIONS: usize = 64;
const MAX_DRAINED_MUTATION_BYTES: usize = 1024 * 1024;
const MUTATION_RECORD_OVERHEAD_ESTIMATE: usize = 256;

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
    InvalidCompactionPolicy,
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

/// Actual successful foreground append counters reported by one owner shard.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShardStorageTelemetry {
    pub shard: usize,
    pub storage: StoreIoTelemetry,
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
    StorageTelemetry {
        reply: SyncSender<StoreIoTelemetry>,
    },
    Shutdown,
    #[cfg(test)]
    Barrier {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

struct QueuedBatch {
    commands: Vec<(usize, BatchCommand)>,
    reply: SyncSender<Vec<(usize, Result<BatchValue, RuntimeError>)>>,
}

struct ShardIngress {
    sender: SyncSender<Request>,
    gate: Mutex<()>,
    maintenance: Arc<MaintenanceCounters>,
}

struct Shared {
    ingresses: Vec<ShardIngress>,
    accepting: AtomicBool,
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
        route(key, self.shared.ingresses.len())
    }

    /// Returns monotonic maintenance counters for every owned shard.
    #[must_use]
    pub fn maintenance_snapshots(&self) -> Vec<MaintenanceSnapshot> {
        self.shared
            .ingresses
            .iter()
            .enumerate()
            .map(|(shard, ingress)| ingress.maintenance.snapshot(shard))
            .collect()
    }

    /// Retrieves actual append/cohort/flush/rotation counters from every owner.
    ///
    /// # Errors
    /// Returns backpressure if any owner cannot accept the observation request,
    /// or shutdown if admission has closed.
    pub fn storage_telemetry(&self) -> Result<Vec<ShardStorageTelemetry>, RuntimeError> {
        let mut replies = Vec::with_capacity(self.shared.ingresses.len());
        for (shard, ingress) in self.shared.ingresses.iter().enumerate() {
            let (reply, receiver) = mpsc::sync_channel(1);
            let _guard = lock_ingress(ingress);
            if !self.shared.accepting.load(Ordering::Acquire) {
                return Err(RuntimeError::Shutdown);
            }
            try_send(&ingress.sender, Request::StorageTelemetry { reply })?;
            replies.push((shard, receiver));
        }
        replies
            .into_iter()
            .map(|(shard, receiver)| {
                receiver
                    .recv()
                    .map(|storage| ShardStorageTelemetry { shard, storage })
                    .map_err(|_| RuntimeError::Shutdown)
            })
            .collect()
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
        let mut shards: Vec<Vec<(usize, BatchCommand)>> = (0..self.shared.ingresses.len())
            .map(|_| Vec::new())
            .collect();
        for (index, command) in commands.into_iter().enumerate() {
            shards[self.shard_for(command.key())].push((index, command));
        }

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
            let ingress = &self.shared.ingresses[shard];
            let _guard = lock_ingress(ingress);
            let submission = if self.shared.accepting.load(Ordering::Acquire) {
                ingress.sender.try_send(Request::Batch { commands, reply })
            } else {
                for index in indexes {
                    results[index] = Some(Err(RuntimeError::Shutdown));
                }
                continue;
            };
            match submission {
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
        let mut replies = Vec::with_capacity(self.shared.ingresses.len());
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Shutdown);
        }
        for ingress in &self.shared.ingresses {
            let (reply, result) = mpsc::sync_channel(1);
            let _guard = lock_ingress(ingress);
            if !self.shared.accepting.load(Ordering::Acquire) {
                return Err(RuntimeError::Shutdown);
            }
            try_send(
                &ingress.sender,
                Request::Cleanup {
                    budget: budget_per_shard,
                    reply,
                },
            )?;
            replies.push(result);
        }
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
        let ingress = &self.shared.ingresses[self.shard_for(key)];
        let _guard = lock_ingress(ingress);
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Shutdown);
        }
        try_send(&ingress.sender, request)
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
        Self::open_with_compaction(config, BackgroundCompaction::Disabled)
    }

    /// Opens shard stores with an explicit idle-time compaction policy.
    ///
    /// # Errors
    /// Returns invalid configuration, shard storage, or worker-spawn failures.
    pub fn open_with_compaction(
        config: &RuntimeConfig,
        compaction: BackgroundCompaction,
    ) -> Result<Self, OpenError> {
        if config.shard_count == 0 || !config.shard_count.is_power_of_two() {
            return Err(OpenError::InvalidShardCount);
        }
        if config.queue_capacity == 0 {
            return Err(OpenError::InvalidQueueCapacity);
        }
        if !compaction.is_valid() {
            return Err(OpenError::InvalidCompactionPolicy);
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

        let mut ingresses = Vec::with_capacity(config.shard_count);
        let mut workers = Vec::with_capacity(config.shard_count);
        let runtime_epoch = NEXT_RUNTIME_EPOCH.fetch_add(1, Ordering::Relaxed);
        let drain_batches = matches!(config.durability, Durability::LocalDurable);
        for (shard, store) in stores.into_iter().enumerate() {
            let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
            let namespace = config.namespace.clone();
            let maintenance = Arc::new(MaintenanceCounters::default());
            let worker_maintenance = Arc::clone(&maintenance);
            let worker = thread::Builder::new()
                .name(format!("punah-shard-{shard:04}"))
                .spawn(move || {
                    run_worker(
                        store,
                        &namespace,
                        runtime_epoch,
                        &receiver,
                        drain_batches,
                        MaintenanceScheduler::new(compaction, worker_maintenance),
                    );
                })
                .map_err(OpenError::WorkerSpawn)?;
            ingresses.push(ShardIngress {
                sender,
                gate: Mutex::new(()),
                maintenance,
            });
            workers.push(worker);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                ingresses,
                accepting: AtomicBool::new(true),
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
        self.shared.accepting.store(false, Ordering::Release);
        for ingress in &self.shared.ingresses {
            let _guard = lock_ingress(ingress);
            let _ = ingress.sender.send(Request::Shutdown);
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn lock_ingress(ingress: &ShardIngress) -> MutexGuard<'_, ()> {
    ingress
        .gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    drain_batches: bool,
    mut maintenance: MaintenanceScheduler,
) {
    let mut leases = LeaseTable::default();
    let mut next_token = 1_u64;
    let mut deferred = None;
    loop {
        let request = if let Some(request) = deferred.take() {
            request
        } else if maintenance.has_active_compaction() {
            match receiver.try_recv() {
                Ok(request) => request,
                Err(TryRecvError::Empty) => {
                    maintenance.run_once(&mut store);
                    continue;
                }
                Err(TryRecvError::Disconnected) => break,
            }
        } else {
            let Ok(request) = receiver.recv() else {
                break;
            };
            request
        };
        let mut completed_mutations = 0_u64;
        match request {
            Request::Batch { commands, reply } => {
                let (mut mutation_count, mut mutation_bytes) = batch_weight(&commands);
                let mut batches = vec![QueuedBatch { commands, reply }];
                while drain_batches && batches.len() < MAX_DRAINED_BATCH_MESSAGES {
                    match receiver.try_recv() {
                        Ok(Request::Batch { commands, reply }) => {
                            let (next_count, next_bytes) = batch_weight(&commands);
                            let exceeds_limit = mutation_count > 0
                                && (mutation_count.saturating_add(next_count)
                                    > MAX_DRAINED_MUTATIONS
                                    || mutation_bytes.saturating_add(next_bytes)
                                        > MAX_DRAINED_MUTATION_BYTES);
                            if exceeds_limit {
                                deferred = Some(Request::Batch { commands, reply });
                                break;
                            }
                            mutation_count = mutation_count.saturating_add(next_count);
                            mutation_bytes = mutation_bytes.saturating_add(next_bytes);
                            batches.push(QueuedBatch { commands, reply });
                        }
                        Ok(request) => {
                            deferred = Some(request);
                            break;
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
                execute_batch_requests(&mut store, batches);
                completed_mutations = u64::try_from(mutation_count).unwrap_or(u64::MAX);
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
                completed_mutations = 1;
            }
            Request::Delete { key, reply } => {
                let _ = reply.send(store.delete(&key).map_err(RuntimeError::Store));
                completed_mutations = 1;
            }
            Request::Cleanup { budget, reply } => {
                let result = store.cleanup_expired(budget).map_err(RuntimeError::Store);
                if let Ok(report) = &result {
                    completed_mutations = u64::try_from(report.tombstoned).unwrap_or(u64::MAX);
                }
                let _ = reply.send(result);
            }
            Request::StorageTelemetry { reply } => {
                let _ = reply.send(store.io_telemetry());
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
                if result.is_ok() {
                    completed_mutations = 1;
                }
                let _ = reply.send(result);
            }
            Request::Shutdown => break,
            #[cfg(test)]
            Request::Barrier { entered, release } => {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        maintenance.record_mutations(completed_mutations);
        if maintenance.is_due() && deferred.is_none() {
            match receiver.try_recv() {
                Ok(request) => deferred = Some(request),
                Err(TryRecvError::Empty) => maintenance.run_once(&mut store),
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

fn batch_weight(commands: &[(usize, BatchCommand)]) -> (usize, usize) {
    commands
        .iter()
        .fold((0_usize, 0_usize), |(count, bytes), (_, command)| {
            let mutation_bytes = match command {
                BatchCommand::Get { .. } => return (count, bytes),
                BatchCommand::Set { key, value, .. } => key
                    .len()
                    .saturating_add(value.len())
                    .saturating_add(MUTATION_RECORD_OVERHEAD_ESTIMATE),
                BatchCommand::Delete { key } => {
                    key.len().saturating_add(MUTATION_RECORD_OVERHEAD_ESTIMATE)
                }
            };
            (
                count.saturating_add(1),
                bytes.saturating_add(mutation_bytes),
            )
        })
}

fn execute_batch_requests(store: &mut Store, batches: Vec<QueuedBatch>) {
    let mut replies = Vec::with_capacity(batches.len());
    let mut results: Vec<Vec<(usize, Result<BatchValue, RuntimeError>)>> = batches
        .iter()
        .map(|batch| Vec::with_capacity(batch.commands.len()))
        .collect();
    let mut mutations = Vec::new();
    for (batch_slot, batch) in batches.into_iter().enumerate() {
        replies.push(batch.reply);
        for (index, command) in batch.commands {
            match command {
                BatchCommand::Get { key } => {
                    execute_mutation_run(store, &mut mutations, &mut results);
                    results[batch_slot].push((
                        index,
                        store
                            .get(&key)
                            .map(BatchValue::Get)
                            .map_err(RuntimeError::Store),
                    ));
                }
                BatchCommand::Set { key, value, ttl } => queue_mutation(
                    store,
                    &mut mutations,
                    &mut results,
                    batch_slot,
                    index,
                    StoreMutation::Set {
                        key,
                        payload: value,
                        ttl,
                    },
                ),
                BatchCommand::Delete { key } => {
                    queue_mutation(
                        store,
                        &mut mutations,
                        &mut results,
                        batch_slot,
                        index,
                        StoreMutation::Delete { key },
                    );
                }
            }
        }
    }
    execute_mutation_run(store, &mut mutations, &mut results);
    for (reply, results) in replies.into_iter().zip(results) {
        let _ = reply.send(results);
    }
}

fn queue_mutation(
    store: &mut Store,
    mutations: &mut Vec<(usize, usize, StoreMutation)>,
    results: &mut [Vec<(usize, Result<BatchValue, RuntimeError>)>],
    batch_slot: usize,
    index: usize,
    mutation: StoreMutation,
) {
    match store.validate_mutation(&mutation) {
        Ok(()) => mutations.push((batch_slot, index, mutation)),
        Err(error) => {
            execute_mutation_run(store, mutations, results);
            results[batch_slot].push((index, Err(RuntimeError::Store(error))));
        }
    }
}

fn execute_mutation_run(
    store: &mut Store,
    mutations: &mut Vec<(usize, usize, StoreMutation)>,
    results: &mut [Vec<(usize, Result<BatchValue, RuntimeError>)>],
) {
    if mutations.is_empty() {
        return;
    }
    let run = std::mem::take(mutations);
    let (locations, mutations): (Vec<_>, Vec<_>) = run
        .into_iter()
        .map(|(batch_slot, index, mutation)| ((batch_slot, index), mutation))
        .unzip();
    match store.mutate_batch(mutations) {
        Ok(values) => {
            debug_assert_eq!(locations.len(), values.len());
            for ((batch_slot, index), value) in locations.into_iter().zip(values) {
                let value = match value {
                    MutationValue::Set => BatchValue::Set,
                    MutationValue::Delete(existed) => BatchValue::Delete(existed),
                };
                results[batch_slot].push((index, Ok(value)));
            }
        }
        Err(error) => {
            let mut cause = Some(error);
            for (batch_slot, index) in locations {
                let error = cause.take().unwrap_or(StoreError::BatchAborted);
                results[batch_slot].push((index, Err(RuntimeError::Store(error))));
            }
        }
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
