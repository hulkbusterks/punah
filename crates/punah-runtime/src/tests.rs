use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;

fn config(name: &str, shards: usize, queue: usize) -> RuntimeConfig {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    RuntimeConfig {
        directory: std::env::temp_dir().join(format!("punah-runtime-{name}-{nonce}")),
        namespace: Namespace::new(b"test".to_vec()).unwrap(),
        shard_count: shards,
        queue_capacity: queue,
        segment_capacity: 256 * 1024,
        durability: Durability::Buffered,
    }
}

#[test]
fn routing_is_stable_and_power_of_two_bounded() {
    let cfg = config("routing", 8, 4);
    let runtime = Runtime::open(&cfg).unwrap();
    let first = runtime.handle();
    let second = first.clone();
    for key in [b"alpha".as_slice(), b"beta", b"gamma", b"\0binary"] {
        assert_eq!(first.shard_for(key), second.shard_for(key));
        assert!(first.shard_for(key) < 8);
    }
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn idle_background_compaction_reclaims_superseded_segments_and_reports_pause() {
    let mut cfg = config("background-compaction", 1, 8);
    cfg.segment_capacity = 320;
    let runtime = Runtime::open_with_compaction(
        &cfg,
        BackgroundCompaction::bounded_with_policy(1, 320, 320, 0, 1, Duration::from_secs(1)),
    )
    .unwrap();
    let handle = runtime.handle();
    handle.set(b"key", b"old".to_vec(), None).unwrap();
    handle.set(b"key", b"new".to_vec(), None).unwrap();
    handle.set(b"filler", vec![7; 40], None).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let snapshot = loop {
        let snapshot = handle.maintenance_snapshots()[0];
        if snapshot.compactions > 0 {
            break snapshot;
        }
        assert!(std::time::Instant::now() < deadline, "compaction timed out");
        thread::yield_now();
    };
    assert!(snapshot.checks >= 2);
    assert_eq!(snapshot.compactions, 1);
    assert!(snapshot.input_bytes > 0);
    assert!(snapshot.reclaimable_bytes > 0);
    assert!(snapshot.copied_bytes > 0);
    assert_eq!(snapshot.max_input_bytes, snapshot.input_bytes);
    assert_eq!(snapshot.max_copied_bytes, snapshot.copied_bytes);
    assert!(snapshot.last_pause_ns > 0);
    assert!(snapshot.max_pause_ns >= snapshot.last_pause_ns);
    assert_eq!(snapshot.pause_budget_exceeded, 0);
    assert_eq!(handle.get(b"key").unwrap(), Some(b"new".to_vec()));

    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn invalid_background_compaction_policy_is_rejected_before_opening_shards() {
    let cfg = config("invalid-background-compaction", 1, 8);
    assert!(matches!(
        Runtime::open_with_compaction(
            &cfg,
            BackgroundCompaction::bounded(0, 320, 320, Duration::from_secs(1)),
        ),
        Err(OpenError::InvalidCompactionPolicy)
    ));
    assert!(!cfg.directory.exists());

    assert!(matches!(
        Runtime::open_with_compaction(
            &cfg,
            BackgroundCompaction::bounded_with_minimum_reclaim_ratio(
                1,
                320,
                320,
                10_001,
                Duration::from_secs(1),
            ),
        ),
        Err(OpenError::InvalidCompactionPolicy)
    ));
    assert!(!cfg.directory.exists());
}

#[test]
fn exact_binary_keys_remain_distinct() {
    let cfg = config("keys", 2, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    handle.set(b"a", b"one".to_vec(), None).unwrap();
    handle.set(b"a\0", b"two".to_vec(), None).unwrap();
    assert_eq!(handle.get(b"a").unwrap(), Some(b"one".to_vec()));
    assert_eq!(handle.get(b"a\0").unwrap(), Some(b"two".to_vec()));
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn batch_preserves_input_order_across_shards() {
    let cfg = config("batch", 4, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    let results = handle
        .execute_batch(vec![
            BatchCommand::Set {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
                ttl: None,
            },
            BatchCommand::Set {
                key: b"beta".to_vec(),
                value: b"two".to_vec(),
                ttl: None,
            },
            BatchCommand::Get {
                key: b"alpha".to_vec(),
            },
            BatchCommand::Delete {
                key: b"beta".to_vec(),
            },
        ])
        .unwrap();
    assert!(matches!(results[0], Ok(BatchValue::Set)));
    assert!(matches!(results[1], Ok(BatchValue::Set)));
    assert!(matches!(
        &results[2],
        Ok(BatchValue::Get(Some(value))) if value == b"one"
    ));
    assert!(matches!(results[3], Ok(BatchValue::Delete(true))));
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn storage_telemetry_reports_actual_runtime_cohort_and_flush_count() {
    let mut cfg = config("storage-telemetry", 1, 8);
    cfg.durability = Durability::LocalDurable;
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    handle
        .execute_batch(vec![
            BatchCommand::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
                ttl: None,
            },
            BatchCommand::Set {
                key: b"b".to_vec(),
                value: b"two".to_vec(),
                ttl: None,
            },
            BatchCommand::Delete { key: b"a".to_vec() },
        ])
        .unwrap();
    let telemetry = handle.storage_telemetry().unwrap();
    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry[0].storage.cohort_appends, 1);
    assert_eq!(telemetry[0].storage.appended_records, 3);
    assert_eq!(telemetry[0].storage.durability_flushes, 2);
    assert_eq!(telemetry[0].storage.max_records_per_append, 3);
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn drained_batches_share_mutation_runs_without_crossing_get_barriers() {
    let cfg = config("drained-batches", 1, 8);
    let mut store = Store::open(StoreConfig {
        directory: cfg.directory.clone(),
        namespace: cfg.namespace.clone(),
        segment_capacity: cfg.segment_capacity,
        durability: Durability::Buffered,
    })
    .unwrap();
    let (first_reply, first_result) = mpsc::sync_channel(1);
    let (second_reply, second_result) = mpsc::sync_channel(1);
    execute_batch_requests(
        &mut store,
        vec![
            QueuedBatch {
                commands: vec![(
                    0,
                    BatchCommand::Set {
                        key: b"key".to_vec(),
                        value: b"one".to_vec(),
                        ttl: None,
                    },
                )],
                reply: first_reply,
            },
            QueuedBatch {
                commands: vec![
                    (
                        0,
                        BatchCommand::Get {
                            key: b"key".to_vec(),
                        },
                    ),
                    (
                        1,
                        BatchCommand::Delete {
                            key: b"key".to_vec(),
                        },
                    ),
                    (
                        2,
                        BatchCommand::Set {
                            key: b"key".to_vec(),
                            value: b"two".to_vec(),
                            ttl: None,
                        },
                    ),
                    (
                        3,
                        BatchCommand::Get {
                            key: b"key".to_vec(),
                        },
                    ),
                ],
                reply: second_reply,
            },
        ],
    );

    assert!(matches!(
        first_result.recv().unwrap()[0].1,
        Ok(BatchValue::Set)
    ));
    let second = second_result.recv().unwrap();
    assert!(matches!(
        &second[0].1,
        Ok(BatchValue::Get(Some(value))) if value == b"one"
    ));
    assert!(matches!(second[1].1, Ok(BatchValue::Delete(true))));
    assert!(matches!(second[2].1, Ok(BatchValue::Set)));
    assert!(matches!(
        &second[3].1,
        Ok(BatchValue::Get(Some(value))) if value == b"two"
    ));
    drop(store);
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn malformed_drained_request_does_not_abort_an_unrelated_valid_request() {
    let cfg = config("drained-invalid", 1, 8);
    let mut store = Store::open(StoreConfig {
        directory: cfg.directory.clone(),
        namespace: cfg.namespace.clone(),
        segment_capacity: cfg.segment_capacity,
        durability: Durability::Buffered,
    })
    .unwrap();
    let (valid_reply, valid_result) = mpsc::sync_channel(1);
    let (invalid_reply, invalid_result) = mpsc::sync_channel(1);
    execute_batch_requests(
        &mut store,
        vec![
            QueuedBatch {
                commands: vec![(
                    0,
                    BatchCommand::Set {
                        key: b"valid".to_vec(),
                        value: b"value".to_vec(),
                        ttl: None,
                    },
                )],
                reply: valid_reply,
            },
            QueuedBatch {
                commands: vec![(
                    0,
                    BatchCommand::Set {
                        key: vec![0; usize::from(u16::MAX) + 1],
                        value: b"invalid".to_vec(),
                        ttl: None,
                    },
                )],
                reply: invalid_reply,
            },
        ],
    );

    assert!(matches!(
        valid_result.recv().unwrap()[0].1,
        Ok(BatchValue::Set)
    ));
    assert!(matches!(
        invalid_result.recv().unwrap()[0].1,
        Err(RuntimeError::Store(StoreError::Encode(_)))
    ));
    assert_eq!(store.get(b"valid").unwrap(), Some(b"value".to_vec()));
    drop(store);
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn concurrent_handles_share_one_node_without_store_locking() {
    let cfg = config("concurrent", 4, 64);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = Arc::new(runtime.handle());
    let threads: Vec<_> = (0..8)
        .map(|client| {
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                for item in 0..20 {
                    let key = format!("client-{client}-item-{item}");
                    handle
                        .set(key.as_bytes(), key.as_bytes().to_vec(), None)
                        .unwrap();
                    assert_eq!(handle.get(key.as_bytes()).unwrap(), Some(key.into_bytes()));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn one_shard_admission_gate_does_not_block_another_shard() {
    let cfg = config("independent-ingress", 2, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    let shard_one_key = (0_u64..10_000)
        .map(|value| value.to_le_bytes().to_vec())
        .find(|key| handle.shard_for(key) == 1)
        .expect("one key routes to shard one");
    let shard_zero_guard = lock_ingress(&handle.shared.ingresses[0]);
    let (completed, completion) = mpsc::sync_channel(1);
    let other_shard = handle.clone();
    let worker = thread::spawn(move || {
        completed.send(other_shard.get(&shard_one_key)).unwrap();
    });

    assert!(matches!(
        completion.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(None)
    ));
    drop(shard_zero_guard);
    worker.join().unwrap();
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn bounded_queue_reports_backpressure() {
    let cfg = config("pressure", 1, 1);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    let (release, blocked) = mpsc::sync_channel(0);
    let (entered, started) = mpsc::sync_channel(0);
    handle.shared.ingresses[0]
        .sender
        .send(Request::Barrier {
            entered,
            release: blocked,
        })
        .unwrap();
    started.recv().unwrap();
    let (reply, _ignored) = mpsc::sync_channel(1);
    handle.shared.ingresses[0]
        .sender
        .try_send(Request::Get {
            key: b"queued".to_vec(),
            reply,
        })
        .unwrap();
    assert!(matches!(
        handle.get(b"overflow"),
        Err(RuntimeError::Backpressure)
    ));
    release.send(()).unwrap();
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn restart_recovers_each_owned_shard_and_shutdown_rejects_handles() {
    let cfg = config("restart", 4, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let stale = runtime.handle();
    stale.set(b"persistent", b"value".to_vec(), None).unwrap();
    stale.set(b"deleted", b"gone".to_vec(), None).unwrap();
    assert!(stale.delete(b"deleted").unwrap());
    runtime.shutdown();
    assert!(matches!(
        stale.get(b"persistent"),
        Err(RuntimeError::Shutdown)
    ));

    let reopened = Runtime::open(&cfg).unwrap();
    let handle = reopened.handle();
    assert_eq!(handle.get(b"persistent").unwrap(), Some(b"value".to_vec()));
    assert_eq!(handle.get(b"deleted").unwrap(), None);
    reopened.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn cleanup_is_bounded_per_shard() {
    let cfg = config("cleanup", 2, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    handle
        .set(b"short", b"value".to_vec(), Some(Duration::ZERO))
        .unwrap();
    let report = handle.cleanup_expired(1).unwrap();
    assert_eq!(report.tombstoned, 1);
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

fn lease_policy(seconds: u64) -> LeasePolicy {
    LeasePolicy {
        duration: Duration::from_secs(seconds),
    }
}

#[test]
fn concurrent_acquires_coalesce_on_one_owner() {
    let cfg = config("lease-coalesce", 4, 64);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = Arc::new(runtime.handle());
    let operation = [42_u8; 32];
    let now = UNIX_EPOCH + Duration::from_secs(100);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                handle
                    .acquire_lease(operation, now, lease_policy(10))
                    .unwrap()
            })
        })
        .collect();
    let outcomes: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, LeaseAcquire::Granted { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, LeaseAcquire::Held { .. }))
            .count(),
        7
    );
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn expiry_reacquires_and_fences_stale_publisher() {
    let cfg = config("lease-fencing", 2, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    let operation = [7_u8; 32];
    let now = UNIX_EPOCH + Duration::from_secs(50);
    let LeaseAcquire::Granted { token: old, .. } = handle
        .acquire_lease(operation, now, lease_policy(2))
        .unwrap()
    else {
        panic!("first lease must be granted");
    };
    let LeaseAcquire::Granted { token: new, .. } = handle
        .acquire_lease(operation, now + Duration::from_secs(2), lease_policy(5))
        .unwrap()
    else {
        panic!("expired lease must be replaced");
    };
    assert_ne!(old, new);
    assert!(matches!(
        handle.publish_result(
            operation,
            old,
            now + Duration::from_secs(2),
            b"stale".to_vec(),
            None,
        ),
        Err(RuntimeError::Lease(LeaseError::NotOwner))
    ));
    handle
        .publish_result(
            operation,
            new,
            now + Duration::from_secs(3),
            b"fresh".to_vec(),
            None,
        )
        .unwrap();
    assert_eq!(
        handle.get_result(operation).unwrap(),
        Some(b"fresh".to_vec())
    );
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn renew_and_release_require_live_fenced_token() {
    let cfg = config("lease-renew", 1, 8);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    let operation = [9_u8; 32];
    let now = UNIX_EPOCH + Duration::from_secs(10);
    let LeaseAcquire::Granted { token, .. } = handle
        .acquire_lease(operation, now, lease_policy(2))
        .unwrap()
    else {
        panic!("lease must be granted");
    };
    let expiry = handle
        .renew_lease(
            operation,
            token,
            now + Duration::from_secs(1),
            lease_policy(4),
        )
        .unwrap();
    assert_eq!(expiry, now + Duration::from_secs(5));
    assert!(matches!(
        handle.release_lease(operation, LeaseToken([255; 16]), now),
        Err(RuntimeError::Lease(LeaseError::NotOwner))
    ));
    handle
        .release_lease(operation, token, now + Duration::from_secs(4))
        .unwrap();
    assert!(matches!(
        handle.release_lease(operation, token, now + Duration::from_secs(4)),
        Err(RuntimeError::Lease(LeaseError::NotOwner))
    ));
    runtime.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}

#[test]
fn leases_are_ephemeral_but_published_results_survive_restart() {
    let cfg = config("lease-restart", 2, 8);
    let operation = [3_u8; 32];
    let now = UNIX_EPOCH + Duration::from_secs(100);
    let runtime = Runtime::open(&cfg).unwrap();
    let stale_handle = runtime.handle();
    let LeaseAcquire::Granted { token, .. } = stale_handle
        .acquire_lease(operation, now, lease_policy(10))
        .unwrap()
    else {
        panic!("lease must be granted");
    };
    stale_handle
        .publish_result(operation, token, now, b"durable".to_vec(), None)
        .unwrap();
    runtime.shutdown();
    assert!(matches!(
        stale_handle.acquire_lease(operation, now, lease_policy(10)),
        Err(RuntimeError::Shutdown)
    ));

    let reopened = Runtime::open(&cfg).unwrap();
    assert_eq!(
        reopened.handle().get_result(operation).unwrap(),
        Some(b"durable".to_vec())
    );
    reopened.shutdown();
    fs::remove_dir_all(cfg.directory).unwrap();
}
