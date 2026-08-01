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
fn bounded_queue_reports_backpressure() {
    let cfg = config("pressure", 1, 1);
    let runtime = Runtime::open(&cfg).unwrap();
    let handle = runtime.handle();
    let (release, blocked) = mpsc::sync_channel(0);
    let (entered, started) = mpsc::sync_channel(0);
    handle.shared.senders[0]
        .send(Request::Barrier {
            entered,
            release: blocked,
        })
        .unwrap();
    started.recv().unwrap();
    let (reply, _ignored) = mpsc::sync_channel(1);
    handle.shared.senders[0]
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
