use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use punah_types::{Namespace, OperationHash};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct LeaseToken(pub [u8; 16]);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct LeaseKey {
    namespace: Namespace,
    operation_hash: OperationHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub token: LeaseToken,
    pub expires_at: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquireResult {
    Acquired(Lease),
    HeldUntil(SystemTime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseError {
    ZeroDuration,
    NotOwner,
    Expired,
}

/// Reference implementation of operation-level request coalescing.
///
/// The production data plane will shard this table by operation hash and keep
/// each shard on its owning worker. This implementation prioritizes semantics.
#[derive(Default)]
pub struct LeaseTable {
    leases: HashMap<LeaseKey, Lease>,
}

impl LeaseTable {
    /// Verifies that `token` owns a lease which is live at `now`.
    ///
    /// This is the fencing check used immediately before publishing a result.
    /// It intentionally does not mutate or extend the lease.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError::NotOwner`] for a missing or mismatched token and
    /// [`LeaseError::Expired`] when the matching lease is no longer live.
    pub fn authorize(
        &self,
        namespace: &Namespace,
        operation_hash: &OperationHash,
        token: LeaseToken,
        now: SystemTime,
    ) -> Result<(), LeaseError> {
        let key = LeaseKey {
            namespace: namespace.clone(),
            operation_hash: *operation_hash,
        };
        let Some(lease) = self.leases.get(&key) else {
            return Err(LeaseError::NotOwner);
        };
        if lease.token != token {
            return Err(LeaseError::NotOwner);
        }
        if lease.expires_at <= now {
            return Err(LeaseError::Expired);
        }
        Ok(())
    }

    /// Acquires an operation lease or reports the current holder's deadline.
    /// Expired leases are replaced atomically by the caller-provided token.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError::ZeroDuration`] when the requested duration is zero.
    pub fn try_acquire(
        &mut self,
        namespace: Namespace,
        operation_hash: OperationHash,
        token: LeaseToken,
        now: SystemTime,
        duration: Duration,
    ) -> Result<AcquireResult, LeaseError> {
        if duration.is_zero() {
            return Err(LeaseError::ZeroDuration);
        }
        let key = LeaseKey {
            namespace,
            operation_hash,
        };
        if let Some(lease) = self.leases.get(&key)
            && lease.expires_at > now
        {
            return Ok(AcquireResult::HeldUntil(lease.expires_at));
        }
        let lease = Lease {
            token,
            expires_at: now + duration,
        };
        self.leases.insert(key, lease.clone());
        Ok(AcquireResult::Acquired(lease))
    }

    /// Extends a live lease held by `token`.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError::NotOwner`] for a missing or mismatched token,
    /// [`LeaseError::Expired`] after its deadline, and
    /// [`LeaseError::ZeroDuration`] for a zero extension.
    pub fn renew(
        &mut self,
        namespace: &Namespace,
        operation_hash: &OperationHash,
        token: LeaseToken,
        now: SystemTime,
        duration: Duration,
    ) -> Result<SystemTime, LeaseError> {
        if duration.is_zero() {
            return Err(LeaseError::ZeroDuration);
        }
        let key = LeaseKey {
            namespace: namespace.clone(),
            operation_hash: *operation_hash,
        };
        let Some(lease) = self.leases.get_mut(&key) else {
            return Err(LeaseError::NotOwner);
        };
        if lease.token != token {
            return Err(LeaseError::NotOwner);
        }
        if lease.expires_at <= now {
            return Err(LeaseError::Expired);
        }
        lease.expires_at = now + duration;
        Ok(lease.expires_at)
    }

    /// Releases a lease after the owner publishes a result or abandons work.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError::NotOwner`] when `token` does not own the lease.
    pub fn release(
        &mut self,
        namespace: &Namespace,
        operation_hash: &OperationHash,
        token: LeaseToken,
    ) -> Result<(), LeaseError> {
        let key = LeaseKey {
            namespace: namespace.clone(),
            operation_hash: *operation_hash,
        };
        if !self
            .leases
            .get(&key)
            .is_some_and(|lease| lease.token == token)
        {
            return Err(LeaseError::NotOwner);
        }
        self.leases.remove(&key);
        Ok(())
    }

    pub fn remove_expired(&mut self, now: SystemTime) -> usize {
        let before = self.leases.len();
        self.leases.retain(|_, lease| lease.expires_at > now);
        before - self.leases.len()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.leases.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> Namespace {
        Namespace::new("swarm").expect("namespace")
    }

    #[test]
    fn concurrent_miss_is_coalesced_until_expiry() {
        let now = SystemTime::UNIX_EPOCH;
        let duration = Duration::from_secs(5);
        let operation = [7; 32];
        let first = LeaseToken([1; 16]);
        let second = LeaseToken([2; 16]);
        let mut leases = LeaseTable::default();

        assert!(matches!(
            leases.try_acquire(namespace(), operation, first, now, duration),
            Ok(AcquireResult::Acquired(Lease { token, .. })) if token == first
        ));
        assert_eq!(
            leases.try_acquire(namespace(), operation, second, now, duration),
            Ok(AcquireResult::HeldUntil(now + duration))
        );
        assert!(matches!(
            leases.try_acquire(
                namespace(),
                operation,
                second,
                now + duration,
                duration,
            ),
            Ok(AcquireResult::Acquired(Lease { token, .. })) if token == second
        ));
    }

    #[test]
    fn old_owner_cannot_release_reacquired_lease() {
        let now = SystemTime::UNIX_EPOCH;
        let duration = Duration::from_secs(1);
        let operation = [7; 32];
        let first = LeaseToken([1; 16]);
        let second = LeaseToken([2; 16]);
        let mut leases = LeaseTable::default();
        leases
            .try_acquire(namespace(), operation, first, now, duration)
            .expect("first acquire");
        leases
            .try_acquire(namespace(), operation, second, now + duration, duration)
            .expect("second acquire");

        assert_eq!(
            leases.release(&namespace(), &operation, first),
            Err(LeaseError::NotOwner)
        );
        assert_eq!(leases.len(), 1);
    }
}
