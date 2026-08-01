#![forbid(unsafe_code)]

//! Deterministic model of swarm-wide exact-result reuse.
//!
//! Requests arrive in synchronized waves. This intentionally models the worst
//! cold-miss race: results produced in a wave become visible only at the end of
//! that wave. There is no wall-clock timing and therefore no synthetic
//! throughput claim.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use punah_engine::{AcquireResult, LeaseError, LeaseTable, LeaseToken};
use punah_types::{Namespace, NamespaceError, OperationId};

pub mod cluster;

/// Reuse policy applied to the same deterministic request stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Policy {
    /// Every request computes independently.
    NoReuse,
    /// Completed results are reused, but simultaneous cold misses race.
    ResultReuse,
    /// Completed results are reused and cold misses are coalesced by leases.
    FencedLeases,
}

/// Shape of one deterministic simulation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    pub agents: usize,
    pub unique_operations: usize,
    pub waves: usize,
    /// Also execute an expiry/reacquisition probe against the lease table.
    pub exercise_stale_fence: bool,
}

/// Counts produced by a simulation. None are elapsed-time measurements.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Counters {
    pub requests: u64,
    pub hits: u64,
    pub attempted_computations: u64,
    pub actual_computations: u64,
    pub held_leases: u64,
    pub stale_publish_rejects: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimError {
    ZeroAgents,
    ZeroOperations,
    ZeroWaves,
    Namespace(NamespaceError),
    Lease(LeaseError),
}

impl From<NamespaceError> for SimError {
    fn from(value: NamespaceError) -> Self {
        Self::Namespace(value)
    }
}

impl From<LeaseError> for SimError {
    fn from(value: LeaseError) -> Self {
        Self::Lease(value)
    }
}

/// Runs one policy over a stable request stream.
///
/// Operation identities are distinct full-width [`OperationId`] values. The
/// index encoding is deliberately collision-free rather than probabilistic.
///
/// # Errors
/// Returns an error for an empty workload or an unexpected lease-table error.
pub fn simulate(config: Config, policy: Policy) -> Result<Counters, SimError> {
    validate(config)?;
    let namespace = Namespace::new(b"punah-sim".to_vec())?;
    let operations: Vec<_> = (0..config.unique_operations).map(operation_id).collect();
    let mut cache = HashSet::new();
    let mut leases = LeaseTable::default();
    let mut next_token = 1_u64;
    let mut counters = Counters::default();

    for wave in 0..config.waves {
        let now = tick(wave as u64);
        let mut completed = Vec::new();
        let mut owners = Vec::new();
        for agent in 0..config.agents {
            counters.requests += 1;
            let operation = operations[(wave * config.agents + agent) % operations.len()];
            if policy != Policy::NoReuse && cache.contains(&operation) {
                counters.hits += 1;
                continue;
            }

            match policy {
                Policy::NoReuse => {
                    counters.attempted_computations += 1;
                    counters.actual_computations += 1;
                }
                Policy::ResultReuse => {
                    counters.attempted_computations += 1;
                    counters.actual_computations += 1;
                    completed.push(operation);
                }
                Policy::FencedLeases => {
                    let hash = *operation.as_bytes();
                    let token = token(next_token);
                    next_token += 1;
                    match leases.try_acquire(
                        namespace.clone(),
                        hash,
                        token,
                        now,
                        Duration::from_secs(10),
                    )? {
                        AcquireResult::Acquired(_) => {
                            counters.attempted_computations += 1;
                            counters.actual_computations += 1;
                            owners.push((operation, token));
                        }
                        AcquireResult::HeldUntil(_) => counters.held_leases += 1,
                    }
                }
            }
        }

        for operation in completed {
            cache.insert(operation);
        }
        for (operation, token) in owners {
            let hash = *operation.as_bytes();
            leases.authorize(&namespace, &hash, token, now)?;
            cache.insert(operation);
            leases.release(&namespace, &hash, token)?;
        }
    }

    if policy == Policy::FencedLeases && config.exercise_stale_fence {
        counters.stale_publish_rejects +=
            stale_fence_probe(&mut leases, &namespace, &mut next_token)?;
    }
    Ok(counters)
}

fn stale_fence_probe(
    leases: &mut LeaseTable,
    namespace: &Namespace,
    next_token: &mut u64,
) -> Result<u64, LeaseError> {
    let operation = *operation_id(usize::MAX).as_bytes();
    let old = token(*next_token);
    *next_token += 1;
    leases.try_acquire(
        namespace.clone(),
        operation,
        old,
        tick(100),
        Duration::from_secs(1),
    )?;
    let replacement = token(*next_token);
    *next_token += 1;
    leases.try_acquire(
        namespace.clone(),
        operation,
        replacement,
        tick(101),
        Duration::from_secs(10),
    )?;
    let rejected = u64::from(matches!(
        leases.authorize(namespace, &operation, old, tick(101)),
        Err(LeaseError::NotOwner)
    ));
    leases.authorize(namespace, &operation, replacement, tick(101))?;
    leases.release(namespace, &operation, replacement)?;
    Ok(rejected)
}

fn validate(config: Config) -> Result<(), SimError> {
    if config.agents == 0 {
        return Err(SimError::ZeroAgents);
    }
    if config.unique_operations == 0 {
        return Err(SimError::ZeroOperations);
    }
    if config.waves == 0 {
        return Err(SimError::ZeroWaves);
    }
    Ok(())
}

pub(crate) fn operation_id(index: usize) -> OperationId {
    let mut bytes = [0_u8; 32];
    bytes[24..].copy_from_slice(&(index as u64).to_be_bytes());
    OperationId::new(bytes)
}

fn token(sequence: u64) -> LeaseToken {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    LeaseToken(bytes)
}

fn tick(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(agents: usize, operations: usize, waves: usize) -> Config {
        Config {
            agents,
            unique_operations: operations,
            waves,
            exercise_stale_fence: true,
        }
    }

    #[test]
    fn one_hot_operation_exposes_cold_wave_duplication() {
        let no_reuse = simulate(config(8, 1, 3), Policy::NoReuse).unwrap();
        let reuse = simulate(config(8, 1, 3), Policy::ResultReuse).unwrap();
        let leases = simulate(config(8, 1, 3), Policy::FencedLeases).unwrap();

        assert_eq!(no_reuse.actual_computations, 24);
        assert_eq!(reuse.actual_computations, 8);
        assert_eq!(reuse.hits, 16);
        assert_eq!(leases.actual_computations, 1);
        assert_eq!(leases.held_leases, 7);
        assert_eq!(leases.hits, 16);
        assert_eq!(leases.stale_publish_rejects, 1);
    }

    #[test]
    fn exact_id_distribution_is_deterministic_across_waves() {
        let result = simulate(config(4, 6, 3), Policy::FencedLeases).unwrap();
        assert_eq!(result.requests, 12);
        assert_eq!(result.actual_computations, 6);
        assert_eq!(result.hits, 6);
        assert_eq!(result.held_leases, 0);
    }

    #[test]
    fn lease_savings_scale_with_simultaneous_agents() {
        for agents in [1, 2, 4, 8, 16, 32] {
            let result = simulate(config(agents, 1, 1), Policy::FencedLeases).unwrap();
            assert_eq!(result.actual_computations, 1);
            assert_eq!(result.held_leases, (agents - 1) as u64);
        }
    }

    #[test]
    fn rejects_empty_workloads() {
        assert_eq!(
            simulate(config(0, 1, 1), Policy::NoReuse),
            Err(SimError::ZeroAgents)
        );
        assert_eq!(
            simulate(config(1, 0, 1), Policy::NoReuse),
            Err(SimError::ZeroOperations)
        );
        assert_eq!(
            simulate(config(1, 1, 0), Policy::NoReuse),
            Err(SimError::ZeroWaves)
        );
    }
}
