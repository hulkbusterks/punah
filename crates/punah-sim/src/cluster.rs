//! Deterministic multi-node ownership and failover model.

use std::collections::{HashMap, HashSet};

pub use punah_cluster::NodeId;
use punah_cluster::owner_for_nodes;
use punah_types::OperationId;

use crate::operation_id;

/// A synchronized cluster workload with one optional failure and recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterConfig {
    pub nodes: usize,
    pub agents: usize,
    pub unique_operations: usize,
    pub waves: usize,
    pub failed_node: NodeId,
    /// The failed node leaves before this wave is served.
    pub fail_at_wave: usize,
    /// The node rejoins before this wave is served. `None` leaves it down.
    pub recover_at_wave: Option<usize>,
}

/// Algorithmic counts; none represent elapsed time or throughput.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClusterCounters {
    pub requests: u64,
    pub cache_hits: u64,
    pub actual_computations: u64,
    pub lease_coalesced: u64,
    pub ownership_changes: u64,
    pub unavailable_requests: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterError {
    FewerThanTwoNodes,
    ZeroAgents,
    ZeroOperations,
    ZeroWaves,
    UnknownFailedNode,
    FailureOutsideWorkload,
    RecoveryNotAfterFailure,
}

/// Chooses the highest-scoring live node for an exact operation.
///
/// Node IDs break the vanishingly unlikely equal-score case, so the result is
/// independent of membership iteration order.
#[must_use]
pub fn rendezvous_owner(operation: OperationId, live_nodes: &[NodeId]) -> Option<NodeId> {
    owner_for_nodes(operation.as_bytes(), live_nodes)
}

/// Counts exact operations whose owner changes between two membership views.
#[must_use]
pub fn ownership_changes(operations: &[OperationId], before: &[NodeId], after: &[NodeId]) -> usize {
    operations
        .iter()
        .filter(|operation| {
            rendezvous_owner(**operation, before) != rendezvous_owner(**operation, after)
        })
        .count()
}

/// Simulates replication-free node failure with lease coalescing at each owner.
///
/// A node's local cached results survive a temporary outage, but are not copied
/// during failover. Operations reassigned while it is down therefore compute
/// once at their new owner. This is intentionally a baseline, not a proposed
/// availability guarantee.
///
/// # Errors
/// Returns an error for an invalid or ambiguous scenario.
pub fn simulate_cluster(config: ClusterConfig) -> Result<ClusterCounters, ClusterError> {
    validate(config)?;
    let all_nodes: Vec<_> = (0..config.nodes as u64).map(NodeId::new).collect();
    let operations: Vec<_> = (0..config.unique_operations).map(operation_id).collect();
    let mut live = all_nodes.clone();
    let mut caches: HashMap<NodeId, HashSet<OperationId>> = all_nodes
        .iter()
        .copied()
        .map(|node| (node, HashSet::new()))
        .collect();
    let mut previous_owners: Vec<_> = operations
        .iter()
        .map(|operation| rendezvous_owner(*operation, &live))
        .collect();
    let mut counters = ClusterCounters::default();

    for wave in 0..config.waves {
        if wave == config.fail_at_wave {
            live.retain(|node| *node != config.failed_node);
        }
        if config.recover_at_wave == Some(wave) {
            live.push(config.failed_node);
            live.sort_unstable();
        }

        let owners: Vec<_> = operations
            .iter()
            .map(|operation| rendezvous_owner(*operation, &live))
            .collect();
        counters.ownership_changes += owners
            .iter()
            .zip(&previous_owners)
            .filter(|(current, previous)| current != previous)
            .count() as u64;
        previous_owners = owners;

        let mut cold_requests: HashMap<(NodeId, OperationId), u64> = HashMap::new();
        for agent in 0..config.agents {
            counters.requests += 1;
            let operation_index = (wave * config.agents + agent) % operations.len();
            let operation = operations[operation_index];
            let Some(owner) = previous_owners[operation_index] else {
                counters.unavailable_requests += 1;
                continue;
            };
            if caches[&owner].contains(&operation) {
                counters.cache_hits += 1;
            } else {
                *cold_requests.entry((owner, operation)).or_default() += 1;
            }
        }
        for ((owner, operation), contenders) in cold_requests {
            counters.actual_computations += 1;
            counters.lease_coalesced += contenders.saturating_sub(1);
            caches.entry(owner).or_default().insert(operation);
        }
    }
    Ok(counters)
}

fn validate(config: ClusterConfig) -> Result<(), ClusterError> {
    if config.nodes < 2 {
        return Err(ClusterError::FewerThanTwoNodes);
    }
    if config.agents == 0 {
        return Err(ClusterError::ZeroAgents);
    }
    if config.unique_operations == 0 {
        return Err(ClusterError::ZeroOperations);
    }
    if config.waves == 0 {
        return Err(ClusterError::ZeroWaves);
    }
    if config.failed_node.get() >= config.nodes as u64 {
        return Err(ClusterError::UnknownFailedNode);
    }
    if config.fail_at_wave >= config.waves {
        return Err(ClusterError::FailureOutsideWorkload);
    }
    if config
        .recover_at_wave
        .is_some_and(|wave| wave <= config.fail_at_wave || wave >= config.waves)
    {
        return Err(ClusterError::RecoveryNotAfterFailure);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ClusterConfig {
        ClusterConfig {
            nodes: 4,
            agents: 64,
            unique_operations: 64,
            waves: 4,
            failed_node: NodeId::new(1),
            fail_at_wave: 1,
            recover_at_wave: Some(3),
        }
    }

    #[test]
    fn ownership_is_order_independent_and_complete() {
        let operation = operation_id(42);
        let forward = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
        ];
        let reverse = [
            NodeId::new(3),
            NodeId::new(2),
            NodeId::new(1),
            NodeId::new(0),
        ];
        assert_eq!(
            rendezvous_owner(operation, &forward),
            rendezvous_owner(operation, &reverse)
        );
        assert_eq!(rendezvous_owner(operation, &[]), None);
    }

    #[test]
    fn removing_one_node_moves_only_its_owned_operations() {
        let operations: Vec<_> = (0..10_000).map(operation_id).collect();
        let before = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
        ];
        let after = [NodeId::new(0), NodeId::new(2), NodeId::new(3)];
        let removed_owned = operations
            .iter()
            .filter(|operation| rendezvous_owner(**operation, &before) == Some(NodeId::new(1)))
            .count();
        assert_eq!(
            ownership_changes(&operations, &before, &after),
            removed_owned
        );
    }

    #[test]
    fn failure_recomputes_only_reassigned_cold_results() {
        let result = simulate_cluster(config()).unwrap();
        assert_eq!(result.requests, 256);
        assert_eq!(result.unavailable_requests, 0);
        assert!(result.ownership_changes > 0);
        assert!(result.actual_computations < result.requests);
    }

    #[test]
    fn invalid_scenarios_are_rejected() {
        assert_eq!(
            simulate_cluster(ClusterConfig {
                nodes: 1,
                ..config()
            }),
            Err(ClusterError::FewerThanTwoNodes)
        );
        assert_eq!(
            simulate_cluster(ClusterConfig {
                recover_at_wave: Some(1),
                ..config()
            }),
            Err(ClusterError::RecoveryNotAfterFailure)
        );
    }
}
