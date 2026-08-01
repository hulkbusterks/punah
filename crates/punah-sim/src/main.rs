#![forbid(unsafe_code)]

use punah_sim::cluster::{ClusterConfig, NodeId, ownership_changes, simulate_cluster};
use punah_sim::{Config, Policy, simulate};
use punah_types::OperationId;

fn main() {
    println!("agents,policy,requests,hits,attempted,actual,held,stale_rejects");
    for agents in [1, 2, 4, 8, 16, 32, 64] {
        for policy in [Policy::NoReuse, Policy::ResultReuse, Policy::FencedLeases] {
            let counters = simulate(
                Config {
                    agents,
                    unique_operations: 1,
                    waves: 4,
                    exercise_stale_fence: true,
                },
                policy,
            )
            .expect("static simulation configuration is valid");
            println!(
                "{agents},{policy:?},{},{},{},{},{},{}",
                counters.requests,
                counters.hits,
                counters.attempted_computations,
                counters.actual_computations,
                counters.held_leases,
                counters.stale_publish_rejects
            );
        }
    }

    println!();
    println!("nodes,operations,failed_node,moved,moved_percent,computations,hits,coalesced");
    for nodes in [2_usize, 4, 8, 16] {
        let operations: Vec<_> = (0..10_000).map(operation_id).collect();
        let before: Vec<_> = (0..nodes)
            .map(|node| NodeId::new(u64::try_from(node).expect("static node count fits u64")))
            .collect();
        let after: Vec<_> = before
            .iter()
            .copied()
            .filter(|node| *node != NodeId::new(0))
            .collect();
        let moved = ownership_changes(&operations, &before, &after);
        let counters = simulate_cluster(ClusterConfig {
            nodes,
            agents: 1_024,
            unique_operations: 256,
            waves: 4,
            failed_node: NodeId::new(0),
            fail_at_wave: 1,
            recover_at_wave: Some(3),
        })
        .expect("static cluster simulation configuration is valid");
        let moved_basis_points = moved * 10_000 / operations.len();
        println!(
            "{nodes},{},{},{moved},{}.{:02},{},{},{}",
            operations.len(),
            0,
            moved_basis_points / 100,
            moved_basis_points % 100,
            counters.actual_computations,
            counters.cache_hits,
            counters.lease_coalesced,
        );
    }
}

fn operation_id(index: u64) -> OperationId {
    let mut bytes = [0_u8; 32];
    bytes[24..].copy_from_slice(&index.to_be_bytes());
    OperationId::new(bytes)
}
