# Deterministic swarm reuse simulation

`punah-sim` compares three policies over exactly the same request stream:

- `NoReuse`: every agent computes every request.
- `ResultReuse`: agents reuse completed results, but synchronized cold misses all
  compute because none can see another result until the wave finishes.
- `FencedLeases`: agents reuse results and only the lease owner computes a cold
  operation; other agents observe a held lease.

The input is a number of agents, unique exact `OperationId` values, and
synchronized request waves. Operation selection is deterministic. Results
become visible between waves. The lease policy uses the same `LeaseTable` and
authorization rules as the shard runtime, including expiry, reacquisition, and
rejection of a stale fencing token.

This is an algorithmic scaling model, not a performance benchmark. It reports
counts, never invented elapsed time or throughput:

- requests and completed-result hits;
- attempted and actual computations;
- lease-held responses that suppress duplicate work;
- rejected stale publishes from the fencing probe.

Run the default one-operation contention curve with:

```console
cargo run --release -p punah-sim
```

For one cold operation requested by `A` simultaneous agents over `W` waves,
the expected work curves are:

| Policy | Actual computations |
| --- | ---: |
| No reuse | `A * W` |
| Result reuse only | `A` |
| Fenced leases | `1` |

That difference is the intended value of lease coalescing. Real deployment
latency and throughput still belong in `punah-bench`; they depend on transport,
storage, payload size, queueing, and hardware.

## Multi-node ownership and failure

The same executable also emits a replication-free cluster curve. Operations
are assigned to live nodes with rendezvous hashing over the full `OperationId`
and stable `NodeId`. Removing a node therefore moves only operations previously
owned by that node; it does not reshuffle unrelated ownership as modulo routing
would.

The scenario warms 256 operations through 1,024 simultaneous agents, removes
node zero before wave two, keeps it down for two waves, and restores it before
wave four. Each node retains its own cache while offline, but no result is
replicated during failover. A reassigned cold operation computes exactly once;
same-wave contenders are counted as lease-coalesced.

Reported cluster fields are:

- `moved`: exact ownership changes across 10,000 operation IDs;
- `moved_percent`: the resulting remapping fraction;
- `computations`, `hits`, and `coalesced`: work/reuse counts across the failure
  scenario.

For balanced rendezvous ownership, removing one of `N` nodes should move about
`1/N` of operations. This validates the qualitative scaling rule, not network
availability: the model assumes a single agreed membership view and has no
replication, quorum, split-brain, packet-loss, or timing claims yet.
