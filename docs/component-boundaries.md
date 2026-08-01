# Component and policy boundaries

Punah is knowledgeable about the identity, location, and lifecycle of cached
data. It is not the source of truth for an agent workflow.

## Ownership

| Component | Owns | Does not own |
|---|---|---|
| `punah-node` | Artifact bytes, exact operation results, durability, leases, fencing, expiration, compaction, local shard ownership | Agent plans, model selection, semantic equivalence, global compute scheduling |
| `punah-router` | Replaceable locality and load-based request placement | Storage truth or workflow truth |
| Cluster control plane | Authenticated membership epochs and node admission/removal | Agent workflow or artifact identity |
| `punah-advisor` | Optional predictive prefetch/retention suggestions | Correctness; its absence or mistakes may only affect performance |
| Agent orchestrator | Workflow, dependency readiness, priorities, cancellation, compute assignment | Punah's durability and record authority |

Only the node belongs to the correctness-critical storage core. Router and
advisor components consume immutable snapshots or lifecycle events and issue
explicit commands through public APIs.

## Mechanism versus policy

The core provides mechanisms:

- exact identity and claimed-identity verification;
- artifact/result storage and lookup;
- computation leases with fencing tokens;
- location queries and lifecycle events;
- explicit retain, prefetch, replicate, and evict commands when implemented;
- bounded queues, durability, recovery, and compaction.

Replaceable consumers provide policy:

- which operation should execute;
- which worker should compute it;
- whether a predicted future artifact merits prefetch;
- business priority and deadlines;
- model/provider selection;
- semantic or approximate reuse.

Punah remains correct when no policy component exists, when hints are stale, or
when a consumer disconnects. A policy decision cannot redefine artifact or
operation identity and cannot bypass fencing or durability.

## Event contract

Lifecycle events should describe facts already made authoritative:

```text
ArtifactStored(id, namespace, location, sequence)
ArtifactRemoved(id, namespace, sequence, reason)
ArtifactRelocated(id, old_location, new_location, sequence)
LeaseGranted(operation_id, fencing_token, expires_at)
LeaseReleased(operation_id, fencing_token)
```

Events are observations, not a second source of truth. Consumers must tolerate
duplicates, gaps, reordering, and reconnect by rebuilding from a snapshot.
Event publication must not delay a locally durable write acknowledgement unless
the caller explicitly requests a stronger external guarantee.

The implemented bounded stream, lag contract, and snapshot recovery procedure
are specified in [lifecycle-events.md](lifecycle-events.md).

## Transport independence

TCP, QUIC, local channels, and future RDMA transports invoke the same service
semantics. QUIC can multiplex control, lease, and artifact streams but does not
change which component owns a decision.

The implemented membership vocabulary and its explicit partition limitation
are specified in [cluster-authority.md](cluster-authority.md).
