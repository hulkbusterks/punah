# Cluster membership authority

`punah-cluster` defines the transport-independent authority vocabulary used by
routers, nodes, simulations, and future replication code. It deliberately does
not implement failure detection, gossip, consensus, or networking.

## Immutable membership epochs

A membership snapshot contains:

- a 128-bit `ClusterId`, preventing accidental cross-cluster authority;
- a nonzero, monotonically increasing `MembershipEpoch`;
- a sorted, unique, nonempty set of stable `NodeId` values.

The control plane supplies and authenticates snapshots. Nodes may gossip health
observations, but an observation cannot add or remove an authoritative owner.
A node installs only a strictly newer snapshot for the same cluster; skipped
epochs are valid because intermediate views may be missed.

Requests carrying the exact installed cluster and epoch may exercise local
authority. Stale, future, and foreign-cluster requests are distinct outcomes:

- stale tells the caller to refresh its membership view;
- future tells the receiving node that it is behind and must refresh;
- wrong-cluster is never redirected as if it belonged locally.

## Ownership

Membership snapshots use rendezvous hashing over exact routing-key bytes and a
stable node identity. Input order cannot affect ownership. Removing a node
moves only keys owned by that node; adding one moves only keys won by it.

Routing hashes choose where to ask. They are not artifact identities, durable
checksums, or proof of authority. The receiving node still checks the exact
cluster and epoch.

## Critical partition limitation

An epoch comparison is local knowledge, not consensus. After a partition, a
node that has not learned epoch 8 may still accept a request carrying its local
epoch 7. Consequently, membership epochs alone do **not** prevent two sides of
a partition from behaving as owners.

Before replicated publication is implemented, Punah still needs one of these
external fencing foundations:

- a consensus-backed control-plane lease/term;
- a quorum rule over authoritative replicas;
- or another monotonic token validated at the durable publication boundary.

Gossip and wall-clock expiry are useful for liveness but cannot provide that
safety property. The next partition simulator will compare the alternatives
before one is selected.
