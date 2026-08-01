#![forbid(unsafe_code)]

//! Transport-independent cluster membership authority and ownership.
//!
//! Membership is an immutable, externally supplied epoch. Health observations
//! may motivate a new epoch, but cannot themselves grant ownership. Data-plane
//! requests are accepted only at the node's exact current epoch.

use punah_identity::shard_hash_parts;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClusterId([u8; 16]);

impl ClusterId {
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Stable identity of one node. It must not be reused for a replacement node.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(u64);

impl NodeId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonically increasing authority version within one cluster.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MembershipEpoch(u64);

impl MembershipEpoch {
    /// Creates a nonzero epoch.
    ///
    /// # Errors
    /// Returns [`MembershipError::ZeroEpoch`] for the reserved zero value.
    pub const fn new(value: u64) -> Result<Self, MembershipError> {
        if value == 0 {
            Err(MembershipError::ZeroEpoch)
        } else {
            Ok(Self(value))
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Validated immutable membership view. Nodes are stored sorted and unique.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipSnapshot {
    cluster_id: ClusterId,
    epoch: MembershipEpoch,
    nodes: Vec<NodeId>,
}

impl MembershipSnapshot {
    /// Validates and canonicalizes a membership view.
    ///
    /// # Errors
    /// Returns an error when no node exists or an identity appears twice.
    pub fn new(
        cluster_id: ClusterId,
        epoch: MembershipEpoch,
        mut nodes: Vec<NodeId>,
    ) -> Result<Self, MembershipError> {
        if nodes.is_empty() {
            return Err(MembershipError::EmptyMembership);
        }
        nodes.sort_unstable();
        if nodes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(MembershipError::DuplicateNode);
        }
        Ok(Self {
            cluster_id,
            epoch,
            nodes,
        })
    }

    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    #[must_use]
    pub const fn epoch(&self) -> MembershipEpoch {
        self.epoch
    }

    #[must_use]
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Selects the deterministic owner of exact routing-key bytes.
    #[must_use]
    pub fn owner(&self, routing_key: &[u8]) -> NodeId {
        owner_for_nodes(routing_key, &self.nodes).unwrap_or(self.nodes[0])
    }
}

/// Selects an owner from a caller-supplied live-node view.
///
/// Production authority should normally use [`MembershipSnapshot::owner`].
/// This form exists for deterministic transition analysis and returns `None`
/// for an empty view.
#[must_use]
pub fn owner_for_nodes(routing_key: &[u8], nodes: &[NodeId]) -> Option<NodeId> {
    nodes
        .iter()
        .copied()
        .max_by_key(|node| ownership_score(routing_key, *node))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipError {
    ZeroEpoch,
    EmptyMembership,
    DuplicateNode,
}

/// Why a request cannot exercise authority at this node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityError {
    WrongCluster,
    StaleEpoch { current: MembershipEpoch },
    FutureEpoch { current: MembershipEpoch },
}

/// Locally installed membership authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipState {
    current: MembershipSnapshot,
}

impl MembershipState {
    #[must_use]
    pub const fn new(initial: MembershipSnapshot) -> Self {
        Self { current: initial }
    }

    #[must_use]
    pub const fn current(&self) -> &MembershipSnapshot {
        &self.current
    }

    /// Accepts authority only for the exact installed cluster and epoch.
    ///
    /// # Errors
    /// Distinguishes wrong-cluster, stale, and not-yet-installed future views.
    pub fn authorize(
        &self,
        cluster_id: ClusterId,
        epoch: MembershipEpoch,
    ) -> Result<(), AuthorityError> {
        if cluster_id.0 != self.current.cluster_id.0 {
            return Err(AuthorityError::WrongCluster);
        }
        if epoch.0 < self.current.epoch.0 {
            return Err(AuthorityError::StaleEpoch {
                current: self.current.epoch,
            });
        }
        if epoch.0 > self.current.epoch.0 {
            return Err(AuthorityError::FutureEpoch {
                current: self.current.epoch,
            });
        }
        Ok(())
    }

    /// Installs a strictly newer snapshot for the same cluster.
    ///
    /// Gaps are permitted because a node may miss intermediate views. The
    /// control plane remains responsible for authenticating snapshots.
    ///
    /// # Errors
    /// Rejects another cluster or a snapshot that does not advance authority.
    pub fn install(&mut self, next: MembershipSnapshot) -> Result<(), InstallError> {
        if next.cluster_id != self.current.cluster_id {
            return Err(InstallError::WrongCluster);
        }
        if next.epoch <= self.current.epoch {
            return Err(InstallError::NotNewer {
                current: self.current.epoch,
                proposed: next.epoch,
            });
        }
        self.current = next;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallError {
    WrongCluster,
    NotNewer {
        current: MembershipEpoch,
        proposed: MembershipEpoch,
    },
}

fn ownership_score(routing_key: &[u8], node: NodeId) -> (u64, u64) {
    (
        shard_hash_parts([routing_key, &node.0.to_be_bytes()]).get(),
        node.0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLUSTER: ClusterId = ClusterId::new([7; 16]);

    fn epoch(value: u64) -> MembershipEpoch {
        MembershipEpoch::new(value).unwrap()
    }

    fn snapshot(value: u64, nodes: &[u64]) -> MembershipSnapshot {
        MembershipSnapshot::new(
            CLUSTER,
            epoch(value),
            nodes.iter().copied().map(NodeId::new).collect(),
        )
        .unwrap()
    }

    #[test]
    fn membership_is_canonical_and_rejects_ambiguity() {
        assert_eq!(
            snapshot(1, &[3, 1, 2]).nodes(),
            &[NodeId(1), NodeId(2), NodeId(3)]
        );
        assert_eq!(
            MembershipSnapshot::new(CLUSTER, epoch(1), vec![]),
            Err(MembershipError::EmptyMembership)
        );
        assert_eq!(
            MembershipSnapshot::new(CLUSTER, epoch(1), vec![NodeId(1), NodeId(1)]),
            Err(MembershipError::DuplicateNode)
        );
    }

    #[test]
    fn ownership_is_independent_of_input_order() {
        let first = snapshot(1, &[1, 2, 3, 4]);
        let second = snapshot(1, &[4, 3, 2, 1]);
        assert_eq!(first.owner(b"operation"), second.owner(b"operation"));
    }

    #[test]
    fn exact_epoch_is_required_for_authority() {
        let state = MembershipState::new(snapshot(5, &[1, 2]));
        assert_eq!(state.authorize(CLUSTER, epoch(5)), Ok(()));
        assert_eq!(
            state.authorize(CLUSTER, epoch(4)),
            Err(AuthorityError::StaleEpoch { current: epoch(5) })
        );
        assert_eq!(
            state.authorize(CLUSTER, epoch(6)),
            Err(AuthorityError::FutureEpoch { current: epoch(5) })
        );
        assert_eq!(
            state.authorize(ClusterId::new([8; 16]), epoch(5)),
            Err(AuthorityError::WrongCluster)
        );
    }

    #[test]
    fn install_advances_monotonically_but_may_skip() {
        let mut state = MembershipState::new(snapshot(2, &[1, 2]));
        state.install(snapshot(5, &[2, 3])).unwrap();
        assert_eq!(state.current().epoch(), epoch(5));
        assert!(matches!(
            state.install(snapshot(4, &[1, 2])),
            Err(InstallError::NotNewer { .. })
        ));
    }
}
