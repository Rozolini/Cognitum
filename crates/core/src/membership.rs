use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

/// Represents the health status of a node in the cluster.
/// Used by the failure detector to handle transient network partitions safely.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    /// The node hasn't responded recently, but isn't confirmed dead yet.
    /// Gives the network time to recover before triggering rebalancing.
    Suspect,
    Dead,
}

/// Represents a single participating node in the distributed cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub id: Uuid,
    pub addr: SocketAddr,
    pub status: NodeStatus,
    pub last_seen_ts: u64,
}