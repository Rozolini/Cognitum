use cognitum_core::clock::HLC;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};

/// Number of buckets used for the Anti-Entropy Merkle Tree.
pub const MERKLE_BUCKETS: usize = 16;

/// Vector Clock implementation for tracking causal history
/// and detecting concurrent updates (Split-Brain) across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorClock {
    pub versions: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self { versions: HashMap::new() }
    }

    /// Increments the logical clock counter for a specific node.
    pub fn increment(&mut self, node_id: &str) {
        let count = self.versions.entry(node_id.to_string()).or_insert(0);
        *count += 1;
    }

    /// Merges knowledge from another vector clock, keeping the highest counters.
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, other_count) in &other.versions {
            let my_count = self.versions.entry(node.clone()).or_insert(0);
            if other_count > my_count {
                *my_count = *other_count;
            }
        }
    }

    /// Checks if this vector clock strictly descends from (is newer than) another.
    /// Returns false if they are concurrent or if `other` is newer.
    pub fn descends(&self, other: &VectorClock) -> bool {
        let mut strictly_greater = false;
        for (node, other_count) in &other.versions {
            let my_count = self.versions.get(node).unwrap_or(&0);
            if my_count < other_count {
                return false;
            }
            if my_count > other_count {
                strictly_greater = true;
            }
        }
        for node in self.versions.keys() {
            if !other.versions.contains_key(node) {
                strictly_greater = true;
            }
        }
        strictly_greater
    }
}

/// HTTP payload for setting a key-value pair.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
    /// Optional vector clock provided by the client for causal consistency.
    pub vclock: Option<VectorClock>,
}

/// HTTP payload for soft-deleting a key (Tombstone).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DeleteRequest {
    pub key: String,
}

/// Represents a single database operation in memory and on disk.
/// Highly optimized using `rkyv` for Zero-Copy serialization.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub struct LogEntry {
    pub key: String,
    pub value: String,
    pub timestamp: HLC,
}

/// Represents a Merkle Tree used for efficient Anti-Entropy state comparison.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct MerkleTree {
    pub root_hash: u64,
    pub buckets: Vec<u64>,
}

/// Probabilistic data structure used to quickly verify if a key MIGHT exist
/// in an SSTable, saving expensive disk I/O operations.
#[derive(Clone)]
pub struct BloomFilter {
    pub bits: Vec<bool>,
    pub size: usize,
}

/// Health status of a peer in the cluster.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub enum NodeStatus {
    Alive,
    Suspected,
    Dead,
}

/// Payload sent between nodes during the Gossip protocol.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GossipMessage {
    pub node_id: String,
    pub ip_addr: String,
    pub status: NodeStatus,
    pub incarnation: u64,
    pub key_count: usize,
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        Self { bits: vec![false; size], size }
    }

    /// Hashes the key and sets the corresponding bits to true.
    pub fn insert(&mut self, key: &str) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..3 {
            let idx = (h1.wrapping_add(h2.wrapping_mul(i as u64))) as usize % self.size;
            self.bits[idx] = true;
        }
    }

    /// Checks if a key might be in the set.
    /// Can return false positives, but NEVER false negatives.
    pub fn contains(&self, key: &str) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..3 {
            let idx = (h1.wrapping_add(h2.wrapping_mul(i as u64))) as usize % self.size;
            if !self.bits[idx] { return false; }
        }
        true
    }

    /// Double hashing technique to generate multiple indices efficiently.
    fn hash_pair(&self, key: &str) -> (u64, u64) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let h1 = hasher.finish();
        hasher.write_u8(42);
        let h2 = hasher.finish();
        (h1, h2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cognitum_core::clock::HLC;
    use rkyv::Deserialize;

    #[test]
    fn test_zero_copy_parsing() {
        let entry = LogEntry {
            key: "hft_key".to_string(),
            value: "x".repeat(1000),
            timestamp: HLC::new(1690000000, 1, "node_fast".to_string()),
        };

        let bytes = rkyv::to_bytes::<_, 2048>(&entry).unwrap();

        // Verify that we can map the bytes directly to the struct without allocating
        let archived = rkyv::check_archived_root::<LogEntry>(&bytes[..])
            .expect("Memory corrupted!");

        assert_eq!(archived.key.as_str(), "hft_key");
        assert_eq!(archived.timestamp.node_id.as_str(), "node_fast");
        assert_eq!(archived.value.len(), 1000);
    }
}