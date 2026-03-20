use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Hybrid Logical Clock (HLC) for deterministic event ordering
/// across distributed nodes, resolving clock skew issues.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
#[archive(check_bytes)]
pub struct HLC {
    pub physical_time: u64,
    pub logical_counter: u32,
    pub node_id: String,
}

impl HLC {
    /// Creates a new HLC instance.
    pub fn new(physical_time: u64, logical_counter: u32, node_id: String) -> Self {
        Self {
            physical_time,
            logical_counter,
            node_id,
        }
    }
}

impl PartialOrd for HLC {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Defines strict global ordering: physical time -> logical counter -> node ID (tie-breaker).
impl Ord for HLC {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical_time.cmp(&other.physical_time) {
            Ordering::Equal => match self.logical_counter.cmp(&other.logical_counter) {
                Ordering::Equal => self.node_id.cmp(&other.node_id),
                other_ord => other_ord,
            },
            other_ord => other_ord,
        }
    }
}