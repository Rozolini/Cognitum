use crate::clock::HLC;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

/// A Last-Write-Wins (LWW) register holding a value and its causal timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<V> {
    pub value: V,
    pub timestamp: HLC,
}

/// A Conflict-free Replicated Data Type (CRDT) map ensuring Eventual Consistency.
/// It automatically resolves distributed state conflicts using the HLC timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWMap<K, V>
where
    K: Eq + Hash,
{
    state: HashMap<K, LWWRegister<V>>,
}

impl<K: Eq + Hash, V: Clone> LWWMap<K, V> {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
        }
    }

    /// Updates the key only if the provided timestamp is strictly newer.
    pub fn set(&mut self, key: K, value: V, timestamp: HLC) {
        if let Some(existing) = self.state.get(&key) {
            if timestamp <= existing.timestamp {
                return; // Ignore older or concurrent duplicate updates
            }
        }
        self.state.insert(key, LWWRegister { value, timestamp });
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.state.get(key).map(|reg| &reg.value)
    }

    /// Merges another LWWMap into this one, resolving conflicts via Last-Write-Wins.
    pub fn merge(&mut self, other: Self) {
        for (key, other_reg) in other.state {
            match self.state.get_mut(&key) {
                Some(current_reg) => {
                    if other_reg.timestamp > current_reg.timestamp {
                        *current_reg = other_reg;
                    }
                }
                None => {
                    self.state.insert(key, other_reg);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_merge_conflict_resolution() {
        let mut node_a = LWWMap::new();
        let mut node_b = LWWMap::new();

        // node_A writes with time 100
        let time_a = HLC::new(100, 0, "node_A".to_string());
        node_a.set("task_status", "pending", time_a);

        // node_B writes the same key with a newer time 105
        let time_b = HLC::new(105, 0, "node_B".to_string());
        let time_b_clone = time_b.clone();
        node_b.set("task_status", "completed", time_b);

        // Merge node_B state into node_A
        node_a.merge(node_b);

        // Verify the newer record wins (Last-Write-Wins)
        let result = node_a.get(&"task_status").unwrap();
        assert_eq!(*result, "completed");

        // Verify the timestamp is also updated
        let reg = node_a.state.get(&"task_status").unwrap();
        assert_eq!(reg.timestamp, time_b_clone);
    }

    #[test]
    fn test_ignore_older_updates() {
        let mut map = LWWMap::new();

        let new_time = HLC::new(200, 0, "node_A".to_string());
        let old_time = HLC::new(150, 0, "node_B".to_string());

        map.set("config", "v2", new_time);

        // Attempting to overwrite with an older time should be ignored
        map.set("config", "v1", old_time);

        assert_eq!(*map.get(&"config").unwrap(), "v2");
    }
}