use std::collections::{BTreeMap, HashMap, BTreeSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::collections::VecDeque;
use std::time::{Instant, Duration};
use cognitum_core::crdt::LWWMap;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use crate::models::LogEntry;
use crate::models::{MerkleTree, MERKLE_BUCKETS};

/// Number of virtual nodes per physical node to ensure uniform data distribution.
const VNODES_PER_NODE: usize = 256;
const HISTORY_SIZE: usize = 100;
const PHI_THRESHOLD: f64 = 8.0;

/// Simple hashing utility for keys and node IDs.
fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Implements Consistent Hashing using Virtual Nodes (vnodes).
/// Ensures data is evenly distributed across the cluster and minimizes
/// data movement when nodes join or leave.
pub fn get_replicas_consistent_hashing(
    key: &str,
    peers: &BTreeMap<String, String>,
    num_replicas: usize,
) -> Vec<String> {
    if peers.is_empty() {
        return vec![];
    }
    if peers.len() <= num_replicas {
        return peers.keys().cloned().collect();
    }

    let mut ring: BTreeMap<u64, String> = BTreeMap::new();
    // Populate the hash ring with virtual nodes
    for node_id in peers.keys() {
        for vnode in 0..VNODES_PER_NODE {
            let vnode_key = format!("{}-{}", node_id, vnode);
            ring.insert(hash_key(&vnode_key), node_id.clone());
        }
    }

    let data_hash = hash_key(key);
    let mut replicas = Vec::new();

    // Iterate through the ring to find the next available unique nodes
    let mut iter = ring.range(data_hash..).chain(ring.range(..data_hash));

    while replicas.len() < num_replicas {
        if let Some((_, node_id)) = iter.next() {
            if !replicas.contains(node_id) {
                replicas.push(node_id.clone());
            }
        } else {
            break;
        }
    }

    replicas
}

/// Standard Consistent Hashing without virtual nodes.
pub fn get_replicas(key: &str, peers: &BTreeMap<String, String>, n: usize) -> Vec<String> {
    if peers.is_empty() {
        return vec!["node_local".to_string()];
    }

    let key_hash = hash_key(key);
    let mut ring: BTreeMap<u64, String> = BTreeMap::new();

    for peer_id in peers.keys() {
        ring.insert(hash_key(peer_id), peer_id.clone());
    }

    let mut replicas = Vec::new();
    let mut unique_nodes = BTreeSet::new();

    let mut start_idx = 0;
    for (i, (&node_hash, _)) in ring.iter().enumerate() {
        if node_hash >= key_hash {
            start_idx = i;
            break;
        }
    }

    let ring_entries: Vec<_> = ring.values().collect();
    let ring_size = ring_entries.len();

    for i in 0..ring_size {
        let idx = (start_idx + i) % ring_size;
        let node_id = ring_entries[idx];

        if unique_nodes.insert(node_id.clone()) {
            replicas.push(node_id.clone());
        }

        if replicas.len() == n || replicas.len() == peers.len() {
            break;
        }
    }

    replicas
}

/// Consistent Hashing with Bounded Loads.
/// Dynamically routes traffic away from overloaded nodes (Hotspotting prevention).
pub fn get_replicas_bounded(
    key: &str,
    peers: &BTreeMap<String, String>,
    node_loads: &HashMap<String, usize>,
    total_keys: usize,
    n: usize,
) -> Vec<String> {
    if peers.is_empty() {
        return vec!["node_local".to_string()];
    }

    let key_hash = hash_key(key);
    let mut ring: BTreeMap<u64, String> = BTreeMap::new();

    for peer_id in peers.keys() {
        ring.insert(hash_key(peer_id), peer_id.clone());
    }

    let mut replicas = Vec::new();
    let mut unique_nodes = BTreeSet::new();

    let ring_entries: Vec<_> = ring.values().collect();
    let ring_size = ring_entries.len();

    // Calculate maximum allowed load capacity (+25% buffer)
    let avg_load = if ring_size > 0 { (total_keys as f64 / ring_size as f64).ceil() as usize } else { 0 };
    let max_capacity = std::cmp::max((avg_load as f64 * 1.25).ceil() as usize, 5);

    let mut start_idx = 0;
    for (i, (&node_hash, _)) in ring.iter().enumerate() {
        if node_hash >= key_hash {
            start_idx = i;
            break;
        }
    }

    for i in 0..(ring_size * 2) {
        let idx = (start_idx + i) % ring_size;
        let node_id = ring_entries[idx];

        let current_load = *node_loads.get(node_id).unwrap_or(&0);

        if unique_nodes.insert(node_id.clone()) {
            // Skip overloaded nodes unless we've looped through the whole ring
            if current_load < max_capacity || i >= ring_size {
                replicas.push(node_id.clone());
            } else {
                unique_nodes.remove(node_id);
            }
        }

        if replicas.len() == n || replicas.len() == peers.len() {
            break;
        }
    }

    replicas
}

/// Computes a Merkle Tree from the local CRDT state.
/// Used to quickly identify inconsistencies between nodes during Anti-Entropy syncing.
pub fn calculate_merkle_tree(state: &LWWMap<String, String>) -> MerkleTree {
    let json_val = serde_json::to_value(state).unwrap();
    let mut buckets = vec![0u64; MERKLE_BUCKETS];

    let map_obj = if let Some(m) = json_val.get("state").and_then(|s| s.as_object()) { m }
    else if let Some(m) = json_val.as_object() { m } else {
        return MerkleTree { root_hash: 0, buckets };
    };

    for (k, v) in map_obj {
        let bucket_idx = (hash_key(k) as usize) % MERKLE_BUCKETS;

        let mut entry_hasher = DefaultHasher::new();
        k.hash(&mut entry_hasher);
        serde_json::to_string(v).unwrap().hash(&mut entry_hasher);

        buckets[bucket_idx] ^= entry_hasher.finish(); // XOR hashing
    }

    let mut root_hash = 0;
    for &b in &buckets {
        root_hash ^= b; // XOR root hash
    }

    MerkleTree { root_hash, buckets }
}
/// Phi Accrual Failure Detector.
/// Dynamically calculates the probability of a node failure based on network history,
/// rather than using hardcoded timeouts. (Inspired by Amazon Dynamo and Apache Cassandra).
#[derive(Clone)]
pub struct PhiDetector {
    history: VecDeque<Duration>,
    last_heartbeat: Option<Instant>,
}

impl PhiDetector {
    pub fn new() -> Self {
        Self {
            history: VecDeque::with_capacity(HISTORY_SIZE),
            last_heartbeat: None,
        }
    }

    /// Records a successful heartbeat and updates the historical interval distribution.
    pub fn heartbeat(&mut self) {
        let now = Instant::now();
        if let Some(last) = self.last_heartbeat {
            let interval = now.duration_since(last);
            if self.history.len() >= HISTORY_SIZE {
                self.history.pop_front();
            }
            self.history.push_back(interval);
        }
        self.last_heartbeat = Some(now);
    }

    /// Calculates the Phi value. A higher value means a higher probability
    /// that the node has crashed.
    pub fn phi(&self) -> f64 {
        if self.history.len() < 10 {
            return 0.0; // Not enough data to make a reliable decision
        }

        if let Some(last) = self.last_heartbeat {
            let now = Instant::now();
            let time_since_last = now.duration_since(last).as_secs_f64();

            let mean: f64 = self.history.iter().map(|d| d.as_secs_f64()).sum::<f64>() / self.history.len() as f64;

            let variance: f64 = self.history.iter()
                .map(|d| {
                    let diff = d.as_secs_f64() - mean;
                    diff * diff
                })
                .sum::<f64>() / self.history.len() as f64;

            let std_dev = variance.sqrt().max(0.001);

            let y = (time_since_last - mean) / std_dev;
            let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
            let prob = if time_since_last > mean {
                e / (1.0 + e)
            } else {
                1.0 - (e / (1.0 + e))
            };

            let safe_prob = prob.max(1e-12);
            return -safe_prob.log10();
        }
        0.0
    }

    /// Returns true if the calculated Phi value exceeds the configured threshold.
    pub fn is_dead(&self) -> bool {
        self.phi() > PHI_THRESHOLD
    }
}

/// Serializes and compresses a list of LogEntries into an SSTable using LZ4.
pub fn write_sst_compressed(path: &str, data: &Vec<LogEntry>) -> std::io::Result<()> {
    let serialized = bincode::serialize(data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let compressed = compress_prepend_size(&serialized);

    std::fs::write(path, compressed)
}

/// Decompresses and deserializes an SSTable from disk back into memory.
pub fn read_sst_compressed(path: &str) -> std::io::Result<Vec<LogEntry>> {
    let compressed = std::fs::read(path)?;

    if let Ok(decompressed) = decompress_size_prepended(&compressed) {
        if let Ok(data) = bincode::deserialize(&decompressed) {
            return Ok(data);
        }
    }
    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to decompress or parse SSTable"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cognitum_core::clock::HLC;
    use cognitum_core::crdt::LWWMap;
    use std::collections::BTreeMap;

    #[test]
    fn test_merkle_tree_optimization() {
        let mut state = LWWMap::new();
        let ts1 = HLC::new(100, 0, "node_1".to_string());

        state.set("key_A".to_string(), "value1".to_string(), ts1.clone());
        let tree1 = calculate_merkle_tree(&state);

        let ts2 = HLC::new(200, 0, "node_1".to_string());
        state.set("key_B".to_string(), "value2".to_string(), ts2);
        let tree2 = calculate_merkle_tree(&state);

        assert_ne!(tree1.root_hash, tree2.root_hash, "Root hash must change");

        let diff_count = tree1.buckets.iter()
            .zip(tree2.buckets.iter())
            .filter(|(a, b)| a != b)
            .count();

        assert_eq!(diff_count, 1, "Only 1 out of 16 buckets should change!");
    }

    #[test]
    fn test_sstable_compression_roundtrip() {
        use crate::models::LogEntry;
        use cognitum_core::clock::HLC;
        use std::fs;

        let test_path = "test_compressed_sstable.bin";

        let mut original_data = Vec::new();
        for i in 0..100 {
            original_data.push(LogEntry {
                key: format!("user_key_{}", i),
                value: "some_very_long_value_to_make_compression_useful_repeated_many_times".repeat(5),
                timestamp: HLC::new(1710000000 + i, 0, "node_1".to_string()),
            });
        }

        write_sst_compressed(test_path, &original_data).expect("Failed to write compressed SST");

        let metadata = fs::metadata(test_path).unwrap();
        assert!(metadata.len() > 0, "File should not be empty");

        let raw_size = bincode::serialize(&original_data).unwrap().len() as u64;
        let compressed_size = metadata.len();

        println!("📊 Raw size: {} bytes, Compressed size: {} bytes", raw_size, compressed_size);
        assert!(compressed_size < raw_size, "Compression should reduce file size");

        let recovered_data = read_sst_compressed(test_path).expect("Failed to read compressed SST");

        assert_eq!(original_data.len(), recovered_data.len());
        assert_eq!(original_data[0].key, recovered_data[0].key);
        assert_eq!(original_data[99].value, recovered_data[99].value);

        let _ = fs::remove_file(test_path);
    }

    #[test]
    fn test_phi_accrual_detector() {
        use std::thread::sleep;
        use std::time::Duration;

        let mut detector = PhiDetector::new();

        detector.heartbeat();
        assert_eq!(detector.phi(), 0.0, "Phi should be 0 without sufficient history");
        assert!(!detector.is_dead(), "Node cannot be dead immediately");

        for _ in 0..15 {
            detector.heartbeat();
            sleep(Duration::from_millis(10));
        }

        let normal_phi = detector.phi();
        assert!(!detector.is_dead(), "Node should be alive with stable ping");
        assert!(normal_phi < 8.0, "Phi should be below threshold (8.0)");

        sleep(Duration::from_millis(200));

        let dead_phi = detector.phi();
        println!("Normal Phi: {:.2}, Dead Phi: {:.2}", normal_phi, dead_phi);

        assert!(dead_phi > normal_phi, "Phi should increase after connection drop");
        assert!(detector.is_dead(), "Node should be declared dead after a long delay");
    }

    #[test]
    fn test_consistent_hashing_distribution() {
        let mut peers = BTreeMap::new();
        peers.insert("node_A".to_string(), "10.0.0.1".to_string());
        peers.insert("node_B".to_string(), "10.0.0.2".to_string());
        peers.insert("node_C".to_string(), "10.0.0.3".to_string());
        peers.insert("node_D".to_string(), "10.0.0.4".to_string());
        peers.insert("node_E".to_string(), "10.0.0.5".to_string());

        let key1 = "financial_record_101";
        let key2 = "user_profile_777";

        let replicas1 = get_replicas_consistent_hashing(key1, &peers, 3);
        let replicas2 = get_replicas_consistent_hashing(key2, &peers, 3);

        assert_eq!(replicas1.len(), 3, "Should have exactly 3 replicas");
        assert_eq!(replicas2.len(), 3, "Should have exactly 3 replicas");

        let mut unique1 = replicas1.clone();
        unique1.sort();
        unique1.dedup();
        assert_eq!(unique1.len(), 3, "Replicas must be unique physical nodes");

        let replicas1_again = get_replicas_consistent_hashing(key1, &peers, 3);
        assert_eq!(replicas1, replicas1_again, "Routing must be stable");

        println!("Key 1 routed to: {:?}", replicas1);
        println!("Key 2 routed to: {:?}", replicas2);
    }

    #[test]
    fn test_bounded_loads_hashing() {
        let mut peers = BTreeMap::new();
        peers.insert("node_A".to_string(), "ip".to_string());
        peers.insert("node_B".to_string(), "ip".to_string());
        peers.insert("node_C".to_string(), "ip".to_string());

        let mut loads = HashMap::new();

        loads.insert("node_A".to_string(), 100);
        loads.insert("node_B".to_string(), 100);
        loads.insert("node_C".to_string(), 0);

        let total_keys = 200;

        let replicas = get_replicas_bounded("some_random_key", &peers, &loads, total_keys, 1);

        assert_eq!(replicas[0], "node_C", "Algorithm failed to skip overloaded nodes!");
    }

    #[test]
    fn test_get_replicas_quorum() {
        let mut peers = std::collections::BTreeMap::new();
        peers.insert("node_1".to_string(), "10.0.0.1".to_string());
        peers.insert("node_2".to_string(), "10.0.0.2".to_string());
        peers.insert("node_3".to_string(), "10.0.0.3".to_string());
        peers.insert("node_4".to_string(), "10.0.0.4".to_string());
        peers.insert("node_5".to_string(), "10.0.0.5".to_string());

        let replicas = get_replicas("my_test_key", &peers, 3);
        assert_eq!(replicas.len(), 3, "Should return exactly 3 replicas");

        let unique_count = replicas.iter().collect::<std::collections::BTreeSet<_>>().len();
        assert_eq!(unique_count, 3, "All replicas must be unique");

        let mut small_peers = std::collections::BTreeMap::new();
        small_peers.insert("node_1".to_string(), "10.0.0.1".to_string());

        let small_replicas = get_replicas("another_key", &small_peers, 3);
        assert_eq!(small_replicas.len(), 1, "If there is 1 node in the network, only 1 replica should be returned");
    }
}

