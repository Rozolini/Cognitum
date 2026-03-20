use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, RwLock};
use cognitum_core::clock::HLC;
use cognitum_core::crdt::LWWMap;
use cognitum_network::client::CognitumNodeClient;
use cognitum_network::discovery::DiscoveryService;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::net::UdpSocket;
use rand::seq::IteratorRandom;
use crate::models::{GossipMessage, NodeStatus};
use crate::utils::PhiDetector;

use crate::models::{BloomFilter, LogEntry, MerkleTree, SetRequest, MERKLE_BUCKETS};
use crate::utils::calculate_merkle_tree;

/// Hinted Handoff Worker.
/// Continuously checks the local queue for writes that failed to reach their
/// intended replicas during network partitions, and retries delivery.
pub fn start_hinted_handoff_worker(
    hints: Arc<RwLock<HashMap<String, Vec<SetRequest>>>>,
    peers: Arc<RwLock<BTreeMap<String, String>>>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut hints_map = hints.write().await;
            let peers_map = peers.read().await;

            for (target_node, queue) in hints_map.iter_mut() {
                if queue.is_empty() { continue; }

                if let Some(ip) = peers_map.get(target_node) {
                    let url = format!("http://{}:3000/set", ip);
                    let client = reqwest::Client::new();

                    println!("🔄 Attempting to deliver {} hints to {}", queue.len(), target_node);

                    let mut success_count = 0;
                    for req in queue.iter() {
                        if client.post(&url).json(req).send().await.is_ok() {
                            success_count += 1;
                        } else {
                            break; // Stop if the node is still unreachable
                        }
                    }
                    if success_count > 0 {
                        println!("✅ Successfully delivered {} hints to {}", success_count, target_node);
                        queue.drain(0..success_count);
                    }
                }
            }
        }
    });
}

/// Server-Sent Events (SSE) Trigger Worker.
/// Monitors the state's Merkle root hash. If it changes, it broadcasts
/// the new state to all connected web clients in real-time.
pub fn start_sse_trigger_worker(
    memory: Arc<RwLock<LWWMap<String, String>>>,
    tx: broadcast::Sender<String>,
) {
    tokio::spawn(async move {
        let mut last_checksum = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let state = memory.read().await;
            let tree = calculate_merkle_tree(&*state);
            let current_checksum = tree.root_hash;

            if current_checksum != last_checksum {
                last_checksum = current_checksum;
                if let Ok(json) = serde_json::to_string(&*state) {
                    let _ = tx.send(json);
                }
            }
        }
    });
}

/// Discovery Broadcaster.
/// Periodically broadcasts this node's presence and its current data checksum
/// over UDP to discover new peers and signal liveness.
pub fn start_discovery_broadcaster(
    discovery: Arc<DiscoveryService>,
    memory: Arc<RwLock<LWWMap<String, String>>>,
    node_id: String,
) {
    tokio::spawn(async move {
        loop {
            let current_state = memory.read().await;
            let tree = calculate_merkle_tree(&*current_state);
            let checksum = tree.root_hash;

            // Format: NODE_ID:MERKLE_ROOT
            let msg = format!("{}:{}", node_id, checksum);
            let _ = discovery.broadcast_presence(msg).await;

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
}

/// Write-Ahead Log (WAL) Compaction Worker.
/// Periodically compacts the append-only WAL by removing overwritten
/// or deleted keys, keeping the file size manageable and startup fast.
pub fn start_wal_compaction_worker(
    memory: Arc<RwLock<LWWMap<String, String>>>,
    wal_path: String,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let state = memory.read().await;
            let tmp_path = format!("{}.tmp", wal_path);

            if let Ok(mut file) = tokio::fs::File::create(&tmp_path).await {
                let json_val = serde_json::to_value(&*state).unwrap();
                let map_obj = if let Some(m) = json_val.get("state").and_then(|s| s.as_object()) { m }
                else if let Some(m) = json_val.as_object() { m } else { continue; };

                let mut success = true;
                for (k, v) in map_obj {
                    if let (Some(value), Some(timestamp_val)) = (v.get("value").and_then(|val| val.as_str()), v.get("timestamp")) {
                        if let Ok(timestamp) = serde_json::from_value::<HLC>(timestamp_val.clone()) {
                            let entry = LogEntry { key: k.clone(), value: value.to_string(), timestamp };
                            // Serialize state safely to temp file
                            if let Ok(encoded) = bincode::serialize(&entry) {
                                if file.write_all(&encoded).await.is_err() {
                                    success = false;
                                    break;
                                }
                            }
                        }
                    }
                }
                if success {
                    // Atomic replacement of the WAL file
                    let _ = tokio::fs::rename(&tmp_path, &wal_path).await;
                }
            }
        }
    });
}

/// LSM-Tree MemTable Flush Worker.
/// Monitors the size of the in-memory MemTable. When it exceeds a threshold,
/// it sorts the keys, generates a Bloom Filter, compresses the data using LZ4,
/// and writes an immutable SSTable to disk.
pub fn start_lsm_flush_worker(
    memory: Arc<RwLock<LWWMap<String, String>>>,
    bloom_filters: Arc<RwLock<HashMap<String, BloomFilter>>>,
    node_id: String,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut mem_guard = memory.write().await;

            let json_val = serde_json::to_value(&*mem_guard).unwrap();
            let key_count = json_val.get("state").and_then(|s| s.as_object()).map(|m| m.len()).unwrap_or(0);

            // Trigger flush if MemTable has 3 or more keys (low threshold for demonstration)
            if key_count >= 3 {
                println!("⚠️ MemTable limit reached ({} keys). Flushing to Compressed SSTable...", key_count);

                let map_obj = json_val.get("state").unwrap().as_object().unwrap();

                // 1. Sort entries chronologically by key (Crucial for LSM Trees)
                let mut sorted_entries: Vec<(&String, &serde_json::Value)> = map_obj.iter().collect();
                sorted_entries.sort_by_key(|(k, _)| *k);

                let mut sst_data = Vec::new();
                let mut bf = BloomFilter::new(1024);

                for (k, v) in sorted_entries {
                    bf.insert(k); // Populate Bloom Filter for fast reads

                    if let (Some(value), Some(ts_val)) = (v.get("value").and_then(|val| val.as_str()), v.get("timestamp")) {
                        if let Ok(ts) = serde_json::from_value::<HLC>(ts_val.clone()) {
                            sst_data.push(LogEntry { key: k.clone(), value: value.to_string(), timestamp: ts });
                        }
                    }
                }

                let ts_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let sst_path = format!("/app/data/{}_sst_{}.bin", node_id, ts_now);

                // 2. Perform LZ4 compression in a blocking thread to avoid starving the async runtime
                let path_clone = sst_path.clone();
                let res = tokio::task::spawn_blocking(move || {
                    crate::utils::write_sst_compressed(&path_clone, &sst_data)
                }).await.unwrap();

                if res.is_ok() {
                    println!("💾 Flushed Compressed SSTable to disk: {}", sst_path);
                    bloom_filters.write().await.insert(sst_path, bf);
                    // 3. Clear the MemTable after successful flush
                    *mem_guard = LWWMap::new();
                }
            }
        }
    });
}

/// Anti-Entropy Delta Sync Loop.
/// Listens for peer discovery broadcasts over UDP. If a peer's Merkle Tree root hash
/// differs from the local state, it triggers a targeted reconciliation process (Anti-Entropy)
/// to synchronize only the missing or outdated data (Delta Sync).
pub async fn run_delta_sync_loop(
    discovery: Arc<DiscoveryService>,
    peers: Arc<RwLock<BTreeMap<String, String>>>,
    memory: Arc<RwLock<LWWMap<String, String>>>,
    node_id: String,
) {
    loop {
        if let Ok((payload, peer_addr)) = discovery.listen_for_peers().await {
            let parts: Vec<&str> = payload.split(':').collect();
            if parts.len() == 2 {
                let peer_id = parts[0].to_string();
                let peer_checksum: u64 = parts[1].parse().unwrap_or(0);

                if peer_id != node_id {
                    let mut p = peers.write().await;
                    p.insert(peer_id.clone(), peer_addr.ip().to_string());
                    drop(p);

                    let local_state = memory.read().await;
                    let local_tree = calculate_merkle_tree(&*local_state);
                    let local_checksum = local_tree.root_hash;
                    drop(local_state);

                    // 1. Detect state divergence via Merkle Root Hash
                    if local_checksum != peer_checksum {
                        let merkle_url = format!("http://{}:3000/merkle", peer_addr.ip());
                        if let Ok(resp) = reqwest::get(&merkle_url).await {
                            if let Ok(peer_tree) = resp.json::<MerkleTree>().await {
                                let mut mismatched_buckets = std::collections::HashSet::new();

                                // 2. Identify exactly which buckets differ
                                for i in 0..MERKLE_BUCKETS {
                                    if local_tree.buckets[i] != peer_tree.buckets[i] {
                                        mismatched_buckets.insert(i);
                                    }
                                }

                                if !mismatched_buckets.is_empty() {
                                    println!("🌳 Merkle mismatch with {}. Differing buckets: {:?}", peer_id, mismatched_buckets);

                                    let digest_url = format!("http://{}:3000/digest", peer_addr.ip());
                                    if let Ok(resp) = reqwest::get(&digest_url).await {
                                        if let Ok(peer_digest) = resp.json::<HashMap<String, u64>>().await {
                                            let mut delta = LWWMap::new();
                                            let mut delta_size = 0;

                                            let state_guard = memory.read().await;
                                            let json_val = serde_json::to_value(&*state_guard).unwrap();
                                            let map_obj = json_val.get("state").and_then(|s| s.as_object()).unwrap_or(json_val.as_object().unwrap());

                                            // 3. Build a Delta Payload containing only newer keys from mismatched buckets
                                            for (k, v) in map_obj {
                                                let mut key_hasher = DefaultHasher::new();
                                                k.hash(&mut key_hasher);
                                                let bucket_idx = (key_hasher.finish() as usize) % MERKLE_BUCKETS;

                                                if mismatched_buckets.contains(&bucket_idx) {
                                                    let local_ts = v.get("timestamp").and_then(|t| t.get("physical_time")).and_then(|pt| pt.as_u64()).unwrap_or(0);
                                                    let peer_ts = peer_digest.get(k).copied().unwrap_or(0);

                                                    if local_ts > peer_ts {
                                                        if let (Some(value), Some(timestamp_val)) = (v.get("value").and_then(|val| val.as_str()), v.get("timestamp")) {
                                                            if let Ok(timestamp) = serde_json::from_value::<HLC>(timestamp_val.clone()) {
                                                                delta.set(k.clone(), value.to_string(), timestamp);
                                                                delta_size += 1;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            drop(state_guard);

                                            // 4. Transmit the Delta state via high-speed gRPC
                                            if delta_size > 0 {
                                                println!("📦 {} -> {}: Sending DELTA sync ({} keys from Merkle buckets)", node_id, peer_id, delta_size);
                                                let grpc_url = format!("http://{}:50051", peer_addr.ip());
                                                if let Ok(mut client) = CognitumNodeClient::connect(grpc_url).await {
                                                    let _ = client.sync_state(node_id.clone(), &delta).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// LSM-Tree Compaction Worker.
/// Periodically scans the disk for compressed SSTables. When enough files accumulate,
/// it merges them, resolves conflicts using HLC timestamps, purges deleted keys (Tombstones),
/// and writes a single optimized SSTable back to disk.
pub fn start_lsm_compaction_worker(
    bloom_filters: Arc<RwLock<HashMap<String, BloomFilter>>>,
    node_id: String,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(20)).await;

            if let Ok(entries) = std::fs::read_dir("/app/data") {
                let mut sst_files = Vec::new();
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with(&node_id) && name.contains("_sst_") && name.ends_with(".bin") {
                            sst_files.push(path);
                        }
                    }
                }

                // Trigger compaction if 4 or more SSTables exist
                if sst_files.len() >= 4 {
                    println!("🧹 Starting LSM Compaction: merging {} compressed files...", sst_files.len());

                    let mut merged_data: HashMap<String, LogEntry> = HashMap::new();

                    for path in &sst_files {
                        let path_str = path.to_string_lossy().to_string();

                        if let Ok(data) = crate::utils::read_sst_compressed(&path_str) {
                            for log in data {
                                if let Some(existing) = merged_data.get(&log.key) {
                                    // Resolve conflicts using Last-Write-Wins (HLC)
                                    if log.timestamp.physical_time > existing.timestamp.physical_time {
                                        merged_data.insert(log.key.clone(), log);
                                    }
                                } else {
                                    merged_data.insert(log.key.clone(), log);
                                }
                            }
                        }
                    }

                    // Purge deleted keys (Tombstones) to free up disk space
                    merged_data.retain(|_, v| v.value != "__TOMBSTONE__");

                    let mut sorted_entries: Vec<_> = merged_data.into_values().collect();
                    sorted_entries.sort_by(|a, b| a.key.cmp(&b.key));

                    let mut bf = BloomFilter::new(4096);
                    for entry in &sorted_entries {
                        bf.insert(&entry.key);
                    }

                    let ts_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    let new_sst_path = format!("/app/data/{}_sst_compacted_{}.bin", node_id, ts_now);

                    let path_clone = new_sst_path.clone();
                    // Compress and write in a blocking thread
                    let res = tokio::task::spawn_blocking(move || {
                        crate::utils::write_sst_compressed(&path_clone, &sorted_entries)
                    }).await.unwrap();

                    if res.is_ok() {
                        println!("✅ Compaction complete! Created new compressed file: {}", new_sst_path);

                        let mut filters = bloom_filters.write().await;
                        filters.insert(new_sst_path.clone(), bf);

                        // Delete the old, fragmented SSTables
                        for path in sst_files {
                            let path_str = path.to_string_lossy().to_string();
                            let _ = tokio::fs::remove_file(&path).await;
                            filters.remove(&path_str);
                        }
                    }
                }
            }
        }
    });
}

/// UDP-based Gossip Worker.
/// Continuously exchanges heartbeats and load metrics with random peers.
/// Uses the Phi Accrual Failure Detector to intelligently identify dead nodes
/// and dynamically update the cluster topology and routing tables.
pub async fn start_gossip_worker(
    node_id: String,
    ip_addr: String,
    peers: Arc<RwLock<BTreeMap<String, String>>>,
    peer_loads: Arc<RwLock<HashMap<String, usize>>>,
    memory: Arc<RwLock<cognitum_core::crdt::LWWMap<String, String>>>,
) {
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:5000").await {
        let socket = Arc::new(socket);
        let sock_tx = socket.clone();

        let my_id = node_id.clone();
        let my_ip = ip_addr.clone();
        let peers_tx = peers.clone();
        let memory_tx = memory.clone();

        // Sender thread: Periodically sends our status to a random peer
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let my_keys = {
                    let mem = memory_tx.read().await;
                    let json_val = serde_json::to_value(&*mem).unwrap();
                    json_val.get("state").and_then(|s| s.as_object()).map(|m| m.len()).unwrap_or(0)
                };

                let target_addr = {
                    let p = peers_tx.read().await;
                    let mut rng = rand::thread_rng();
                    if let Some((_, target_ip)) = p.iter().choose(&mut rng) {
                        Some(format!("{}:5000", target_ip))
                    } else {
                        None
                    }
                };

                if let Some(addr) = target_addr {
                    let msg = GossipMessage {
                        node_id: my_id.clone(),
                        ip_addr: my_ip.clone(),
                        status: NodeStatus::Alive,
                        incarnation: 1,
                        key_count: my_keys, // Share our current load for Smart Routing
                    };
                    if let Ok(bytes) = bincode::serialize(&msg) {
                        let _ = sock_tx.send_to(&bytes, &addr).await;
                    }
                }
            }
        });

        let mut detectors: HashMap<String, PhiDetector> = HashMap::new();
        let mut buf = [0u8; 1024];

        // Receiver thread: Processes incoming gossip and updates Phi Detectors
        loop {
            tokio::select! {
                res = socket.recv_from(&mut buf) => {
                    if let Ok((len, _addr)) = res {
                        if let Ok(msg) = bincode::deserialize::<GossipMessage>(&buf[..len]) {
                            if msg.node_id != node_id {
                                let mut p = peers.write().await;
                                let mut loads = peer_loads.write().await;

                                let detector = detectors.entry(msg.node_id.clone()).or_insert_with(PhiDetector::new);
                                detector.heartbeat(); // Register the heartbeat

                                p.insert(msg.node_id.clone(), msg.ip_addr.clone());
                                loads.insert(msg.node_id.clone(), msg.key_count);
                            }
                        }
                    }
                }

                // Periodic check for dead nodes
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    let mut p = peers.write().await;
                    let mut loads = peer_loads.write().await;

                    let dead_nodes: Vec<String> = detectors.iter()
                        .filter(|(_, detector)| detector.is_dead())
                        .map(|(id, _)| id.clone())
                        .collect();

                    for dead_id in dead_nodes {
                        println!("💀 [Phi Detector] Network failure! Node {} declared dead (Phi > 8.0)", dead_id);
                        p.remove(&dead_id);
                        loads.remove(&dead_id);
                        detectors.remove(&dead_id);
                    }
                }
            }
        }
    }
}

/// Active Anti-Entropy Worker.
/// Periodically selects a random peer and compares Merkle Trees.
/// If a divergence is detected, it requests a detailed digest and explicitly
/// fetches any missing or outdated keys to bring the local replica up to date.
pub fn start_anti_entropy_worker(
    node_id: String,
    peers: Arc<RwLock<BTreeMap<String, String>>>,
    memory: Arc<RwLock<cognitum_core::crdt::LWWMap<String, String>>>,
    wal_path: String,
    tx: tokio::sync::broadcast::Sender<String>,
) {
    tokio::spawn(async move {
        let client = reqwest::Client::new();

        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;

            let target_ip = {
                let p = peers.read().await;
                let mut rng = rand::thread_rng();
                p.iter()
                    .filter(|(id, _)| **id != node_id)
                    .choose(&mut rng)
                    .map(|(_, ip)| ip.clone())
            };

            if let Some(ip) = target_ip {
                // 1. Fetch remote Merkle Tree
                let merkle_url = format!("http://{}:3000/merkle", ip);
                if let Ok(resp) = client.get(&merkle_url).send().await {
                    if let Ok(remote_tree) = resp.json::<crate::models::MerkleTree>().await {

                        let local_tree = {
                            let mem = memory.read().await;
                            crate::utils::calculate_merkle_tree(&*mem)
                        };

                        // 2. Compare root hashes
                        if local_tree.root_hash != remote_tree.root_hash {
                            println!("🔄 [Anti-Entropy] Divergence detected with {}! Requesting Digest...", ip);

                            // 3. Fetch detailed digest (key-timestamp mapping)
                            let digest_url = format!("http://{}:3000/digest", ip);
                            if let Ok(resp) = client.get(&digest_url).send().await {
                                if let Ok(remote_digest) = resp.json::<HashMap<String, u64>>().await {

                                    let mut missing_keys = Vec::new();
                                    {
                                        let local_mem = memory.read().await;

                                        let mut local_digest = HashMap::new();
                                        let json_val = serde_json::to_value(&*local_mem).unwrap();
                                        let map_obj = json_val.get("state")
                                            .and_then(|s| s.as_object())
                                            .unwrap_or(json_val.as_object().unwrap());

                                        for (k, v) in map_obj {
                                            if let Some(ts) = v.get("timestamp").and_then(|t| t.get("physical_time")).and_then(|pt| pt.as_u64()) {
                                                local_digest.insert(k.clone(), ts);
                                            }
                                        }

                                        // 4. Identify keys where the remote replica has newer data
                                        for (key, remote_ts) in remote_digest {
                                            let needs_update = match local_digest.get(&key) {
                                                Some(&local_ts) => remote_ts > local_ts,
                                                None => true,
                                            };
                                            if needs_update {
                                                missing_keys.push(key);
                                            }
                                        }
                                    }

                                    // 5. Explicitly fetch and apply the missing keys
                                    for key in missing_keys {
                                        let get_url = format!("http://{}:3000/get/{}", ip, key);
                                        if let Ok(resp) = client.get(&get_url).header("x-internal-forward", "true").send().await {
                                            if let Ok(json) = resp.json::<serde_json::Value>().await {
                                                if let Some(key_data) = json.get("key") {
                                                    let val = key_data.get("value").unwrap().as_str().unwrap().to_string();

                                                    let dummy_state = crate::AppState {
                                                        memory: memory.clone(),
                                                        node_id: node_id.clone(),
                                                        wal_path: wal_path.clone(),
                                                        tx: tx.clone(),
                                                        peers: peers.clone(),
                                                        peer_loads: Arc::new(RwLock::new(HashMap::new())),
                                                        hints: Arc::new(RwLock::new(HashMap::new())),
                                                        bloom_filters: Arc::new(RwLock::new(HashMap::new())),
                                                        cache: Arc::new(RwLock::new(lru::LruCache::new(std::num::NonZeroUsize::new(1).unwrap()))),
                                                        vclocks: Arc::new(RwLock::new(HashMap::new())),
                                                    };

                                                    crate::api::write_local_entry(&dummy_state, key.clone(), val, None).await;
                                                    println!("✅ [Anti-Entropy] Recovered key: {}", key);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use cognitum_core::clock::HLC;

    #[tokio::test]
    async fn test_gossip_message_passing() {
        use crate::models::{GossipMessage, NodeStatus};
        use tokio::net::UdpSocket;
        use std::time::Duration;

        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("Failed to create socket");
        let addr = socket.local_addr().unwrap();

        let original_msg = GossipMessage {
            node_id: "test_node_99".to_string(),
            ip_addr: "10.0.0.99".to_string(),
            status: NodeStatus::Alive,
            incarnation: 1,
            key_count: 10,
        };

        let bytes = bincode::serialize(&original_msg).unwrap();
        socket.send_to(&bytes, &addr).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, _src) = tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
            .await
            .expect("Timeout: Gossip message not received!")
            .unwrap();

        let received_msg: GossipMessage = bincode::deserialize(&buf[..len]).unwrap();

        assert_eq!(received_msg.node_id, "test_node_99");
        assert_eq!(received_msg.ip_addr, "10.0.0.99");
        assert_eq!(received_msg.status, NodeStatus::Alive);
    }

    #[test]
    fn test_anti_entropy_digest_comparison() {
        let mut local_digest = HashMap::new();
        local_digest.insert("key_A".to_string(), 100);
        local_digest.insert("key_B".to_string(), 200);
        local_digest.insert("key_C".to_string(), 300);

        let mut remote_digest = HashMap::new();
        remote_digest.insert("key_A".to_string(), 100);
        remote_digest.insert("key_B".to_string(), 250);
        remote_digest.insert("key_C".to_string(), 290);
        remote_digest.insert("key_D".to_string(), 400);

        let mut missing_keys = Vec::new();

        for (key, remote_ts) in remote_digest {
            let needs_update = match local_digest.get(&key) {
                Some(&local_ts) => remote_ts > local_ts,
                None => true,
            };
            if needs_update {
                missing_keys.push(key);
            }
        }

        missing_keys.sort();

        assert_eq!(missing_keys.len(), 2, "Should detect exactly 2 keys to download");
        assert_eq!(missing_keys[0], "key_B", "key_B needs to be updated (stale)");
        assert_eq!(missing_keys[1], "key_D", "key_D needs to be downloaded (missing)");
    }

    #[tokio::test]
    async fn test_compaction_crdt_and_tombstones() {
        let mut merged_data: HashMap<String, LogEntry> = HashMap::new();

        let old_ts = HLC::new(100, 0, "node1".to_string());
        let new_ts = HLC::new(200, 0, "node1".to_string());
        let delete_ts = HLC::new(300, 0, "node1".to_string());

        let mock_logs_from_disk = vec![
            LogEntry { key: "key1".to_string(), value: "old_val".to_string(), timestamp: old_ts.clone() },
            LogEntry { key: "key1".to_string(), value: "new_val".to_string(), timestamp: new_ts.clone() },
            LogEntry { key: "key2".to_string(), value: "keep_me".to_string(), timestamp: old_ts.clone() },
            LogEntry { key: "key3".to_string(), value: "data".to_string(), timestamp: old_ts.clone() },
            LogEntry { key: "key3".to_string(), value: "__TOMBSTONE__".to_string(), timestamp: delete_ts.clone() },
        ];

        for log in mock_logs_from_disk {
            if let Some(existing) = merged_data.get(&log.key) {
                if log.timestamp.physical_time > existing.timestamp.physical_time {
                    merged_data.insert(log.key.clone(), log);
                }
            } else {
                merged_data.insert(log.key.clone(), log);
            }
        }

        merged_data.retain(|_, v| v.value != "__TOMBSTONE__");

        assert_eq!(merged_data.len(), 2, "Only 2 keys should remain");
        assert_eq!(
            merged_data.get("key1").unwrap().value,
            "new_val",
            "CRDT did not select the newest version!"
        );
        assert!(
            merged_data.get("key3").is_none(),
            "Tombstone failed, deleted key remained in memory!"
        );
    }
}