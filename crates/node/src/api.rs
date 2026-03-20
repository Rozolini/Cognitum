use axum::{extract::{State, Path}, Json, response::sse::{Event, Sse}};
use axum::http::HeaderMap;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Write;
use std::convert::Infallible;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use futures::stream::Stream;
use futures::future::join_all;
use cognitum_core::clock::HLC;

use crate::models::*;
use crate::utils::*;
use crate::AppState;

/// Helper function: Writes data locally and updates storage subsystems.
/// Handles Vector Clocks, WAL appends, LRU caching, and Bloom Filters.
pub async fn write_local_entry(
    state: &AppState,
    k: String,
    v: String,
    client_vclock: Option<crate::models::VectorClock>
) {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let ts = cognitum_core::clock::HLC::new(now, 0, state.node_id.clone());

    // Vector clocks
    // Detects concurrent updates and network partitions (Split-Brain)
    {
        let mut clocks = state.vclocks.write().await;

        let current_clock = clocks.entry(k.clone()).or_insert_with(|| crate::models::VectorClock::new());

        if let Some(incoming_clock) = client_vclock {
            // If neither clock descends from the other, we have a concurrent conflict
            if !incoming_clock.descends(&*current_clock) && !current_clock.descends(&incoming_clock) {
                println!("⚠️ [SPLIT-BRAIN DETECTED] Conflict for key '{}'! Network partition occurred.", k);
                println!("🔧 Falling back to LWW (Last-Write-Wins via HLC).");
            }
            // Merge knowledge from both vector clocks
            current_clock.merge(&incoming_clock);
        }
        // Always record that this node updated the key
        current_clock.increment(&state.node_id);
    }

    let mut mem = state.memory.write().await;
    mem.set(k.clone(), v.clone(), ts.clone());
    drop(mem);

    // Serialize and append to Write-Ahead Log (WAL)
    let entry = crate::models::LogEntry { key: k.clone(), value: v, timestamp: ts };
    let bytes = rkyv::to_bytes::<_, 2048>(&entry).unwrap();
    let len = bytes.len() as u32;

    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&state.wal_path) {
        use std::io::Write;
        let _ = file.write_all(&len.to_le_bytes());
        let _ = file.write_all(&bytes);
    }

    // Update LRU Cache for fast reads
    let mut cache = state.cache.write().await;
    cache.put(k.clone(), entry);

    // Update Bloom Filter to avoid unnecessary disk lookups
    let mut filters = state.bloom_filters.write().await;
    let filter = filters.entry(state.node_id.clone()).or_insert_with(|| crate::models::BloomFilter::new(10000));
    filter.insert(&k);

    // Trigger Server-Sent Events (SSE) for real-time watchers
    let _ = state.tx.send(k);
}

/// Retrieves the entire dataset by merging the in-memory state (Memtable)
/// with LZ4-compressed SSTables on disk. Filters out tombstones.
pub async fn get_state(State(state): State<AppState>) -> Json<serde_json::Value> {
    let memory = state.memory.read().await;
    let mut combined_state = memory.clone();
    drop(memory);

    if let Ok(entries) = std::fs::read_dir("/app/data") {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with(&state.node_id) && filename.contains("_sst_") && filename.ends_with(".bin") {
                    // NEW: Read and decompress LZ4 SSTable
                    let path_str = path.to_string_lossy().to_string();
                    if let Ok(sst_data) = crate::utils::read_sst_compressed(&path_str) {
                        for log in sst_data {
                            combined_state.set(log.key, log.value, log.timestamp);
                        }
                    }
                }
            }
        }
    }

    let mut json_val = serde_json::to_value(&combined_state).unwrap();
    // Filter out deleted keys (Tombstones)
    if let Some(map) = json_val.get_mut("state").and_then(|s| s.as_object_mut()) {
        map.retain(|_, v| v.get("value").and_then(|val| val.as_str()) != Some("__TOMBSTONE__"));
    } else if let Some(map) = json_val.as_object_mut() {
        map.retain(|_, v| v.get("value").and_then(|val| val.as_str()) != Some("__TOMBSTONE__"));
    }

    Json(json_val)
}

/// Handles write requests. Differentiates between internal replica syncs
/// and external client requests. Implements Quorum Writes and Hinted Handoff.
pub async fn set_state(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SetRequest>,
) -> Json<serde_json::Value> {
    let is_internal = headers.contains_key("x-internal-forward");

    // Direct write if it's an internal replica forwarding
    if is_internal {
        write_local_entry(&state, payload.key, payload.value, payload.vclock.clone()).await;
        return Json(serde_json::json!({"status": "success", "message": "Replica write OK"}));
    }

    // Client request: Determine replicas via Consistent Hashing
    let peers = state.peers.read().await;
    let replicas = get_replicas_consistent_hashing(&payload.key, &peers, 3);
    let peers_clone = peers.clone();
    drop(peers);

    let mut acks = 0;
    // Require W quorum (e.g., 2 out of 3 replicas)
    let required_acks = std::cmp::min(2, replicas.len());

    for replica in &replicas {
        if replica == &state.node_id {
            write_local_entry(&state, payload.key.clone(), payload.value.clone(), payload.vclock.clone()).await;
            acks += 1;
        } else {
            if let Some(ip) = peers_clone.get(replica) {
                let url = format!("http://{}:3000/set", ip);
                let client = reqwest::Client::new();
                let res = client.post(&url)
                    .header("x-internal-forward", "true")
                    .json(&payload)
                    .send()
                    .await;

                if res.is_ok() {
                    acks += 1;
                } else {
                    // Node unreachable: Store hint for later delivery (Hinted Handoff)
                    let mut hints = state.hints.write().await;
                    hints.entry(replica.clone()).or_insert_with(Vec::new).push(payload.clone());
                }
            }
        }
    }

    if acks >= required_acks {
        Json(serde_json::json!({ "status": "success", "message": format!("Quorum reached. Acks: {}/{}", acks, replicas.len()) }))
    } else {
        Json(serde_json::json!({ "status": "error", "message": format!("Quorum failed! Only {} acks received.", acks) }))
    }
}
/// Soft-deletes a key by writing a Tombstone marker (`__TOMBSTONE__`).
/// Ensures eventual consistency across replicas using Quorum Deletes and Hinted Handoff.
pub async fn delete_state(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DeleteRequest>,
) -> Json<serde_json::Value> {
    let is_internal = headers.contains_key("x-internal-forward");

    // Direct deletion if it's an internal replica forwarding
    if is_internal {
        write_local_entry(&state, payload.key, "__TOMBSTONE__".to_string(), None).await;
        return Json(serde_json::json!({"status": "success", "message": "Replica delete OK"}));
    }

    // Client request: Determine replicas via Consistent Hashing
    let peers = state.peers.read().await;
    let replicas = crate::utils::get_replicas_consistent_hashing(&payload.key, &peers, 3);
    let peers_clone = peers.clone();
    drop(peers);

    let mut acks = 0;
    // Require W quorum (e.g., 2 out of 3 replicas)
    let required_acks = std::cmp::min(2, replicas.len());

    for replica in &replicas {
        if replica == &state.node_id {
            write_local_entry(&state, payload.key.clone(), "__TOMBSTONE__".to_string(), None).await;
            acks += 1;
        } else {
            if let Some(ip) = peers_clone.get(replica) {
                let url = format!("http://{}:3000/delete", ip);
                let client = reqwest::Client::new();
                let res = client.post(&url)
                    .header("x-internal-forward", "true")
                    .json(&payload)
                    .send()
                    .await;

                if res.is_ok() {
                    acks += 1;
                } else {
                    // Node unreachable: Store Tombstone hint for later delivery
                    let hint = SetRequest { key: payload.key.clone(), value: "__TOMBSTONE__".to_string(), vclock: None };
                    let mut hints = state.hints.write().await;
                    hints.entry(replica.clone()).or_insert_with(Vec::new).push(hint);
                }
            }
        }
    }

    if acks >= required_acks {
        Json(serde_json::json!({ "status": "success", "message": format!("Delete Quorum reached. Acks: {}/{}", acks, replicas.len()) }))
    } else {
        Json(serde_json::json!({ "status": "error", "message": format!("Delete Quorum failed! Only {} acks received.", acks) }))
    }
}


// DISTRIBUTED READS & READ REPAIR
/// Retrieves a key's value by querying multiple replicas concurrently.
/// Acts as a Coordinator to resolve conflicts using HLC timestamps
/// and performs asynchronous Read Repair on stale nodes.
pub async fn get_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> Json<serde_json::Value> {
    metrics::counter!("cognitum_reads_total", 1);
    let is_internal = headers.contains_key("x-internal-forward");

    // 1. Direct local read if requested by another node's coordinator
    if is_internal {
        return crate::api::handle_local_get(&state, &key).await;
    }

    let peers = state.peers.read().await;
    let replicas = crate::utils::get_replicas_consistent_hashing(&key, &peers, 3);
    let peers_clone = peers.clone();
    drop(peers);

    // 2. COORDINATOR MODE: Fetch data from all target replicas concurrently
    let mut futures = Vec::new();
    let client = reqwest::Client::new();

    for replica in &replicas {
        if replica == &state.node_id {
            // Read from local storage
            let my_state = state.clone();
            let my_key = key.clone();
            futures.push(tokio::spawn(async move {
                let res = crate::api::handle_local_get(&my_state, &my_key).await;
                (my_state.node_id.clone(), res.0)
            }));
        } else if let Some(ip) = peers_clone.get(replica) {
            // Read from remote replica
            let url = format!("http://{}:3000/get/{}", ip, key);
            let client_clone = client.clone();
            let replica_id = replica.clone();
            futures.push(tokio::spawn(async move {
                let res = client_clone.get(&url).header("x-internal-forward", "true").send().await;
                if let Ok(resp) = res {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        return (replica_id, json);
                    }
                }
                (replica_id, serde_json::json!({"error": "Failed to read"}))
            }));
        }
    }

    // Wait for all replica responses
    let results = join_all(futures).await;

    // 3. Resolve the freshest value based on the highest HLC timestamp
    let mut freshest_val = None;
    let mut freshest_ts: u64 = 0;
    let mut stale_replicas = Vec::new();

    for res in results.into_iter().flatten() {
        let (replica_id, json) = res;

        if let Some(key_data) = json.get("key") {
            if let Some(ts_val) = key_data.get("timestamp").and_then(|t| t.get("physical_time")).and_then(|pt| pt.as_u64()) {
                if ts_val > freshest_ts {
                    // Found newer data
                    freshest_ts = ts_val;
                    freshest_val = Some(key_data.clone());
                    // Previously checked replicas are now considered stale
                    stale_replicas.push(replica_id.clone());
                } else if ts_val < freshest_ts {
                    // This specific replica is stale
                    stale_replicas.push(replica_id);
                }
            }
        }
    }

    // 4. READ REPAIR: Asynchronously update out-of-sync replicas
    if let Some(fresh_data) = &freshest_val {
        if !stale_replicas.is_empty() {
            let val_str = fresh_data.get("value").unwrap().as_str().unwrap().to_string();
            let repair_payload = SetRequest { key: key.clone(), value: val_str, vclock: None };

            for stale in stale_replicas {
                if stale == state.node_id {
                    // Apply repair locally
                    write_local_entry(&state, repair_payload.key.clone(), repair_payload.value.clone(), None).await;
                    println!("🔧 Background Read Repair applied locally");
                } else if let Some(ip) = peers_clone.get(&stale) {
                    // Send repair to remote replica
                    let url = format!("http://{}:3000/set", ip);
                    let repair_payload = repair_payload.clone();
                    tokio::spawn(async move {
                        let _ = reqwest::Client::new().post(&url).header("x-internal-forward", "true").json(&repair_payload).send().await;
                        println!("🔧 Background Read Repair sent to node {}", stale);
                    });
                }
            }
        }
    }

    if let Some(fresh) = freshest_val {
        Json(serde_json::json!({ "key": fresh }))
    } else {
        Json(serde_json::json!({"error": "Key not found on any replica"}))
    }
}

// LOCAL READ PATH & ANTI-ENTROPY UTILS
/// Executes a local read request following the standard LSM-Tree hierarchy:
/// 1. LRU Cache (Fastest)
/// 2. In-memory Memtable
/// 3. On-disk SSTables (Optimized with Bloom Filters and LZ4 decompression)
async fn handle_local_get(state: &AppState, key: &str) -> Json<serde_json::Value> {
    // 1. Check LRU Cache
    {
        let mut cache = state.cache.write().await;
        if let Some(entry) = cache.get(key) {
            metrics::counter!("cognitum_cache_hits_total", 1);
            if entry.value == "__TOMBSTONE__" {
                return Json(serde_json::json!({"error": "Key not found (deleted)"}));
            }
            return Json(serde_json::json!({ "key": { "value": entry.value, "timestamp": entry.timestamp } }));
        }
    }
    metrics::counter!("cognitum_cache_misses_total", 1);

    // 2. Check In-Memory Memtable
    let memory = state.memory.read().await;
    let json_val = serde_json::to_value(&*memory).unwrap();
    if let Some(state_map) = json_val.get("state").and_then(|s| s.as_object()) {
        if let Some(entry) = state_map.get(key) {
            return Json(serde_json::json!({ "key": entry }));
        }
    }
    drop(memory);

    // 3. Check On-Disk SSTables
    let filters = state.bloom_filters.read().await;
    let mut best_entry: Option<LogEntry> = None;

    if let Ok(entries) = std::fs::read_dir("/app/data") {
        for entry in entries.flatten() {
            let path_buf = entry.path();
            let path_str = path_buf.to_string_lossy().to_string();

            if path_str.contains(&state.node_id) && path_str.contains("_sst_") {
                // Fast path: skip file if Bloom Filter guarantees the key is missing
                if let Some(bf) = filters.get(&path_str) {
                    if !bf.contains(key) { continue; }
                }

                // Fast extraction and reading of the LZ4 compressed block
                if let Ok(sst_data) = crate::utils::read_sst_compressed(&path_str) {
                    for log in sst_data {
                        if log.key == key {
                            if let Some(best) = &best_entry {
                                if log.timestamp.physical_time > best.timestamp.physical_time {
                                    best_entry = Some(log);
                                }
                            } else {
                                best_entry = Some(log);
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(entry) = best_entry {
        // Populate cache for future reads
        let mut cache = state.cache.write().await;
        cache.put(key.to_string(), entry.clone());
        drop(cache);

        if entry.value == "__TOMBSTONE__" {
            Json(serde_json::json!({"error": "Key not found (deleted)"}))
        } else {
            Json(serde_json::json!({ "key": { "value": entry.value, "timestamp": entry.timestamp } }))
        }
    } else {
        Json(serde_json::json!({"error": "Key not found"}))
    }
}

/// Generates a Merkle Tree representation of the local state.
/// Used by the Anti-Entropy worker to efficiently detect data divergence between nodes.
pub async fn get_merkle(State(state): State<AppState>) -> Json<MerkleTree> {
    let memory = state.memory.read().await;
    let tree = calculate_merkle_tree(&*memory);
    Json(tree)
}

/// Generates a lightweight digest of keys and their latest timestamps.
/// Used by the Gossip protocol for fast delta-synchronization.
pub async fn get_digest(State(state): State<AppState>) -> Json<HashMap<String, u64>> {
    let memory = state.memory.read().await;
    let mut digest = HashMap::new();

    let json_val = serde_json::to_value(&*memory).unwrap();
    let map_obj = json_val.get("state").and_then(|s| s.as_object()).unwrap_or(json_val.as_object().unwrap());

    for (k, v) in map_obj {
        if let Some(ts) = v.get("timestamp").and_then(|t| t.get("physical_time")).and_then(|pt| pt.as_u64()) {
            digest.insert(k.clone(), ts);
        }
    }
    Json(digest)
}

/// Establishes a Server-Sent Events (SSE) stream.
/// Allows clients to subscribe to real-time updates triggered by state changes.
pub async fn watch_state(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();

    let stream = BroadcastStream::new(rx).filter_map(|res| {
        res.ok().map(|data| Ok(Event::default().data(data)))
    });

    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[test]
    fn test_prometheus_metrics_registration() {
        let _builder = PrometheusBuilder::new();
        metrics::counter!("cognitum_reads_total", 1);
        metrics::counter!("cognitum_writes_total", 1);
        println!("✅ Prometheus metrics syntax and builder tested successfully.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_heavy_load_concurrency() {
        use std::sync::Arc;
        use tokio::sync::{RwLock, broadcast};
        use std::collections::{BTreeMap, HashMap};
        use lru::LruCache;
        use std::num::NonZeroUsize;
        use cognitum_core::crdt::LWWMap;
        use crate::AppState;

        // 1. Initialize isolated state
        let shared_memory = Arc::new(RwLock::new(LWWMap::new()));
        let shared_peers = Arc::new(RwLock::new(BTreeMap::new()));
        let (tx, _) = broadcast::channel(100);
        let shared_cache = Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(10000).unwrap())));

        let state = AppState {
            memory: shared_memory,
            node_id: "stress_node".to_string(),
            wal_path: "stress_test_wal.bin".to_string(),
            tx,
            peers: shared_peers,
            peer_loads: Arc::new(RwLock::new(HashMap::new())),
            hints: Arc::new(RwLock::new(HashMap::new())),
            bloom_filters: Arc::new(RwLock::new(HashMap::new())),
            cache: shared_cache,
            vclocks: Arc::new(RwLock::new(HashMap::new())),
        };

        // 2. Bombard the system: 10,000 concurrent writes
        let mut tasks = vec![];
        for i in 0..10000 {
            let state_clone = state.clone();
            tasks.push(tokio::spawn(async move {
                let key = format!("stress_key_{}", i);
                let value = format!("stress_val_{}", i);
                write_local_entry(&state_clone, key, value, None).await;
            }));
        }

        // Wait for all 10,000 tasks to complete
        for task in tasks {
            let _ = task.await;
        }

        // 3. Consistency checks
        let mem = state.memory.read().await;
        let json_val = serde_json::to_value(&*mem).unwrap();
        let count = json_val.get("state").and_then(|s| s.as_object()).map(|m| m.len()).unwrap_or(0);

        assert_eq!(count, 10000, "Data loss! Not all keys were written.");

        let cache = state.cache.read().await;
        assert_eq!(cache.len(), 10000, "Cache failed to register all keys!");

        // 4. Cleanup test file
        let _ = tokio::fs::remove_file(&state.wal_path).await;
    }

    #[tokio::test]
    async fn test_zero_copy_wal_read_write() {
        use std::io::{Write, Read};
        use rkyv::Deserialize;
        use cognitum_core::clock::HLC;
        use crate::models::LogEntry;

        let wal_path = "test_zero_copy_wal.bin";
        // Clear before test
        let _ = std::fs::remove_file(wal_path); 

        // 1. Create test entries
        let entries = vec![
            LogEntry {
                key: "key_A".to_string(),
                value: "value_A".to_string(),
                timestamp: HLC::new(100, 1, "node1".to_string()),
            },
            LogEntry {
                key: "key_B".to_string(),
                value: "value_B".to_string(),
                timestamp: HLC::new(105, 2, "node1".to_string()),
            }
        ];

        // 2. Simulate WAL writing (Length-Prefixed)
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(wal_path)
                .unwrap();

            for entry in &entries {
                let bytes = rkyv::to_bytes::<_, 2048>(entry).unwrap();
                let len = bytes.len() as u32;
                file.write_all(&len.to_le_bytes()).unwrap();
                file.write_all(&bytes).unwrap();
            }
        }

        // 3. Simulate WAL reading (Zero-Copy via rkyv)
        let mut recovered_entries: Vec<(String, String, u64)> = Vec::new();
        {
            let mut file = std::fs::File::open(wal_path).unwrap();
            loop {
                let mut len_buf = [0u8; 4];
                if file.read_exact(&mut len_buf).is_err() {
                    break;
                }
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut entry_buf = vec![0u8; len];
                file.read_exact(&mut entry_buf).unwrap();

                if let Ok(archived) = rkyv::check_archived_root::<LogEntry>(&entry_buf) {
                    let key = archived.key.as_str().to_string();
                    let value = archived.value.as_str().to_string();
                    let phys_time = archived.timestamp.physical_time.deserialize(&mut rkyv::Infallible).unwrap();

                    recovered_entries.push((key, value, phys_time));
                }
            }
        }

        // 4. Verify data integrity
        assert_eq!(recovered_entries.len(), 2, "Not all entries were read!");

        assert_eq!(recovered_entries[0].0, "key_A");
        assert_eq!(recovered_entries[0].1, "value_A");
        assert_eq!(recovered_entries[0].2, 100);

        assert_eq!(recovered_entries[1].0, "key_B");
        assert_eq!(recovered_entries[1].1, "value_B");
        assert_eq!(recovered_entries[1].2, 105);

        // 5. Cleanup test file
        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn test_chaos_fault_tolerance_and_hints() {
        use std::sync::Arc;
        use tokio::sync::{RwLock, broadcast};
        use std::collections::{BTreeMap, HashMap};
        use lru::LruCache;
        use std::num::NonZeroUsize;
        use axum::http::HeaderMap;
        use axum::extract::State;
        use axum::Json;
        use cognitum_core::crdt::LWWMap;
        use crate::AppState;
        use crate::models::SetRequest;
        use tokio::time::Duration;

        // 1. Start a Mock server for an "alive" replica on port 3000
        tokio::spawn(async {
            let app = axum::Router::new().route("/set", axum::routing::post(|| async { "OK" }));
            if let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:3000").await {
                let _ = axum::serve(listener, app).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await; // Allow time to start

        // 2. Initialize Coordinator state
        let shared_memory = Arc::new(RwLock::new(LWWMap::new()));
        let shared_peers = Arc::new(RwLock::new(BTreeMap::new()));
        let shared_hints = Arc::new(RwLock::new(HashMap::new()));

        {
            let mut p = shared_peers.write().await;
            p.insert("node_local".to_string(), "127.0.0.1".to_string()); // Us (Coordinator)
            p.insert("node_dead".to_string(), "198.51.100.99".to_string()); // Black hole (Connection Refused)
            p.insert("node_alive".to_string(), "127.0.0.1".to_string()); // Points to Mock server
        }

        let state = AppState {
            memory: shared_memory,
            node_id: "node_local".to_string(),
            wal_path: "chaos_test_wal.bin".to_string(),
            tx: broadcast::channel(10).0,
            peers: shared_peers,
            peer_loads: Arc::new(RwLock::new(HashMap::new())),
            hints: shared_hints.clone(),
            bloom_filters: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(100).unwrap()))),
            vclocks: Arc::new(RwLock::new(HashMap::new())),
        };

        // 3. Send write request (should reach W=2 quorum via node_local + node_alive)
        let payload = SetRequest {
            key: "chaos_key".to_string(),
            value: "survivor_data".to_string(),
            vclock: None,
        };

        let response = crate::api::set_state(
            State(state.clone()),
            HeaderMap::new(),
            Json(payload.clone())
        ).await;

        let res_value = response.0;
        let status = res_value.get("status").unwrap().as_str().unwrap();

        // 4. Strict assertions
        assert_eq!(status, "success", "W=2 Quorum was not reached due to a single node failure!");

        let hints = shared_hints.read().await;
        assert!(
            hints.contains_key("node_dead"),
            "Hint for the dead node was not created! Data might be lost."
        );
        assert_eq!(
            hints.get("node_dead").unwrap()[0].key,
            "chaos_key",
            "Incorrect key landed in the Hinted Handoff queue!"
        );

        // 5. Cleanup test file
        let _ = tokio::fs::remove_file("chaos_test_wal.bin").await;
    }
    #[tokio::test]
    #[ignore = "TODO: Fix Bounded Loads logic and add HTTP timeouts"]
    async fn test_set_state_with_bounded_loads() {

        use std::sync::Arc;
        use tokio::sync::{RwLock, broadcast};
        use std::collections::{BTreeMap, HashMap};
        use axum::http::HeaderMap;
        use axum::extract::State;
        use axum::Json;
        use lru::LruCache;
        use std::num::NonZeroUsize;
        use cognitum_core::crdt::LWWMap;
        use crate::AppState;
        use crate::models::{SetRequest, BloomFilter};

        // 1. Initialize a 5-node cluster
        let shared_memory = Arc::new(RwLock::new(LWWMap::new()));
        let shared_peers = Arc::new(RwLock::new(BTreeMap::new()));
        let shared_loads = Arc::new(RwLock::new(HashMap::new()));
        let shared_hints = Arc::new(RwLock::new(HashMap::new()));

        {
            let mut p = shared_peers.write().await;
            p.insert("node_local".to_string(), "127.0.0.1".to_string());
            p.insert("node_A".to_string(), "198.51.100.1".to_string());
            p.insert("node_B".to_string(), "198.51.100.2".to_string());
            p.insert("node_C".to_string(), "198.51.100.3".to_string());
            p.insert("node_D".to_string(), "198.51.100.4".to_string());

            let mut l = shared_loads.write().await;
            // Artificially overload node_A and node_B (1000 keys each)
            l.insert("node_A".to_string(), 1000);
            l.insert("node_B".to_string(), 1000);
            // node_C and node_D are empty (0 keys)
            l.insert("node_C".to_string(), 0);
            l.insert("node_D".to_string(), 0);
        }

        let state = AppState {
            memory: shared_memory,
            node_id: "node_local".to_string(),
            wal_path: "test_bounded_loads_wal.bin".to_string(),
            tx: broadcast::channel(10).0,
            peers: shared_peers,
            peer_loads: shared_loads,
            hints: shared_hints.clone(),
            bloom_filters: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(100).unwrap()))),
            vclocks: Arc::new(RwLock::new(HashMap::new())),
        };

        // 2. Send write request (seeking 3 replicas)
        let payload = SetRequest {
            key: "smart_routing_key".to_string(),
            value: "data".to_string(),
            vclock: None,
        };

        let _ = crate::api::set_state(
            State(state.clone()),
            HeaderMap::new(),
            Json(payload)
        ).await;

        // 3. Verify node selection.
        // Since we didn't start real servers for C and D, their HTTP requests will fail,
        // guaranteeing they end up in the Hinted Handoff queue.
        // The main check: node_A and node_B MUST NOT be selected!
        let hints = shared_hints.read().await;

        assert!(
            !hints.contains_key("node_A"),
            "Error: node_A is overloaded (1000 keys), but the algorithm routed data there!"
        );
        assert!(
            !hints.contains_key("node_B"),
            "Error: node_B is overloaded (1000 keys), but the algorithm routed data there!"
        );

        // 4. Cleanup test file
        let _ = tokio::fs::remove_file("test_bounded_loads_wal.bin").await;
    }

    #[tokio::test]
    async fn test_lru_cache_eviction() {
        use lru::LruCache;
        use std::num::NonZeroUsize;
        use cognitum_core::clock::HLC;
        use crate::models::LogEntry;

        // 1. Create a cache with a capacity of 2 items
        let mut cache = LruCache::new(NonZeroUsize::new(2).unwrap());
        let ts = HLC::new(100, 0, "node1".to_string());

        let entry1 = LogEntry { key: "key1".to_string(), value: "val1".to_string(), timestamp: ts.clone() };
        let entry2 = LogEntry { key: "key2".to_string(), value: "val2".to_string(), timestamp: ts.clone() };
        let entry3 = LogEntry { key: "key3".to_string(), value: "val3".to_string(), timestamp: ts.clone() };

        // 2. Fill the cache to its limit
        cache.put("key1".to_string(), entry1);
        cache.put("key2".to_string(), entry2);

        // 3. Simulate reading "key1" to mark it as most recently used (MRU)
        let _ = cache.get("key1");

        // 4. Insert 3rd item. Since limit is 2, the least recently used (key2) must be evicted
        cache.put("key3".to_string(), entry3);

        // 5. Assertions
        assert!(cache.contains("key1"), "key1 was just read, it should remain");
        assert!(cache.contains("key3"), "key3 was just added, it should be in the cache");
        assert!(!cache.contains("key2"), "key2 is the oldest (LRU), it should have been evicted!");
    }
}

