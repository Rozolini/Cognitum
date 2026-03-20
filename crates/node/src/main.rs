use rkyv::Deserialize;
use std::io::Read;
use lru::LruCache;
use std::num::NonZeroUsize;
use metrics_exporter_prometheus::PrometheusBuilder;
use cognitum_core::crdt::LWWMap;
use cognitum_network::pb::gossip_service_server::GossipServiceServer;
use cognitum_network::server::CognitumNodeServer;
use cognitum_network::discovery::DiscoveryService;
use axum::{routing::{get, post}, Router};
use std::env;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tonic::transport::Server;
use std::collections::{HashMap, BTreeMap};

mod models;
use crate::models::*;
mod utils;
mod api;
use crate::api::*;
mod workers;
use crate::workers::*;

/// Global application state shared safely across HTTP handlers and background workers.
#[derive(Clone)]
pub struct AppState {
    pub memory: Arc<RwLock<LWWMap<String, String>>>,
    pub node_id: String,
    pub wal_path: String,
    pub tx: broadcast::Sender<String>,
    pub peers: Arc<RwLock<BTreeMap<String, String>>>,
    pub peer_loads: Arc<RwLock<HashMap<String, usize>>>,
    pub hints: Arc<RwLock<HashMap<String, Vec<SetRequest>>>>,
    pub bloom_filters: Arc<RwLock<HashMap<String, BloomFilter>>>,
    pub cache: Arc<RwLock<LruCache<String, LogEntry>>>,
    pub vclocks: Arc<RwLock<HashMap<String, crate::models::VectorClock>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = env::var("NODE_ID").unwrap_or_else(|_| "node_local".to_string());
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
    let addr = listen_addr.parse()?;

    let data_dir = "/app/data";
    let _ = std::fs::create_dir_all(data_dir);
    let wal_path = format!("{}/{}_wal.bin", data_dir, node_id);

    let mut initial_state = LWWMap::new();

    // --- ZERO-COPY WAL RECOVERY ---
    // Recovers state on startup by reading raw bytes from the Write-Ahead Log.
    if let Ok(mut file) = std::fs::File::open(&wal_path) {
        let mut count = 0;
        loop {
            let mut len_buf = [0u8; 4];
            // 1. Read the size of the next entry
            if file.read_exact(&mut len_buf).is_err() {
                break; // EOF
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // 2. Read exact entry bytes
            let mut entry_buf = vec![0u8; len];
            if file.read_exact(&mut entry_buf).is_err() {
                break;
            }

            // 3. True Zero-Copy reading using rkyv memory mapping
            if let Ok(archived) = rkyv::check_archived_root::<LogEntry>(&entry_buf) {
                let key = archived.key.as_str().to_string();
                let value = archived.value.as_str().to_string();

                // Instant deserialization for numeric primitives
                let phys_time = archived.timestamp.physical_time.deserialize(&mut rkyv::Infallible).unwrap();
                let log_counter = archived.timestamp.logical_counter.deserialize(&mut rkyv::Infallible).unwrap();
                let node_id = archived.timestamp.node_id.as_str().to_string();

                let ts = cognitum_core::clock::HLC::new(phys_time, log_counter, node_id);

                initial_state.set(key, value, ts);
                count += 1;
            }
        }
        println!("{} loaded {} operations from ZERO-COPY WAL", node_id, count);
    }

    let shared_memory = Arc::new(RwLock::new(initial_state));
    let (tx, _) = broadcast::channel(100);

    println!("Starting {} on {}", node_id, addr);

    // Initialize Prometheus metrics exporter on port 9000
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9000))
        .install()
        .expect("Failed to install Prometheus recorder");

    // 1. Start gRPC Server for node-to-node replication
    let server_memory = shared_memory.clone();
    tokio::spawn(async move {
        let node_server = CognitumNodeServer::new(server_memory);
        Server::builder()
            .add_service(GossipServiceServer::new(node_server))
            .serve(addr)
            .await
            .unwrap();
    });

    // 2. Initialize Shared State
    let mut initial_peers = BTreeMap::new();
    initial_peers.insert(node_id.clone(), "127.0.0.1".to_string());

    let shared_peers = Arc::new(RwLock::new(initial_peers));
    let shared_hints = Arc::new(RwLock::new(HashMap::new()));
    let shared_bloom = Arc::new(RwLock::new(HashMap::new()));
    let shared_cache = Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(10000).unwrap())));
    let shared_peer_loads = Arc::new(RwLock::new(HashMap::new()));

    let app_state = AppState {
        memory: shared_memory.clone(),
        node_id: node_id.clone(),
        wal_path: wal_path.clone(),
        bloom_filters: shared_bloom.clone(),
        tx: tx.clone(),
        peers: shared_peers.clone(),
        peer_loads: shared_peer_loads.clone(),
        hints: shared_hints.clone(),
        cache: shared_cache,
        vclocks: Arc::new(RwLock::new(HashMap::new())),
    };

    // 3. Start HTTP API for client interactions
    let app = Router::new()
        .route("/state", get(get_state))
        .route("/set", post(set_state))
        .route("/delete", post(delete_state))
        .route("/digest", get(get_digest))
        .route("/merkle", get(get_merkle))
        .route("/get/:key", get(get_key))
        .route("/watch", get(watch_state))
        .with_state(app_state);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // 4. Spawn Background Maintenance and Sync Workers
    let local_ip = addr.ip().to_string();
    let gossip_id = node_id.clone();
    let gossip_peers = shared_peers.clone();
    let gossip_loads = shared_peer_loads.clone();
    let gossip_mem = shared_memory.clone();

    let anti_entropy_id = node_id.clone();
    let anti_entropy_peers = shared_peers.clone();
    let anti_entropy_mem = shared_memory.clone();
    let anti_entropy_wal = wal_path.clone();
    let anti_entropy_tx = tx.clone();

    start_anti_entropy_worker(
        anti_entropy_id,
        anti_entropy_peers,
        anti_entropy_mem,
        anti_entropy_wal,
        anti_entropy_tx
    );

    tokio::spawn(async move {
        start_gossip_worker(gossip_id, local_ip, gossip_peers, gossip_loads, gossip_mem).await;
    });

    start_hinted_handoff_worker(shared_hints, shared_peers.clone());
    start_sse_trigger_worker(shared_memory.clone(), tx);
    start_wal_compaction_worker(shared_memory.clone(), wal_path);
    start_lsm_flush_worker(shared_memory.clone(), shared_bloom.clone(), node_id.clone());
    start_lsm_compaction_worker(shared_bloom, node_id.clone());

    let discovery = Arc::new(DiscoveryService::new(8888, 8888).await?);
    start_discovery_broadcaster(discovery.clone(), shared_memory.clone(), node_id.clone());

    // 5. Main thread enters UDP Discovery loop
    run_delta_sync_loop(discovery, shared_peers, shared_memory, node_id).await;

    Ok(())
}