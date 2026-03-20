use crate::pb::{gossip_service_server::GossipService, SyncRequest, SyncResponse};
use cognitum_core::crdt::LWWMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// gRPC server implementation responsible for handling incoming
/// state synchronization requests from other cluster nodes.
pub struct CognitumNodeServer {
    /// Thread-safe, shared reference to the node's local CRDT state.
    pub memory: Arc<RwLock<LWWMap<String, String>>>,
}

impl CognitumNodeServer {
    pub fn new(memory: Arc<RwLock<LWWMap<String, String>>>) -> Self {
        Self { memory }
    }
}

#[tonic::async_trait]
impl GossipService for CognitumNodeServer {
    /// Handles incoming Gossip requests by deserializing the remote state
    /// and performing a conflict-free merge with the local CRDT.
    async fn sync_state(
        &self,
        request: Request<SyncRequest>,
    ) -> Result<Response<SyncResponse>, Status> {
        let req = request.into_inner();

        // 1. Deserialize the binary payload back into an LWWMap structure
        let incoming_state: LWWMap<String, String> = bincode::deserialize(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Deserialization error: {}", e)))?;

        // 2. Acquire a write lock and perform a conflict-free merge
        let mut local_state = self.memory.write().await;
        local_state.merge(incoming_state);

        // 3. Acknowledge successful synchronization
        Ok(Response::new(SyncResponse {
            success: true,
            message: "State merged successfully".into(),
        }))
    }
}