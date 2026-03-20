use crate::pb::{gossip_service_client::GossipServiceClient, SyncRequest};
use cognitum_core::crdt::LWWMap;
use tonic::transport::Channel;

/// gRPC client for node-to-node communication.
pub struct CognitumNodeClient {
    client: GossipServiceClient<Channel>,
}

impl CognitumNodeClient {
    /// Establishes a gRPC connection to a remote node (e.g., "http://127.0.0.1:50051").
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = GossipServiceClient::connect(addr).await?;
        Ok(Self { client })
    }

    /// Serializes and transmits the local CRDT state to a remote peer.
    pub async fn sync_state(
        &mut self,
        node_id: String,
        local_state: &LWWMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize the CRDT state into a byte array for efficient network transmission
        let payload = bincode::serialize(local_state)?;

        let request = tonic::Request::new(SyncRequest { node_id, payload });

        // Execute the RPC call to the target node
        let response = self.client.sync_state(request).await?;

        println!("Sync successful: {}", response.into_inner().message);

        Ok(())
    }
}