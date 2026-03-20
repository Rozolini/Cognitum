//! Network layer for the Cognitum database.
//!
//! Manages node-to-node communication, service discovery,
//! and gRPC-based state synchronization.

/// Auto-generated gRPC structures and traits from `cognitum.proto`.
pub mod pb {
    tonic::include_proto!("cognitum");
}

/// gRPC server implementation for handling peer requests.
pub mod server;

/// gRPC client for initiating state syncs with other nodes.
pub mod client;

/// Peer discovery mechanism to dynamically find cluster members.
pub mod discovery;