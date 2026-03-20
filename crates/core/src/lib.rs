//! Cognitum core database engine.

/// Hybrid Logical Clocks (HLC) for distributed time synchronization.
pub mod clock;

/// Conflict-free Replicated Data Types (CRDT) like LWW-Map.
pub mod crdt;

/// Cluster membership and failure detection.
pub mod membership;