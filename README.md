# Cognitum

Cognitum is a distributed, masterless key-value storage engine written in Rust. It provides high availability and eventual consistency, utilizing an LSM-Tree storage architecture, quorum-based replication, and gossip-based cluster topology to handle network partitions and node failures gracefully.

## Architecture

The project is structured as a Cargo workspace with three highly decoupled crates:

### 1. cognitum-node (Storage Engine & API Layer)
The primary daemon responsible for local state, caching, and HTTP routing.
* **Storage Path**: Employs an in-memory MemTable backed by a Zero-Copy Write-Ahead Log (WAL) using `rkyv`.
* **Caching**: Implements a strict LRU Cache to minimize disk I/O on hot keys.
* **Cold Storage**: Flushes data to disk (located in the `data/` directory) as compressed SSTables.

### 2. cognitum-core (State & Conflict Resolution)
Provides the foundational mathematical and logical primitives for distributed state.
* **Conflict Resolution**: Resolves concurrent mutations using Last-Write-Wins (LWWMap) CRDTs to prevent Split-Brain scenarios.
* **Timekeeping**: Implements Hybrid Logical Clocks (HLC) and Vector Clocks for causal ordering.
* **Topology**: Manages cluster membership and peer health tracking.

### 3. cognitum-network (Transport & Gossip)
The communication backbone for internal cluster operations.
* **Routing**: Implements Consistent Hashing to distribute data across the topology.
* **Transport**: Utilizes gRPC (`tonic`) and Protocol Buffers for high-performance RPC calls, node discovery, and data replication.

## Getting Started

### Prerequisites
* Rust toolchain (stable)
* Protocol Buffers compiler (`protoc`)
* Docker & Docker Compose (for local cluster simulation)

### Quickstart: Running a Local Cluster

The easiest way to experience Cognitum's masterless topology is by spinning up a local multi-node cluster using Docker.

```bash
docker-compose up --build
```
This will initialize a cluster with multiple peer nodes. They will automatically discover each other via the Gossip protocol and establish a quorum.

### Interacting with the Database
Once the cluster is up, you can interact with any node's HTTP API (since the architecture is masterless, any node can act as a coordinator).

### 1. Write Data (Set)
```bash
curl -X POST http://localhost:3000/set \
-H "Content-Type: application/json" \
-d '{"key": "system_status", "value": "operational"}'
```
### 2. Read Data (Get)
```bash
curl -X GET "http://localhost:3000/get?key=system_status"
```
The coordinator will transparently route the read/write requests to the appropriate nodes using Consistent Hashing, handle the W+R>N quorum, and return the result.

### Running the Test Suite
Cognitum includes a comprehensive integration test suite that verifies distributed consensus, zero-copy serialization, and fault tolerance (e.g., network partitions, hinted handoff, and concurrent state merges).

To run the full suite with compiler optimizations enabled:
```bash
cargo test --release --workspace --nocapture
```
## Design Considerations
* **Memory Safety:** Strict adherence to safe Rust. Advanced memory optimizations (like zero-copy deserialization) are tightly scoped.


* **Fault Domain:** The architecture handles local persistence in the data/ directory. Network anomalies are handled via Hinted Handoff and Active Anti-Entropy.


* **CAP Theorem:** Cognitum is designed as an AP (Available and Partition-Tolerant) system. It prioritizes cluster availability and accepts eventual consistency under network partition conditions.

## License
This project is licensed under the MIT License.