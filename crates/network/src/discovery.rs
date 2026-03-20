use std::net::SocketAddr;
use tokio::net::UdpSocket;
use std::time::Duration;

/// UDP-based peer discovery service.
/// Allows nodes to dynamically find each other on the local network using broadcast messages.
pub struct DiscoveryService {
    socket: UdpSocket,
    broadcast_addr: SocketAddr,
}

impl DiscoveryService {
    /// Initializes the UDP socket for listening and configures it for broadcasting.
    pub async fn new(listen_port: u16, broadcast_port: u16) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", listen_port)).await?;
        socket.set_broadcast(true)?;

        let broadcast_addr = format!("255.255.255.255:{}", broadcast_port).parse().unwrap();

        Ok(Self { socket, broadcast_addr })
    }

    /// Announces this node's presence to the network.
    pub async fn broadcast_presence(&self, node_id: String) -> std::io::Result<()> {
        let msg = format!("COGNITUM_ALIVE:{}", node_id);
        self.socket.send_to(msg.as_bytes(), self.broadcast_addr).await?;
        Ok(())
    }

    /// Listens for broadcast messages from other peers to discover their IDs and addresses.
    pub async fn listen_for_peers(&self) -> std::io::Result<(String, SocketAddr)> {
        let mut buf = [0u8; 1024];
        let (len, addr) = self.socket.recv_from(&mut buf).await?;
        let msg = String::from_utf8_lossy(&buf[..len]);

        if msg.starts_with("COGNITUM_ALIVE:") {
            let id = msg.strip_prefix("COGNITUM_ALIVE:").unwrap().to_string();
            return Ok((id, addr));
        }

        Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid discovery message"))
    }
}