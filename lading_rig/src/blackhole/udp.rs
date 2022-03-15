use metrics::counter;
use serde::Deserialize;
use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;
use tracing::info;

use crate::signals::Shutdown;

pub enum Error {
    Io(io::Error),
}

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}

pub struct Udp {
    binding_addr: SocketAddr,
    shutdown: Shutdown,
}

impl Udp {
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self {
            binding_addr: config.binding_addr,
            shutdown,
        }
    }

    pub async fn run(mut self) -> Result<(), Error> {
        let socket = UdpSocket::bind(&self.binding_addr)
            .await
            .map_err(Error::Io)?;
        let mut buf: Vec<u8> = vec![0; 4096];

        loop {
            tokio::select! {
                packet = socket.recv_from(&mut buf) => {
                    counter!("packet_received", 1);
                    packet.map_err(Error::Io)?;
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
