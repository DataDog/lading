//! The UDP protocol speaking blackhole.

use std::{io, net::SocketAddr};

use metrics::register_counter;
use serde::Deserialize;
use tokio::net::UdpSocket;
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug)]
/// Errors produced by [`Udp`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
/// Configuration for [`Udp`].
pub struct Config {
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}

#[derive(Debug)]
/// The UDP blackhole.
pub struct Udp {
    binding_addr: SocketAddr,
    shutdown: Shutdown,
}

impl Udp {
    /// Create a new [`Udp`] server instance
    #[must_use]
    pub fn new(config: &Config, shutdown: Shutdown) -> Self {
        Self {
            binding_addr: config.binding_addr,
            shutdown,
        }
    }

    /// Run [`Udp`] to completion
    ///
    /// This function runs the UDP server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if receiving a packet fails.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn run(mut self) -> Result<(), Error> {
        let socket = UdpSocket::bind(&self.binding_addr)
            .await
            .map_err(Error::Io)?;
        let mut buf = [0; 65536];

        let bytes_received = register_counter!("bytes_received");
        let packet_received = register_counter!("packet_received");

        loop {
            tokio::select! {
                packet = socket.recv_from(&mut buf) => {
                    let (bytes, _) = packet.map_err(Error::Io)?;
                    packet_received.increment(1);
                    bytes_received.increment(bytes as u64);
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
