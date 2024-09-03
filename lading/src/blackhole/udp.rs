//! The UDP protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `packet_received`: Total packets received
//!

use std::{io, net::SocketAddr};

use metrics::counter;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tracing::info;

use super::General;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Udp`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(io::Error),
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Udp`].
pub struct Config {
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}

#[derive(Debug)]

/// The UDP blackhole.
pub struct Udp {
    binding_addr: SocketAddr,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl Udp {
    /// Create a new [`Udp`] server instance
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "udp".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            binding_addr: config.binding_addr,
            shutdown,
            metric_labels,
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

        let bytes_received = counter!("bytes_received", &self.metric_labels);
        let packet_received = counter!("packet_received", &self.metric_labels);

        loop {
            tokio::select! {
                packet = socket.recv_from(&mut buf) => {
                    let (bytes, _) = packet.map_err(Error::Io)?;
                    packet_received.increment(1);
                    bytes_received.increment(bytes as u64);
                }
                () = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
