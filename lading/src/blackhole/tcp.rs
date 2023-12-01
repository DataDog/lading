//! The TCP protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `connection_accepted`: Incoming connections received
//! `bytes_received`: Total bytes received
//! `message_received`: Total messages received
//!

use std::{io, net::SocketAddr};

use futures::stream::StreamExt;
use metrics::register_counter;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::signals::Shutdown;

use super::General;

#[derive(Debug)]
/// Errors emitted by [`Tcp`]
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Tcp`]
pub struct Config {
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}

#[derive(Debug)]
/// The TCP blackhole.
pub struct Tcp {
    binding_addr: SocketAddr,
    shutdown: Shutdown,
    metric_labels: Vec<(String, String)>,
}

impl Tcp {
    /// Create a new [`Tcp`] server instance
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: Shutdown) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "tcp".to_string()),
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

    async fn handle_connection(socket: TcpStream, labels: &'static [(String, String)]) {
        let mut stream = ReaderStream::new(socket);
        let bytes_received = register_counter!("bytes_received", labels);
        let message_received = register_counter!("message_received", labels);

        while let Some(msg) = stream.next().await {
            message_received.increment(1);
            if let Ok(msg) = msg {
                bytes_received.increment(msg.len() as u64);
            }
        }
    }

    /// Run [`Tcp`] to completion
    ///
    /// This function runs the TCP server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if binding to the assigned address fails.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn run(mut self) -> Result<(), Error> {
        let listener = TcpListener::bind(self.binding_addr)
            .await
            .map_err(Error::Io)?;

        let connection_accepted = register_counter!("connection_accepted", &self.metric_labels);
        let labels: &'static _ = Box::new(self.metric_labels.clone()).leak();

        loop {
            tokio::select! {
                conn = listener.accept() => {
                    let (socket, _) = conn.map_err(Error::Io)?;
                    connection_accepted.increment(1);
                    tokio::spawn(
                        Self::handle_connection(socket, labels)
                    );
                }
                () = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
