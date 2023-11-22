//! The Unix Domain Socket stream speaking blackhole.
//!
//! ## Metrics
//!
//! `connection_accepted`: Incoming connections received
//! `bytes_received`: Total bytes received
//! `requests_received`: Total requests received
//!

use std::{io, path::PathBuf};

use futures::StreamExt;
use metrics::register_counter;
use serde::{Deserialize, Serialize};
use tokio::net;
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::signals::Shutdown;

use super::General;

#[derive(Debug)]
/// Errors produced by [`UnixStream`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Serialize)]
/// Configuration for [`UnixStream`].
pub struct Config {
    /// The path of the socket to read from.
    pub path: PathBuf,
}

#[derive(Debug)]
/// The `UnixStream` blackhole.
pub struct UnixStream {
    path: PathBuf,
    shutdown: Shutdown,
    metric_labels: Vec<(String, String)>,
}

impl UnixStream {
    /// Create a new [`UnixStream`] server instance
    #[must_use]
    pub fn new(general: General, config: Config, shutdown: Shutdown) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "unix_stream".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            path: config.path,
            shutdown,
            metric_labels,
        }
    }

    /// Run [`UnixStream`] to completion
    ///
    /// This function runs the UDS server forever, unless a shutdown signal is
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
        let listener = net::UnixListener::bind(&self.path).map_err(Error::Io)?;

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
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }

    async fn handle_connection(socket: net::UnixStream, labels: &'static [(String, String)]) {
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
}
