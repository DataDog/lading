//! The Unix Domain Socket datagram speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//!

use std::{io, path::PathBuf};

use futures::TryFutureExt;
use metrics::counter;
use serde::{Deserialize, Serialize};
use tokio::net;
use tracing::info;

use super::General;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`UnixDatagram`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(io::Error),
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`UnixDatagram`].
pub struct Config {
    /// The path of the socket to read from.
    pub path: PathBuf,
}

#[derive(Debug)]
/// The `UnixDatagram` blackhole.
pub struct UnixDatagram {
    path: PathBuf,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl UnixDatagram {
    /// Create a new [`UnixDatagram`] server instance
    #[must_use]
    pub fn new(general: General, config: Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "unix_datagram".to_string()),
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

    /// Run [`UnixDatagram`] to completion
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
    pub async fn run(self) -> Result<(), Error> {
        // Sockets cannot be rebound if they existed previously. Delete the
        // socket, ignore any errors.
        let _res = tokio::fs::remove_file(&self.path).map_err(Error::Io);
        let socket = net::UnixDatagram::bind(&self.path).map_err(Error::Io)?;
        let mut buf = [0; 65536];

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                res = socket.recv(&mut buf) => {
                    let n: usize = res.map_err(Error::Io)?;
                    counter!("bytes_received", &self.metric_labels).increment(n as u64);
                }
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
