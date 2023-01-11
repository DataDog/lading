//! The Unix Domain Socket datagram speaking blackhole.

use std::{io, path::PathBuf};

use futures::TryFutureExt;
use metrics::counter;
use serde::Deserialize;
use tokio::net;
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug)]
/// Errors produced by [`UnixDatagram`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
/// Configuration for [`UnixDatagram`].
pub struct Config {
    /// The path of the socket to read from.
    pub path: PathBuf,
}

#[derive(Debug)]
/// The `UnixDatagram` blackhole.
pub struct UnixDatagram {
    path: PathBuf,
    shutdown: Shutdown,
}

impl UnixDatagram {
    /// Create a new [`UnixDatagram`] server instance
    #[must_use]
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self {
            path: config.path,
            shutdown,
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
    pub async fn run(mut self) -> Result<(), Error> {
        // Sockets cannot be rebound if they existed previously. Delete the
        // socket, ignore any errors.
        let _res = tokio::fs::remove_file(&self.path).map_err(Error::Io);
        let socket = net::UnixDatagram::bind(&self.path).map_err(Error::Io)?;

        loop {
            let mut buf = [0; 65536];

            tokio::select! {
                res = socket.recv(&mut buf) => {
                    let n: usize = res.map_err(Error::Io)?;
                    counter!("bytes_received", n as u64);
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
