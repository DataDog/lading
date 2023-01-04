//! The UDS speaking blackhole.

use std::{io, path::PathBuf};

use metrics::counter;
use serde::Deserialize;
use tokio::net::UnixStream;
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug)]
/// Errors produced by [`Uds`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
/// Configuration for [`Uds`].
pub struct Config {
    /// The path of the socket to read from.
    pub path: PathBuf,
}

#[derive(Debug)]
/// The UDP blackhole.
pub struct Uds {
    path: PathBuf,
    shutdown: Shutdown,
}

impl Uds {
    /// Create a new [`Uds`] server instance
    #[must_use]
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self {
            path: config.path,
            shutdown,
        }
    }

    /// Run [`Uds`] to completion
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
        let stream = UnixStream::connect(&self.path).await.map_err(Error::Io)?;

        loop {
            tokio::select! {
                readable = stream.readable() => {
                    readable.map_err(Error::Io)?;
                    let mut buf = [0; 65536];

                    match stream.try_read(&mut buf) {
                        Ok(0) => {
                            // Write side has hung up.
                            return Ok(());
                        }
                        Ok(n) => {
                            counter!("bytes_received", n as u64);
                        }
                        Err(ref err) if err.kind() == tokio::io::ErrorKind::WouldBlock => {
                            tokio::task::yield_now().await;
                        }
                        Err(err) => return Err(Error::Io(err)),
                    }
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
