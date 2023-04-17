//! The Unix Domain Socket stream speaking blackhole.

use std::{io, path::PathBuf};

use futures::StreamExt;
use metrics::{register_counter, Counter};
use once_cell::sync;
use serde::Deserialize;
use tokio::net;
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::signals::Shutdown;

static BYTES_RECEIVED: sync::OnceCell<Counter> = sync::OnceCell::new();
#[inline]
fn bytes_received() -> &'static Counter {
    BYTES_RECEIVED.get_or_init(|| register_counter!("bytes_received"))
}

static MESSAGE_RECEIVED: sync::OnceCell<Counter> = sync::OnceCell::new();
#[inline]
fn message_received() -> &'static Counter {
    MESSAGE_RECEIVED.get_or_init(|| register_counter!("message_received"))
}

#[derive(Debug)]
/// Errors produced by [`UnixStream`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
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
}

impl UnixStream {
    /// Create a new [`UnixStream`] server instance
    #[must_use]
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self {
            path: config.path,
            shutdown,
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

        let connection_accepted = register_counter!("connection_accepted");

        loop {
            tokio::select! {
                conn = listener.accept() => {
                    let (socket, _) = conn.map_err(Error::Io)?;
                    connection_accepted.increment(1);
                    tokio::spawn(async move {
                        Self::handle_connection(socket).await;
                    });
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }

    async fn handle_connection(socket: net::UnixStream) {
        let mut stream = ReaderStream::new(socket);

        while let Some(msg) = stream.next().await {
            message_received().increment(1);
            if let Ok(msg) = msg {
                bytes_received().increment(msg.len() as u64);
            }
        }
    }
}
