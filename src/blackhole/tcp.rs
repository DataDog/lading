//! The TCP protocol speaking blackhole.

use std::{io, net::SocketAddr};

use futures::stream::StreamExt;
use metrics::{register_counter, Counter};
use once_cell::sync;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
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
/// Errors emitted by [`Tcp`]
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
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
}

impl Tcp {
    /// Create a new [`Tcp`] server instance
    #[must_use]
    pub fn new(config: &Config, shutdown: Shutdown) -> Self {
        Self {
            binding_addr: config.binding_addr,
            shutdown,
        }
    }

    async fn handle_connection(socket: TcpStream) {
        let mut stream = ReaderStream::new(socket);

        while let Some(msg) = stream.next().await {
            message_received().increment(1);
            if let Ok(msg) = msg {
                bytes_received().increment(msg.len() as u64);
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
}
