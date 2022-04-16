use std::{io, net::SocketAddr};

use futures::stream::StreamExt;
use metrics::counter;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct Config {
    /// address -- IP plus port -- to bind to
    binding_addr: SocketAddr,
}

#[derive(Debug)]
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

        while stream.next().await.is_some() {
            counter!("message_received", 1);
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

        loop {
            tokio::select! {
                conn = listener.accept() => {
                    let (socket, _) = conn.map_err(Error::Io)?;
                    counter!("connection_accepted", 1);
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
