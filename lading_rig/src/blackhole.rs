use crate::signals::Shutdown;
use futures::stream::StreamExt;
use metrics::counter;
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::io::ReaderStream;
use tracing::info;

pub struct Server {
    addr: SocketAddr,
    shutdown: Shutdown,
}

impl Server {
    pub fn new(addr: SocketAddr, shutdown: Shutdown) -> Self {
        Self { addr, shutdown }
    }

    async fn handle_connection(socket: TcpStream) {
        let mut stream = ReaderStream::new(socket);

        while let Some(_) = stream.next().await {
            counter!("message_received", 1);
        }
    }

    pub async fn run(mut self) -> Result<(), io::Error> {
        let listener = TcpListener::bind(self.addr).await.unwrap();

        loop {
            tokio::select! {
                conn = listener.accept() => {
                    let (socket, _) = conn?;
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
