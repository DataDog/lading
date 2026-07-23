//! TCP sink binary for the lading Antithesis harness.
//!
//! Binds a fixed TCP port, counts every byte it receives, and fires the
//! Antithesis `sometimes!` assertion that load arrived. It is the oracle in the
//! "general" scenario: always on, never faulted, and independent of the lading
//! config under test.

use std::sync::Arc;

use anyhow::Context;
use sink::Counter;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

/// Address the sink listens on unless `SINK_LISTEN_ADDR` overrides it.
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:9000";

/// Per-connection read buffer size.
const READ_BUFFER_BYTES: usize = 64 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Initialize the Antithesis SDK so the sink's assertion reaches the platform.
    lading_antithesis::init();

    let addr =
        std::env::var("SINK_LISTEN_ADDR").unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string());
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind sink listener on {addr}"))?;
    info!("sink listening on {addr}");

    let counter = Arc::new(Counter::new());

    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    debug!("accepted connection from {peer}");
                    let counter = Arc::clone(&counter);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, &counter).await {
                            error!("connection from {peer} ended with error: {e}");
                        }
                    });
                }
                Err(e) => error!("accept failed: {e}"),
            },
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received, sink exiting");
                break;
            }
        }
    }

    Ok(())
}

/// Read from `stream` until EOF, recording received bytes into `counter` and
/// asserting that load arrived.
async fn handle_connection(mut stream: TcpStream, counter: &Counter) -> anyhow::Result<()> {
    let mut buf = vec![0u8; READ_BUFFER_BYTES];
    loop {
        let n = stream
            .read(&mut buf)
            .await
            .context("read from connection")?;
        if n == 0 {
            return Ok(()); // peer closed the connection
        }
        let recorded = u64::try_from(n).unwrap_or(u64::MAX);
        let total = counter.record(recorded);
        // The claim: lading pushed load and it arrived here.
        lading_antithesis::sometimes!(total > 0, "sink received bytes");
    }
}
