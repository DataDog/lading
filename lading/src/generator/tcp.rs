//! The TCP protocol speaking generator.
//!
//! ## Metrics
//!
//! `bytes_written`: Bytes sent successfully
//! `packets_sent`: Packets sent successfully
//! `request_failure`: Number of failed writes; each occurrence causes a reconnect
//! `connection_failure`: Number of connection failures
//! `bytes_per_second`: Configured rate to send data
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU32,
    thread,
};

use byte_unit::ByteError;
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};
use tracing::{info, trace};

use crate::{common::PeekableReceiver, signals::Phase};
use lading_payload::block::{self, Block};

use super::General;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The address for the target, must be a valid SocketAddr
    pub addr: String,
    /// The payload variant
    pub variant: lading_payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Tcp`].
pub enum Error {
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] ByteError),
    /// Zero value error
    #[error("Value cannot be zero")]
    Zero,
}

#[derive(Debug)]
/// The TCP generator.
///
/// This generator is responsible for connecting to the target via TCP
pub struct Tcp {
    addr: SocketAddr,
    throttle: Throttle,
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    shutdown: Phase,
}

impl Tcp {
    /// Create a new [`Tcp`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(general: General, config: &Config, shutdown: Phase) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "tcp".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let bytes_per_second =
            NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).ok_or(Error::Zero)?;
        gauge!("bytes_per_second", &labels).set(f64::from(bytes_per_second.get()));

        let block_cache = block::Cache::fixed(
            &mut rng,
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                .ok_or(Error::Zero)?,
            config.maximum_block_size.get_bytes(),
            &config.variant,
        )?;

        let addr = config
            .addr
            .to_socket_addrs()
            .expect("could not convert to socket")
            .next()
            .expect("could not convert to socket addr");
        Ok(Self {
            addr,
            block_cache,
            throttle: Throttle::new_with_config(config.throttle, bytes_per_second),
            metric_labels: labels,
            shutdown,
        })
    }

    /// Run [`Tcp`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the TCP socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;

        let bytes_written = counter!("bytes_written", &self.metric_labels);
        let packets_sent = counter!("packets_sent", &self.metric_labels);
        let mut current_connection = None;

        loop {
            let Some(ref mut connection) = current_connection else {
                match TcpStream::connect(self.addr).await {
                    Ok(client) => {
                        current_connection = Some(client);
                    }
                    Err(err) => {
                        trace!("connection to {} failed: {}", self.addr, err);

                        let mut error_labels = self.metric_labels.clone();
                        error_labels.push(("error".to_string(), err.to_string()));
                        counter!("connection_failure", &error_labels).increment(1);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
                continue;
            };

            let blk = rcv.peek().await.expect("block cache should never be empty");
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let blk = rcv.next().await.expect("failed to advance through the blocks"); // actually advance through the blocks
                    match connection.write_all(&blk.bytes).await {
                        Ok(()) => {
                            bytes_written.increment(u64::from(blk.total_bytes.get()));
                            packets_sent.increment(1);
                        }
                        Err(err) => {
                            trace!("write failed: {}", err);

                            let mut error_labels = self.metric_labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("request_failure", &error_labels).increment(1);
                            current_connection = None;
                        }
                    }
                }
                () = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}
