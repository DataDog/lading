//! The UDP protocol speaking generator.
//!
//! ## Metrics
//!
//! `bytes_written`: Bytes written successfully
//! `packets_sent`: Packets written successfully
//! `request_failure`: Number of failed writes; each occurrence causes a socket re-bind
//! `connection_failure`: Number of socket bind failures
//! `bytes_per_second`: Configured rate to send data
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU32,
    thread,
    time::Duration,
};

use byte_unit::{Byte, ByteError, ByteUnit};
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{debug, info, trace};

use crate::common::PeekableReceiver;
use lading_payload::block::{self, Block};

use super::General;

// https://stackoverflow.com/a/42610200
fn maximum_block_size() -> Byte {
    Byte::from_unit(65_507f64, ByteUnit::B).expect("catastrophic programming bug")
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The address for the target, must be a valid `SocketAddr`
    pub addr: String,
    /// The payload variant
    pub variant: lading_payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

/// Errors produced by [`Udp`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Creation of payload blocks failed.
    #[error("Creation of payload blocks failed: {0}")]
    Block(#[from] block::Error),
    /// Generic IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] ByteError),
    /// Failed to convert, value is 0
    #[error("Value provided is zero")]
    Zero,
}

#[derive(Debug)]
/// The UDP generator.
///
/// This generator is responsible for sending data to the target via UDP
pub struct Udp {
    addr: SocketAddr,
    throttle: Throttle,
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl Udp {
    /// Create a new [`Udp`] instance
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
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "udp".to_string()),
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
            .expect("block cache has no next block");

        Ok(Self {
            addr,
            block_cache,
            throttle: Throttle::new_with_config(config.throttle, bytes_per_second),
            metric_labels: labels,
            shutdown,
        })
    }

    /// Run [`Udp`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the UDP socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        debug!("UDP generator running");
        let mut connection = Option::<UdpSocket>::None;

        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let blk = rcv.peek().await.expect("block cache should never be empty");
            let total_bytes = blk.total_bytes;

            tokio::select! {
                conn = UdpSocket::bind("127.0.0.1:0"), if connection.is_none() => {
                    match conn {
                        Ok(sock) => {
                            debug!("UDP port bound");
                            connection = Some(sock);
                        }
                        Err(err) => {
                            trace!("binding UDP port failed: {}", err);

                            let mut error_labels = self.metric_labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("connection_failure", &error_labels).increment(1);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = self.throttle.wait_for(total_bytes), if connection.is_some() => {
                    let sock = connection.expect("connection failed");
                    let blk = rcv.next().await.expect("failed to advance through the blocks"); // actually advance through the blocks
                    match sock.send_to(&blk.bytes, self.addr).await {
                        Ok(bytes) => {
                            // This metric must be written with a single context
                            // or it will crash analysis. The simple way to
                            // accomplish that is to attach no labels to it.
                            counter!("bytes_written").increment(bytes as u64);

                            counter!("packets_sent", &self.metric_labels).increment(1);
                            connection = Some(sock);
                        }
                        Err(err) => {
                            debug!("write failed: {}", err);

                            let mut error_labels = self.metric_labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("request_failure", &error_labels).increment(1);
                            connection = None;
                        }
                    }
                }
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}
