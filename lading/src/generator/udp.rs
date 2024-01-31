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
use metrics::{counter, gauge, register_counter};
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{debug, info, trace};

use crate::{common::PeekableReceiver, signals::Phase};
use lading_payload::block::{self, Block};

use super::General;

#[derive(Debug, Deserialize, PartialEq)]
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
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
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
    #[error("Failed to convert into bytes {0}")]
    Byte(#[from] ByteError),
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
    shutdown: Phase,
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
    pub fn new(general: General, config: &Config, shutdown: Phase) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroU32> = config
            .block_sizes
            .clone()
            .map_or(
                vec![
                    Byte::from_unit(1.0 / 32.0, ByteUnit::MB),
                    Byte::from_unit(1.0 / 16.0, ByteUnit::MB),
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB),
                    Byte::from_unit(1.0 / 4.0, ByteUnit::MB),
                    Byte::from_unit(1.0 / 2.0, ByteUnit::MB),
                    Byte::from_unit(1_f64, ByteUnit::MB),
                    Byte::from_unit(2_f64, ByteUnit::MB),
                    Byte::from_unit(4_f64, ByteUnit::MB),
                ],
                |sizes| sizes.iter().map(|sz| Ok(*sz)).collect::<Vec<_>>(),
            )
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(|sz| NonZeroU32::new(sz.get_bytes() as u32).expect("bytes must be non-zero"))
            .collect();
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "udp".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32)
            .expect("bytes must be non-zero");
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

        let block_cache = block::Cache::fixed(
            &mut rng,
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                .expect("bytes must be non-zero"),
            &block_sizes,
            &config.variant,
        )?;

        let addr = config
            .addr
            .to_socket_addrs()
            .expect("could not convert to socket")
            .next()
            .expect("iterator already finished");

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

        let bytes_written = register_counter!("bytes_written", &self.metric_labels);
        let packets_sent = register_counter!("packets_sent", &self.metric_labels);

        loop {
            let blk = rcv.peek().await.expect("block cache has no next block");
            let total_bytes = blk.total_bytes;
            assert!(
                total_bytes.get() <= 65507,
                "UDP packet too large (over 65507 B)"
            );

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
                            counter!("connection_failure", 1, &error_labels);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = self.throttle.wait_for(total_bytes), if connection.is_some() => {
                    let sock = connection.expect("connection failed");
                    let blk = rcv.next().await.expect("failed to advance through blocks"); // actually advance through the blocks
                    match sock.send_to(&blk.bytes, self.addr).await {
                        Ok(bytes) => {
                            bytes_written.increment(bytes as u64);
                            packets_sent.increment(1);
                            connection = Some(sock);
                        }
                        Err(err) => {
                            debug!("write failed: {}", err);

                            let mut error_labels = self.metric_labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("request_failure", 1, &error_labels);
                            connection = None;
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
