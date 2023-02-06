//! The UDP protocol speaking generator.

use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use byte_unit::{Byte, ByteUnit};
use metrics::{counter, gauge};
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::net::UdpSocket;
use tracing::{debug, info, trace};

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
    throttle::Throttle,
};

#[derive(Debug, Deserialize, PartialEq, Eq)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The address for the target, must be a valid SocketAddr
    pub addr: String,
    /// The payload variant
    pub variant: payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
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
}

#[derive(Debug)]
/// The UDP generator.
///
/// This generator is responsible for sending data to the target via UDP
pub struct Udp {
    addr: SocketAddr,
    throttle: Throttle,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
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
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroUsize> = config
            .block_sizes
            .clone()
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1.0 / 32.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 16.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 4.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 2.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| NonZeroUsize::new(sz.get_bytes() as usize).expect("bytes must be non-zero"))
            .collect();
        let labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "udp".to_string()),
        ];

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

        let block_chunks = chunk_bytes(
            &mut rng,
            NonZeroUsize::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as usize)
                .expect("bytes must be non-zero"),
            &block_sizes,
        )?;
        let block_cache = construct_block_cache(&mut rng, &config.variant, &block_chunks, &labels);

        let addr = config
            .addr
            .to_socket_addrs()
            .expect("could not convert to socket")
            .next()
            .unwrap();
        Ok(Self {
            addr,
            block_cache,
            throttle: Throttle::new(bytes_per_second),
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
        let labels = self.metric_labels;

        let mut connection = Option::<UdpSocket>::None;
        let mut blocks = self.block_cache.iter().cycle().peekable();

        loop {
            let blk = blocks.peek().unwrap();
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

                            let mut error_labels = labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("connection_failure", 1, &error_labels);

                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = self.throttle.wait_for(total_bytes), if connection.is_some() => {
                    let sock = connection.unwrap();
                    let blk = blocks.next().unwrap(); // actually advance through the blocks
                    match sock.send_to(&blk.bytes, self.addr).await {
                        Ok(bytes) => {
                            counter!("bytes_written", bytes as u64, &labels);
                            connection = Some(sock);
                        }
                        Err(err) => {
                            debug!("write failed: {}", err);

                            let mut error_labels = labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("request_failure", 1, &error_labels);
                            connection = None;
                        }
                    }
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}
