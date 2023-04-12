//! The TCP protocol speaking generator.

use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::{NonZeroU32, NonZeroUsize},
};

use byte_unit::{Byte, ByteUnit};
use metrics::{counter, gauge, register_counter};
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tracing::{info, trace};

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

#[derive(thiserror::Error, Debug, Copy, Clone)]
/// Errors produced by [`Tcp`].
pub enum Error {
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(block::Error),
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Self {
        Error::Block(error)
    }
}

#[derive(Debug)]
/// The TCP generator.
///
/// This generator is responsible for connecting to the target via TCP
pub struct Tcp {
    addr: SocketAddr,
    throttle: Throttle,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
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
            ("component_name".to_string(), "tcp".to_string()),
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
        let labels = self.metric_labels;

        let mut connection = None;
        let mut blocks = self.block_cache.iter().cycle().peekable();

        let bytes_written = register_counter!("bytes_written", &labels);

        loop {
            let blk = blocks.peek().unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                biased;

                _ = self.throttle.wait_for(total_bytes), if connection.is_some() => {
                    let mut client: TcpStream = connection.unwrap();
                    let blk = blocks.next().unwrap(); // actually advance through the blocks
                    match client.write_all(&blk.bytes).await {
                        Ok(()) => {
                            bytes_written.increment(u64::from(blk.total_bytes.get()));
                            connection = Some(client);
                        }
                        Err(err) => {
                            trace!("write failed: {}", err);

                            let mut error_labels = labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("request_failure", 1, &error_labels);
                            connection = None;
                        }
                    }
                }
                conn = TcpStream::connect(self.addr), if connection.is_none() => {
                    match conn {
                        Ok(client) => {
                            connection = Some(client);
                        }
                        Err(err) => {
                            // TODO should sleep on connection failure since we
                            // generate 77k attempts in capture output in one
                            // run alone
                            trace!("connection to {} failed: {}", self.addr, err);

                            let mut error_labels = labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("connection_failure", 1, &error_labels);
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
