use byte_unit::{Byte, ByteUnit};
use governor::{
    clock, state,
    state::direct::{self, InsufficientCapacity},
    Quota, RateLimiter,
};
use lading_common::{
    block::{chunk_bytes, construct_block_cache, Block},
    payload,
};
use metrics::counter;
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU32,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The address for the target, must be a valid SocketAddr
    pub addr: String,
    /// The payload variant
    pub variant: GeneratorVariant,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorVariant {
    /// Generates Fluent messages
    Fluent,
    /// Generates syslog5424 messages
    Syslog5424,
}

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
}

impl From<InsufficientCapacity> for Error {
    fn from(error: InsufficientCapacity) -> Self {
        Error::Governor(error)
    }
}

pub struct Tcp {
    addr: SocketAddr,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
}

impl Tcp {
    /// Create a new [`Server`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed non-zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<usize> = config
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
            .map(|sz| sz.get_bytes() as usize)
            .collect();
        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        let rate_limiter = RateLimiter::direct(Quota::per_second(bytes_per_second));
        let labels = vec![];
        let block_chunks = chunk_bytes(
            &mut rng,
            config.maximum_prebuild_cache_size_bytes.get_bytes() as usize,
            &block_sizes,
        );
        let block_cache = match config.variant {
            GeneratorVariant::Syslog5424 => construct_block_cache(
                &mut rng,
                &payload::Syslog5424::default(),
                &block_chunks,
                &labels,
            ),
            GeneratorVariant::Fluent => construct_block_cache(
                &mut rng,
                &payload::Fluent::default(),
                &block_chunks,
                &labels,
            ),
        };

        let addr = config
            .addr
            .to_socket_addrs()
            .expect("could not convert to socket")
            .next()
            .unwrap();
        Ok(Self {
            addr,
            block_cache,
            rate_limiter,
            metric_labels: labels,
            shutdown,
        })
    }

    /// Enter the main loop of this [`Worker`]
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

        'connection: loop {
            tokio::select! {
                conn = TcpStream::connect(self.addr) => {
                    match conn {
                        Ok(mut client) => {
                            for blk in self.block_cache.iter().cycle() {
                                self.rate_limiter
                                    .until_n_ready(blk.total_bytes)
                                    .await
                                    .unwrap();
                                let block_length = blk.bytes.len();
                                match client.write_all(&blk.bytes).await {
                                    Ok(()) => {
                                        counter!(
                                            "bytes_written",
                                            u64::from(blk.total_bytes.get()),
                                            &labels
                                        );
                                    }
                                    Err(err) => {
                                        let mut error_labels = labels.clone();
                                        error_labels.push(("error".to_string(), err.to_string()));
                                        error_labels
                                            .push(("body_size".to_string(), block_length.to_string()));
                                        counter!("request_failure", 1, &error_labels);
                                        continue 'connection;
                                    }
                                }
                            }
                        }
                        Err(err) => {
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
