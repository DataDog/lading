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
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
    time::Duration,
};

use byte_unit::{Byte, Unit};
use lading_throttle::Throttle;
use metrics::counter;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    task::{JoinError, JoinSet},
};
use tracing::{debug, error, info, trace};

use lading_payload::block;

use super::General;
use crate::generator::common::{
    ConcurrencyStrategy, MetricsBuilder, ThrottleBuilder, ThrottleBuilderError,
};

fn default_parallel_connections() -> u16 {
    1
}

// https://stackoverflow.com/a/42610200
fn maximum_block_size() -> Byte {
    Byte::from_u64_with_unit(65_507, Unit::B).expect("catastrophic programming bug")
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
    pub bytes_per_second: Option<byte_unit::Byte>,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The total number of parallel connections to maintain
    #[serde(default = "default_parallel_connections")]
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<crate::generator::common::BytesThrottleConfig>,
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
    Byte(#[from] byte_unit::ParseError),
    /// Failed to convert, value is 0
    #[error("Value provided is zero")]
    Zero,
    /// Throttle builder error
    #[error("Throttle configuration error: {0}")]
    ThrottleBuilder(#[from] ThrottleBuilderError),
    /// Child sub-task error.
    #[error("Child join error: {0}")]
    Child(JoinError),
}

#[derive(Debug)]
/// The UDP generator.
///
/// This generator is responsible for sending data to the target via UDP
pub struct Udp {
    handles: JoinSet<Result<(), Error>>,
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
        let labels = MetricsBuilder::new("udp").with_id(general.id).build();

        let total_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;

        let block_cache = Arc::new(block::Cache::fixed_with_max_overhead(
            &mut rng,
            total_bytes,
            config.maximum_block_size.as_u128(),
            &config.variant,
            total_bytes.get() as usize,
        )?);

        let addr = config
            .addr
            .to_socket_addrs()
            .expect("could not convert to socket")
            .next()
            .expect("could not convert to socket addr");

        let concurrency = ConcurrencyStrategy::new(
            NonZeroU16::new(config.parallel_connections),
            true, // Use Workers for UDP
        );
        let worker_count = concurrency.connection_count();
        let mut handles = JoinSet::new();

        for i in 0..worker_count {
            let throttle = ThrottleBuilder::new()
                .bytes_per_second(config.bytes_per_second.as_ref())
                .throttle_config(config.throttle.as_ref())
                .parallel_connections(NonZeroU16::new(worker_count))
                .build()?;

            let mut worker_labels = labels.clone();
            if worker_count > 1 {
                worker_labels.push(("worker".to_string(), i.to_string()));
            }

            let worker = UdpWorker {
                addr,
                throttle,
                block_cache: Arc::clone(&block_cache),
                metric_labels: worker_labels,
                shutdown: shutdown.clone(),
            };
            handles.spawn(worker.spin());
        }

        Ok(Self { handles, shutdown })
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
        self.shutdown.recv().await;
        info!("shutdown signal received");

        while let Some(res) = self.handles.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(Error::Child(err)),
            }
        }
        Ok(())
    }
}

struct UdpWorker {
    addr: SocketAddr,
    throttle: Throttle,
    block_cache: Arc<block::Cache>,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl UdpWorker {
    async fn spin(mut self) -> Result<(), Error> {
        debug!("UDP generator worker running");
        let mut connection = Option::<UdpSocket>::None;
        let mut handle = self.block_cache.handle();

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let total_bytes = self.block_cache.peek_next_size(&handle);

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
                result = self.throttle.wait_for(total_bytes), if connection.is_some() => {
                    match result {
                        Ok(()) => {
                            let sock = connection.expect("connection failed");
                            let block = self.block_cache.advance(&mut handle);
                            match sock.send_to(&block.bytes, self.addr).await {
                                Ok(bytes) => {
                                    counter!("bytes_written", &self.metric_labels).increment(bytes as u64);
                                    counter!("packets_sent", &self.metric_labels).increment(1);
                                    connection = Some(sock);
                                }
                                Err(err) => {
                                    debug!("write failed: {}", err);

                                    let mut error_labels = self.metric_labels.clone();
                                    error_labels.push(("error".to_string(), err.to_string()));
                                    counter!("write_failure", &error_labels).increment(1);
                                    connection = None;
                                }
                            }
                        }
                        Err(err) => {
                            error!("Throttle request of {total_bytes} is larger than throttle capacity. Block will be discarded. Error: {err}");
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
