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
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
};

use metrics::counter;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    task::{JoinError, JoinSet},
    time::Duration,
};
use tracing::{error, info, trace};

use lading_payload::block;

use super::General;
use crate::generator::common::{
    BlockThrottle, ConcurrencyStrategy, MetricsBuilder, ThrottleConfig, ThrottleConversionError,
    create_throttle,
};

fn default_parallel_connections() -> u16 {
    1
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
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
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The total number of parallel connections to maintain
    #[serde(default = "default_parallel_connections")]
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<ThrottleConfig>,
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
    Byte(#[from] byte_unit::ParseError),
    /// Zero value error
    #[error("Value cannot be zero")]
    Zero,
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] ThrottleConversionError),
    /// Throttle error
    #[error("Throttle error: {0}")]
    Throttle(#[from] lading_throttle::Error),
    /// Child sub-task error.
    #[error("Child join error: {0}")]
    Child(JoinError),
    /// Error connecting to TCP endpoint
    #[error("Failed to connect to TCP address {addr}: {source}")]
    ConnectionFailed {
        /// Target address
        addr: String,
        /// Underlying IO error
        #[source]
        source: Box<std::io::Error>,
    },
    /// Error writing to TCP socket
    #[error("Failed to write to TCP address {addr}: {source}")]
    WriteFailed {
        /// Target address
        addr: String,
        /// Bytes sent before error
        bytes_sent: usize,
        /// Underlying IO error
        #[source]
        source: Box<std::io::Error>,
    },
}

#[derive(Debug)]
/// The TCP generator.
///
/// This generator is responsible for connecting to the target via TCP
pub struct Tcp {
    handles: JoinSet<Result<(), Error>>,
    shutdown: lading_signal::Watcher,
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
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let labels = MetricsBuilder::new("tcp").with_id(general.id).build();

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
            true, // Use Workers for TCP
        );
        let worker_count = concurrency.connection_count();
        let mut handles = JoinSet::new();

        for i in 0..worker_count {
            let throttle =
                create_throttle(config.throttle.as_ref(), config.bytes_per_second.as_ref())?
                    .divide(
                        NonZeroU32::new(worker_count.into()).expect("worker_count is always >= 1"),
                    )?;

            let mut worker_labels = labels.clone();
            if worker_count > 1 {
                worker_labels.push(("worker".to_string(), i.to_string()));
            }

            let worker = TcpWorker {
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

struct TcpWorker {
    addr: SocketAddr,
    throttle: BlockThrottle,
    block_cache: Arc<block::Cache>,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl TcpWorker {
    async fn spin(mut self) -> Result<(), Error> {
        let mut handle = self.block_cache.handle();
        let mut current_connection = None;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let Some(ref mut connection) = current_connection else {
                match TcpStream::connect(self.addr).await {
                    Ok(client) => {
                        current_connection = Some(client);
                    }
                    Err(source) => {
                        trace!(
                            "Failed to connect to TCP address {addr}: {source}",
                            addr = self.addr
                        );

                        let mut error_labels = self.metric_labels.clone();
                        error_labels.push(("error".to_string(), source.to_string()));
                        counter!("connection_failure", &error_labels).increment(1);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                continue;
            };

            tokio::select! {
                result = self.throttle.wait_for_block(&self.block_cache, &handle) => {
                    match result {
                        Ok(()) => {
                            let block = self.block_cache.advance(&mut handle);
                            match connection.write_all(&block.bytes).await {
                                Ok(()) => {
                                    counter!("bytes_written", &self.metric_labels).increment(u64::from(block.total_bytes.get()));
                                    counter!("packets_sent", &self.metric_labels).increment(1);
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
                        Err(err) => {
                            error!("Discarding block due to throttle error: {err}");
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
