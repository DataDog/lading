//! The Unix Domain Socket datagram speaking generator.
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

use crate::common::PeekableReceiver;
use byte_unit::{Byte, Unit};
use futures::future::join_all;
use lading_payload::block::{self, Block};
use lading_throttle::Throttle;
use metrics::counter;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, path::PathBuf, thread};
use tokio::{
    net,
    sync::{broadcast::Receiver, mpsc},
    task::{JoinError, JoinHandle},
};
use tracing::{debug, error, info};

use super::General;

fn default_parallel_connections() -> u16 {
    1
}

// Mimic the belief of Datadog Agent, although correctly we should be reading
// sysctl values on Linux.
fn maximum_block_size() -> Byte {
    Byte::from_u64_with_unit(8_192, Unit::B).expect("catastrophic programming bug")
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path of the socket to write to.
    pub path: PathBuf,
    /// The payload variant
    pub variant: lading_payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: Option<byte_unit::Byte>,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    pub block_cache_method: block::CacheMethod,
    /// The total number of parallel connections to maintain
    #[serde(default = "default_parallel_connections")]
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<crate::generator::common::BytesThrottleConfig>,
}

/// Errors produced by [`UnixDatagram`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Creation of payload blocks failed.
    #[error("Creation of payload blocks failed: {0}")]
    Block(#[from] block::Error),
    /// Generic IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Subtask error
    #[error("Subtask failure: {0}")]
    Subtask(#[from] JoinError),
    /// Child sub-task error.
    #[error("Child join error: {0}")]
    Child(JoinError),
    /// Startup send error.
    #[error("Startup send error: {0}")]
    StartupSend(#[from] tokio::sync::broadcast::error::SendError<()>),
    /// Child startup wait error.
    #[error("Child startup wait error: {0}")]
    StartupWait(#[from] tokio::sync::broadcast::error::RecvError),
    /// Failed to convert, value is 0
    #[error("Value provided is zero")]
    Zero,
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    /// Both `bytes_per_second` and throttle configurations provided
    #[error("Both bytes_per_second and throttle configurations provided. Use only one.")]
    ConflictingThrottleConfig,
    /// No throttle configuration provided
    #[error("Either bytes_per_second or throttle configuration must be provided")]
    NoThrottleConfig,
}

#[derive(Debug)]
/// The Unix Domain Socket datagram generator.
///
/// This generator is responsible for sending data to the target via UDS
/// datagrams.
pub struct UnixDatagram {
    handles: Vec<JoinHandle<Result<(), Error>>>,
    shutdown: lading_signal::Watcher,
    startup: tokio::sync::broadcast::Sender<()>,
}

impl UnixDatagram {
    /// Create a new [`UnixDatagram`] instance
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
            ("component_name".to_string(), "unix_datagram".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let (startup, _startup_rx) = tokio::sync::broadcast::channel(1);

        let mut handles = Vec::new();
        for _ in 0..config.parallel_connections {
            let throttle_config = match (&config.bytes_per_second, &config.throttle) {
                (Some(bps), None) => {
                    let bytes_per_second =
                        NonZeroU32::new(bps.as_u128() as u32).ok_or(Error::Zero)?;
                    lading_throttle::Config::Stable {
                        maximum_capacity: bytes_per_second,
                    }
                }
                (None, Some(throttle_config)) => (*throttle_config).into(),
                (Some(_), Some(_)) => return Err(Error::ConflictingThrottleConfig),
                (None, None) => return Err(Error::NoThrottleConfig),
            };
            let throttle = Throttle::new_with_config(throttle_config);

            let total_bytes =
                NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                    .ok_or(Error::Zero)?;
            let block_cache = match config.block_cache_method {
                block::CacheMethod::Fixed => block::Cache::fixed(
                    &mut rng,
                    total_bytes,
                    config.maximum_block_size.as_u128(),
                    &config.variant,
                )?,
            };

            let child = Child {
                path: config.path.clone(),
                block_cache,
                throttle,
                metric_labels: labels.clone(),
                shutdown: shutdown.clone(),
            };

            handles.push(tokio::spawn(child.spin(startup.subscribe())));
        }

        Ok(Self {
            handles,
            shutdown,
            startup,
        })
    }

    /// Run [`UnixDatagram`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the UDS socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        self.startup.send(())?;
        self.shutdown.recv().await;
        info!("shutdown signal received");
        for res in join_all(self.handles.drain(..)).await {
            match res {
                Ok(Ok(())) => continue,
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(Error::Child(err)),
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Child {
    path: PathBuf,
    throttle: Throttle,
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl Child {
    async fn spin(mut self, mut startup_receiver: Receiver<()>) -> Result<(), Error> {
        startup_receiver.recv().await?;
        debug!("UnixDatagram generator running");
        let socket = net::UnixDatagram::unbound().map_err(Error::Io)?;
        loop {
            match socket.connect(&self.path).map_err(Error::Io) {
                Ok(()) => {
                    info!("Connected socket to {path}", path = &self.path.display(),);
                    break;
                }
                Err(err) => {
                    error!(
                        "Unable to connect to socket {path}: {err}",
                        path = &self.path.display()
                    );

                    let mut error_labels = self.metric_labels.clone();
                    error_labels.push(("error".to_string(), err.to_string()));
                    counter!("connection_failure", &error_labels).increment(1);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

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
                result = self.throttle.wait_for(total_bytes) => {
                    match result {
                        Ok(()) => {
                            // NOTE When we write into a unix socket it may be that only
                            // some of the written bytes make it through in which case
                            // we DO NOT cycle back around and try to write the
                            // remainder of the buffer. To do so would be to shear the
                            // block across multiple datagrams which we cannot do
                            // without cooperation of the client, which we are not
                            // guaranteed.
                            let blk = rcv.next().await.expect("failed to advance through blocks"); // actually advance through the blocks
                            match socket.send(&blk.bytes).await {
                                Ok(bytes) => {
                                    counter!("bytes_written", &self.metric_labels).increment(bytes as u64);
                                    counter!("packets_sent", &self.metric_labels).increment(1);
                                }
                                Err(err) => {
                                    debug!("write failed: {}", err);

                                    let mut error_labels = self.metric_labels.clone();
                                    error_labels.push(("error".to_string(), err.to_string()));
                                    counter!("request_failure", &error_labels).increment(1);
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
