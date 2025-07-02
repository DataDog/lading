//! The Unix Domain Socket stream speaking generator.
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
use lading_payload::block::{self, Block};
use lading_throttle::Throttle;
use metrics::counter;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, path::PathBuf, thread};
use tokio::{net, sync::mpsc, task::JoinError};
use tracing::{debug, error, info, warn};

use super::General;

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
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    pub block_cache_method: block::CacheMethod,
    /// The load throttle configuration
    pub throttle: Option<crate::generator::common::BytesThrottleConfig>,
}

/// Errors produced by [`UnixStream`].
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
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    /// Failed to convert, value is 0
    #[error("Value provided must not be zero")]
    Zero,
    /// Both `bytes_per_second` and throttle config were specified
    #[error("Cannot specify both bytes_per_second and throttle configuration")]
    ConflictingThrottleConfig,
    /// No throttle configuration provided
    #[error("Must specify either bytes_per_second or throttle configuration")]
    NoThrottleConfig,
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] crate::generator::common::ThrottleConversionError),
}

#[derive(Debug)]
/// The Unix Domain Socket stream generator.
///
/// This generator is responsible for sending data to the target via UDS
/// streams.
pub struct UnixStream {
    path: PathBuf,
    throttle: Throttle,
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl UnixStream {
    /// Create a new [`UnixStream`] instance
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
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "unix_stream".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let throttle_config = match (config.bytes_per_second, &config.throttle) {
            (Some(bytes_per_second), None) => {
                let bytes_per_second =
                    NonZeroU32::new(bytes_per_second.as_u128() as u32).ok_or(Error::Zero)?;
                lading_throttle::Config::Stable {
                    maximum_capacity: bytes_per_second,
                }
            }
            (None, Some(throttle)) => throttle.clone().try_into()?,
            (Some(_), Some(_)) => return Err(Error::ConflictingThrottleConfig),
            (None, None) => return Err(Error::NoThrottleConfig),
        };

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

        Ok(Self {
            path: config.path,
            block_cache,
            throttle: Throttle::new_with_config(throttle_config),
            metric_labels: labels,
            shutdown,
        })
    }

    /// Run [`UnixStream`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the UDS socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        debug!("UnixStream generator running");

        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;

        let mut current_connection = None;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let Some(ref socket) = current_connection else {
                match net::UnixStream::connect(&self.path).await {
                    Ok(socket) => {
                        info!(
                            "Connected socket to path {path}",
                            path = &self.path.display()
                        );
                        current_connection = Some(socket);
                    }
                    Err(err) => {
                        error!("Failed to connect to UDS socket at path: {}", err);

                        let mut error_labels = self.metric_labels.clone();
                        error_labels.push(("error".to_string(), err.to_string()));
                        counter!("connection_failure", &error_labels).increment(1);

                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                }
                continue;
            };

            let blk = rcv.peek().await.expect("block cache should never be empty");
            let total_bytes = blk.total_bytes;

            tokio::select! {
                result = self.throttle.wait_for(total_bytes) => {
                    match result {
                        Ok(()) => {
                            // NOTE When we write into a unix stream it may be that only
                            // some of the written bytes make it through in which case we
                            // must cycle back around and try to write the remainder of the
                            // buffer.
                            let blk_max: usize = total_bytes.get() as usize;
                            let mut blk_offset = 0;
                            let blk = rcv.next().await.expect("failed to advance to the next block"); // advance to the block that was previously peeked
                            while blk_offset < blk_max {
                                let stream = &socket;

                                let ready = stream
                                    .ready(tokio::io::Interest::WRITABLE)
                                    .await
                                    .map_err(Error::Io)?;
                                if ready.is_writable() {
                                    // Try to write data, this may still fail with `WouldBlock`
                                    // if the readiness event is a false positive.
                                    match stream.try_write(&blk.bytes[blk_offset..]) {
                                        Ok(bytes) => {
                                            counter!("bytes_written", &self.metric_labels).increment(bytes as u64);
                                            counter!("packets_sent", &self.metric_labels).increment(1);
                                            blk_offset += bytes;
                                        }
                                        Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                                            // If the read side has hung up we will never
                                            // know and will keep attempting to write into
                                            // the stream. This yield means we won't hog the
                                            // whole CPU.
                                            tokio::task::yield_now().await;
                                        }
                                        Err(err ) => {
                                            warn!("write failed: {}", err);
                                            if err.kind() == tokio::io::ErrorKind::BrokenPipe {
                                                warn!("Broken pipe, reconnecting");
                                                current_connection = None;
                                                break;
                                            }

                                            let mut error_labels = self.metric_labels.clone();
                                            error_labels.push(("error".to_string(), err.to_string()));
                                            counter!("request_failure", &error_labels).increment(1);
                                        }
                                    }
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
