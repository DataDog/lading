//! The passthru-file generator
//!
//! ## Metrics
//!
//! `bytes_written`: Bytes written successfully
//! `bytes_per_second`: Configured rate to send data
//! `file_open_failure`: Number of failed file opens
//! `file_write_failure`: Number of failed file writes
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

use std::{num::NonZeroU32, path::PathBuf, thread, time::Duration};
use tokio::io::AsyncWriteExt;

use byte_unit::Byte;
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::common::PeekableReceiver;
use lading_payload::block::{self, Block};

use super::General;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The local filesystem path to write data to
    pub path: String,
    /// The payload variant
    pub variant: lading_payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: Option<Byte>,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: Byte,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: Byte,
    /// The load throttle configuration
    pub throttle: Option<crate::generator::common::BytesThrottleConfig>,
}

/// Errors produced by [`PassthruFile`].
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
/// The passthru file generator.
///
/// This generator is responsible for sending data to a file on disk.
pub struct PassthruFile {
    path: PathBuf,
    throttle: Throttle,
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl PassthruFile {
    /// Create a new [`PassthruFile`] instance
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
            ("component_name".to_string(), "passthru_file".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let throttle_config = match (config.bytes_per_second, &config.throttle) {
            (Some(bytes_per_second), None) => {
                let bytes_per_second =
                    NonZeroU32::new(bytes_per_second.as_u128() as u32).ok_or(Error::Zero)?;
                gauge!("bytes_per_second", &labels).set(f64::from(bytes_per_second.get()));
                lading_throttle::Config::Stable {
                    maximum_capacity: bytes_per_second,
                }
            }
            (None, Some(throttle)) => (*throttle).try_into()?,
            (Some(_), Some(_)) => return Err(Error::ConflictingThrottleConfig),
            (None, None) => return Err(Error::NoThrottleConfig),
        };

        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;

        let maximum_block_size = config.maximum_block_size.as_u128();

        let block_cache = block::Cache::fixed(
            &mut rng,
            maximum_prebuild_cache_size_bytes,
            maximum_block_size,
            &config.variant,
        )?;

        let path = PathBuf::from(&config.path);

        Ok(Self {
            path,
            block_cache,
            throttle: Throttle::new_with_config(throttle_config),
            metric_labels: labels,
            shutdown,
        })
    }

    /// Run [`PassthruFile`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// If the underlying block cache fails to spin, an error will be returned.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        info!("PassthruFile generator running");

        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        let mut current_file = None;
        loop {
            let Some(ref mut current_file) = current_file else {
                match tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&self.path)
                    .await
                {
                    Ok(file) => {
                        info!("PassthruFile opened {path}", path = self.path.display());
                        current_file = Some(file);
                    }
                    Err(err) => {
                        warn!("file open failed: {}", err);

                        let mut error_labels = self.metric_labels.clone();
                        error_labels.push(("error".to_string(), err.to_string()));
                        counter!("file_open_failure", &error_labels).increment(1);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                continue;
            };

            let blk = rcv.peek().await.expect("block cache should never be empty");
            let total_bytes = blk.total_bytes;
            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let blk = rcv.next().await.expect("failed to advance through the blocks"); // actually advance through the blocks
                    match current_file.write_all(&blk.bytes).await {
                        Ok(()) => {
                            counter!("bytes_written", &self.metric_labels).increment(u64::from(blk.total_bytes.get()));
                        }
                        Err(err) => {
                            warn!("write failed: {}", err);

                            let mut error_labels = self.metric_labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("file_write_failure", &error_labels).increment(1);
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
