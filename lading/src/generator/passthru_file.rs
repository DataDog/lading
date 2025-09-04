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

use std::{num::NonZeroU32, path::PathBuf, time::Duration};
use tokio::io::AsyncWriteExt;

use byte_unit::Byte;
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use lading_payload::block;

use super::General;
use lading_throttle::{BytesThrottleConfig, ThrottleBuilder, ThrottleBuilderError};

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
    pub throttle: Option<BytesThrottleConfig>,
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
    /// Throttle builder error
    #[error("Throttle configuration error: {0}")]
    ThrottleBuilder(#[from] lading_throttle::ThrottleBuilderError),
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

        let throttle = ThrottleBuilder::new()
            .bytes_per_second(config.bytes_per_second.as_ref())
            .throttle_config(config.throttle.as_ref())
            .build()?;

        if let Some(bytes_per_second) = config.bytes_per_second {
            gauge!("bytes_per_second", &labels).set(bytes_per_second.as_u128() as f64 / 1000.0);
        }

        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;

        let maximum_block_size = config.maximum_block_size.as_u128();

        let block_cache = block::Cache::fixed_with_max_overhead(
            &mut rng,
            maximum_prebuild_cache_size_bytes,
            maximum_block_size,
            &config.variant,
            // NOTE we bound payload generation to have overhead only
            // equivalent to the prebuild cache size,
            // `maximum_prebuild_cache_size_bytes`. This means on systems with plentiful
            // memory we're under generating entropy, on systems with
            // minimal memory we're over-generating.
            //
            // `lading::get_available_memory` suggests we can learn to
            // divvy this up in the future.
            maximum_prebuild_cache_size_bytes.get() as usize,
        )?;

        let path = PathBuf::from(&config.path);

        Ok(Self {
            path,
            block_cache,
            throttle,
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

        let mut handle = self.block_cache.handle();

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

            let total_bytes = self.block_cache.peek_next_size(&handle);
            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let block = self.block_cache.advance(&mut handle);
                    match current_file.write_all(&block.bytes).await {
                        Ok(()) => {
                            counter!("bytes_written", &self.metric_labels).increment(u64::from(block.total_bytes.get()));
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
