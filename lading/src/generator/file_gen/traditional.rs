//! The lading traditional file generator.
//!
//! Unlike the other generators the traditional file generator does not
//! "connect" however losely to the target but instead, without coordination,
//! merely writes files on disk. All files are written and cycled in parallel to
//! one another. A file is _not_ written to after it is deleted although its
//! file handler may be live after that point.
//!
//! ## Metrics
//!
//! `bytes_written`: Total bytes written
//! `bytes_per_second`: Configured rate to send data
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!
use std::{
    num::NonZeroU32,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::time::Instant;

use byte_unit::Byte;
use metrics::counter;
use rand::{SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    task::{JoinError, JoinSet},
};
use tracing::{error, info};

use lading_payload::{self, block};

use super::General;
use crate::generator::common::{
    BlockThrottle, MetricsBuilder, ThrottleConfig, ThrottleConversionError, create_throttle,
};

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`FileGen`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
    /// Failed to open a file for writing.
    #[error("Failed to open file {path:?}: {source}. Ensure parent directory exists.")]
    FileOpen {
        /// The path that failed to open
        path: PathBuf,
        /// The underlying IO error
        source: ::std::io::Error,
    },
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
    /// Child sub-task error.
    #[error("Child join error: {0}")]
    Child(#[from] JoinError),
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    /// Failed to convert, value is 0
    #[error("Value provided must not be zero")]
    Zero,
    /// Throttle error
    #[error("Throttle error: {0}")]
    Throttle(#[from] lading_throttle::Error),
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] ThrottleConversionError),
}

fn default_rotation() -> bool {
    true
}

#[allow(clippy::unnecessary_wraps)]
fn default_flush_every() -> Option<Duration> {
    Some(Duration::from_secs(1))
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration of [`FileGen`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path template for logs. "%NNN%" will be replaced in the template
    /// with the duplicate number.
    pub path_template: String,
    /// Total number of duplicates to make from this template.
    pub duplicates: u8,
    /// Sets the [`crate::payload::Config`] of this template.
    pub variant: lading_payload::Config,
    /// Sets the **soft** maximum bytes to be written into the `LogTarget`. This
    /// limit is soft, meaning a burst may go beyond this limit by no more than
    /// `maximum_token_burst`.
    ///
    /// After this limit is breached the target is closed and deleted. A new
    /// target with the same name is created to be written to.
    maximum_bytes_per_file: Byte,
    /// Defines the number of bytes that are added into the `LogTarget`'s rate
    /// limiting mechanism per second. This sets the maximum bytes that can be
    /// written _continuously_ per second from this target. Higher bursts are
    /// possible as the internal governor accumulates, up to
    /// `maximum_bytes_burst`.
    #[deprecated(note = "Use throttle.stable.bytes_per_second instead")]
    bytes_per_second: Option<Byte>,
    /// Defines the maximum internal cache of this log target. `file_gen` will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    maximum_block_size: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    block_cache_method: block::CacheMethod,
    /// Determines whether the file generator mimics log rotation or not. If
    /// true, files will be rotated. If false, it is the responsibility of
    /// tailing software to remove old files.
    #[serde(default = "default_rotation")]
    rotate: bool,
    /// The load throttle configuration
    pub throttle: Option<ThrottleConfig>,
    /// Force flush at a regular interval. Defaults to 1 second.
    /// Accepts human-readable durations (e.g., "1s", "500ms", "2s").
    #[serde(default = "default_flush_every", with = "humantime_serde")]
    pub flush_every: Option<Duration>,
}

#[derive(Debug)]
/// The file generator.
///
/// This generator writes files to disk, rotating them as appropriate. It does
/// this without coordination to the target.
pub struct Server {
    handles: JoinSet<Result<(), Error>>,
    shutdown: lading_signal::Watcher,
}

impl Server {
    /// Create a new [`FileGen`]
    ///
    /// # Errors
    ///
    /// Creation will fail if the target file cannot be opened for writing.
    ///
    /// # Panics
    ///
    /// Function will panic if variant is Static and the `static_path` is not
    /// set.
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        general: General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let _labels = MetricsBuilder::new("file_gen").with_id(general.id).build();

        let maximum_bytes_per_file =
            NonZeroU32::new(config.maximum_bytes_per_file.as_u128() as u32).ok_or(Error::Zero)?;

        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;

        let maximum_block_size = config.maximum_block_size.as_u128();

        let mut handles = JoinSet::new();
        let file_index = Arc::new(AtomicU32::new(0));

        for _ in 0..config.duplicates {
            // Accept the deprecated `bytes_per_second` path for legacy configs while
            // newer callers migrate to `throttle`.
            #[allow(deprecated)]
            let throttle =
                create_throttle(config.throttle.as_ref(), config.bytes_per_second.as_ref())?;

            let block_cache = match config.block_cache_method {
                block::CacheMethod::Fixed => block::Cache::fixed_with_max_overhead(
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
                )?,
            };

            let child = Child {
                path_template: config.path_template.clone(),
                maximum_bytes_per_file,
                throttle,
                block_cache: Arc::new(block_cache),
                maximum_block_size: maximum_block_size as u32,
                file_index: Arc::clone(&file_index),
                rotate: config.rotate,
                shutdown: shutdown.clone(),
                flush_every: config.flush_every,
            };

            handles.spawn(child.spin());
        }

        Ok(Self { handles, shutdown })
    }

    /// Run [`FileGen`] to completion or until a shutdown signal is received.
    ///
    /// In this loop the target file will be populated with lines of the variant
    /// dictated by the end user.
    ///
    /// # Errors
    ///
    /// This function will terminate with an error if file permissions are not
    /// correct, if the file cannot be written to etc. Any error from
    /// `std::io::Error` is possible.
    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_possible_truncation)]
    pub async fn spin(mut self) -> Result<(), Error> {
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        loop {
            tokio::select! {
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // Graceful shutdown: wait for all children to complete
                    while let Some(child_result) = self.handles.join_next().await {
                        let child_result: Result<Result<(), Error>, JoinError> = child_result;
                        let child_spin_result: Result<(), Error> = child_result.map_err(Error::Child)?;
                        child_spin_result?;
                    }
                    return Ok(());
                }
                Some(child_result) = self.handles.join_next() => {
                    let child_result: Result<Result<(), Error>, JoinError> = child_result;

                    let child_spin_result: Result<(), Error> = match child_result {
                        Ok(r) => r,
                        Err(join_err) => {
                            error!("Child task panicked: {join_err}");
                            return Err(Error::Child(join_err));
                        }
                    };

                    match child_spin_result {
                        Ok(()) => {
                            // Child completed successfully before shutdown
                            if self.handles.is_empty() {
                                error!("All child tasks completed unexpectedly before shutdown");
                                return Ok(());
                            }
                        }
                        Err(err) => {
                            error!("Child task failed: {err}");
                            return Err(err);
                        }
                    }
                }
            }
        }
    }
}

struct Child {
    path_template: String,
    maximum_bytes_per_file: NonZeroU32,
    throttle: BlockThrottle,
    block_cache: Arc<block::Cache>,
    maximum_block_size: u32,
    rotate: bool,
    file_index: Arc<AtomicU32>,
    shutdown: lading_signal::Watcher,
    flush_every: Option<Duration>,
}

impl Child {
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn spin(mut self) -> Result<(), Error> {
        let mut total_bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        let mut file_index = self.file_index.fetch_add(1, Ordering::Relaxed);
        let mut path = path_from_template(&self.path_template, file_index);

        let mut handle = self.block_cache.handle();
        let buffer_capacity = self
            .throttle
            .maximum_capacity_bytes(self.maximum_block_size);
        let mut fp = BufWriter::with_capacity(
            buffer_capacity,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|source| {
                    error!(
                        "Failed to open file {path:?}: {source}. Ensure parent directory exists."
                    );
                    Error::FileOpen {
                        path: path.clone(),
                        source,
                    }
                })?,
        );
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        let mut last_flush = Instant::now();
        loop {
            tokio::select! {
                result = self.throttle.wait_for_block(&self.block_cache, &handle) => {
                    match result {
                        Ok(()) => {
                            let block = self.block_cache.advance(&mut handle);
                            let total_bytes = u64::from(block.total_bytes.get());

                            {
                                fp.write_all(&block.bytes).await?;
                                counter!("bytes_written").increment(total_bytes);
                                total_bytes_written += total_bytes;
                            }

                            if total_bytes_written > maximum_bytes_per_file {
                                fp.flush().await?;
                                last_flush = Instant::now();
                                if self.rotate {
                                    // Delete file, leaving any open file handlers intact. This
                                    // includes our own `fp` for the time being.
                                    fs::remove_file(&path).await?;
                                }
                                // Update `path` to point to the next indexed file.
                                file_index = self.file_index.fetch_add(1, Ordering::Relaxed);
                                path = path_from_template(&self.path_template, file_index);
                                // Open a new fp to `path`, replacing `fp`. Any holders of the
                                // file pointer still have it but the file no longer has a name.
                                fp = BufWriter::with_capacity(
                                    buffer_capacity,
                                    fs::OpenOptions::new()
                                        .create(true)
                                        .truncate(false)
                                        .write(true)
                                        .open(&path)
                                        .await
                                        .map_err(|source| {
                                            error!("Failed to open file {path:?}: {source}. Ensure parent directory exists.");
                                            Error::FileOpen {
                                                path: path.clone(),
                                                source,
                                            }
                                        })?,
                                );
                                total_bytes_written = 0;
                            } else if self.flush_every.is_some_and(|d| last_flush.elapsed() >= d) {
                                fp.flush().await?;
                                last_flush = Instant::now();
                            }
                        }
                        Err(err) => {
                            error!("Discarding block due to throttle error: {err}");
                        }
                    }
                }
                () = &mut shutdown_wait => {
                    fp.flush().await?;
                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}

#[inline]
fn path_from_template(path_template: &str, index: u32) -> PathBuf {
    let fidx = format!("{index:04}");
    let full_path = path_template.replace("%NNN%", &fidx);
    PathBuf::from(full_path)
}
