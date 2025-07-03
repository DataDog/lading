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
    thread,
};

use byte_unit::Byte;
use futures::future::join_all;
use lading_throttle::Throttle;
use metrics::counter;
use rand::{SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::{JoinError, JoinHandle},
};
use tracing::{error, info};

use crate::common::PeekableReceiver;
use lading_payload::{
    self,
    block::{self, Block},
};

use super::General;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`FileGen`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
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

fn default_rotation() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
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
    pub throttle: Option<crate::generator::common::BytesThrottleConfig>,
}

#[derive(Debug)]
/// The file generator.
///
/// This generator writes files to disk, rotating them as appropriate. It does
/// this without coordination to the target.
pub struct Server {
    handles: Vec<JoinHandle<Result<(), Error>>>,
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
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "file_gen".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let throttle_config = match (config.bytes_per_second, config.throttle) {
            (Some(bytes_per_second), None) => {
                let bytes_per_second =
                    NonZeroU32::new(bytes_per_second.as_u128() as u32).ok_or(Error::Zero)?;
                lading_throttle::Config::Stable {
                    maximum_capacity: bytes_per_second,
                }
            }
            (None, Some(throttle)) => throttle.try_into()?,
            (Some(_), Some(_)) => return Err(Error::ConflictingThrottleConfig),
            (None, None) => return Err(Error::NoThrottleConfig),
        };

        let maximum_bytes_per_file =
            NonZeroU32::new(config.maximum_bytes_per_file.as_u128() as u32).ok_or(Error::Zero)?;

        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;

        let maximum_block_size = config.maximum_block_size.as_u128();

        let mut handles = Vec::new();
        let file_index = Arc::new(AtomicU32::new(0));

        for _ in 0..config.duplicates {
            let throttle = Throttle::new_with_config(throttle_config);

            let block_cache = match config.block_cache_method {
                block::CacheMethod::Fixed => block::Cache::fixed(
                    &mut rng,
                    maximum_prebuild_cache_size_bytes,
                    maximum_block_size,
                    &config.variant,
                )?,
            };

            let child = Child {
                path_template: config.path_template.clone(),
                maximum_bytes_per_file,
                throttle,
                throttle_config,
                block_cache,
                file_index: Arc::clone(&file_index),
                rotate: config.rotate,
                shutdown: shutdown.clone(),
            };

            handles.push(tokio::spawn(child.spin()));
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

struct Child {
    path_template: String,
    maximum_bytes_per_file: NonZeroU32,
    throttle: Throttle,
    throttle_config: lading_throttle::Config,
    block_cache: block::Cache,
    rotate: bool,
    file_index: Arc<AtomicU32>,
    shutdown: lading_signal::Watcher,
}

impl Child {
    pub(crate) async fn spin(mut self) -> Result<(), Error> {
        let buffer_capacity = match self.throttle_config {
            lading_throttle::Config::Stable { maximum_capacity }
            | lading_throttle::Config::Linear {
                maximum_capacity, ..
            } => maximum_capacity.get() as usize,
            lading_throttle::Config::AllOut => 1024 * 1024, // 1MiB buffer for all-out
        };
        let mut total_bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        let mut file_index = self.file_index.fetch_add(1, Ordering::Relaxed);
        let mut path = path_from_template(&self.path_template, file_index);

        let mut fp = BufWriter::with_capacity(
            buffer_capacity,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await?,
        );

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
                            let blk = rcv.next().await.expect("failed to advance through blocks"); // actually advance through the blocks
                            let total_bytes = u64::from(total_bytes.get());

                            {
                                fp.write_all(&blk.bytes).await?;
                                counter!("bytes_written").increment(total_bytes);
                                total_bytes_written += total_bytes;
                            }

                            if total_bytes_written > maximum_bytes_per_file {
                                fp.flush().await?;
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
                                        .await?,
                                );
                                total_bytes_written = 0;
                            }
                        }
                        Err(err) => {
                            error!("Throttle request of {total_bytes} is larger than throttle capacity. Block will be discarded. Error: {err}");
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
