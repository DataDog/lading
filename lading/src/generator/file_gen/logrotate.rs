//! The lading 'logrotate' file generator.
//!
//! The logrotate file generator does not "connect" however loosely to the target
//! but instead, without coordination, merely writes files on disk. We mimic the
//! fashion in which logrotate operates. All log files are written up to some
//! maximum amount, with a fixed number of 'rotations' happening per file. Once
//! a file reaches its maximum size its name is removed although writes may
//! still arrive after the name removal.
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
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use byte_unit::Byte;
use futures::future::join_all;
use metrics::counter;
use rand::{Rng, SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    task::{JoinError, JoinHandle},
};
use tracing::{error, info};

use lading_payload::block;

use super::General;
use crate::generator::common::{
    BlockThrottle, MetricsBuilder, ThrottleConfig, ThrottleConversionError, create_throttle,
};

/// An enum to allow us to determine what operation caused an IO errror as the
/// default error message lacks detail.
#[derive(Debug, Clone, Copy)]
pub enum IoOp {
    /// Operation for `fs::try_exists`
    TryExists,
    /// Operation for `fs::remove_file`
    RemoveFile,
    /// Operation for `fs::create_dir_all`
    CreateDirAll,
    /// Operation for `fs::OpenOptions` etc
    Open,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`FileGen`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("IO error [{path}]: {err}")]
    Io {
        /// The path being operated on
        path: PathBuf,
        /// The operation
        operation: IoOp,
        /// The error
        err: std::io::Error,
    },
    /// Error for `fs::rename` operation
    #[error("Rename error [{from} -> {to}]: {err}")]
    IoRename {
        /// The path being moved from
        from: PathBuf,
        /// The path being moved to
        to: PathBuf,
        /// The actual error
        err: std::io::Error,
    },
    /// Error for `fp.write_all` operation
    #[error("Write all bytes error: {err}")]
    IoWriteAll {
        /// The actual error
        err: std::io::Error,
    },
    /// Error for thread spawning
    #[error("Unable to spawn thread: {err}")]
    IoThreadSpawn {
        /// The actual error
        err: std::io::Error,
    },
    /// Error for `fs::flush` operation
    #[error("Flush error: {err}")]
    IoFlush {
        /// The error, actual
        err: std::io::Error,
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
    /// Name provided but no parent on the path
    #[error("Name provided but no parent on the path")]
    NameWithNoParent,
    /// Throttle error
    #[error("Throttle error: {0}")]
    Throttle(#[from] lading_throttle::Error),
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] ThrottleConversionError),
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
    /// The root path for writing logs.
    pub root: PathBuf,
    /// Total number of concurrent logs.
    pub concurrent_logs: u16,
    /// The **soft** maximum byte size of each log.
    pub maximum_bytes_per_log: Byte,
    /// The number of rotations per log file.
    pub total_rotations: u8,
    /// The maximum directory depth allowed below the root path. If 0 all log
    /// files will be present in the root path.
    pub max_depth: u8,
    /// Sets the [`crate::payload::Config`] of this template.
    pub variant: lading_payload::Config,
    /// Defines the number of bytes that written in each log file.
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
    /// Throughput profile controlling emission rate (bytes or blocks).
    #[serde(default)]
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
        let labels = MetricsBuilder::new("logrotate").with_id(general.id).build();

        let maximum_bytes_per_log =
            NonZeroU32::new(config.maximum_bytes_per_log.as_u128() as u32).ok_or(Error::Zero)?;

        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;

        let maximum_block_size =
            NonZeroU32::new(config.maximum_block_size.as_u128() as u32).ok_or(Error::Zero)?;

        let block_cache = match config.block_cache_method {
            block::CacheMethod::Fixed => block::Cache::fixed_with_max_overhead(
                &mut rng,
                maximum_prebuild_cache_size_bytes,
                u128::from(maximum_block_size.get()),
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
        let block_cache = Arc::new(block_cache);

        let mut handles = Vec::new();

        for idx in 0..config.concurrent_logs {
            let throughput_throttle =
                create_throttle(config.throttle.as_ref(), config.bytes_per_second.as_ref())?;

            let mut dir_path = config.root.clone();
            let depth = rng.random_range(0..config.max_depth);
            for _ in 0..depth {
                dir_path.push(format!("{}", rng.random::<u16>()));
            }

            let file_name = format!("{}.log", rng.random::<u64>());
            let mut basename = dir_path.clone();
            basename.push(&file_name);

            let mut child_labels = labels.clone();
            child_labels.push(("child_idx".to_string(), idx.to_string()));

            let child = Child::new(
                &basename,
                config.total_rotations,
                maximum_bytes_per_log,
                maximum_block_size.get(),
                Arc::clone(&block_cache),
                throughput_throttle,
                shutdown.clone(),
                child_labels,
                config.flush_every,
            );

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
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    error!("join_all error: {err}");
                    return Err(err);
                }
                Err(err) => {
                    error!("logrotate child error: {err}");
                    return Err(Error::Child(err));
                }
            }
        }
        Ok(())
    }
}

struct Child {
    // Child maintains a set vector of names to use when 'rotating'.
    names: Vec<PathBuf>,
    // The soft limit bytes per file that will trigger a rotation.
    maximum_bytes_per_log: NonZeroU32,
    maximum_block_size: u32,
    block_cache: Arc<block::Cache>,
    throttle: BlockThrottle,
    shutdown: lading_signal::Watcher,
    labels: Vec<(String, String)>,
    flush_every: Option<Duration>,
}

impl Child {
    #[allow(clippy::too_many_arguments)]
    fn new(
        basename: &Path,
        total_rotations: u8,
        maximum_bytes_per_log: NonZeroU32,
        maximum_block_size: u32,
        block_cache: Arc<block::Cache>,
        throttle: BlockThrottle,
        shutdown: lading_signal::Watcher,
        labels: Vec<(String, String)>,
        flush_every: Option<Duration>,
    ) -> Self {
        let mut names = Vec::with_capacity((total_rotations + 1).into());
        names.push(PathBuf::from(basename));

        let parent_dir = basename.parent().unwrap_or_else(|| Path::new(""));
        let original_file_name = basename.file_name().unwrap_or_default().to_string_lossy();

        for i in 0..total_rotations {
            let rotated_file_name = format!("{original_file_name}.{i}");
            let mut pth = PathBuf::from(parent_dir);
            pth.push(rotated_file_name);
            names.push(pth);
        }

        Self {
            names,
            maximum_bytes_per_log,
            maximum_block_size,
            block_cache,
            throttle,
            shutdown,
            labels,
            flush_every,
        }
    }

    async fn spin(mut self) -> Result<(), Error> {
        let mut handle = self.block_cache.handle();
        let buffer_capacity = self
            .throttle
            .maximum_capacity_bytes(self.maximum_block_size);
        let mut total_bytes_written: u64 = 0;
        let maximum_bytes_per_log: u64 = u64::from(self.maximum_bytes_per_log.get());
        let mut last_flush = Instant::now();

        let total_names = self.names.len();
        // SAFETY: By construction there is guaranteed to be at least one name.
        let last_name = &self
            .names
            .last()
            .expect("there is no last element in names");

        // SAFETY: By construction there is at least one name present.
        let parent: &Path = self.names[0].parent().ok_or(Error::NameWithNoParent)?;
        fs::create_dir_all(parent).await.map_err(|err| Error::Io {
            path: PathBuf::from(parent),
            operation: IoOp::CreateDirAll,
            err,
        })?;
        let mut fp: BufWriter<fs::File> = BufWriter::with_capacity(
            buffer_capacity,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.names[0])
                .await
                .map_err(|err| Error::Io {
                    path: PathBuf::from(parent),
                    operation: IoOp::Open,
                    err,
                })?,
        );

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            // SAFETY: By construction the block cache will never be empty
            // except in the event of a catastrophic failure.
            tokio::select! {
                result = self.throttle.wait_for_block(&self.block_cache, &handle) => {
                    match result {
                        Ok(()) => {
                            let did_rotate = write_bytes(self.block_cache.advance(&mut handle),
                                    &mut fp,
                                    &mut total_bytes_written,
                                    buffer_capacity,
                                    total_names,
                                    maximum_bytes_per_log,
                                    &self.names,
                                    last_name,
                                    &self.labels).await?;
                            if did_rotate {
                                last_flush = Instant::now();
                            } else if self.flush_every.is_some_and(|d| last_flush.elapsed() >= d) {
                                fp.flush().await.map_err(|err| Error::IoFlush { err })?;
                                last_flush = Instant::now();
                            }
                        }
                        Err(err) => {
                            error!("Discarding block due to throttle error: {err}");
                        }
                    }
                }
                () = &mut shutdown_wait => {
                    fp.flush().await.map_err(|err| Error::IoFlush { err })?;
                    drop(fp);

                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}

/// Writes a block to the file, rotating if necessary.
/// Returns `true` if a rotation occurred (and thus the file was flushed).
#[allow(clippy::too_many_arguments)]
async fn write_bytes(
    blk: &block::Block,
    fp: &mut BufWriter<fs::File>,
    total_bytes_written: &mut u64,
    buffer_capacity: usize,
    total_names: usize,
    maximum_bytes_per_log: u64,
    names: &[PathBuf],
    last_name: &Path,
    labels: &[(String, String)],
) -> Result<bool, Error> {
    let total_bytes = u64::from(blk.total_bytes.get());

    {
        fp.write_all(&blk.bytes)
            .await
            .map_err(|err| Error::IoWriteAll { err })?;
        counter!("bytes_written", labels).increment(total_bytes);
        *total_bytes_written += total_bytes;
    }

    if *total_bytes_written > maximum_bytes_per_log {
        fp.flush().await.map_err(|err| Error::IoFlush { err })?;

        // Delete the last name file, if it exists. Move all files to their next highest.
        if fs::try_exists(&last_name).await.map_err(|err| Error::Io {
            path: PathBuf::from(last_name),
            operation: IoOp::TryExists,
            err,
        })? {
            fs::remove_file(&last_name).await.map_err(|err| Error::Io {
                path: PathBuf::from(last_name),
                operation: IoOp::RemoveFile,
                err,
            })?;
        }
        if total_names > 1 {
            // If there's only one name this rotation is k8s
            // default style and we've just dropped the only
            // named log file.
            for i in (0..total_names - 1).rev() {
                let from = &names[i];
                let to = &names[i + 1];

                // If the 'from' file exists we can move to the next name, else
                // there's nothing to move.
                if fs::try_exists(from).await.map_err(|err| Error::Io {
                    path: PathBuf::from(from),
                    operation: IoOp::TryExists,
                    err,
                })? {
                    fs::rename(from, to).await.map_err(|err| Error::IoRename {
                        from: PathBuf::from(from),
                        to: PathBuf::from(to),
                        err,
                    })?;
                }
            }
        }

        // Open a new fp to `path`, replacing `fp`. Any holders of the
        // file pointer still have it but the file no longer has a name.
        *fp = BufWriter::with_capacity(
            buffer_capacity,
            fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&names[0])
                .await
                .map_err(|err| Error::Io {
                    path: PathBuf::from(&names[0]),
                    operation: IoOp::Open,
                    err,
                })?,
        );
        *total_bytes_written = 0;
        return Ok(true);
    }

    Ok(false)
}
