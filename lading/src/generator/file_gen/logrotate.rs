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
    thread,
};

use byte_unit::{Byte, ByteError};
use futures::future::join_all;
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use rand::{Rng, SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::{JoinError, JoinHandle},
};
use tracing::{error, info};

use crate::common::PeekableReceiver;
use lading_payload::block::{self, Block};

use super::General;

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
    Byte(#[from] ByteError),
    /// Failed to convert, value is 0
    #[error("Value provided must not be zero")]
    Zero,
    /// Name provided but no parent on the path
    #[error("Name provided but no parent on the path")]
    NameWithNoParent,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
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
    bytes_per_second: Byte,
    /// Defines the maximum internal cache of this log target. `file_gen` will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    maximum_block_size: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    block_cache_method: block::CacheMethod,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
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
            ("component_name".to_string(), "logrotate".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32)
            .expect("Expect: config bytes per second must be non-zero");
        gauge!("bytes_per_second", &labels).set(f64::from(bytes_per_second.get()));

        let maximum_bytes_per_file =
            NonZeroU32::new(config.maximum_bytes_per_log.get_bytes() as u32).ok_or(Error::Zero)?;

        let mut handles = Vec::new();

        for idx in 0..config.concurrent_logs {
            let throttle = Throttle::new_with_config(config.throttle, bytes_per_second);

            let total_bytes =
                NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .ok_or(Error::Zero)?;
            let block_cache = match config.block_cache_method {
                block::CacheMethod::Fixed => block::Cache::fixed(
                    &mut rng,
                    total_bytes,
                    config.maximum_block_size.get_bytes(),
                    &config.variant,
                )?,
            };

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
                bytes_per_second,
                maximum_bytes_per_file,
                block_cache,
                throttle,
                shutdown.clone(),
                child_labels,
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
                Ok(Ok(())) => continue,
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
    bytes_per_second: NonZeroU32,
    // The soft limit bytes per file that will trigger a rotation.
    maximum_bytes_per_log: NonZeroU32,
    block_cache: block::Cache,
    throttle: Throttle,
    shutdown: lading_signal::Watcher,
    labels: Vec<(String, String)>,
}

impl Child {
    #[allow(clippy::too_many_arguments)]
    fn new(
        basename: &Path,
        total_rotations: u8,
        bytes_per_second: NonZeroU32,
        maximum_bytes_per_log: NonZeroU32,
        block_cache: block::Cache,
        throttle: Throttle,
        shutdown: lading_signal::Watcher,
        labels: Vec<(String, String)>,
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
            bytes_per_second,
            maximum_bytes_per_log,
            block_cache,
            throttle,
            shutdown,
            labels,
        }
    }

    async fn spin(mut self) -> Result<(), Error> {
        let bytes_per_second = self.bytes_per_second.get() as usize;
        let mut total_bytes_written: u64 = 0;
        let maximum_bytes_per_log: u64 = u64::from(self.maximum_bytes_per_log.get());

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
            bytes_per_second,
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

        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new()
            .spawn(|| block_cache.spin(snd))
            .map_err(|err| Error::IoThreadSpawn { err })?;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            // SAFETY: By construction the block cache will never be empty
            // except in the event of a catastrophic failure.
            let blk = rcv.peek().await.expect("block cache should never be empty");

            tokio::select! {
                _ = self.throttle.wait_for(blk.total_bytes) => {
                    let blk = rcv.next().await.expect("failed to advance through the blocks");
                    write_bytes(&blk,
                                &mut fp,
                                &mut total_bytes_written,
                                bytes_per_second,
                                total_names,
                                maximum_bytes_per_log,
                                &self.names,
                                last_name,
                                &self.labels).await?;
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

#[allow(clippy::too_many_arguments)]
async fn write_bytes(
    blk: &Block,
    fp: &mut BufWriter<fs::File>,
    total_bytes_written: &mut u64,
    bytes_per_second: usize,
    total_names: usize,
    maximum_bytes_per_log: u64,
    names: &[PathBuf],
    last_name: &Path,
    _labels: &[(String, String)],
) -> Result<(), Error> {
    let total_bytes = u64::from(blk.total_bytes.get());

    {
        fp.write_all(&blk.bytes)
            .await
            .map_err(|err| Error::IoWriteAll { err })?;

        // This metric must be written with a single context or it will crash
        // analysis. The simple way to accomplish that is to attach no labels to
        // it.
        counter!("bytes_written").increment(total_bytes);
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
            bytes_per_second,
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
    }

    Ok(())
}
