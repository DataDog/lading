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

use byte_unit::{Byte, ByteUnit};
use futures::future::join_all;
use lading_throttle::Throttle;
use metrics::{gauge, register_counter};
use rand::{prelude::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::{JoinError, JoinHandle},
};
use tracing::info;

use crate::{common::PeekableReceiver, signals::Shutdown};
use lading_payload::block::{self, Block};

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
}

#[derive(Debug, Deserialize, PartialEq)]
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
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// Defines the maximum internal cache of this log target. file_gen will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: Byte,
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
    shutdown: Shutdown,
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
    pub fn new(general: General, config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroU32> = config
            .block_sizes
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(8_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(16_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(32_f64, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| NonZeroU32::new(sz.get_bytes() as u32).expect("bytes must be non-zero"))
            .collect();
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "logrotate".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

        let maximum_bytes_per_file =
            NonZeroU32::new(config.maximum_bytes_per_log.get_bytes() as u32).unwrap();

        let mut handles = Vec::new();

        for _ in 0..config.concurrent_logs {
            let throttle = Throttle::new_with_config(config.throttle, bytes_per_second);

            let total_bytes =
                NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                    .expect("bytes must be non-zero");
            let block_cache = match config.block_cache_method {
                block::CacheMethod::Streaming => block::Cache::stream(
                    config.seed,
                    total_bytes,
                    &block_sizes,
                    config.variant.clone(),
                )?,
                block::CacheMethod::Fixed => {
                    block::Cache::fixed(&mut rng, total_bytes, &block_sizes, &config.variant)?
                }
            };

            let mut basename = config.root.clone();
            let depth = rng.gen_range(0..config.max_depth);
            for _ in 0..depth {
                basename.push(format!("{}", rng.gen::<u16>()));
            }
            basename.set_file_name(format!("{}", rng.gen::<u64>()));
            basename.set_extension("log");

            let child = Child::new(
                &basename,
                config.total_rotations,
                bytes_per_second,
                maximum_bytes_per_file,
                block_cache,
                throttle,
                shutdown.clone(),
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
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(Error::Child(err)),
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
    shutdown: Shutdown,
}

impl Child {
    fn new(
        basename: &Path,
        total_rotations: u8,
        bytes_per_second: NonZeroU32,
        maximum_bytes_per_log: NonZeroU32,
        block_cache: block::Cache,
        throttle: Throttle,
        shutdown: Shutdown,
    ) -> Self {
        let mut names = Vec::with_capacity((total_rotations + 1).into());
        names.push(PathBuf::from(basename));
        for i in 0..total_rotations {
            let name = format!(
                "{orig}.{i}",
                orig = basename.file_name().unwrap_or_default().to_string_lossy()
            );
            let mut pth = PathBuf::new();
            pth.push(basename);
            pth.set_file_name(name);
            names.push(pth);
        }

        Self {
            names,
            bytes_per_second,
            maximum_bytes_per_log,
            block_cache,
            throttle,
            shutdown,
        }
    }

    async fn spin(mut self) -> Result<(), Error> {
        let bytes_per_second = self.bytes_per_second.get() as usize;
        let mut total_bytes_written: u64 = 0;
        let maximum_bytes_per_log: u64 = u64::from(self.maximum_bytes_per_log.get());

        let total_names = self.names.len();
        // SAFETY: By construction there is guaranteed to be at least one name.
        let last_name = &self.names.last().unwrap();

        // SAFETY: By construction the name is guaranteed to have a parent.
        fs::create_dir_all(&self.names[0].parent().unwrap()).await?;
        let mut fp = BufWriter::with_capacity(
            bytes_per_second,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.names[0])
                .await?,
        );

        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;
        let bytes_written = register_counter!("bytes_written");

        loop {
            // SAFETY: By construction the block cache will never be empty
            // except in the event of a catastrophic failure.
            let blk = rcv.peek().await.unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let blk = rcv.next().await.unwrap(); // actually advance through the blocks
                    let total_bytes = u64::from(total_bytes.get());

                    {
                        fp.write_all(&blk.bytes).await?;
                        bytes_written.increment(total_bytes);
                        total_bytes_written += total_bytes;
                    }


                    if total_bytes_written > maximum_bytes_per_log {
                        fp.flush().await?;

                        // Delete the last name file, if it exists. Move all files to their next highest.
                        if fs::try_exists(&last_name).await? {
                            fs::remove_file(&last_name).await?;
                        }
                        if total_names > 1 {
                            // If there's only one name this rotation is k8s
                            // default style and we've just dropped the only
                            // named log file.
                            for i in (0..total_names-1).rev() {
                                let from = &self.names[i];
                                let to = &self.names[i+1];
                                fs::rename(from, to).await?;
                            }
                        }

                        // Open a new fp to `path`, replacing `fp`. Any holders of the
                        // file pointer still have it but the file no longer has a name.
                        fp = BufWriter::with_capacity(
                            bytes_per_second,
                            fs::OpenOptions::new()
                                .create(true)
                                .truncate(false)
                                .write(true)
                                .open(&self.names[0])
                                .await?,
                        );
                        total_bytes_written = 0;
                    }
                }
                () = self.shutdown.recv() => {
                    fp.flush().await?;
                    info!("shutdown signal received");
                    return Ok(());
                },

            }
        }
    }
}
