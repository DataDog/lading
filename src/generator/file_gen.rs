//! The file generator.
//!
//! Unlike the other generators the file generator does not "connect" however
//! losely to the target but instead, without coordination, merely writes files
//! on disk.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    str,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use byte_unit::{Byte, ByteUnit};
use governor::{
    clock, state,
    state::direct::{self, InsufficientCapacity},
    Quota, RateLimiter,
};
use metrics::{counter, gauge};
use rand::{prelude::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    task::JoinHandle,
};
use tracing::info;

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
};

#[derive(Debug)]
/// Errors produced by [`FileGen`].
pub enum Error {
    /// Rate limiter has insuficient capacity for payload. Indicates a serious
    /// bug.
    Governor(InsufficientCapacity),
    /// Wrapper around [`std::io::Error`].
    Io(::std::io::Error),
    /// Creation of payload blocks failed.
    Block(block::Error),
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Self {
        Error::Block(error)
    }
}

impl From<InsufficientCapacity> for Error {
    fn from(error: InsufficientCapacity) -> Self {
        Error::Governor(error)
    }
}

impl From<::std::io::Error> for Error {
    fn from(error: ::std::io::Error) -> Self {
        Error::Io(error)
    }
}

fn default_rotation() -> bool {
    true
}

#[derive(Debug, Deserialize)]
/// Configuration of [`FileGen`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path template for logs. "%NNN%" will be replaced in the template
    /// with the duplicate number.
    pub path_template: String,
    /// Total number of duplicates to make from this template.
    pub duplicates: u8,
    /// Sets the [`lading::payload::Config`] of this template.
    pub variant: payload::Config,
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
    bytes_per_second: Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// Defines the maximum internal cache of this log target. file_gen will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: Byte,
    /// Determines whether the file generator mimics log rotation or not. If
    /// true, files will be rotated. If false, it is the responsibility of
    /// tailing software to remove old files.
    #[serde(default = "default_rotation")]
    rotate: bool,
}

#[derive(Debug)]
/// The file generator.
///
/// This generator writes files to disk, rotating them as appropriate. It does
/// this without coordination to the target.
pub struct FileGen {
    handles: Vec<JoinHandle<Result<(), Error>>>,
    shutdown: Shutdown,
}

impl FileGen {
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
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroUsize> = config
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
            .map(|sz| NonZeroUsize::new(sz.get_bytes() as usize).expect("bytes must be non-zero"))
            .collect();

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        let maximum_bytes_per_file =
            NonZeroU32::new(config.maximum_bytes_per_file.get_bytes() as u32).unwrap();
        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32).unwrap();

        let block_chunks = chunk_bytes(
            &mut rng,
            NonZeroUsize::new(maximum_prebuild_cache_size_bytes.get() as usize)
                .expect("bytes must be non-zero"),
            &block_sizes,
        )?;

        let labels = vec![];
        let mut handles = Vec::new();
        let file_index = Arc::new(AtomicU32::new(0));
        for _ in 0..config.duplicates {
            let rate_limiter: RateLimiter<
                direct::NotKeyed,
                state::InMemoryState,
                clock::QuantaClock,
            > = RateLimiter::direct(Quota::per_second(bytes_per_second));

            let block_cache =
                construct_block_cache(&mut rng, &config.variant, &block_chunks, &labels);

            let child = Child {
                path_template: config.path_template.clone(),
                maximum_bytes_per_file,
                bytes_per_second,
                rate_limiter,
                block_cache,
                file_index: Arc::clone(&file_index),
                rotate: config.rotate,
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
        for handle in self.handles.drain(..) {
            handle.abort();
        }
        Ok(())
    }
}

struct Child {
    path_template: String,
    maximum_bytes_per_file: NonZeroU32,
    bytes_per_second: NonZeroU32,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    rotate: bool,
    file_index: Arc<AtomicU32>,
}

impl Child {
    pub(crate) async fn spin(self) -> Result<(), Error> {
        let bytes_per_second = self.bytes_per_second.get() as usize;
        let mut bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        let mut file_index = self.file_index.fetch_add(1, Ordering::Relaxed);
        let mut path = path_from_template(&self.path_template, file_index);

        let mut fp = BufWriter::with_capacity(
            bytes_per_second,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await?,
        );

        for blk in self.block_cache.iter().cycle() {
            let total_bytes = blk.total_bytes;
            let total_newlines = blk.lines;
            let block = &blk.bytes;

            self.rate_limiter.until_n_ready(total_bytes).await?;

            {
                fp.write_all(block).await?;
                fp.flush().await?;
                // block.len() and total_bytes are the same numeric value but we
                // avoid needing to get a plain value from a non-zero by calling
                // len here.
                counter!("bytes_written", block.len() as u64);
                counter!("lines_written", total_newlines);

                bytes_written += block.len() as u64;
                gauge!("current_target_size_bytes", bytes_written as f64);
            }

            if bytes_written > maximum_bytes_per_file {
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
                    bytes_per_second,
                    fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(&path)
                        .await?,
                );
                bytes_written = 0;
                counter!("file_rotated", 1);
            }
        }
        unreachable!()
    }
}

#[inline]
fn path_from_template(path_template: &str, index: u32) -> PathBuf {
    let fidx = format!("{:04}", index);
    let full_path = path_template.replace("%NNN%", &fidx);
    PathBuf::from(full_path)
}
