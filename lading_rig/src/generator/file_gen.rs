use byte_unit::{Byte, ByteUnit};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use lading_common::block::{self, chunk_bytes, construct_block_cache, Block};
use lading_common::payload;
use metrics::{counter, gauge};
use rand::prelude::StdRng;
use rand::SeedableRng;
use serde::Deserialize;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::str;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::task::JoinHandle;
use tracing::info;

use crate::signals::Shutdown;

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    Io(::std::io::Error),
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

/// Variant of the [`LogTarget`]
///
/// This variant controls what kind of line text is created by this program.
#[derive(Debug, Deserialize, Clone)]
pub enum Variant {
    /// Generates Datadog Logs JSON messages
    DatadogLog,
    /// Generates a limited subset of FoundationDB logs
    FoundationDb,
    /// Generates a static, user supplied data
    Static {
        /// Defines the file path to read static variant data from. Content is
        /// assumed to be line-oriented but no other claim is made on the file.
        static_path: PathBuf,
    },
    /// Generates a line of printable ascii characters
    Ascii,
    /// Generates a json encoded line
    Json,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path template for [`LogTarget`]. "%NNN%" will be replaced in the
    /// template with the duplicate number.
    pub path_template: String,
    /// Total number of duplicates to make from this template.
    pub duplicates: u8,
    /// Sets the [`Variant`] of this template.
    pub variant: Variant,
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
}

#[derive(Debug)]
pub struct FileGen {
    handles: Vec<JoinHandle<Result<(), Error>>>,
    shutdown: Shutdown,
}

impl FileGen {
    /// Create a new [`Log`]
    ///
    /// A new instance of this type requires a random generator, its name and
    /// the [`LogTarget`] for this task. The name will be used in telemetry and
    /// should be unique, though no check is done here to ensure that it is.
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
        let block_sizes: Vec<usize> = config
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
            .map(|sz| sz.get_bytes() as usize)
            .collect();

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        let maximum_bytes_per_file =
            NonZeroU32::new(config.maximum_bytes_per_file.get_bytes() as u32).unwrap();
        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32).unwrap();

        let block_chunks = chunk_bytes(
            &mut rng,
            maximum_prebuild_cache_size_bytes.get() as usize,
            &block_sizes,
        );

        let labels = vec![];
        let mut handles = Vec::new();
        for duplicate in 0..config.duplicates {
            let rate_limiter: RateLimiter<
                direct::NotKeyed,
                state::InMemoryState,
                clock::QuantaClock,
            > = RateLimiter::direct(Quota::per_second(bytes_per_second));

            let block_cache = match config.variant {
                Variant::Ascii => construct_block_cache(
                    &mut rng,
                    &payload::Ascii::default(),
                    &block_chunks,
                    &labels,
                ),
                Variant::DatadogLog => construct_block_cache(
                    &mut rng,
                    &payload::DatadogLog::default(),
                    &block_chunks,
                    &labels,
                ),
                Variant::Json => construct_block_cache(
                    &mut rng,
                    &payload::Json::default(),
                    &block_chunks,
                    &labels,
                ),
                Variant::FoundationDb => construct_block_cache(
                    &mut rng,
                    &payload::FoundationDb::default(),
                    &block_chunks,
                    &labels,
                ),
                Variant::Static { ref static_path } => construct_block_cache(
                    &mut rng,
                    &payload::Static::new(&static_path),
                    &block_chunks,
                    &labels,
                ),
            };

            let duplicate = format!("{}", duplicate);
            let full_path = config.path_template.replace("%NNN%", &duplicate);
            let path = PathBuf::from(full_path);
            let child = Child {
                path,
                maximum_bytes_per_file,
                bytes_per_second,
                rate_limiter,
                block_cache,
            };

            handles.push(tokio::spawn(child.spin()));
        }

        Ok(Self { handles, shutdown })
    }

    /// Enter the main loop of this [`LogTarget`]
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
    path: PathBuf,
    maximum_bytes_per_file: NonZeroU32,
    bytes_per_second: NonZeroU32,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
}

impl Child {
    pub async fn spin(self) -> Result<(), Error> {
        let bytes_per_second = self.bytes_per_second.get() as usize;
        let mut bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        let mut fp = BufWriter::with_capacity(
            bytes_per_second,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.path)
                .await?,
        );

        for blk in self.block_cache.iter().cycle() {
            let total_bytes = blk.total_bytes;
            let total_newlines = blk.lines;
            let block = &blk.bytes;

            self.rate_limiter.until_n_ready(total_bytes).await?;

            {
                fp.write_all(block).await?;
                // block.len() and total_bytes are the same numeric value but we
                // avoid needing to get a plain value from a non-zero by calling
                // len here.
                counter!("bytes_written", block.len() as u64);
                counter!("lines_written", total_newlines);

                bytes_written += block.len() as u64;
                gauge!("current_target_size_bytes", bytes_written as f64);
            }

            if bytes_written > maximum_bytes_per_file {
                // Delete file, leaving any open file handlers intact. This
                // includes our own `fp` for the time being.
                fs::remove_file(&self.path).await?;
                // Open a new fp to `self.path`, replacing `fp`. Any holders of
                // the file pointer still have it but the file no longer has a
                // name.
                fp = BufWriter::with_capacity(
                    bytes_per_second,
                    fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(&self.path)
                        .await?,
                );
                bytes_written = 0;
                counter!("file_rotated", 1);
            }
        }
        unreachable!()
    }
}
