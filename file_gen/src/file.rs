use crate::config::{LogTarget, Variant};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use lading_common::block::{self, construct_block_cache, Block};
use lading_common::payload;
use metrics::{counter, gauge};
use rand::prelude::SliceRandom;
use rand::Rng;
use std::num::NonZeroU32;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};

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

const ONE_MEBIBYTE: usize = 1_000_000;
const BLOCK_BYTE_SIZES: [usize; 6] = [
    ONE_MEBIBYTE,
    2_000_000,
    4_000_000,
    8_000_000,
    16_000_000,
    32_000_000,
];

fn chunk_bytes<R>(rng: &mut R, input: usize, bytes_per_second: usize) -> Vec<usize>
where
    R: Rng + Sized,
{
    let mut chunks = Vec::new();
    let mut bytes_remaining = input;
    while bytes_remaining > ONE_MEBIBYTE {
        let bytes_max = std::cmp::min(bytes_per_second, bytes_remaining);
        let block_bytes = BLOCK_BYTE_SIZES.choose(rng).unwrap();
        if *block_bytes > bytes_max {
            continue;
        }
        chunks.push(*block_bytes);
        bytes_remaining = bytes_remaining.saturating_sub(*block_bytes);
    }
    chunks
}

/// The [`Log`] defines a task that emits variant lines to a file, managing
/// rotation and controlling rate limits.
#[derive(Debug)]
pub struct Log {
    path: PathBuf,
    name: String, // this is the stringy version of `path`
    maximum_bytes_per_file: NonZeroU32,
    bytes_per_second: NonZeroU32,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
}

impl Log {
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
    pub fn new(name: String, target: LogTarget) -> Result<Self, Error> {
        let mut rng = rand::thread_rng();
        let rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock> =
            RateLimiter::direct(Quota::per_second(target.bytes_per_second));

        let maximum_bytes_per_file = target.maximum_bytes_per_file;

        let bytes_per_second: usize = target.bytes_per_second.get() as usize;
        let block_chunks = chunk_bytes(
            &mut rng,
            target.maximum_prebuild_cache_size_bytes.get() as usize,
            bytes_per_second,
        );

        let labels = vec![("target".to_string(), name.clone())];
        let block_cache = match target.variant {
            Variant::Ascii => {
                construct_block_cache(&payload::Ascii::default(), &block_chunks, &labels)
            }
            Variant::DatadogLog => {
                construct_block_cache(&payload::DatadogLog::default(), &block_chunks, &labels)
            }
            Variant::Json => {
                construct_block_cache(&payload::Json::default(), &block_chunks, &labels)
            }
            Variant::FoundationDb => {
                construct_block_cache(&payload::FoundationDb::default(), &block_chunks, &labels)
            }
            Variant::Static => construct_block_cache(
                &payload::Static::new(&target.static_path.unwrap()),
                &block_chunks,
                &labels,
            ),
        };

        Ok(Self {
            maximum_bytes_per_file,
            name,
            path: target.path,
            bytes_per_second: target.bytes_per_second,
            rate_limiter,
            block_cache,
        })
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
    pub async fn spin(self) -> Result<(), Error> {
        let labels = vec![("target", self.name.clone())];

        let bytes_per_second = self.bytes_per_second.get() as usize;
        let mut bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        gauge!(
            "maximum_bytes_per_file",
            maximum_bytes_per_file as f64,
            &labels
        );
        gauge!("bytes_per_second", bytes_per_second as f64, &labels);

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
                counter!("bytes_written", block.len() as u64, &labels);
                counter!("lines_written", total_newlines, &labels);

                bytes_written += block.len() as u64;
                gauge!("current_target_size_bytes", bytes_written as f64, &labels);
            }

            if bytes_written > maximum_bytes_per_file {
                let slop = (bytes_written - maximum_bytes_per_file).max(0) as f64;
                gauge!("file_rotation_slop", slop, &labels);
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
                counter!("file_rotated", 1, &labels);
            }
        }
        unreachable!()
    }
}
