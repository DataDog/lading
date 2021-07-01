use crate::config::{LogTarget, Variant};
use crate::payload::{self, Serialize};
use arbitrary::{self, Arbitrary, Unstructured};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use metrics::{counter, gauge};
use rand::prelude::SliceRandom;
use rand::RngCore;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use std::convert::TryInto;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    Io(::std::io::Error),
    Payload(payload::Error),
    Arbitrary(arbitrary::Error),
    BlockEmpty,
}

impl From<arbitrary::Error> for Error {
    fn from(error: arbitrary::Error) -> Self {
        Error::Arbitrary(error)
    }
}

impl From<payload::Error> for Error {
    fn from(error: payload::Error) -> Self {
        Error::Payload(error)
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

fn total_newlines(input: &[u8]) -> NonZeroU64 {
    NonZeroU64::new(bytecount::count(input, b'\n') as u64).unwrap()
}

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

fn construct_block(
    block_bytes: usize,
    variant: Variant,
) -> Result<(NonZeroU32, NonZeroU64, Vec<u8>), Error> {
    let mut rng = thread_rng();
    let mut bytes: Vec<u8> = vec![0; block_bytes];
    rng.fill_bytes(&mut bytes);
    let unstructured: Unstructured = Unstructured::new(&bytes);
    let mut block: Vec<u8> = Vec::new();
    match variant {
        Variant::Ascii => {
            payload::Ascii::arbitrary_take_rest(unstructured)?.to_bytes(&mut block)?;
        }
        Variant::Json => {
            payload::Json::arbitrary_take_rest(unstructured)?.to_bytes(&mut block)?;
        }
    }
    block.shrink_to_fit();
    if block.is_empty() {
        return Err(Error::BlockEmpty);
    }

    let newlines = total_newlines(&block);
    let nz_bytes = NonZeroU32::new(block.len().try_into().unwrap()).unwrap();
    Ok((nz_bytes, newlines, block))
}

#[allow(clippy::ptr_arg)]
#[allow(clippy::cast_precision_loss)]
fn construct_block_cache<R>(
    mut rng: R,
    target: &LogTarget,
    labels: &Vec<(String, String)>,
) -> Vec<(NonZeroU32, u64, Vec<u8>)>
where
    R: Rng + Sized,
{
    let bytes_per_second: usize = target.bytes_per_second.get() as usize;
    let block_chunks = chunk_bytes(
        &mut rng,
        target.maximum_prebuild_cache_size_bytes.get() as usize,
        bytes_per_second,
    );

    let block_cache: Vec<(NonZeroU32, u64, Vec<u8>)> = block_chunks
        .into_par_iter()
        .map(|block_size| construct_block(block_size, target.variant))
        .map(std::result::Result::unwrap)
        .map(|(nzu32, nzu64, bytes)| (nzu32, nzu64.get(), bytes))
        .collect();
    gauge!("block_construction_complete", 1.0, labels);
    block_cache
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
    block_cache: Vec<(NonZeroU32, u64, Vec<u8>)>,
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
    pub fn new<R>(rng: R, name: String, target: LogTarget) -> Result<Self, Error>
    where
        R: Rng + Sized,
    {
        let rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock> =
            RateLimiter::direct(Quota::per_second(target.bytes_per_second));

        let maximum_bytes_per_file = target.maximum_bytes_per_file;

        let labels = vec![("target".to_string(), name.clone())];
        let block_cache = construct_block_cache(rng, &target, &labels);

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

        let mut bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        gauge!(
            "maximum_bytes_per_file",
            maximum_bytes_per_file as f64,
            &labels
        );
        gauge!(
            "bytes_per_second",
            f64::from(self.bytes_per_second.get()),
            &labels
        );

        let mut fp = BufWriter::with_capacity(
            ONE_MEBIBYTE * 100,
            fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&self.path)
                .await?,
        );

        for (total_bytes, total_newlines, block) in self.block_cache.iter().cycle() {
            self.rate_limiter.until_n_ready(*total_bytes).await?;

            {
                fp.write_all(block).await?;
                // block.len() and total_bytes are the same numeric value but we
                // avoid needing to get a plain value from a non-zero by calling
                // len here.
                counter!("bytes_written", block.len() as u64, &labels);
                counter!("lines_written", *total_newlines, &labels);

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
                    ONE_MEBIBYTE * 100,
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
