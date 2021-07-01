use crate::config::{LogTarget, Variant};
use crate::payload::{self, Serialize};
use arbitrary::{self, Arbitrary, Unstructured};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use metrics::{counter, gauge};
use rand::prelude::SliceRandom;
use rand::Rng;
use std::convert::TryInto;
use std::num::NonZeroU32;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    Io(::std::io::Error),
    Payload(payload::Error),
    Arbitrary(arbitrary::Error),
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

#[allow(clippy::ptr_arg)]
#[allow(clippy::cast_precision_loss)]
fn construct_block_cache<R>(
    mut rng: R,
    target: &LogTarget,
    labels: &Vec<(String, String)>,
) -> Result<Vec<(NonZeroU32, Vec<u8>)>, Error>
where
    R: Rng + Sized,
{
    let bytes_per_second: usize = target.bytes_per_second.get() as usize;
    let mut block_cache: Vec<(NonZeroU32, Vec<u8>)> =
        Vec::with_capacity(bytes_per_second / ONE_MEBIBYTE);

    let mut block_cache_bytes_remaining = target.maximum_prebuild_cache_size_bytes.get() as usize;
    while block_cache_bytes_remaining > ONE_MEBIBYTE {
        gauge!(
            "block_cache_bytes_remaining",
            block_cache_bytes_remaining as f64,
            labels
        );
        let bytes = {
            let block_bytes_max = std::cmp::min(bytes_per_second, block_cache_bytes_remaining);

            let block_bytes = BLOCK_BYTE_SIZES.choose(&mut rng).unwrap();
            if *block_bytes > block_bytes_max {
                continue;
            }
            let mut bytes: Vec<u8> = vec![0; *block_bytes];
            rng.fill_bytes(&mut bytes);
            bytes
        };
        let unstructured: Unstructured = Unstructured::new(&bytes);

        let mut block: Vec<u8> = Vec::new();
        match target.variant {
            Variant::Ascii => {
                payload::Ascii::arbitrary_take_rest(unstructured)?.to_bytes(&mut block)?;
            }
            Variant::Json => {
                payload::Json::arbitrary_take_rest(unstructured)?.to_bytes(&mut block)?;
            }
        }

        block.shrink_to_fit();
        if block.is_empty() {
            counter!("block_rejected_empty", 1, labels);
            continue;
        }
        if block.len() > bytes_per_second {
            counter!("block_rejected_too_big", 1, labels);
            continue;
        }

        let nz_bytes = NonZeroU32::new(block.len().try_into().unwrap()).unwrap();
        block_cache_bytes_remaining = block_cache_bytes_remaining.saturating_sub(block.len());
        block_cache.push((nz_bytes, block));
    }
    gauge!("block_cache_bytes_remaining", 0.0, labels);
    block_cache.shrink_to_fit();
    block_cache.shuffle(&mut rng);
    gauge!("block_construction_complete", 1.0, labels);
    Ok(block_cache)
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
    block_cache: Vec<(NonZeroU32, Vec<u8>)>,
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
        let block_cache = construct_block_cache(rng, &target, &labels)?;

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

        let mut fp = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.path)
            .await?;

        for (nz_bytes, block) in self.block_cache.iter().cycle() {
            self.rate_limiter.until_n_ready(*nz_bytes).await?;

            {
                // NOTE we intentionally do not wait on the write to
                // complete. The rate_limiter is the sole governor of whether
                // writes are attempted, avoiding coordinated omission.
                let _ = fp.write(block);
                counter!("bytes_written", block.len() as u64, &labels);
                counter!("lines_written", 1, &labels);

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
                fp = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&self.path)
                    .await?;
                bytes_written = 0;
                counter!("file_rotated", 1, &labels);
            }
        }
        unreachable!()
    }
}
