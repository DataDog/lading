use crate::buffer;
use crate::config::{LogTarget, Variant};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state};
use governor::{Quota, RateLimiter};
use metrics::{counter, gauge};
use rand::Rng;
use std::convert::TryInto;
use std::mem;
use std::num::NonZeroU32;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    Io(::std::io::Error),
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

/// The [`Log`] defines a task that emits variant lines to a file, managing
/// rotation and controlling rate limits.
#[derive(Debug)]
pub struct Log {
    path: PathBuf,
    name: String, // this is the stringy version of `path`
    fp: BufWriter<fs::File>,
    maximum_bytes_per_file: NonZeroU32,
    maximum_line_size_bytes: NonZeroU32,
    bytes_per_second: NonZeroU32,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    line_cache: Vec<(NonZeroU32, Vec<u8>)>,
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
    /// This function will panic if the value of `maximum_line_size_bytes`
    /// cannot be coerced into a machine word size.
    pub async fn new<R>(mut rng: R, name: String, target: LogTarget) -> Result<Self, Error>
    where
        R: Rng + Sized,
    {
        let rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock> =
            RateLimiter::direct(
                Quota::per_second(target.bytes_per_second)
                    .allow_burst(target.maximum_line_size_bytes),
            );

        let maximum_line_size_bytes = target.maximum_line_size_bytes;
        let maximum_bytes_per_file = target.maximum_bytes_per_file;
        let fp = BufWriter::with_capacity(
            target.bytes_per_second.get() as usize,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&target.path)
                .await?,
        );

        let mut line_cache = Vec::with_capacity(1024);
        let mut bytes_remaining = target.maximum_prebuild_cache_size_bytes.get() as usize;
        while bytes_remaining > 0 {
            let bytes = rng.gen_range(1..maximum_line_size_bytes.get()) as usize;
            let mut buffer: Vec<u8> = vec![0; bytes];
            let res = match target.variant {
                Variant::Ascii => buffer::fill_ascii(&mut rng, &mut buffer),
                Variant::Constant => buffer::fill_constant(&mut rng, &mut buffer),
                Variant::Json => buffer::fill_json(&mut rng, &mut buffer),
            };
            res.expect("could not pre-fill line");
            let nz_bytes = NonZeroU32::new(bytes.try_into().unwrap()).unwrap();
            line_cache.push((nz_bytes, buffer));
            bytes_remaining = bytes_remaining.saturating_sub(bytes);
        }

        Ok(Self {
            fp,
            maximum_bytes_per_file,
            name,
            path: target.path,
            maximum_line_size_bytes,
            bytes_per_second: target.bytes_per_second,
            rate_limiter,
            line_cache,
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
    pub async fn spin(mut self) -> Result<(), Error> {
        let labels = vec![("target", self.name.clone())];

        let mut bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());
        let bytes_per_second: usize = self.bytes_per_second.get() as usize;

        gauge!(
            "maximum_bytes_per_file",
            maximum_bytes_per_file as f64,
            &labels
        );
        gauge!(
            "maximum_line_size_bytes",
            maximum_bytes_per_file as f64,
            &labels
        );

        for (nz_bytes, line) in self.line_cache.iter().cycle() {
            self.rate_limiter.until_n_ready(*nz_bytes).await?;

            {
                self.fp.write(&line).await?;
                counter!("bytes_written", line.len() as u64, &labels);
                counter!("lines_written", 1, &labels);

                bytes_written += line.len() as u64;
                gauge!("current_target_size_bytes", bytes_written as f64, &labels);
            }

            if bytes_written > maximum_bytes_per_file {
                let slop = (bytes_written - maximum_bytes_per_file).max(0) as f64;
                gauge!("file_rotation_slop", slop, &labels);
                // Delete file, leaving any open file handlers intact. This
                // includes our own `self.fp` for the time being.
                fs::remove_file(&self.path).await?;
                // Open a new fp to `self.path`. Our `self.fp` will point to the
                // original file but it no longer has a name.
                let fp = BufWriter::with_capacity(
                    bytes_per_second,
                    fs::OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(&self.path)
                        .await?,
                );
                // Replace and close the old file handler. At this point any
                // other file handlers open are still valid but we have lost
                // access to the old file pointer.
                drop(mem::replace(&mut self.fp, fp));
                bytes_written = 0;
                counter!("file_rotated", 1, &labels);
            }
        }
        Ok(())
    }
}
