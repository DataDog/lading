use crate::buffer;
use crate::config::{LogTarget, Variant};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state};
use governor::{Quota, RateLimiter};
use metrics::{counter, gauge};
use rand::Rng;
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
pub struct Log<R>
where
    R: Rng + Sized,
{
    path: PathBuf,
    name: String, // this is the stringy version of `path`
    fp: BufWriter<fs::File>,
    variant: Variant,
    maximum_bytes_per: NonZeroU32,
    maximum_line_size_bytes: NonZeroU32,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    rng: R,
}

impl<R> Log<R>
where
    R: Rng + Sized,
{
    /// Create a new [`Log`]
    ///
    /// A new instance of this type requires a random generator, its name and
    /// the [`LogTarget`] for this task. The name will be used in telemetry and
    /// should be unique, though no check is done here to ensure that it is.
    ///
    /// # Errors
    ///
    /// Creation will fail if the target file cannot be opened for writing.
    pub async fn new(rng: R, name: String, target: LogTarget) -> Result<Self, Error> {
        let rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock> =
            RateLimiter::direct(
                Quota::per_second(target.bytes_per_second)
                    .allow_burst(target.maximum_line_size_bytes),
            );

        let maximum_line_size_bytes = target.maximum_line_size_bytes;
        let maximum_bytes_per = target.maximum_bytes_per;
        let fp = BufWriter::with_capacity(
            maximum_line_size_bytes.get() as usize,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&target.path)
                .await?,
        );

        Ok(Self {
            fp,
            maximum_bytes_per,
            name,
            path: target.path,
            variant: target.variant,
            maximum_line_size_bytes,
            rate_limiter,
            rng,
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
        let maximum_bytes_per: u64 = u64::from(self.maximum_bytes_per.get());
        let maximum_line_size_bytes: u32 = self.maximum_line_size_bytes.get();

        gauge!("maximum_bytes_per", maximum_bytes_per as f64, &labels);
        gauge!("maximum_line_size_bytes", maximum_bytes_per as f64, &labels);

        let mut buffer: Vec<u8> = vec![0; self.maximum_line_size_bytes.get() as usize];

        loop {
            {
                let bytes = self.rng.gen_range(1..maximum_line_size_bytes);
                let nz_bytes =
                    NonZeroU32::new(bytes).expect("invalid condition, should never trigger");
                self.rate_limiter.until_n_ready(nz_bytes).await?;

                let slice = &mut buffer[0..bytes as usize];
                let res = match self.variant {
                    Variant::Ascii => buffer::fill_ascii(&mut self.rng, slice),
                    Variant::Constant => buffer::fill_constant(&mut self.rng, slice),
                    Variant::Json => buffer::fill_json(&mut self.rng, slice),
                };
                if let Ok(filled_bytes) = res {
                    self.fp.write(&slice[0..filled_bytes]).await?;
                    bytes_written += filled_bytes as u64;
                    counter!("bytes_written", bytes_written, &labels);
                    gauge!("current_target_size_bytes", bytes_written as f64, &labels);
                } else {
                    counter!("unable_to_write_to_target", 1, &labels);
                    continue;
                }
            }

            if bytes_written > maximum_bytes_per {
                let slop = (bytes_written - maximum_bytes_per).max(0) as f64;
                counter!("file_rotated", 1, &labels);
                gauge!("file_rotation_slop", slop, &labels);
                let fp = BufWriter::with_capacity(
                    maximum_line_size_bytes as usize,
                    fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&self.path)
                        .await?,
                );
                drop(mem::replace(&mut self.fp, fp));
                bytes_written = 0;
            }
        }
    }
}
