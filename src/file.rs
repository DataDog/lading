use crate::buffer;
use crate::config::{self, LogTarget, Variant};
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
    Config(config::Error),
}

impl From<InsufficientCapacity> for Error {
    fn from(error: InsufficientCapacity) -> Self {
        Error::Governor(error)
    }
}

impl From<config::Error> for Error {
    fn from(error: config::Error) -> Self {
        Error::Config(error)
    }
}

impl From<::std::io::Error> for Error {
    fn from(error: ::std::io::Error) -> Self {
        Error::Io(error)
    }
}

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
    pub async fn new(rng: R, name: String, target: LogTarget) -> Result<Self, Error> {
        let rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock> =
            RateLimiter::direct(
                Quota::per_second(target.bytes_per_second()?)
                    .allow_burst(target.maximum_line_size_bytes()?),
            );

        let maximum_line_size_bytes = target.maximum_line_size_bytes()?;
        let maximum_bytes_per = target.maximum_bytes_per()?;
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
                let nz_bytes = NonZeroU32::new(bytes).unwrap();
                self.rate_limiter.until_n_ready(nz_bytes).await?;

                let slice = &mut buffer[0..bytes as usize];
                let res = match self.variant {
                    Variant::Ascii => buffer::fill_ascii_buffer(&mut self.rng, slice),
                    Variant::Constant => buffer::fill_constant_buffer(&mut self.rng, slice),
                    Variant::Json => buffer::fill_json_buffer(&mut self.rng, slice),
                };
                match res {
                    Ok(filled_bytes) => {
                        self.fp.write(&slice[0..filled_bytes]).await?;
                        bytes_written += filled_bytes as u64;
                        counter!("bytes_written", u64::from(bytes_written), &labels);
                        gauge!("current_target_size_bytes", bytes_written as f64, &labels);
                    }
                    Err(e) => {
                        println!("{:?}", e);
                        continue;
                    }
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
