//! The Unix Domain Socket stream speaking generator.

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
    throttle::Throttle,
};
use byte_unit::{Byte, ByteUnit};
use metrics::{counter, gauge};
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
};
use tokio::{net, task::JoinError};
use tracing::{debug, error, info};

#[derive(Debug, Deserialize, PartialEq, Eq)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path of the socket to write to.
    pub path: PathBuf,
    /// The payload variant
    pub variant: payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
}

/// Errors produced by [`UnixStream`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Creation of payload blocks failed.
    #[error("Creation of payload blocks failed: {0}")]
    Block(#[from] block::Error),
    /// Generic IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Subtask error
    #[error("Subtask failure: {0}")]
    Subtask(#[from] JoinError),
}

#[derive(Debug)]
/// The Unix Domain Socket stream generator.
///
/// This generator is responsible for sending data to the target via UDS
/// streams.
pub struct UnixStream {
    path: PathBuf,
    throttle: Throttle,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
}

impl UnixStream {
    /// Create a new [`UnixStream`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroUsize> = config
            .block_sizes
            .clone()
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1.0 / 32.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 16.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 4.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 2.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| NonZeroUsize::new(sz.get_bytes() as usize).expect("bytes must be non-zero"))
            .collect();
        let labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "uds".to_string()),
        ];

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

        let block_chunks = chunk_bytes(
            &mut rng,
            NonZeroUsize::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as usize)
                .expect("bytes must be non-zero"),
            &block_sizes,
        )?;
        let block_cache = construct_block_cache(&mut rng, &config.variant, &block_chunks, &labels);

        Ok(Self {
            path: config.path,
            block_cache,
            throttle: Throttle::new(bytes_per_second),
            metric_labels: labels,
            shutdown,
        })
    }

    /// Run [`UnixStream`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the UDS socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        debug!("UnixStream generator running");
        let labels = self.metric_labels;

        let mut blocks = self.block_cache.iter().cycle().peekable();
        let mut unix_stream = Option::<net::UnixStream>::None;

        loop {
            let blk = blocks.peek().unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                sock = net::UnixStream::connect(&self.path), if unix_stream.is_none() => {
                    match sock {
                        Ok(stream) => {
                            debug!("UDS socket opened for writing.");
                            unix_stream = Some(stream);
                        }
                        Err(err) => {
                            error!("Opening UDS path failed: {}", err);

                            let mut error_labels = labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("connection_failure", 1, &error_labels);
                        }
                    }
                }
                _ = self.throttle.wait_for(total_bytes), if unix_stream.is_some() => {
                    // NOTE When we write into a unix stream it may be that only
                    // some of the written bytes make it through in which case we
                    // must cycle back around and try to write the remainder of the
                    // buffer.
                    let blk_max: usize = total_bytes.get() as usize;
                    let mut blk_offset = 0;
                    while blk_offset < blk_max {
                        let stream = unix_stream.unwrap();
                        unix_stream = None;

                        let ready = stream
                            .ready(tokio::io::Interest::WRITABLE)
                            .await
                            .map_err(Error::Io)
                            .unwrap(); // Cannot ? in a spawned task :<. Mimics UDP generator.
                        if ready.is_writable() {
                            let blk = blocks.next().unwrap(); // actually advance through the blocks
                            // Try to write data, this may still fail with `WouldBlock`
                            // if the readiness event is a false positive.
                            match stream.try_write(&blk.bytes[blk_offset..]) {
                                Ok(bytes) => {
                                    counter!("bytes_written", bytes as u64, &labels);
                                    blk_offset = bytes;
                                }
                                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                                    // If the read side has hung up we will never
                                    // know and will keep attempting to write into
                                    // the stream. This yield means we won't hog the
                                    // whole CPU.
                                    tokio::task::yield_now().await;
                                }
                                Err(err) => {
                                    debug!("write failed: {}", err);

                                    let mut error_labels = labels.clone();
                                    error_labels.push(("error".to_string(), err.to_string()));
                                    counter!("request_failure", 1, &error_labels);
                                    // NOTE we here skip replacing `stream` into
                                    // `unix_stream` and will attempt a new
                                    // connection.
                                    break;
                                }
                            }
                        }
                        unix_stream = Some(stream);
                    }
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}
