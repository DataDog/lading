//! The Unix Domain Socket datagram speaking generator.

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
    throttle::{self, Throttle},
};
use byte_unit::{Byte, ByteUnit};
use futures::future::join_all;
use metrics::{counter, gauge, register_counter};
use rand::{rngs::StdRng, SeedableRng};
use serde::Deserialize;
use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
};
use tokio::{
    net,
    task::{JoinError, JoinHandle},
};
use tracing::{debug, error, info, trace};

fn default_parallel_connections() -> u16 {
    1
}

#[derive(Debug, Deserialize, PartialEq)]
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
    /// The total number of parallel connections to maintain
    #[serde(default = "default_parallel_connections")]
    pub parallel_connections: u16,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: throttle::Config,
}

/// Errors produced by [`UnixDatagram`].
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
    /// Child sub-task error.
    #[error("Child join error: {0}")]
    Child(JoinError),
}

#[derive(Debug)]
/// The Unix Domain Socket datagram generator.
///
/// This generator is responsible for sending data to the target via UDS
/// datagrams.
pub struct UnixDatagram {
    handles: Vec<JoinHandle<Result<(), Error>>>,
    shutdown: Shutdown,
}

impl UnixDatagram {
    /// Create a new [`UnixDatagram`] instance
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
    pub fn new(config: &Config, shutdown: Shutdown) -> Result<Self, Error> {
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
            ("component_name".to_string(), "unix_datagram".to_string()),
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

        let mut handles = Vec::new();
        for _ in 0..config.parallel_connections {
            let block_cache =
                construct_block_cache(&mut rng, &config.variant, &block_chunks, &labels);

            let child = Child {
                path: config.path.clone(),
                block_cache,
                throttle: Throttle::new_with_config(config.throttle, bytes_per_second),
                metric_labels: labels.clone(),
                shutdown: shutdown.clone(),
            };

            handles.push(tokio::spawn(child.spin()));
        }

        Ok(Self { handles, shutdown })
    }

    /// Run [`UnixDatagram`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the UDS socket cannot be written to.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
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

#[derive(Debug)]
struct Child {
    path: PathBuf,
    throttle: Throttle,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
}

impl Child {
    async fn spin(mut self) -> Result<(), Error> {
        debug!("UnixDatagram generator running");
        let socket = net::UnixDatagram::unbound().map_err(Error::Io)?;
        loop {
            match socket.connect(&self.path).map_err(Error::Io) {
                Ok(()) => {
                    info!("Connected socket to {path}", path = &self.path.display(),);
                    break;
                }
                Err(err) => {
                    trace!(
                        "Unable to connect to socket {path}: {err}",
                        path = &self.path.display()
                    );

                    let mut error_labels = self.metric_labels.clone();
                    error_labels.push(("error".to_string(), err.to_string()));
                    counter!("connection_failure", 1, &error_labels);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        let mut blocks = self.block_cache.iter().cycle().peekable();
        let bytes_written = register_counter!("bytes_written", &self.metric_labels);
        let packets_sent = register_counter!("packets_sent", &self.metric_labels);

        loop {
            let blk = blocks.peek().unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    // NOTE When we write into a unix socket it may be that only
                    // some of the written bytes make it through in which case we
                    // must cycle back around and try to write the remainder of the
                    // buffer.
                    let blk = blocks.next().unwrap(); // actually advance through the blocks
                    let blk_max: usize = total_bytes.get() as usize;
                    let mut blk_offset = 0;
                    while blk_offset < blk_max {
                        match socket.send(&blk.bytes[blk_offset..]).await {
                            Ok(bytes) => {
                                bytes_written.increment(bytes as u64);
                                packets_sent.increment(1);
                                blk_offset = bytes;
                            }
                            Err(err) => {
                                debug!("write failed: {}", err);

                                let mut error_labels = self.metric_labels.clone();
                                error_labels.push(("error".to_string(), err.to_string()));
                                counter!("request_failure", 1, &error_labels);
                                break; // while blk_offset
                            }
                        }
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
