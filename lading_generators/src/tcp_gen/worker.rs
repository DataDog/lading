use crate::tcp_gen::config::{Target, Variant};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use lading_common::block::{chunk_bytes, construct_block_cache, Block};
use lading_common::payload;
use metrics::counter;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const ONE_MEBIBYTE: usize = 1_000_000;
const BLOCK_BYTE_SIZES: [usize; 7] = [
    ONE_MEBIBYTE / 32,
    ONE_MEBIBYTE / 16,
    ONE_MEBIBYTE / 8,
    ONE_MEBIBYTE / 4,
    ONE_MEBIBYTE / 2,
    ONE_MEBIBYTE,
    ONE_MEBIBYTE * 2,
];

/// The [`Worker`] defines a task that emits variant lines to an HTTP server
/// controlling throughput.
#[derive(Debug)]
pub struct Worker {
    name: String,
    addr: SocketAddr,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    // Io(::std::io::Error),
    // Block(block::Error),
    // Hyper(hyper::Error),
    // Http(hyper::http::Error),
}

impl From<InsufficientCapacity> for Error {
    fn from(error: InsufficientCapacity) -> Self {
        Error::Governor(error)
    }
}

impl Worker {
    /// Create a new [`Worker`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed non-zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(name: String, target: &Target) -> Result<Self, Error> {
        let mut rng = rand::thread_rng();
        let bytes_per_second = NonZeroU32::new(target.bytes_per_second.get_bytes() as u32).unwrap();
        let rate_limiter = RateLimiter::direct(Quota::per_second(bytes_per_second));
        let labels = vec![
            ("name".to_string(), name.clone()),
            ("target".to_string(), target.addr.to_string()),
        ];
        let block_chunks = chunk_bytes(
            &mut rng,
            target.maximum_prebuild_cache_size_bytes.get_bytes() as usize,
            &BLOCK_BYTE_SIZES,
        );
        let block_cache = match target.variant {
            Variant::Syslog5424 => {
                construct_block_cache(&payload::Syslog5424::default(), &block_chunks, &labels)
            }
            Variant::Fluent => {
                construct_block_cache(&payload::Fluent::default(), &block_chunks, &labels)
            }
        };

        Ok(Self {
            addr: target.addr,
            block_cache,
            name,
            rate_limiter,
            metric_labels: labels,
        })
    }

    /// Enter the main loop of this [`Worker`]
    ///
    /// # Errors
    ///
    /// TODO
    ///
    /// # Panics
    ///
    /// Function will panic if it is unable to create HTTP requests for the
    /// target.
    pub async fn spin(self) -> Result<(), Error> {
        let mut client: TcpStream = TcpStream::connect(self.addr).await.unwrap();

        for blk in self.block_cache.iter().cycle() {
            self.rate_limiter
                .until_n_ready(blk.total_bytes)
                .await
                .unwrap();
            client.write_all(&blk.bytes).await.unwrap();
            counter!(
                "bytes_written",
                u64::from(blk.total_bytes.get()),
                &self.metric_labels
            );
        }
        unreachable!()
    }
}
