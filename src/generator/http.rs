use std::{
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
};

use byte_unit::{Byte, ByteUnit};
use governor::{
    clock, state,
    state::direct::{self, InsufficientCapacity},
    Quota, RateLimiter,
};
use hyper::{
    client::{Client, HttpConnector},
    header::CONTENT_LENGTH,
    Body, HeaderMap, Request, Uri,
};
use metrics::counter;
use once_cell::sync::OnceCell;
use rand::{prelude::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tracing::info;

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
};

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();

/// The HTTP method to be used in requests
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// Make HTTP Post requests
    Post {
        /// The payload generator to use for this target
        variant: Variant,
        /// The maximum size in bytes of the cache of prebuilt messages
        maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Variant {
    /// Generates Splunk HEC messages
    SplunkHec,
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
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// The method to use against the URI
    pub method: Method,
    /// Headers to include in the request
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
}

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    Io(::std::io::Error),
    Block(block::Error),
    Hyper(hyper::Error),
    Http(hyper::http::Error),
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Self {
        Error::Block(error)
    }
}

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Error::Hyper(error)
    }
}

impl From<hyper::http::Error> for Error {
    fn from(error: hyper::http::Error) -> Self {
        Error::Http(error)
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

/// The [`Worker`] defines a task that emits variant lines to an HTTP server
/// controlling throughput.
#[derive(Debug)]
pub struct Http {
    uri: Uri,
    method: hyper::Method,
    headers: hyper::HeaderMap,
    parallel_connections: u16,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
}

impl Http {
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
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroUsize> = config
            .block_sizes
            .unwrap_or_else(|| {
                vec![
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
        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        let rate_limiter = RateLimiter::direct(Quota::per_second(bytes_per_second));
        let labels = vec![];
        match config.method {
            Method::Post {
                variant,
                maximum_prebuild_cache_size_bytes,
            } => {
                let block_chunks = chunk_bytes(
                    &mut rng,
                    NonZeroUsize::new(maximum_prebuild_cache_size_bytes.get_bytes() as usize)
                        .expect("bytes must be non-zero"),
                    &block_sizes,
                )?;
                let block_cache = match variant {
                    Variant::Ascii => construct_block_cache(
                        &mut rng,
                        &payload::Ascii::default(),
                        &block_chunks,
                        &labels,
                    ),
                    Variant::SplunkHec => construct_block_cache(
                        &mut rng,
                        &payload::SplunkHec::default(),
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
                    Variant::Static { static_path } => construct_block_cache(
                        &mut rng,
                        &payload::Static::new(&static_path),
                        &block_chunks,
                        &labels,
                    ),
                };

                CONNECTION_SEMAPHORE
                    .set(Semaphore::new(config.parallel_connections as usize))
                    .unwrap();

                Ok(Self {
                    parallel_connections: config.parallel_connections,
                    uri: config.target_uri,
                    method: hyper::Method::POST,
                    headers: config.headers,
                    block_cache,
                    rate_limiter,
                    metric_labels: labels,
                    shutdown,
                })
            }
        }
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
    pub async fn spin(mut self) -> Result<(), Error> {
        let client: Client<HttpConnector, Body> = Client::builder()
            .pool_max_idle_per_host(self.parallel_connections as usize)
            .retry_canceled_requests(false)
            .set_host(false)
            .build_http();
        let rate_limiter = Arc::new(self.rate_limiter);
        let method = self.method;
        let uri = self.uri;

        let labels = self.metric_labels;
        let mut blocks = self.block_cache.iter().cycle();

        loop {
            let blk = blocks.next().unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = rate_limiter.until_n_ready(total_bytes) => {
                    let client = client.clone();
                    let labels = labels.clone();
                    let method = method.clone();
                    let uri = uri.clone();

                    let body = Body::from(blk.bytes.clone());
                    let block_length = blk.bytes.len();

                    let mut request: Request<Body> = Request::builder()
                        .method(method)
                        .uri(uri)
                        .header(CONTENT_LENGTH, block_length)
                        .body(body)
                        .unwrap();
                    let headers = request.headers_mut();
                    for (k,v) in self.headers.clone().drain() {
                        if let Some(k) = k {
                            headers.insert(k,v);
                        }
                    }

                    let permit = CONNECTION_SEMAPHORE.get().unwrap().acquire().await.unwrap();
                    tokio::spawn(async move {
                        counter!("requests_sent", 1, &labels);
                        match client.request(request).await {
                            Ok(response) => {
                                counter!("bytes_written", block_length as u64, &labels);
                                let status = response.status();
                                let mut status_labels = labels.clone();
                                status_labels
                                    .push(("status_code".to_string(), status.as_u16().to_string()));
                                counter!("request_ok", 1, &status_labels);
                            }
                            Err(err) => {
                                let mut error_labels = labels.clone();
                                error_labels.push(("error".to_string(), err.to_string()));
                                counter!("request_failure", 1, &error_labels);
                            }
                        }
                        drop(permit);
                    });
                },
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    // Acquire all available connections, meaning that we have
                    // no outstanding tasks in flight.
                    let _semaphore = CONNECTION_SEMAPHORE.get().unwrap().acquire_many(u32::from(self.parallel_connections)).await.unwrap();
                    return Ok(());
                },
            }
        }
    }
}
