use crate::http_gen::config::{Method, Target, Variant};
use byte_unit::{Byte, ByteUnit};
use futures::stream::{self, StreamExt};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use hyper::client::Client;
use hyper::client::HttpConnector;
use hyper::header::CONTENT_LENGTH;
use hyper::{Body, Request, Uri};
use lading_common::block::{self, chunk_bytes, construct_block_cache, Block};
use lading_common::payload;
use metrics::{counter, gauge};
use std::num::NonZeroU32;
use std::sync::Arc;

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
pub struct Worker {
    uri: Uri,
    method: hyper::Method,
    headers: hyper::HeaderMap,
    name: String, // this is the stringy version of `path`
    parallel_connections: u16,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
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
    pub fn new(name: String, target: Target) -> Result<Self, Error> {
        let mut rng = rand::thread_rng();
        let block_sizes: Vec<usize> = target
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
            .map(|sz| sz.get_bytes() as usize)
            .collect();
        let bytes_per_second = NonZeroU32::new(target.bytes_per_second.get_bytes() as u32).unwrap();
        let rate_limiter = RateLimiter::direct(Quota::per_second(bytes_per_second));
        let labels = vec![
            ("name".to_string(), name.clone()),
            ("target".to_string(), target.target_uri.to_string()),
        ];
        match target.method {
            Method::Post {
                variant,
                maximum_prebuild_cache_size_bytes,
            } => {
                let block_chunks = chunk_bytes(
                    &mut rng,
                    maximum_prebuild_cache_size_bytes.get_bytes() as usize,
                    &block_sizes,
                );
                let block_cache = match variant {
                    Variant::Ascii => {
                        construct_block_cache(&payload::Ascii::default(), &block_chunks, &labels)
                    }
                    Variant::SplunkHec => construct_block_cache(
                        &payload::SplunkHec::default(),
                        &block_chunks,
                        &labels,
                    ),
                    Variant::DatadogLog => construct_block_cache(
                        &payload::DatadogLog::default(),
                        &block_chunks,
                        &labels,
                    ),
                    Variant::Json => {
                        construct_block_cache(&payload::Json::default(), &block_chunks, &labels)
                    }
                    Variant::FoundationDb => construct_block_cache(
                        &payload::FoundationDb::default(),
                        &block_chunks,
                        &labels,
                    ),
                    Variant::Static { static_path } => construct_block_cache(
                        &payload::Static::new(&static_path),
                        &block_chunks,
                        &labels,
                    ),
                };

                Ok(Self {
                    parallel_connections: target.parallel_connections,
                    uri: target.target_uri,
                    method: hyper::Method::POST,
                    headers: target.headers,
                    block_cache,
                    name,
                    rate_limiter,
                    metric_labels: labels,
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
    pub async fn spin(self) -> Result<(), Error> {
        let client: Client<HttpConnector, Body> = Client::builder()
            .pool_max_idle_per_host(self.parallel_connections as usize)
            .retry_canceled_requests(false)
            .set_host(false)
            .build_http();
        let rate_limiter = Arc::new(self.rate_limiter);
        let method = self.method;
        let uri = self.uri;

        let labels = self.metric_labels;
        gauge!(
            "maximum_requests",
            f64::from(self.parallel_connections),
            &labels
        );
        stream::iter(self.block_cache.iter().cycle())
            .for_each_concurrent(self.parallel_connections as usize, |blk| {
                let client = client.clone();
                let rate_limiter = Arc::clone(&rate_limiter);
                let labels = labels.clone();
                let method = method.clone();
                let uri = uri.clone();

                let total_bytes = blk.total_bytes;
                let body = Body::from(blk.bytes.clone());
                let block_length = blk.bytes.len();

                let request: Request<Body> = Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(CONTENT_LENGTH, block_length)
                    .body(body)
                    .unwrap();

                async move {
                    rate_limiter.until_n_ready(total_bytes).await.unwrap();
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
                            error_labels.push(("body_size".to_string(), block_length.to_string()));
                            counter!("request_failure", 1, &error_labels);
                        }
                    }
                }
            })
            .await;
        unreachable!()
    }
}
