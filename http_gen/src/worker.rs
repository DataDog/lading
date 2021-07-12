use crate::config::{Method, Target, Variant};
use futures::stream::{self, StreamExt};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use hyper::client::Client;
use hyper::client::HttpConnector;
use hyper::{Body, Request, Uri};
use lading_common::block::{self, chunk_bytes, construct_block_cache, Block};
use lading_common::payload;
use metrics::{counter, gauge, increment_gauge};
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

const ONE_MEBIBYTE: usize = 1_000_000;
const BLOCK_BYTE_SIZES: [usize; 12] = [
    ONE_MEBIBYTE / 1024,
    ONE_MEBIBYTE / 512,
    ONE_MEBIBYTE / 256,
    ONE_MEBIBYTE / 128,
    ONE_MEBIBYTE / 64,
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
                    &BLOCK_BYTE_SIZES,
                );
                let block_cache = match variant {
                    Variant::Ascii => {
                        construct_block_cache(&payload::Ascii::default(), &block_chunks, &labels)
                    }
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
                    Variant::Static { .. } => unimplemented!(),
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
        let client: Arc<Client<HttpConnector, Body>> = Arc::new(
            Client::builder()
                .pool_max_idle_per_host(self.parallel_connections as usize)
                .set_host(false)
                .build_http(),
        );
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
                let client = Arc::clone(&client);
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
                    .body(body)
                    .unwrap();

                async move {
                    rate_limiter.until_n_ready(total_bytes).await.unwrap();
                    increment_gauge!("inflight_requests", 1.0, &labels);
                    if let Ok(response) = client.request(request).await {
                        counter!("bytes_written", block_length as u64, &labels);
                        let status = response.status();
                        let mut status_labels = labels.clone();
                        status_labels
                            .push(("status_code".to_string(), status.as_u16().to_string()));
                        counter!("response", 1, &status_labels);
                    } else {
                        counter!("request_failure", 1, &labels);
                    }
                    increment_gauge!("inflight_requests", -1.0, &labels);
                }
            })
            .await;
        unimplemented!()
    }
}
