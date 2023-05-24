//! The HTTP protocol speaking generator.

use std::num::{NonZeroU32, NonZeroUsize};

use byte_unit::{Byte, ByteUnit};
use hyper::{
    client::{Client, HttpConnector},
    header::CONTENT_LENGTH,
    Body, HeaderMap, Request, Uri,
};
use metrics::{counter, gauge};
use once_cell::sync::OnceCell;
use rand::{prelude::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tracing::info;

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
    throttle::{self, Throttle},
};

use super::General;

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();

/// The HTTP method to be used in requests
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// Make HTTP Post requests
    Post {
        /// The payload generator to use for this target
        variant: payload::Config,
        /// The maximum size in bytes of the cache of prebuilt messages
        maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    },
}

#[derive(Debug, Deserialize, PartialEq)]
/// Configuration of this generator.
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
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: throttle::Config,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Http`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
    /// Wrapper around [`hyper::Error`].
    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),
    /// Wrapper around [`hyper::http::Error`].
    #[error("HTTP error: {0}")]
    Http(#[from] hyper::http::Error),
}

/// The HTTP generator.
///
/// This generator is reposnsible for connecting to the target via HTTP. Today
/// only POST and GET are supported.
#[derive(Debug)]
pub struct Http {
    uri: Uri,
    method: hyper::Method,
    headers: hyper::HeaderMap,
    parallel_connections: u16,
    throttle: Throttle,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    shutdown: Shutdown,
}

impl Http {
    /// Create a new [`Http`] instance
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
    pub fn new(general: General, config: Config, shutdown: Shutdown) -> Result<Self, Error> {
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
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "http".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

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
                let block_cache = construct_block_cache(&mut rng, &variant, &block_chunks, &labels);

                CONNECTION_SEMAPHORE
                    .set(Semaphore::new(config.parallel_connections as usize))
                    .unwrap();

                Ok(Self {
                    parallel_connections: config.parallel_connections,
                    uri: config.target_uri,
                    method: hyper::Method::POST,
                    headers: config.headers,
                    block_cache,
                    throttle: Throttle::new_with_config(config.throttle, bytes_per_second),
                    metric_labels: labels,
                    shutdown,
                })
            }
        }
    }

    /// Run [`Http`] to completion or until a shutdown signal is received.
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
        let method = self.method;
        let uri = self.uri;

        let labels = self.metric_labels;
        let mut blocks = self.block_cache.iter().cycle();

        loop {
            let blk = blocks.next().unwrap();
            let total_bytes = blk.total_bytes;

            let body = Body::from(blk.bytes.clone());
            let block_length = blk.bytes.len();

            let mut request: Request<Body> = Request::builder()
                .method(method.clone())
                .uri(&uri)
                .header(CONTENT_LENGTH, block_length)
                .body(body)
                .unwrap();
            let headers = request.headers_mut();
            for (k, v) in self.headers.clone().drain() {
                if let Some(k) = k {
                    headers.insert(k, v);
                }
            }

            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let client = client.clone();
                    let labels = labels.clone();

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
