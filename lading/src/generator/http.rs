//! The HTTP protocol speaking generator.
//!
//! ## Metrics
//!
//! `requests_sent`: Total number of requests sent
//! `request_ok`: Successful requests
//! `request_failure`: Failed requests
//! `bytes_written`: Total bytes written
//! `bytes_per_second`: Configured rate to send data
//! `data_points_transmitted`: Total data points transmitted (for OpenTelemetry metrics)
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

use std::{num::NonZeroU32, thread};

use hyper::{HeaderMap, Request, Uri, header::CONTENT_LENGTH};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use lading_throttle::Throttle;
use metrics::counter;
use once_cell::sync::OnceCell;
use rand::{SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};
use tracing::{error, info};

use crate::common::PeekableReceiver;
use lading_payload::block::{self, Block};

use super::General;

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();

/// The HTTP method to be used in requests
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Method {
    /// Make HTTP Post requests
    Post {
        /// The payload generator to use for this target
        variant: lading_payload::Config,
        /// The maximum size in bytes of the cache of prebuilt messages
        maximum_prebuild_cache_size_bytes: byte_unit::Byte,
        /// Whether to use a fixed or streaming block cache
        #[serde(default = "lading_payload::block::default_cache_method")]
        block_cache_method: block::CacheMethod,
    },
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
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
    pub bytes_per_second: Option<byte_unit::Byte>,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<crate::generator::common::BytesThrottleConfig>,
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
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    /// Failed to convert, value is 0
    #[error("Value provided must not be zero")]
    Zero,
    /// Both `bytes_per_second` and throttle configurations provided
    #[error("Both bytes_per_second and throttle configurations provided. Use only one.")]
    ConflictingThrottleConfig,
    /// No throttle configuration provided
    #[error("Either bytes_per_second or throttle configuration must be provided")]
    NoThrottleConfig,
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] crate::generator::common::ThrottleConversionError),
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
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
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
    pub fn new(
        general: General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "http".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let throttle_config = match (&config.bytes_per_second, &config.throttle) {
            (Some(bps), None) => {
                let bytes_per_second = NonZeroU32::new(bps.as_u128() as u32).ok_or(Error::Zero)?;
                lading_throttle::Config::Stable {
                    maximum_capacity: bytes_per_second,
                }
            }
            (None, Some(throttle_config)) => (*throttle_config).try_into()?,
            (Some(_), Some(_)) => return Err(Error::ConflictingThrottleConfig),
            (None, None) => return Err(Error::NoThrottleConfig),
        };
        let throttle = Throttle::new_with_config(throttle_config);

        match config.method {
            Method::Post {
                variant,
                maximum_prebuild_cache_size_bytes,
                block_cache_method,
            } => {
                let maximum_prebuild_cache_size_bytes =
                    NonZeroU32::new(maximum_prebuild_cache_size_bytes.as_u128() as u32)
                        .ok_or(Error::Zero)?;
                let maximum_block_size = config.maximum_block_size.as_u128();
                let block_cache = match block_cache_method {
                    block::CacheMethod::Fixed => block::Cache::fixed(
                        &mut rng,
                        maximum_prebuild_cache_size_bytes,
                        maximum_block_size,
                        &variant,
                    )?,
                };

                CONNECTION_SEMAPHORE
                    .set(Semaphore::new(config.parallel_connections as usize))
                    .expect("failed to set semaphore");

                Ok(Self {
                    parallel_connections: config.parallel_connections,
                    uri: config.target_uri,
                    method: hyper::Method::POST,
                    headers: config.headers,
                    block_cache,
                    throttle,
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
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(self.parallel_connections as usize)
            .retry_canceled_requests(false)
            .build_http();
        let method = self.method;
        let uri = self.uri;

        let labels = self.metric_labels;
        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let blk = rcv.next().await.expect("block cache should never be empty");
            let total_bytes = blk.total_bytes;

            let body = crate::full(blk.bytes.clone());
            let block_length = blk.bytes.len();

            let mut request = Request::builder()
                .method(method.clone())
                .uri(&uri)
                .header(CONTENT_LENGTH, block_length)
                .body(body)?;
            let headers = request.headers_mut();
            for (k, v) in self.headers.clone().drain() {
                if let Some(k) = k {
                    headers.insert(k, v);
                }
            }

            tokio::select! {
                result = self.throttle.wait_for(total_bytes) => {
                    match result {
                        Ok(()) => {
                            let client = client.clone();
                            let labels = labels.clone();

                            let data_points = blk.metadata.data_points;
                            let permit = CONNECTION_SEMAPHORE.get().expect("Connection Semaphore is being initialized or cell is empty").acquire().await.expect("Connection Semaphore has already closed");
                            tokio::spawn(async move {
                                counter!("requests_sent", &labels).increment(1);
                                match client.request(request).await {
                                    Ok(response) => {
                                        counter!("bytes_written", &labels).increment(block_length as u64);

                                        if let Some(dp) = data_points {
                                            counter!("data_points_transmitted", &labels).increment(dp);
                                        }

                                        let status = response.status();
                                        let mut status_labels = labels.clone();
                                        status_labels
                                            .push(("status_code".to_string(), status.as_u16().to_string()));
                                        counter!("request_ok", &status_labels).increment(1);
                                    }
                                    Err(err) => {
                                        let mut error_labels = labels.clone();
                                        error_labels.push(("error".to_string(), err.to_string()));
                                        counter!("request_failure", &error_labels).increment(1);
                                    }
                                }
                                drop(permit);
                            });

                        }
                        Err(err) => {
                            error!("Throttle request of {total_bytes} is larger than throttle capacity. Block will be discarded. Error: {err}");
                        }
                    }
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // Acquire all available connections, meaning that we have
                    // no outstanding tasks in flight.
                    let _semaphore = CONNECTION_SEMAPHORE.get().expect("Connection Semaphore is being initialized or cell is empty").acquire_many(u32::from(self.parallel_connections)).await.expect("Connection Semaphore has already closed");
                    return Ok(());
                },
            }
        }
    }
}
