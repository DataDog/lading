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

use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
};

use hyper::{HeaderMap, Request, Uri, header::CONTENT_LENGTH};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use metrics::{counter, histogram};
use once_cell::sync::OnceCell;
use rand::{SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{error, info};

use lading_payload::block;

use super::General;
use crate::generator::common::{
    BlockThrottle, ConcurrencyStrategy, MetricsBuilder, ThrottleConfig, ThrottleConversionError,
    create_throttle,
};

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();

/// The HTTP method to be used in requests
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
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
    pub throttle: Option<ThrottleConfig>,
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
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] ThrottleConversionError),
    /// Error making HTTP request
    #[error("Failed to send HTTP request to {uri}: {source}")]
    RequestFailed {
        /// Target URI
        uri: String,
        /// Underlying hyper error
        #[source]
        source: Box<hyper::Error>,
    },
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
    concurrency: ConcurrencyStrategy,
    throttle: BlockThrottle,
    block_cache: Arc<block::Cache>,
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

        let labels = MetricsBuilder::new("http").with_id(general.id).build();

        let throttle = create_throttle(config.throttle.as_ref(), config.bytes_per_second.as_ref())?;

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
                    block::CacheMethod::Fixed => block::Cache::fixed_with_max_overhead(
                        &mut rng,
                        maximum_prebuild_cache_size_bytes,
                        maximum_block_size,
                        &variant,
                        // NOTE we bound payload generation to have overhead only
                        // equivalent to the prebuild cache size,
                        // `maximum_prebuild_cache_size_bytes`. This means on systems with plentiful
                        // memory we're under generating entropy, on systems with
                        // minimal memory we're over-generating.
                        //
                        // `lading::get_available_memory` suggests we can learn to
                        // divvy this up in the future.
                        maximum_prebuild_cache_size_bytes.get() as usize,
                    )?,
                };

                let concurrency =
                    ConcurrencyStrategy::new(NonZeroU16::new(config.parallel_connections), false);

                // Set the global semaphore based on the concurrency strategy
                CONNECTION_SEMAPHORE
                    .set(Semaphore::new(concurrency.connection_count() as usize))
                    .expect("failed to set semaphore");

                Ok(Self {
                    concurrency,
                    uri: config.target_uri,
                    method: hyper::Method::POST,
                    headers: config.headers,
                    block_cache: Arc::new(block_cache),
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
            .pool_max_idle_per_host(self.concurrency.connection_count() as usize)
            .retry_canceled_requests(false)
            .build_http();
        let method = self.method;
        let uri = self.uri;
        let labels = self.metric_labels;

        let mut handle = self.block_cache.handle();
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                result = self.throttle.wait_for_block(&self.block_cache, &handle) => {
                    match result {
                        Ok(()) => {
                            let block = self.block_cache.advance(&mut handle);
                            let bytes = block.bytes.clone();
                            let metadata = block.metadata;
                            let block_length = bytes.len();

                            let body = crate::full(bytes);

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
                            let client = client.clone();
                            let labels = labels.clone();
                            let data_points = metadata.data_points;

                            let uri_clone = uri.clone();
                            let permit = CONNECTION_SEMAPHORE.get().expect("Connection Semaphore is being initialized or cell is empty").acquire().await.expect("Connection Semaphore has already closed");
                            tokio::spawn(async move {
                                counter!("requests_sent", &labels).increment(1);
                                match client.request(request).await {
                                    Ok(response) => {
                                        counter!("bytes_written", &labels).increment(block_length as u64);
                                        histogram!("bytes_written_histogram", &labels).record(block_length as f64);

                                        if let Some(dp) = data_points {
                                            counter!("data_points_transmitted", &labels).increment(dp);
                                        }

                                        let status = response.status();
                                        let mut status_labels = labels.clone();
                                        status_labels
                                            .push(("status_code".to_string(), status.as_u16().to_string()));
                                        counter!("request_ok", &status_labels).increment(1);
                                    }
                                    Err(source) => {
                                        error!(
                                            "Failed to send HTTP request to {uri}: {source}",
                                            uri = uri_clone
                                        );
                                        let mut error_labels = labels.clone();
                                        error_labels.push(("error".to_string(), source.to_string()));
                                        counter!("request_failure", &error_labels).increment(1);
                                    }
                                }
                                drop(permit);
                            });

                        }
                        Err(err) => {
                            error!("Discarding block due to throttle error: {err}");
                        }
                    }
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // Acquire all available connections, meaning that we have
                    // no outstanding tasks in flight.
                    let _semaphore = CONNECTION_SEMAPHORE.get().expect("Connection Semaphore is being initialized or cell is empty").acquire_many(u32::from(self.concurrency.connection_count())).await.expect("Connection Semaphore has already closed");
                    return Ok(());
                },
            }
        }
    }
}
