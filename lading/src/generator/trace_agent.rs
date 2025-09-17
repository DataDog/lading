//! The Datadog trace-agent generator.
//!
//! Unlike most generators in the lading project this generator is specific to a
//! single target, the Datadog Agent trace-agent. The reason for this is that
//! the trace-agent has non-trivial semantic demands on the client, meaning our
//! generic HTTP generator is not sufficient. This generator, also uniquely,
//! does not allow for generic payloads: only the trace-agent payload is
//! configurable.
//!
//! ## Metrics
//!
//! `requests_sent`: Total number of requests sent to trace-agent
//! `request_ok`: Successful requests (2xx responses)
//! `request_failure`: Failed requests
//! `bytes_written`: Total bytes written
//!
//! Additional metrics may be emitted by this generator's [throttle].
use super::General;
use crate::generator::common::{
    BytesThrottleConfig, ConcurrencyStrategy, MetricsBuilder, ThrottleConversionError,
    create_throttle,
};
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use hyper::{Request, Uri};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use lading_payload::block;
use metrics::counter;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

const MAX_RETRY_MILLIS: u16 = 6_400;

/// Validates cache size is non-zero and fits within `u32::MAX`
///
/// Returns the validated cache size as `NonZeroU32` or an error if invalid.
///
/// # Errors
///
/// Returns an error if the cache size is zero or exceeds `u32::MAX`.
pub fn validate_cache_size(cache_size_bytes: byte_unit::Byte) -> Result<NonZeroU32, Error> {
    let cache_size = cache_size_bytes.as_u128();
    if cache_size == 0 {
        return Err(Error::Zero);
    }
    if cache_size > u128::from(u32::MAX) {
        return Err(Error::CacheSizeExceedsLimit { size: cache_size });
    }
    // Safe cast: we've validated cache_size <= u32::MAX and > 0
    #[allow(clippy::cast_possible_truncation)] // Validated to be <= u32::MAX above
    NonZeroU32::new(cache_size as u32).ok_or(Error::Zero)
}

/// Backoff behavior when receiving 429/503 responses from trace-agent
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackoffBehavior {
    /// Obey 429/503 responses with exponential backoff
    Obey {
        /// Maximum number of retry attempts
        max_retries: u16,
    },
    /// Ignore all backoff signals
    Ignore,
}

impl BackoffBehavior {
    fn max_retries(self) -> u16 {
        match self {
            Self::Ignore => 0,
            Self::Obey { max_retries } => max_retries,
        }
    }
}

impl Default for BackoffBehavior {
    fn default() -> Self {
        Self::Obey { max_retries: 3 }
    }
}

struct Backoff {
    behavior: BackoffBehavior,
    attempts: u16,
}

impl Backoff {
    fn new(behavior: BackoffBehavior) -> Self {
        Self {
            behavior,
            attempts: 0,
        }
    }

    /// Wait for unspecified amount of time based on previous wait attempts, up
    /// to `max_attempts` times.
    ///
    /// Returns Some(()) if the wait was success, None otherwise.
    async fn wait(&mut self) -> Option<()> {
        match self.behavior {
            BackoffBehavior::Obey { .. } => {
                if self.attempts >= self.behavior.max_retries() {
                    return None;
                }

                // 2^attempts * 100 milliseconds, capped at 6400 milliseconds
                let delay_ms = (1u16 << self.attempts).min(MAX_RETRY_MILLIS);
                let delay = Duration::from_millis(delay_ms.into());

                self.attempts = self.attempts.saturating_add(1);
                // NOTE(blt): I'm becoming increasingly
                // disatisfied with the use of `sleep` in this
                // project, outside of code that rigorously
                // specifies time.
                tokio::time::sleep(delay).await;
            }
            BackoffBehavior::Ignore => {}
        }
        Some(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
/// Version of the trace-agent protocol to use
pub enum Version {
    /// v0.4
    #[serde(rename = "v0.4")]
    V04,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Version::V04 => write!(f, "v0.4"),
        }
    }
}

/// Configuration of the trace-agent generator.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The base URI for the trace-agent
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// Backoff behavior, governs how this generator responds to trace-agent
    /// signals
    #[serde(default)]
    pub backoff_behavior: BackoffBehavior,
    /// The payload generator configuration
    pub variant: lading_payload::trace_agent::Config,
    /// The bytes per second to send to trace-agent
    pub bytes_per_second: Option<byte_unit::Byte>,
    /// The maximum size in bytes of the largest block in the prebuilt cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    pub block_cache_method: block::CacheMethod,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<BytesThrottleConfig>,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`TraceAgent`].
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
    /// Throttle error
    #[error("Throttle error: {0}")]
    Throttle(#[from] lading_throttle::Error),
    /// Invalid version
    #[error("Unsupported trace-agent version: {0}")]
    InvalidVersion(String),
    /// Cache size exceeds maximum
    #[error("Maximum prebuild cache size ({size} bytes) exceeds u32::MAX")]
    CacheSizeExceedsLimit {
        /// The cache size that exceeded the limit
        size: u128,
    },
    /// Target URI missing required component
    #[error("Target URI missing {component}: {uri}")]
    MissingUriComponent {
        /// The missing component
        component: &'static str,
        /// The URI that was missing the component
        uri: String,
    },
}

/// The Datadog Trace Agent generator.
///
/// This generator sends trace payloads to a Datadog trace-agent using the
/// appropriate API version and handles backoff responses.
#[derive(Debug)]
pub struct TraceAgent {
    trace_endpoint: Uri,
    backoff_behavior: BackoffBehavior,
    concurrency: ConcurrencyStrategy,
    throttle: lading_throttle::Throttle,
    block_cache: Arc<block::Cache>,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
    semaphore: Arc<Semaphore>,
}

impl TraceAgent {
    /// Create a new [`TraceAgent`] instance
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
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);

        let labels = MetricsBuilder::new("trace_agent")
            .with_id(general.id)
            .build();

        let throttle = create_throttle(config.throttle.as_ref(), config.bytes_per_second.as_ref())?;

        let maximum_prebuild_cache_size_bytes =
            validate_cache_size(config.maximum_prebuild_cache_size_bytes)?;
        let maximum_block_size = config.maximum_block_size.as_u128();
        let block_config = lading_payload::Config::TraceAgent(config.variant);
        let block_cache = Arc::new(match config.block_cache_method {
            block::CacheMethod::Fixed => block::Cache::fixed_with_max_overhead(
                &mut rng,
                maximum_prebuild_cache_size_bytes,
                maximum_block_size,
                &block_config,
                maximum_prebuild_cache_size_bytes.get() as usize,
            )?,
        });

        let trace_endpoint =
            Uri::builder()
                .authority(
                    config
                        .target_uri
                        .authority()
                        .ok_or_else(|| Error::MissingUriComponent {
                            component: "authority",
                            uri: config.target_uri.to_string(),
                        })?
                        .as_str(),
                )
                .scheme(config.target_uri.scheme_str().ok_or_else(|| {
                    Error::MissingUriComponent {
                        component: "scheme",
                        uri: config.target_uri.to_string(),
                    }
                })?)
                .path_and_query(config.variant.endpoint_path())
                .build()?;

        let concurrency =
            ConcurrencyStrategy::new(NonZeroU16::new(config.parallel_connections), false);
        let semaphore = Arc::new(Semaphore::new(concurrency.connection_count() as usize));

        Ok(Self {
            trace_endpoint,
            backoff_behavior: config.backoff_behavior,
            concurrency,
            throttle,
            block_cache,
            metric_labels: labels,
            shutdown,
            semaphore,
        })
    }

    /// Run the trace agent generator
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying hyper client signals
    /// error or if throttle operations fail.
    ///
    /// # Panics
    ///
    /// Function will panic if it is unable to create HTTP requests for the
    /// trace-agent endpoint.
    pub async fn spin(mut self) -> Result<(), Error> {
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(self.concurrency.connection_count() as usize)
            .retry_canceled_requests(false)
            .build_http();

        let labels = self.metric_labels;
        let backoff_behavior = self.backoff_behavior;

        let mut handle = self.block_cache.handle();
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let total_bytes = self.block_cache.peek_next_size(&handle);
            tokio::select! {
                result = self.throttle.wait_for(total_bytes) => {
                    match result {
                        Ok(()) => {
                            let block = self.block_cache.advance(&mut handle);
                            let bytes = block.bytes.clone();
                            let metadata = block.metadata;

                            let client = client.clone();
                            let trace_endpoint = self.trace_endpoint.clone();
                            let labels = labels.clone();
                            let data_points = metadata.data_points;
                            let semaphore = Arc::clone(&self.semaphore);

                            tokio::spawn(async move {
                                let Ok(permit) = semaphore.acquire().await else {
                                    error!("Connection semaphore closed");
                                    return;
                                };
                                handle_request(client, &trace_endpoint, bytes, backoff_behavior, &labels, data_points).await;
                                drop(permit);
                            });
                        }
                        Err(err) => {
                            error!("Throttle request of {total_bytes} is larger than throttle capacity. Block will be discarded. Error: {err}", total_bytes = total_bytes);
                        }
                    }
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // Acquire all available connections, meaning that we have
                    // no outstanding tasks in flight.
                    let _permit = self.semaphore
                        .acquire_many(u32::from(self.concurrency.connection_count()))
                        .await;
                    return Ok(());
                },
            }
        }
    }
}

/// Handle a single request with backoff behavior
///
/// Implicitly this is confusing a generic request with a v0.4 request. Given
/// that we only handle v0.4 load currently we consider this acceptable for the
/// time being. Once `lading_payload` supports more than v0.4 this will need to
/// be addressed.
async fn handle_request(
    client: Client<HttpConnector, BoxBody<Bytes, hyper::Error>>,
    trace_endpoint: &Uri,
    bytes: Bytes,
    backoff_behavior: BackoffBehavior,
    labels: &[(String, String)],
    data_points: Option<u64>,
) {
    let block_length = bytes.len();

    let mut backoff = Backoff::new(backoff_behavior);
    loop {
        let body = crate::full(bytes.clone());

        let request = Request::builder()
            .method(hyper::Method::POST)
            .uri(trace_endpoint)
            .header("Content-Type", "application/msgpack")
            .header("Content-Length", block_length)
            .header("Datadog-Send-Real-Http-Status", "true")
            .body(body);

        let request = match request {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to build request: {e}");
                return;
            }
        };
        counter!("requests_sent", labels).increment(1);

        match client.request(request).await {
            Ok(response) => {
                let status = response.status();
                let status_code = status.as_u16();

                let mut status_labels = Vec::with_capacity(labels.len() + 1);
                status_labels.extend_from_slice(labels);
                status_labels.push(("status_code".to_string(), status_code.to_string()));

                match status_code {
                    200..=299 => {
                        counter!("request_ok", &status_labels).increment(1);
                        counter!("bytes_written", labels).increment(block_length as u64);
                        if let Some(dp) = data_points {
                            counter!("data_points_transmitted", labels).increment(dp);
                        }
                        return;
                    }
                    429 | 503 => {
                        info!("Received {status_code} response, will attempt to retry.",);
                        if let Some(()) = backoff.wait().await {
                        } else {
                            info!("Retries exceeded.",);
                            counter!("request_failure", &status_labels).increment(1);
                            return;
                        }
                    }
                    code => {
                        let location = response
                            .headers()
                            .get("location")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("none");
                        warn!(
                            code = ?code,
                            uri = %trace_endpoint,
                            location = location,
                            "Unhandled status code, ignoring"
                        );
                        return;
                    }
                }
            }
            Err(err) => {
                let mut error_labels = Vec::with_capacity(labels.len() + 1);
                error_labels.extend_from_slice(labels);
                error_labels.push(("error".to_string(), err.to_string()));
                counter!("request_failure", &error_labels).increment(1);
                return;
            }
        }
    }
}
