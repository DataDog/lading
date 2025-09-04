//! The Splunk HEC generator.
//!
//! ## Metrics
//!
//! `maximum_requests`: Total number of parallel connections to maintain
//! `requests_sent`: Total number of requests sent
//! `request_ok`: Successful requests
//! `request_failure`: Failed requests
//! `request_timeout`: Requests that timed out (these are not included in `request_failure`)
//! `bytes_written`: Total bytes written
//! `bytes_per_second`: Configured rate to send data
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!
//!

mod acknowledgements;

use std::{future::ready, num::NonZeroU32, sync::Arc, time::Duration};

use acknowledgements::Channels;
use http::{
    Method, Request, Uri,
    header::{AUTHORIZATION, CONTENT_LENGTH},
};
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use once_cell::sync::OnceCell;
use rand::{SeedableRng, prelude::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Semaphore, SemaphorePermit},
    time::timeout,
};
use tracing::{error, info};

use crate::generator::splunk_hec::acknowledgements::Channel;
use lading_payload::block;

use super::General;
use lading_throttle::{BytesThrottleConfig, ThrottleBuilder, ThrottleBuilderError};

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();
const SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH: &str = "/services/collector/ack";
const SPLUNK_HEC_JSON_PATH: &str = "/services/collector/event";
const SPLUNK_HEC_TEXT_PATH: &str = "/services/collector/raw";
const SPLUNK_HEC_CHANNEL_HEADER: &str = "x-splunk-request-channel";

/// Optional Splunk HEC indexer acknowledgements configuration
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AckSettings {
    /// The time in seconds between queries to /services/collector/ack
    pub ack_query_interval_seconds: u64,
    /// The time in seconds an ackId can remain pending before assuming data was
    /// dropped
    pub ack_timeout_seconds: u64,
}

/// Configuration for [`SplunkHec`]
#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// Format used when submitting event data to Splunk HEC
    pub format: lading_payload::splunk_hec::Encoding,
    /// Splunk HEC authentication token
    pub token: String,
    /// Splunk HEC indexer acknowledgements behavior options
    pub acknowledgements: Option<AckSettings>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    pub block_cache_method: block::CacheMethod,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: Option<byte_unit::Byte>,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<BytesThrottleConfig>,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`SplunkHec`].
pub enum Error {
    /// User supplied HEC path is invalid.
    #[error("User supplied HEC path is not valid")]
    InvalidHECPath,
    /// Interior acknowledgement error.
    #[error("Interior error: {0}")]
    Acknowledgements(acknowledgements::Error),
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Wrapper around [`hyper::http::Error`].
    #[error("HTTP error: {0}")]
    Http(#[from] hyper::http::Error),
    /// Empty URI authority
    #[error("URI authority is empty")]
    EmptyAuthorityURI,
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    /// Failed to convert, value is 0
    #[error("Value provided must not be zero")]
    Zero,
    /// Wrapper around [`acknowledgements::Error`]
    #[error(transparent)]
    Acknowledge(#[from] acknowledgements::Error),
    /// Wrapper around [`hyper::Error`].
    #[error("HTTP error: {0}")]
    Hyper(#[from] hyper::Error),
    /// Throttle builder error
    #[error("Throttle configuration error: {0}")]
    ThrottleBuilder(#[from] lading_throttle::ThrottleBuilderError),
}

/// Defines a task that emits variant lines to a Splunk HEC server controlling
/// throughput.
#[derive(Debug)]
pub struct SplunkHec {
    uri: Uri,
    token: String,
    parallel_connections: u16,
    throttle: Throttle,
    block_cache: Arc<block::Cache>,
    metric_labels: Vec<(String, String)>,
    channels: Channels,
    shutdown: lading_signal::Watcher,
}

/// Derive the intended path from the format configuration
// https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector#Event_data
fn get_uri_by_format(
    base_uri: &Uri,
    format: lading_payload::splunk_hec::Encoding,
) -> Result<Uri, Error> {
    let path = match format {
        lading_payload::splunk_hec::Encoding::Text => SPLUNK_HEC_TEXT_PATH,
        lading_payload::splunk_hec::Encoding::Json => SPLUNK_HEC_JSON_PATH,
    };

    let uri = Uri::builder()
        .authority(
            base_uri
                .authority()
                .ok_or(Error::EmptyAuthorityURI)?
                .to_string(),
        )
        .scheme("http")
        .path_and_query(path)
        .build()?;
    Ok(uri)
}

impl SplunkHec {
    /// Create a new [`SplunkHec`] instance
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
            ("component_name".to_string(), "splunk_hec".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let throttle = ThrottleBuilder::new()
            .bytes_per_second(config.bytes_per_second.as_ref())
            .throttle_config(config.throttle.as_ref())
            .build()?;

        let uri = get_uri_by_format(&config.target_uri, config.format)?;

        let payload_config = lading_payload::Config::SplunkHec {
            encoding: config.format,
        };
        let total_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;
        let block_cache = match config.block_cache_method {
            block::CacheMethod::Fixed => block::Cache::fixed_with_max_overhead(
                &mut rng,
                total_bytes,
                config.maximum_block_size.as_u128(),
                &payload_config,
                // NOTE we bound payload generation to have overhead only
                // equivalent to the prebuild cache size,
                // `total_bytes`. This means on systems with plentiful
                // memory we're under generating entropy, on systems with
                // minimal memory we're over-generating.
                //
                // `lading::get_available_memory` suggests we can learn to
                // divvy this up in the future.
                total_bytes.get() as usize,
            )?,
        };

        let mut channels = Channels::new(config.parallel_connections);
        if let Some(ack_settings) = config.acknowledgements {
            let ack_uri = Uri::builder()
                .authority(uri.authority().ok_or(Error::EmptyAuthorityURI)?.to_string())
                .scheme("http")
                .path_and_query(SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH)
                .build()?;
            channels.enable_acknowledgements(ack_uri, config.token.clone(), ack_settings);
        }

        CONNECTION_SEMAPHORE
            .set(Semaphore::new(config.parallel_connections as usize))
            .expect("semaphore already set");

        Ok(Self {
            channels,
            parallel_connections: config.parallel_connections,
            uri,
            token: config.token,
            block_cache: Arc::new(block_cache),
            throttle,
            metric_labels: labels,
            shutdown,
        })
    }

    /// Run [`SplunkHec`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will error if unable to enable acknowledgements when configured
    /// to do so.
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

        let uri = self.uri;
        let labels = self.metric_labels;

        gauge!("maximum_requests", &labels).set(f64::from(self.parallel_connections));
        let mut handle = self.block_cache.handle();
        let mut channels = self.channels.iter().cycle();

        let request_shutdown = self.shutdown.clone();
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            let channel: Channel = channels
                .next()
                .expect("channel should never be empty")
                .clone();
            let total_bytes = self.block_cache.peek_next_size(&handle);

            tokio::select! {
                result = self.throttle.wait_for(total_bytes) => {
                    match result {
                        Ok(()) => {
                            let client = client.clone();
                            let labels = labels.clone();
                            let uri = uri.clone();

                            let block = self.block_cache.advance(&mut handle);
                            let body = crate::full(block.bytes.clone());
                            let block_length = block.bytes.len();

                            let request = Request::builder()
                                .method(Method::POST)
                                .uri(uri)
                                .header(AUTHORIZATION, format!("Splunk {}", self.token))
                                .header(CONTENT_LENGTH, block_length)
                                .header(SPLUNK_HEC_CHANNEL_HEADER, channel.id())
                                .body(body)?;

                            // NOTE once JoinSet is in tokio stable we can make this
                            // much, much tidier by spawning requests in the JoinSet. I
                            // think we could also possibly have the send request return
                            // the AckID, meaning we could just keep the channel logic
                            // in this main loop here and avoid the AckService entirely.
                            let permit = CONNECTION_SEMAPHORE.get().expect("Connecton Semaphore is empty or being initialized").acquire().await.expect("Semaphore has already been closed");
                            tokio::spawn(send_hec_request(permit, block_length, labels, channel, client, request, request_shutdown.clone()));
                        }
                        Err(err) => {
                            error!("Throttle request of {total_bytes} is larger than throttle capacity. Block will be discarded. Error: {err}");
                        }
                    }
                }
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // When we shut down we may leave dangling, active
                    // requests. This is acceptable. As we do not today
                    // coordinate with the target it's possible that the target
                    // will have been shut down and an in-flight request is
                    // waiting for an ack from it, causing this to jam forever
                    // if we attempt to acquire all semaphore permits.
                    return Ok(());
                },
            }
        }
    }
}

async fn send_hec_request<B>(
    permit: SemaphorePermit<'_>,
    block_length: usize,
    labels: Vec<(String, String)>,
    channel: Channel,
    client: Client<HttpConnector, B>,
    request: Request<B>,
    shutdown: lading_signal::Watcher,
) -> Result<(), Error>
where
    B: Body + Send + 'static + Unpin,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    counter!("requests_sent", &labels).increment(1);
    let work = client.request(request);

    tokio::select! {
        tm = timeout(Duration::from_secs(1), work) => {
            match tm {
                Ok(tm) => match tm {
                    Ok(response) => {
                        counter!("bytes_written", &labels).increment(block_length as u64);
                        let (parts, body) = response.into_parts();
                        let status = parts.status;
                        let mut status_labels = labels.clone();
                        status_labels.push(("status_code".to_string(), status.as_u16().to_string()));
                        counter!("request_ok", &status_labels).increment(1);
                        let body_bytes = body.boxed().collect().await?.to_bytes();
                        let hec_ack_response =
                            serde_json::from_slice::<HecResponse>(&body_bytes).expect("unable to parse response body");
                        channel.send(ready(hec_ack_response.ack_id)).await?;
                    }
                    Err(err) => {
                        let mut error_labels = labels.clone();
                        error_labels.push(("error".to_string(), err.to_string()));
                        counter!("request_failure", &error_labels).increment(1);
                    }
                }
                Err(err) => {
                    let mut error_labels = labels.clone();
                    error_labels.push(("error".to_string(), err.to_string()));
                    counter!("request_timeout", &error_labels).increment(1);
                }
            }
        }
        () = shutdown.recv() => {},
    }
    drop(permit);
    Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
struct HecResponse {
    #[allow(dead_code)]
    text: String,
    #[allow(dead_code)]
    code: u8,
    #[serde(rename = "ackId")]
    ack_id: Option<u64>,
}
