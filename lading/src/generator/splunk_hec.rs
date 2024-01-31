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

use std::{num::NonZeroU32, thread, time::Duration};

use acknowledgements::Channels;
use byte_unit::{Byte, ByteUnit};
use http::{
    header::{AUTHORIZATION, CONTENT_LENGTH},
    Method, Request, Uri,
};
use hyper::{client::HttpConnector, Body, Client};
use lading_throttle::Throttle;
use metrics::{counter, gauge};
use once_cell::sync::OnceCell;
use rand::{prelude::StdRng, SeedableRng};
use serde::Deserialize;
use tokio::{
    sync::{mpsc, Semaphore, SemaphorePermit},
    time::timeout,
};
use tracing::info;

use crate::{
    common::PeekableReceiver, generator::splunk_hec::acknowledgements::Channel, signals::Phase,
};
use lading_payload::block::{self, Block};

use super::General;

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();
const SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH: &str = "/services/collector/ack";
const SPLUNK_HEC_JSON_PATH: &str = "/services/collector/event";
const SPLUNK_HEC_TEXT_PATH: &str = "/services/collector/raw";
const SPLUNK_HEC_CHANNEL_HEADER: &str = "x-splunk-request-channel";

/// Optional Splunk HEC indexer acknowledgements configuration
#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AckSettings {
    /// The time in seconds between queries to /services/collector/ack
    pub ack_query_interval_seconds: u64,
    /// The time in seconds an ackId can remain pending before assuming data was
    /// dropped
    pub ack_timeout_seconds: u64,
}

/// Configuration for [`SplunkHec`]
#[derive(Deserialize, Debug, PartialEq)]
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
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    pub block_cache_method: block::CacheMethod,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
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
}

/// Defines a task that emits variant lines to a Splunk HEC server controlling
/// throughput.
#[derive(Debug)]
pub struct SplunkHec {
    uri: Uri,
    token: String,
    parallel_connections: u16,
    throttle: Throttle,
    block_cache: block::Cache,
    metric_labels: Vec<(String, String)>,
    channels: Channels,
    shutdown: Phase,
}

/// Derive the intended path from the format configuration
// https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector#Event_data
fn get_uri_by_format(base_uri: &Uri, format: lading_payload::splunk_hec::Encoding) -> Uri {
    let path = match format {
        lading_payload::splunk_hec::Encoding::Text => SPLUNK_HEC_TEXT_PATH,
        lading_payload::splunk_hec::Encoding::Json => SPLUNK_HEC_JSON_PATH,
    };

    Uri::builder()
        .authority(
            base_uri
                .authority()
                .expect("base uri authority is empty")
                .to_string(),
        )
        .scheme("http")
        .path_and_query(path)
        .build()
        .expect("unable to build URI")
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
    pub fn new(general: General, config: Config, shutdown: Phase) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroU32> = config
            .block_sizes
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB).expect("Bytes must not be negative"),
                    Byte::from_unit(1.0 / 4.0, ByteUnit::MB).expect("Bytes must not be negative"),
                    Byte::from_unit(1.0 / 2.0, ByteUnit::MB).expect("Bytes must not be negative"),
                    Byte::from_unit(1_f64, ByteUnit::MB).expect("Bytes must not be negative"),
                    Byte::from_unit(2_f64, ByteUnit::MB).expect("Bytes must not be negative"),
                    Byte::from_unit(4_f64, ByteUnit::MB).expect("Bytes must not be negative"),
                ]
            })
            .iter()
            .map(|sz| NonZeroU32::new(sz.get_bytes() as u32).expect("bytes must be non-zero"))
            .collect();
        let mut labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "splunk_hec".to_string()),
        ];
        if let Some(id) = general.id {
            labels.push(("id".to_string(), id));
        }

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32)
            .expect("config bytes per second should not be zero");
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

        let uri = get_uri_by_format(&config.target_uri, config.format);

        let payload_config = lading_payload::Config::SplunkHec {
            encoding: config.format,
        };
        let total_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as u32)
                .expect("bytes must be non-zero");
        let block_cache = match config.block_cache_method {
            block::CacheMethod::Streaming => {
                block::Cache::stream(config.seed, total_bytes, &block_sizes, payload_config)?
            }
            block::CacheMethod::Fixed => {
                block::Cache::fixed(&mut rng, total_bytes, &block_sizes, &payload_config)?
            }
        };

        let mut channels = Channels::new(config.parallel_connections);
        if let Some(ack_settings) = config.acknowledgements {
            let ack_uri = Uri::builder()
                .authority(uri.authority().expect("Uri authority is empty").to_string())
                .scheme("http")
                .path_and_query(SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH)
                .build()
                .expect("unable to build ack URI");
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
            block_cache,
            throttle: Throttle::new_with_config(config.throttle, bytes_per_second),
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
        let client: Client<HttpConnector, Body> = Client::builder()
            .pool_max_idle_per_host(self.parallel_connections as usize)
            .retry_canceled_requests(false)
            .set_host(false)
            .build_http();

        let uri = self.uri;
        let labels = self.metric_labels;

        gauge!(
            "maximum_requests",
            f64::from(self.parallel_connections),
            &labels
        );
        // Move the block_cache into an OS thread, exposing a channel between it
        // and this async context.
        let block_cache = self.block_cache;
        let (snd, rcv) = mpsc::channel(1024);
        let mut rcv: PeekableReceiver<Block> = PeekableReceiver::new(rcv);
        thread::Builder::new().spawn(|| block_cache.spin(snd))?;
        let mut channels = self.channels.iter().cycle();

        loop {
            let channel: Channel = channels
                .next()
                .expect("channel iteration already finished")
                .clone();
            let blk = rcv
                .peek()
                .await
                .expect("block cache does not have any blocks");
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let client = client.clone();
                    let labels = labels.clone();
                    let uri = uri.clone();

                    let blk = rcv.next().await.expect("block cache failed to find next value"); // actually advance through the blocks
                    let body = Body::from(blk.bytes.clone());
                    let block_length = blk.bytes.len();

                    let request: Request<Body> = Request::builder()
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
                    tokio::spawn(send_hec_request(permit, block_length, labels, channel, client, request, self.shutdown.clone()));
                }
                () = self.shutdown.recv() => {
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

async fn send_hec_request(
    permit: SemaphorePermit<'_>,
    block_length: usize,
    labels: Vec<(String, String)>,
    channel: Channel,
    client: Client<HttpConnector>,
    request: Request<Body>,
    mut shutdown: Phase,
) {
    counter!("requests_sent", 1, &labels);
    let work = client.request(request);

    tokio::select! {
        tm = timeout(Duration::from_secs(1), work) => {
            match tm {
                Ok(tm) => match tm {
                    Ok(response) => {
                        counter!("bytes_written", block_length as u64, &labels);
                        let (parts, body) = response.into_parts();
                        let status = parts.status;
                        let mut status_labels = labels.clone();
                        status_labels.push(("status_code".to_string(), status.as_u16().to_string()));
                        counter!("request_ok", 1, &status_labels);
                        channel
                            .send(async {
                                let body_bytes = hyper::body::to_bytes(body).await.expect("unable to convert response body to bytes");
                                let hec_ack_response =
                                    serde_json::from_slice::<HecAckResponse>(&body_bytes).expect("unable to parse response body");
                                hec_ack_response.ack_id
                            })
                            .await;
                    }
                    Err(err) => {
                        let mut error_labels = labels.clone();
                        error_labels.push(("error".to_string(), err.to_string()));
                        counter!("request_failure", 1, &error_labels);
                    }
                }
                Err(err) => {
                    let mut error_labels = labels.clone();
                    error_labels.push(("error".to_string(), err.to_string()));
                    counter!("request_timeout", 1, &error_labels);
                }
            }
        }
        () = shutdown.recv() => {},
    }
    drop(permit);
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct HecAckResponse {
    #[allow(dead_code)]
    text: String,
    #[allow(dead_code)]
    code: u8,
    #[serde(rename = "ackId")]
    ack_id: u64,
}
