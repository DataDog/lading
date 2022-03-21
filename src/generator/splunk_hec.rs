mod acknowledgements;

use crate::{
    block::{chunk_bytes, construct_block_cache, Block},
    payload,
    payload::SplunkHecEncoding,
};
use acknowledgements::Channels;
use byte_unit::{Byte, ByteUnit};
use governor::{
    clock,
    state::{self, direct},
    Quota, RateLimiter,
};
use http::{
    header::{AUTHORIZATION, CONTENT_LENGTH},
    Method, Request, Uri,
};
use hyper::{client::HttpConnector, Body, Client};
use metrics::{counter, gauge};
use once_cell::sync::OnceCell;
use rand::{prelude::StdRng, SeedableRng};
use serde::Deserialize;
use std::{num::NonZeroU32, sync::Arc};
use tokio::sync::Semaphore;
use tracing::info;

use crate::signals::Shutdown;

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();
const SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH: &str = "/services/collector/ack";
const SPLUNK_HEC_JSON_PATH: &str = "/services/collector/event";
const SPLUNK_HEC_TEXT_PATH: &str = "/services/collector/raw";
const SPLUNK_HEC_CHANNEL_HEADER: &str = "x-splunk-request-channel";

/// Optional Splunk HEC indexer acknowledgements configuration
#[derive(Deserialize, Debug)]
pub struct AckSettings {
    /// The time in seconds between queries to /services/collector/ack
    pub ack_query_interval_seconds: u64,
    /// The time in seconds an ackId can remain pending before assuming data was
    /// dropped
    pub ack_timeout_seconds: u64,
}

/// The [`Target`] instance from which to derive workers
#[derive(Deserialize, Debug)]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// Format used when submitting event data to Splunk HEC
    pub format: SplunkHecEncoding,
    /// Splunk HEC authentication token
    pub token: String,
    /// Splunk HEC indexer acknowledgements behavior options
    pub acknowledgements: Option<AckSettings>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
}

#[derive(Debug)]
pub enum Error {
    InvalidHECPath,
    Acknowledgements(acknowledgements::Error),
}

/// The [`Worker`] defines a task that emits variant lines to a Splunk HEC server
/// controlling throughput.
pub struct SplunkHec {
    uri: Uri,
    token: String,
    parallel_connections: u16,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    ack_settings: Option<AckSettings>,
    shutdown: Shutdown,
}

/// Derive the intended path from the format configuration
// https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector#Event_data
fn get_uri_by_format(base_uri: &Uri, format: &payload::SplunkHecEncoding) -> Uri {
    let path = match format {
        payload::SplunkHecEncoding::Text => SPLUNK_HEC_TEXT_PATH,
        payload::SplunkHecEncoding::Json => SPLUNK_HEC_JSON_PATH,
    };

    Uri::builder()
        .authority(base_uri.authority().unwrap().to_string())
        .scheme("http")
        .path_and_query(path)
        .build()
        .unwrap()
}

impl SplunkHec {
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
        let block_sizes: Vec<usize> = config
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
        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        let rate_limiter = RateLimiter::direct(Quota::per_second(bytes_per_second));
        let labels = vec![];
        let uri = get_uri_by_format(&config.target_uri, &config.format);

        let block_chunks = chunk_bytes(
            &mut rng,
            config.maximum_prebuild_cache_size_bytes.get_bytes() as usize,
            &block_sizes,
        );
        let block_cache = construct_block_cache(
            &mut rng,
            &payload::SplunkHec::new(config.format),
            &block_chunks,
            &labels,
        );

        CONNECTION_SEMAPHORE
            .set(Semaphore::new(config.parallel_connections as usize))
            .unwrap();

        Ok(Self {
            parallel_connections: config.parallel_connections,
            uri,
            token: config.token,
            block_cache,
            rate_limiter,
            metric_labels: labels,
            ack_settings: config.acknowledgements,
            shutdown,
        })
    }

    /// Enter the main loop of this [`Worker`]
    ///
    /// # Errors
    ///
    /// Function will error if unable to enable acknowledgements when configured to do so.
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
        let uri = self.uri;
        let token = self.token;
        let labels = self.metric_labels;

        let mut channels = Channels::new(self.parallel_connections);
        if let Some(ack_settings) = self.ack_settings {
            let ack_uri = Uri::builder()
                .authority(uri.authority().unwrap().to_string())
                .scheme("http")
                .path_and_query(SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH)
                .build()
                .unwrap();
            channels
                .enable_acknowledgements(ack_uri, token.clone(), ack_settings)
                .map_err(Error::Acknowledgements)?;
        }

        let channel_info = channels.get_channel_info();
        let mut channel_info = channel_info.iter().cycle();
        gauge!(
            "maximum_requests",
            f64::from(self.parallel_connections),
            &labels
        );
        let mut blocks = self.block_cache.iter().cycle();
        loop {
            let blk = blocks.next().unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = rate_limiter.until_n_ready(total_bytes) => {
                    let client = client.clone();
                    let labels = labels.clone();
                    let uri = uri.clone();

                    let body = Body::from(blk.bytes.clone());
                    let block_length = blk.bytes.len();
                    let (channel_id, ack_id_tx) = channel_info.next().unwrap().to_owned();

                    let request: Request<Body> = Request::builder()
                        .method(Method::POST)
                        .uri(uri)
                        .header(AUTHORIZATION, format!("Splunk {}", token))
                        .header(CONTENT_LENGTH, block_length)
                        .header(SPLUNK_HEC_CHANNEL_HEADER, channel_id)
                        .body(body)
                        .unwrap();

                    let permit = CONNECTION_SEMAPHORE.get().unwrap().acquire().await.unwrap();
                    tokio::spawn(async move {
                        counter!("requests_sent", 1, &labels);
                        match client.request(request).await {
                            Ok(response) => {
                                counter!("bytes_written", block_length as u64, &labels);
                                let (parts, body) = response.into_parts();
                                let status = parts.status;
                                let mut status_labels = labels.clone();
                                status_labels
                                    .push(("status_code".to_string(), status.as_u16().to_string()));
                                counter!("request_ok", 1, &status_labels);
                                if let Some(mut ack_id_tx) = ack_id_tx {
                                    let body_bytes = hyper::body::to_bytes(body).await.unwrap();
                                    let hec_ack_response =
                                        serde_json::from_slice::<HecAckResponse>(&body_bytes).unwrap();
                                    let _ = ack_id_tx.try_send(hec_ack_response.ack_id);
                                }
                            }
                            Err(err) => {
                                let mut error_labels = labels.clone();
                                error_labels.push(("error".to_string(), err.to_string()));
                                counter!("request_failure", 1, &error_labels);
                            }
                        }
                        drop(permit);
                    });
                }
                _ = self.shutdown.recv() => {
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

#[derive(Deserialize, Debug)]
struct HecAckResponse {
    #[allow(dead_code)]
    text: String,
    #[allow(dead_code)]
    code: u8,
    #[serde(rename = "ackId")]
    ack_id: u64,
}
