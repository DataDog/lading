use std::{num::NonZeroU32, sync::Arc};

use byte_unit::{Byte, ByteUnit};
use futures::{stream, StreamExt};
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
use lading_common::{
    block::{chunk_bytes, construct_block_cache, Block},
    payload,
};
use metrics::{counter, gauge};
use serde::Deserialize;

use crate::splunk_hec_gen::{
    acknowledgements::Channels, SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH, SPLUNK_HEC_CHANNEL_HEADER,
};

use super::{
    config::{AckConfig, Target},
    SPLUNK_HEC_JSON_PATH, SPLUNK_HEC_TEXT_PATH,
};

#[derive(Debug)]
pub enum Error {
    InvalidHECPath,
    AcksAlreadyEnabled,
}

/// The [`Worker`] defines a task that emits variant lines to a Splunk HEC server
/// controlling throughput.
pub struct Worker {
    uri: Uri,
    token: String,
    parallel_connections: u16,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    ack_config: Option<AckConfig>,
}

/// Derive the intended path from the format configuration
/// https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector#Event_data
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
        let uri = get_uri_by_format(&target.target_uri, &target.format);
        let block_chunks = chunk_bytes(
            &mut rng,
            target.maximum_prebuild_cache_size_bytes.get_bytes() as usize,
            &block_sizes,
        );
        let block_cache = construct_block_cache(
            &payload::SplunkHec::new(target.format),
            &block_chunks,
            &labels,
        );

        Ok(Self {
            parallel_connections: target.parallel_connections,
            uri,
            token: target.token,
            block_cache,
            rate_limiter,
            metric_labels: labels,
            ack_config: target.acknowledgements,
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
    pub async fn spin(self) -> Result<(), Error> {
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
        if let Some(ack_config) = self.ack_config {
            let ack_uri = Uri::builder()
                .authority(uri.authority().unwrap().to_string())
                .scheme("http")
                .path_and_query(SPLUNK_HEC_ACKNOWLEDGEMENTS_PATH)
                .build()
                .unwrap();
            channels.enable_acknowledgements(ack_uri, token.clone(), ack_config)?;
        }

        let channel_info = channels.get_channel_info();
        let mut channel_info = channel_info.iter().cycle();
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
                let uri = uri.clone();

                let total_bytes = blk.total_bytes;
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
                            if let Some(mut ack_id_tx) = ack_id_tx {
                                let body = String::from_utf8(
                                    hyper::body::to_bytes(response.into_body())
                                        .await
                                        .unwrap()
                                        .to_vec(),
                                )
                                .unwrap();
                                let hec_ack_response =
                                    serde_json::from_str::<HecAckResponse>(body.as_str()).unwrap();
                                let _ = ack_id_tx.try_send(hec_ack_response.ack_id);
                            }
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
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
struct HecAckResponse {
    text: String,
    code: u8,
    #[serde(rename = "ackId")]
    ack_id: u64,
}
