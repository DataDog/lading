//! The Splunk HEC protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `requests_received`: Total requests received
//!

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::{header, Method, Request, Response, StatusCode};
use metrics::counter;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::General;

static ACK_ID: AtomicU64 = AtomicU64::new(0);

fn default_concurrent_requests_max() -> usize {
    100
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`SplunkHec`].
pub enum Error {
    /// Wrapper for [`hyper::Error`].
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    /// Deserialization Error
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    /// Wrapper for [`std::io::Error`].
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Wrapper for [`crate::blackhole::common::Error`].
    #[error(transparent)]
    Common(#[from] crate::blackhole::common::Error),
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`SplunkHec`].
pub struct Config {
    /// number of concurrent HTTP connections to allow
    #[serde(default = "default_concurrent_requests_max")]
    pub concurrent_requests_max: usize,
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}

#[derive(Deserialize)]
struct HecAckRequest {
    acks: Vec<u64>,
}

#[derive(Serialize)]
struct HecAckResponse {
    acks: FxHashMap<u64, bool>,
}

impl From<HecAckRequest> for HecAckResponse {
    fn from(ack_request: HecAckRequest) -> Self {
        let acks = ack_request
            .acks
            .into_iter()
            .map(|ack_id| (ack_id, true))
            .collect();
        HecAckResponse { acks }
    }
}

#[derive(Serialize)]
struct HecResponse {
    text: &'static str,
    // https://docs.splunk.com/Documentation/Splunk/8.2.3/Data/TroubleshootHTTPEventCollector#Possible_error_codes
    code: u8,
    #[serde(rename = "ackId")]
    ack_id: u64,
}

async fn srv(
    req: Request<hyper::body::Incoming>,
    labels: Vec<(String, String)>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    counter!("requests_received", &*labels).increment(1);

    let (parts, body) = req.into_parts();
    let bytes = body.boxed().collect().await?.to_bytes();
    counter!("bytes_received", &*labels).increment(bytes.len() as u64);

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), bytes) {
        Err(response) => Ok(response),
        Ok(body) => {
            counter!("decoded_bytes_received", &*labels).increment(body.len() as u64);

            let mut resp = Response::default();
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json"),
            );

            match (parts.method, parts.uri.path()) {
                // Path for submitting event data
                (
                    Method::POST,
                    "/services/collector/event"
                    | "/services/collector/event/1.0"
                    | "/services/collector/raw"
                    | "/services/collector/raw/1.0",
                ) => {
                    let ack_id = ACK_ID.fetch_add(1, Ordering::Relaxed);
                    match serde_json::to_vec(&HecResponse {
                        text: "Success",
                        code: 0,
                        ack_id,
                    }) {
                        Ok(b) => {
                            *resp.body_mut() = crate::full(b);
                        }
                        Err(_e) => {
                            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *resp.body_mut() = crate::full(vec![]);
                        }
                    }
                }
                // Path for querying indexer acknowledgements
                (Method::POST, "/services/collector/ack") => {
                    match serde_json::from_slice::<HecAckRequest>(&body) {
                        Ok(ack_request) => {
                            match serde_json::to_vec(&HecAckResponse::from(ack_request)) {
                                Ok(b) => {
                                    *resp.body_mut() = crate::full(b);
                                }
                                Err(_e) => {
                                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                    *resp.body_mut() = crate::full(vec![]);
                                }
                            }
                        }
                        Err(_) => {
                            *resp.status_mut() = StatusCode::BAD_REQUEST;
                        }
                    }
                }
                _ => {}
            }

            Ok(resp)
        }
    }
}

#[derive(Debug)]
/// The Splunk HEC blackhole.
pub struct SplunkHec {
    concurrency_limit: usize,
    httpd_addr: SocketAddr,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl SplunkHec {
    /// Create a new [`SplunkHec`] server instance
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "splunk_hec".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            httpd_addr: config.binding_addr,
            concurrency_limit: config.concurrent_requests_max,
            shutdown,
            metric_labels,
        }
    }

    /// Run [`SplunkHec`] to completion
    ///
    /// This function runs the `SplunkHec` server forever, unless a shutdown
    /// signal is received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if receiving a packet fails.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn run(self) -> Result<(), Error> {
        crate::blackhole::common::run_httpd(
            self.httpd_addr,
            self.concurrency_limit,
            self.shutdown,
            self.metric_labels.clone(),
            move || {
                let metric_labels = self.metric_labels.clone();
                hyper::service::service_fn(move |req| srv(req, metric_labels.clone()))
            },
        )
        .await?;

        Ok(())
    }
}
