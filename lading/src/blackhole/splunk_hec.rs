//! The Splunk HEC protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `requests_received`: Total requests received
//!

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use hyper::{
    body::HttpBody,
    header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use metrics::counter;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::{error, info};

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
    req: Request<Body>,
    labels: Arc<Vec<(String, String)>>,
) -> Result<Response<Body>, Error> {
    counter!("requests_received", &*labels).increment(1);

    let (parts, body) = req.into_parts();
    let bytes = body.collect().await?.to_bytes();
    counter!("bytes_received", &*labels).increment(bytes.len() as u64);

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), bytes) {
        Err(response) => Ok(response),
        Ok(body) => {
            counter!("decoded_bytes_received", &*labels).increment(body.len() as u64);

            let mut okay = Response::default();
            *okay.status_mut() = StatusCode::OK;
            okay.headers_mut().insert(
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
                    let body_bytes = serde_json::to_vec(&HecResponse {
                        text: "Success",
                        code: 0,
                        ack_id,
                    })?;
                    *okay.body_mut() = Body::from(body_bytes);
                }
                // Path for querying indexer acknowledgements
                (Method::POST, "/services/collector/ack") => {
                    match serde_json::from_slice::<HecAckRequest>(&body) {
                        Ok(ack_request) => {
                            let body_bytes =
                                serde_json::to_vec(&HecAckResponse::from(ack_request))?;
                            *okay.body_mut() = Body::from(body_bytes);
                        }
                        Err(_) => {
                            *okay.status_mut() = StatusCode::BAD_REQUEST;
                        }
                    }
                }
                _ => {}
            }

            Ok(okay)
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
        let labels = Arc::new(self.metric_labels.clone());
        let service = make_service_fn(|_: &AddrStream| {
            let labels = labels.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req| {
                    let labels = Arc::clone(&labels);
                    srv(req, labels)
                }))
            }
        });
        let svc = ServiceBuilder::new()
            .load_shed()
            .concurrency_limit(self.concurrency_limit)
            .timeout(Duration::from_secs(1))
            .service(service);

        let addr = AddrIncoming::bind(&self.httpd_addr)
            .map(|mut addr| {
                addr.set_keepalive(Some(Duration::from_secs(60)));
                addr
            })
            .map_err(Error::Hyper)?;
        let server = Server::builder(addr).serve(svc);
        tokio::select! {
            res = server => {
                error!("server shutdown unexpectedly");
                res.map_err(Error::Hyper)
            }
            () = self.shutdown.recv() => {
                info!("shutdown signal received");
                Ok(())
            }
        }
    }
}
