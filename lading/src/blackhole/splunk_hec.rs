//! The Splunk HEC protocol speaking blackhole.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use hyper::{
    body, header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::{error, info};

use crate::signals::Shutdown;

use super::General;

static ACK_ID: AtomicU64 = AtomicU64::new(0);

fn default_concurrent_requests_max() -> usize {
    100
}

#[derive(Debug)]
/// Errors produced by [`SplunkHec`].
pub enum Error {
    /// Wrapper for [`hyper::Error`].
    Hyper(hyper::Error),
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
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
    acks: HashMap<u64, bool>,
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
) -> Result<Response<Body>, hyper::Error> {
    metrics::counter!("requests_received", 1, &*labels);

    let (parts, body) = req.into_parts();
    let bytes = body::to_bytes(body).await?;

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), bytes) {
        Err(response) => Ok(response),
        Ok(body) => {
            metrics::counter!("bytes_received", body.len() as u64, &*labels);

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
                    })
                    .unwrap();
                    *okay.body_mut() = Body::from(body_bytes);
                }
                // Path for querying indexer acknowledgements
                (Method::POST, "/services/collector/ack") => {
                    match serde_json::from_slice::<HecAckRequest>(&body) {
                        Ok(ack_request) => {
                            let body_bytes =
                                serde_json::to_vec(&HecAckResponse::from(ack_request)).unwrap();
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
    shutdown: Shutdown,
    metric_labels: Vec<(String, String)>,
}

impl SplunkHec {
    /// Create a new [`SplunkHec`] server instance
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: Shutdown) -> Self {
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
    pub async fn run(mut self) -> Result<(), Error> {
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
        loop {
            tokio::select! {
                res = server => {
                    error!("server shutdown unexpectedly");
                    return res.map_err(Error::Hyper);
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}
