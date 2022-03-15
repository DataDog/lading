use hyper::{
    body, header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tower::ServiceBuilder;
use tracing::{error, info};

use crate::signals::Shutdown;

static ACK_ID: AtomicU64 = AtomicU64::new(0);

fn default_concurrent_requests_max() -> usize {
    100
}

pub enum Error {
    Hyper(hyper::Error),
}

#[derive(Debug, Deserialize)]
/// Main configuration struct for this program
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

async fn srv(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    metrics::counter!("requests_received", 1);

    let (parts, body) = req.into_parts();
    let bytes = body::to_bytes(body).await?;
    metrics::counter!("bytes_received", bytes.len() as u64);

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
            match serde_json::from_slice::<HecAckRequest>(&bytes) {
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

pub struct SplunkHec {
    concurrency_limit: usize,
    httpd_addr: SocketAddr,
    shutdown: Shutdown,
}

impl SplunkHec {
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self {
            httpd_addr: config.binding_addr,
            concurrency_limit: config.concurrent_requests_max,
            shutdown,
        }
    }

    pub async fn run(mut self) -> Result<(), Error> {
        let service =
            make_service_fn(|_: &AddrStream| async move { Ok::<_, hyper::Error>(service_fn(srv)) });
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
            .unwrap();
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
