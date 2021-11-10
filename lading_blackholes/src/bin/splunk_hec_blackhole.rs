use std::{
    collections::HashMap,
    fs::read_to_string,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use argh::FromArgs;
use hyper::{
    body, header,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::{Deserialize, Serialize};
use tokio::runtime::Builder;
use tower::ServiceBuilder;

static ACK_ID: AtomicU64 = AtomicU64::new(0);

fn default_concurrent_requests_max() -> usize {
    100
}

fn default_config_path() -> String {
    "/etc/lading/splunk_hec_blackhole.toml".to_string()
}

#[derive(FromArgs)]
/// `splunk_hec_blackhole` options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option, default = "default_config_path()")]
    config_path: String,
}

#[derive(Debug, Deserialize)]
/// Main configuration struct for this program
struct Config {
    /// number of worker threads to use in this program
    pub worker_threads: u16,
    /// number of concurrent HTTP connections to allow
    #[serde(default = "default_concurrent_requests_max")]
    pub concurrent_requests_max: usize,
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
    /// address -- IP plus port -- for prometheus exporting to bind to
    pub prometheus_addr: SocketAddr,
}

fn get_config() -> Config {
    let opts = argh::from_env::<Opts>();
    let contents = read_to_string(&opts.config_path).unwrap();
    toml::from_str::<Config>(&contents).unwrap()
}

struct SplunkHecServer {
    prometheus_addr: SocketAddr,
    httpd_addr: SocketAddr,
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

#[allow(clippy::borrow_interior_mutable_const)]
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

impl SplunkHecServer {
    fn new(httpd_addr: SocketAddr, prometheus_addr: SocketAddr) -> Self {
        Self {
            prometheus_addr,
            httpd_addr,
        }
    }

    async fn run(self, concurrency_limit: usize) -> Result<(), hyper::Error> {
        let _: () = PrometheusBuilder::new()
            .listen_address(self.prometheus_addr)
            .install()
            .unwrap();

        let service =
            make_service_fn(|_: &AddrStream| async move { Ok::<_, hyper::Error>(service_fn(srv)) });
        let svc = ServiceBuilder::new()
            .load_shed()
            .concurrency_limit(concurrency_limit)
            .timeout(Duration::from_secs(1))
            .service(service);

        let addr = AddrIncoming::bind(&self.httpd_addr)
            .map(|mut addr| {
                addr.set_keepalive(Some(Duration::from_secs(60)));
                addr
            })
            .unwrap();
        let server = Server::builder(addr).serve(svc);
        server.await?;
        Ok(())
    }
}

fn main() {
    let config: Config = get_config();
    let splunk_server = SplunkHecServer::new(config.binding_addr, config.prometheus_addr);
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime
        .block_on(splunk_server.run(config.concurrent_requests_max))
        .unwrap();
}
