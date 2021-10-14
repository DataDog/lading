use argh::FromArgs;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::service::{make_service_fn, service_fn};
use hyper::{body, header};
use hyper::{Body, Request, Response, Server, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use once_cell::unsync::OnceCell;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::runtime::Builder;
use tower::ServiceBuilder;

#[allow(clippy::declare_interior_mutable_const)]

const RESPONSE: OnceCell<Vec<u8>> = OnceCell::new();

fn default_concurrent_requests_max() -> usize {
    100
}

fn default_config_path() -> String {
    "/etc/lading/http_blackhole.toml".to_string()
}

#[derive(Debug, Copy, Clone, Deserialize)]
enum BodyVariant {
    Nothing,
    AwsKinesis,
}

impl FromStr for BodyVariant {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "nothing" => Ok(BodyVariant::Nothing),
            "aws_kinesis" | "kinesis" => Ok(BodyVariant::AwsKinesis),
            _ => Err("unknown variant"),
        }
    }
}

fn default_body_variant() -> BodyVariant {
    BodyVariant::AwsKinesis
}

#[derive(FromArgs)]
/// `http_blackhole` options
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
    /// the body variant to respond with, default nothing
    #[serde(default = "default_body_variant")]
    pub body_variant: BodyVariant,
}

struct HttpServer {
    prometheus_addr: SocketAddr,
    httpd_addr: SocketAddr,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct KinesisPutRecordBatchResponseEntry {
    error_code: Option<String>,
    error_message: Option<String>,
    record_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct KinesisPutRecordBatchResponse {
    encrypted: Option<bool>,
    failed_put_count: u32,
    request_responses: Vec<KinesisPutRecordBatchResponseEntry>,
}

#[allow(clippy::borrow_interior_mutable_const)]
async fn srv(
    body_variant: BodyVariant,
    req: Request<Body>,
) -> Result<Response<Body>, hyper::Error> {
    metrics::counter!("requests_received", 1);

    let bytes = body::to_bytes(req).await?;
    metrics::counter!("bytes_received", bytes.len() as u64);

    let mut okay = Response::default();
    *okay.status_mut() = StatusCode::OK;
    okay.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );

    let body_bytes = RESPONSE
        .get_or_init(|| match body_variant {
            BodyVariant::AwsKinesis => {
                let response = KinesisPutRecordBatchResponse {
                    encrypted: None,
                    failed_put_count: 0,
                    request_responses: vec![KinesisPutRecordBatchResponseEntry {
                        error_code: None,
                        error_message: None,
                        record_id: "foobar".to_string(),
                    }],
                };
                serde_json::to_vec(&response).unwrap()
            }
            BodyVariant::Nothing => vec![],
        })
        .clone();
    *okay.body_mut() = Body::from(body_bytes);
    Ok(okay)
}

impl HttpServer {
    fn new(httpd_addr: SocketAddr, prometheus_addr: SocketAddr) -> Self {
        Self {
            prometheus_addr,
            httpd_addr,
        }
    }

    async fn run(
        self,
        body_variant: BodyVariant,
        concurrency_limit: usize,
    ) -> Result<(), hyper::Error> {
        let _: () = PrometheusBuilder::new()
            .listen_address(self.prometheus_addr)
            .install()
            .unwrap();

        let service = make_service_fn(|_: &AddrStream| async move {
            Ok::<_, hyper::Error>(service_fn(move |request| srv(body_variant, request)))
        });
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

fn get_config() -> Config {
    let ops: Opts = argh::from_env();
    let mut file: std::fs::File = std::fs::OpenOptions::new()
        .read(true)
        .open(ops.config_path)
        .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    toml::from_str(&contents).unwrap()
}

fn main() {
    let config: Config = get_config();
    let httpd = HttpServer::new(config.binding_addr, config.prometheus_addr);
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime
        .block_on(httpd.run(config.body_variant, config.concurrent_requests_max))
        .unwrap();
}
