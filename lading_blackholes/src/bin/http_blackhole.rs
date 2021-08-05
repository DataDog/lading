use argh::FromArgs;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::service::{make_service_fn, service_fn};
use hyper::{body, header};
use hyper::{Body, Request, Response, Server, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use once_cell::unsync::OnceCell;
use serde::Serialize;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::runtime::Builder;
use tower::ServiceBuilder;

const RESPONSE: OnceCell<Vec<u8>> = OnceCell::new();

fn default_concurrent_requests_max() -> usize {
    100
}

#[derive(FromArgs)]
/// `http_blackhole` options
struct Opts {
    /// number of worker threads to use in this program
    #[argh(option)]
    pub worker_threads: u16,
    /// number of concurrent HTTP connections to allow
    #[argh(option, default = "default_concurrent_requests_max()")]
    pub concurrent_requests_max: usize,
    /// address -- IP plus port -- to bind to
    #[argh(option)]
    pub binding_addr: SocketAddr,
    /// address -- IP plus port -- for prometheus exporting to bind to
    #[argh(option)]
    pub prometheus_addr: SocketAddr,
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

async fn srv(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    metrics::counter!("requests_received", 1);

    let bytes = body::to_bytes(req).await?;
    metrics::counter!("bytes_received", bytes.len() as u64);

    let mut okay = Response::default();
    *okay.status_mut() = StatusCode::OK;
    okay.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    *okay.body_mut() = Body::from(
        RESPONSE
            .get_or_init(|| {
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
            })
            .clone(),
    );
    Ok(okay)
}

impl HttpServer {
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
            make_service_fn(|_: &AddrStream| async { Ok::<_, hyper::Error>(service_fn(srv)) });
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
    let ops: Opts = argh::from_env();
    let httpd = HttpServer::new(ops.binding_addr, ops.prometheus_addr);
    let runtime = Builder::new_multi_thread()
        .worker_threads(ops.worker_threads as usize)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime
        .block_on(httpd.run(ops.concurrent_requests_max))
        .unwrap();
}
