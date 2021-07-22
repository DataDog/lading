use argh::FromArgs;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::service::{make_service_fn, service_fn};
use hyper::{body, header};
use hyper::{Body, Request, Response, Server, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::runtime::Builder;
use tower::ServiceBuilder;

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

async fn srv(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        _ => {
            metrics::counter!("requests_received", 1);

            let bytes = body::to_bytes(req).await?;
            metrics::counter!("bytes_received", bytes.len() as u64);

            let mut okay = Response::default();
            *okay.status_mut() = StatusCode::OK;
            okay.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/text"),
            );
            Ok(okay)
        }
    }
}

impl HttpServer {
    fn new(httpd_addr: SocketAddr, prometheus_addr: SocketAddr) -> Self {
        Self {
            httpd_addr,
            prometheus_addr,
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
