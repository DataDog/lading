use argh::FromArgs;
use hyper::header;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tokio::runtime::Builder;

#[derive(FromArgs)]
/// `http_blackhole` options
struct Opts {
    /// number of worker threads to use in this program
    #[argh(option)]
    pub worker_threads: u16,
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
            if let Some(content_length) = req.headers().get(header::CONTENT_LENGTH) {
                let cl = content_length.to_str().unwrap();
                let content_length = cl.parse::<u64>().unwrap();

                metrics::counter!("bytes_received", content_length);
            }
            let mut okay = Response::default();
            *okay.status_mut() = StatusCode::OK;
            okay.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/text"),
            );
            //            *okay.body_mut() = req.into_body();
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

    async fn run(self) -> Result<(), hyper::Error> {
        let _: () = PrometheusBuilder::new()
            .listen_address(self.prometheus_addr)
            .install()
            .unwrap();

        let service = make_service_fn(|_| async { Ok::<_, hyper::Error>(service_fn(srv)) });
        let server = Server::bind(&self.httpd_addr).serve(service);
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
        .build()
        .unwrap();
    runtime.block_on(httpd.run()).unwrap();
}
