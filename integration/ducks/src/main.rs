//! Ducks is an integration testing target for lading.
//!
//! Ducks exists to enable correctness testing on lading. Any high-level
//! validation tasks are in-scope, but preference should be given to
//! unit-testing within lading over testing with ducks where possible.
//!
//! Currently, ducks can perform these tasks:
//! - Receive data
//! - Emit metrics with relevant counters
//!
//! Upcoming goals:
//! - Validate some forms of received data
//! - Send data

use hyper::{
    body,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use once_cell::sync::OnceCell;
use shared::{
    integration_api::{integration_client::IntegrationClient, ListenInfo, MetricsReport},
    DucksConfig,
};
use sketches_ddsketch::DDSketch;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::UnixStream, sync::Mutex};
use tonic::{transport::Endpoint, IntoRequest};
use tower::ServiceBuilder;
use tracing::{debug, info, trace};

#[derive(Debug)]
enum Error {
    Hyper(hyper::Error),
}

static GENERIC_HTTP_COUNTERS: OnceCell<Arc<Mutex<GenericHttpCounters>>> = OnceCell::new();

/// Corresponds to MeasurementConfig::GenericHttp
struct GenericHttpCounters {
    body_size: DDSketch,
    entropy: DDSketch,
    request_count: u64,
    total_bytes: u64,
    // record http methods received
}

impl Default for GenericHttpCounters {
    fn default() -> Self {
        let config = sketches_ddsketch::Config::defaults();
        let message_len = DDSketch::new(config);

        let config = sketches_ddsketch::Config::defaults();
        let entropy = DDSketch::new(config);

        Self {
            body_size: message_len,
            entropy,
            request_count: Default::default(),
            total_bytes: Default::default(),
        }
    }
}
impl IntoRequest<MetricsReport> for &GenericHttpCounters {
    fn into_request(self) -> tonic::Request<MetricsReport> {
        tonic::Request::new(MetricsReport {
            request_count: self.request_count,
            total_bytes: self.total_bytes,
            median_entropy: self.entropy.quantile(0.5).unwrap().unwrap_or_default(),
            median_size: self.body_size.quantile(0.5).unwrap().unwrap_or_default(),
        })
    }
}

#[tracing::instrument(level = "trace")]
async fn http_req_handler(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let (_parts, body) = req.into_parts();
    let body = body::to_bytes(body).await?;

    {
        let metric = GENERIC_HTTP_COUNTERS.get().unwrap();
        let mut m = metric.lock().await;
        m.request_count += 1;

        m.total_bytes = body.len() as u64;
        m.entropy.add(entropy::metric_entropy(&body) as f64);

        m.body_size.add(body.len() as f64);
    }

    let mut okay = Response::default();
    *okay.status_mut() = StatusCode::OK;

    let body_bytes = vec![];
    *okay.body_mut() = Body::from(body_bytes);
    Ok(okay)
}

#[tracing::instrument]
async fn http_server(addr: AddrIncoming) -> Result<(), Error> {
    GENERIC_HTTP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(GenericHttpCounters::default())));

    let service = make_service_fn(|_: &AddrStream| async move {
        Ok::<_, hyper::Error>(service_fn(move |request: Request<Body>| {
            trace!("REQUEST: {:?}", request);

            http_req_handler(request)
        }))
    });
    let svc = ServiceBuilder::new()
        .load_shed()
        .concurrency_limit(1_000)
        .timeout(Duration::from_secs(1))
        .service(service);

    let server = Server::builder(addr).serve(svc);
    server.await.map_err(Error::Hyper)
}

async fn http_listen(
    mut client: IntegrationClient<tonic::transport::Channel>,
    _config: DucksConfig,
) -> Result<(), anyhow::Error> {
    // bind to a random open TCP port
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0));
    let addr = AddrIncoming::bind(&bind_addr)?;
    let port = addr.local_addr().port() as u32;

    // let the harness know what port this duck is listening on
    client.listening(ListenInfo { port }).await?;

    // start serving requests
    tokio::spawn(http_server(addr));

    // this request will block until `sheepdog` is ready for the test to end
    info!("Waiting for shutdown signal");
    client.await_shutdown(()).await?;

    info!("Ducks is finishing up");

    // Report interesting metrics / test results

    let metric = GENERIC_HTTP_COUNTERS.get().unwrap();
    let m = metric.lock().await;
    client.report_metrics(&*m).await?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    debug!("Hello from ducks");

    let channel = Endpoint::try_from("http://127.0.0.1/this-is-not-used")?
        .connect_with_connector(tower::service_fn(|_| {
            let path = std::env::args().nth(1).unwrap();
            UnixStream::connect(path)
        }))
        .await?;
    let client = IntegrationClient::new(channel);
    debug!("Ducks is connected");

    // todo Ask for a DucksConfig

    let config = DucksConfig {
        listen: shared::ListenConfig::Http,
        measurements: shared::MeasurementConfig::GenericHttp,
        emit: shared::EmitConfig::None,
        assertions: shared::AssertionConfig::None,
    };

    match config.listen {
        shared::ListenConfig::Http => http_listen(client, config).await?,
    }

    Ok(())
}
