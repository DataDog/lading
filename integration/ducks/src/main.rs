//! Ducks is an integration testing target for lading.
//!
//! Ducks exists to enable correctness testing on lading. Any high-level
//! validation tasks are in-scope, but preference should be given to
//! unit-testing within lading over testing with ducks where possible.
//!
//! Currently, ducks can perform these tasks:
//! - Receive data
//! - Emit metrics collected during a test
//!
//! Upcoming goals:
//! - Validate some forms of received data
//! - Send data to lading
//! - Receive data on other protocols & formats

use anyhow::Context;
use hyper::{
    body,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, StatusCode,
};
use once_cell::sync::OnceCell;
use shared::{
    integration_api::{
        integration_target_server::{IntegrationTarget, IntegrationTargetServer},
        Empty, ListenInfo, MetricsReport, TestConfig,
    },
    DucksConfig,
};
use sketches_ddsketch::DDSketch;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::UnixListener,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{IntoRequest, Status};
use tower::ServiceBuilder;
use tracing::{debug, trace};

static GENERIC_HTTP_COUNTERS: OnceCell<Arc<Mutex<GenericHttpCounters>>> = OnceCell::new();

/// Corresponds to MeasurementConfig::GenericHttp
struct GenericHttpCounters {
    body_size: DDSketch,
    entropy: DDSketch,
    request_count: u64,
    total_bytes: u64,
    // todo: record http methods received
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

impl Into<MetricsReport> for &GenericHttpCounters {
    fn into(self) -> MetricsReport {
        MetricsReport {
            request_count: self.request_count,
            total_bytes: self.total_bytes,
            median_entropy: self.entropy.quantile(0.5).unwrap().unwrap_or_default(),
            median_size: self.body_size.quantile(0.5).unwrap().unwrap_or_default(),
        }
    }
}

#[tracing::instrument(level = "trace")]
async fn http_req_handler(req: Request<Body>) -> Result<hyper::Response<Body>, hyper::Error> {
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

    let mut okay = hyper::Response::default();
    *okay.status_mut() = StatusCode::OK;

    let body_bytes = vec![];
    *okay.body_mut() = Body::from(body_bytes);
    Ok(okay)
}

#[tracing::instrument]
async fn http_server(addr: AddrIncoming) -> Result<(), anyhow::Error> {
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

    let server = hyper::Server::builder(addr).serve(svc);
    server.await?;
    Ok(())
}

/// Descries an HTTP test
struct HttpTest {
    _task: JoinHandle<Result<(), anyhow::Error>>,
}

/// Describes the currently running ducks test
enum RunningTest {
    None,
    Http(HttpTest),
}

/// Tracks state for a ducks instance
pub struct DucksTarget {
    /// Stores state information for the current test.
    running_test: Mutex<RunningTest>,

    /// Shutdown channel. Send on this to exit the process immediately.
    shutdown_tx: mpsc::Sender<()>,
}

#[tonic::async_trait]
impl IntegrationTarget for DucksTarget {
    #[tracing::instrument(skip(self))]
    async fn shutdown(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<shared::integration_api::Empty>, Status> {
        self.shutdown_tx.send(()).await.unwrap();
        Ok(tonic::Response::new(Empty {}))
    }

    #[tracing::instrument(skip(self))]
    async fn start_test(
        &self,
        _config: tonic::Request<TestConfig>,
    ) -> Result<tonic::Response<shared::integration_api::ListenInfo>, Status> {
        // todo get this from `config`
        let config = DucksConfig {
            listen: shared::ListenConfig::Http,
            measurements: shared::MeasurementConfig::GenericHttp,
            emit: shared::EmitConfig::None,
            assertions: shared::AssertionConfig::None,
        };

        // bind to a random open TCP port
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let addr = AddrIncoming::bind(&bind_addr)
            .map_err(|_e| Status::internal("unable to bind a port"))?;
        let port = addr.local_addr().port() as u32;

        match config.listen {
            shared::ListenConfig::Http => {
                let task = tokio::spawn(Self::http_listen(config, addr));

                let mut state = self.running_test.lock().await;
                *state = RunningTest::Http(HttpTest { _task: task });
            }
        }

        Ok(tonic::Response::new(ListenInfo { port }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_test_results(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<shared::integration_api::MetricsReport>, Status> {
        let metric = GENERIC_HTTP_COUNTERS.get().unwrap();
        let m = metric.lock().await;
        return Ok(tonic::Response::new((&*m).into()));
    }
}

impl DucksTarget {
    async fn http_listen(_config: DucksConfig, addr: AddrIncoming) -> Result<(), anyhow::Error> {
        // start serving requests
        http_server(addr).await?;

        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    debug!("Hello from ducks");

    // Every ducks-sheepdog pair is connected by a unique socket file
    let ducks_comm_file = std::env::args().nth(1).unwrap();
    let ducks_comm =
        UnixListener::bind(&ducks_comm_file).context("ducks failed to bind to RPC socket")?;
    let ducks_comm = UnixListenerStream::new(ducks_comm);

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

    let server = DucksTarget {
        running_test: Mutex::new(RunningTest::None),
        shutdown_tx,
    };

    let rpc_server = tonic::transport::Server::builder()
        .add_service(IntegrationTargetServer::new(server))
        .serve_with_incoming(ducks_comm);

    tokio::select! {
        result = rpc_server => {
            if let Err(e) = result {
                panic!("Server error: {}", e);
            }
        },
        _ = shutdown_rx.recv() => {},
    }

    Ok(())
}
