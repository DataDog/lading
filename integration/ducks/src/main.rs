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
use bytes::BytesMut;
use hyper::{
    body,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Method, Request, StatusCode,
};
use once_cell::sync::OnceCell;
use shared::{
    integration_api::{
        self,
        integration_target_server::{IntegrationTarget, IntegrationTargetServer},
        Empty, HttpMetrics, ListenInfo, LogMessage, Metrics, TcpMetrics, TestConfig,
    },
    DucksConfig,
};
use sketches_ddsketch::DDSketch;
use std::{collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream, UnixListener},
    sync::{mpsc, Mutex},
};
use tokio_stream::{wrappers::UnixListenerStream, Stream};
use tonic::Status;
use tower::ServiceBuilder;
use tracing::{debug, trace};

static HTTP_COUNTERS: OnceCell<Arc<Mutex<HttpCounters>>> = OnceCell::new();
static TCP_COUNTERS: OnceCell<Arc<Mutex<TcpCounters>>> = OnceCell::new();

struct HttpCounters {
    body_size: DDSketch,
    entropy: DDSketch,
    request_count: u64,
    total_bytes: u64,
    methods: HashMap<Method, u64>,
}

impl Default for HttpCounters {
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
            methods: HashMap::new(),
        }
    }
}

impl Into<HttpMetrics> for &HttpCounters {
    fn into(self) -> HttpMetrics {
        HttpMetrics {
            request_count: self.request_count,
            total_bytes: self.total_bytes,
            median_entropy: self.entropy.quantile(0.5).unwrap().unwrap_or_default(),
            median_size: self.body_size.quantile(0.5).unwrap().unwrap_or_default(),
        }
    }
}

struct TcpCounters {
    entropy: DDSketch,
    read_count: u64,
    total_bytes: u64,
}

impl Default for TcpCounters {
    fn default() -> Self {
        let config = sketches_ddsketch::Config::defaults();
        let entropy = DDSketch::new(config);

        Self {
            entropy,
            read_count: Default::default(),
            total_bytes: Default::default(),
        }
    }
}

impl Into<TcpMetrics> for &TcpCounters {
    fn into(self) -> TcpMetrics {
        TcpMetrics {
            read_count: self.read_count,
            total_bytes: self.total_bytes,
            median_entropy: self.entropy.quantile(0.5).unwrap().unwrap_or_default(),
        }
    }
}

#[tracing::instrument(level = "trace")]
async fn http_req_handler(req: Request<Body>) -> Result<hyper::Response<Body>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let body = body::to_bytes(body).await?;

    {
        let metric = HTTP_COUNTERS.get().unwrap();
        let mut m = metric.lock().await;
        m.request_count += 1;

        m.total_bytes = body.len() as u64;
        m.entropy.add(entropy::metric_entropy(&body) as f64);

        m.body_size.add(body.len() as f64);

        let method_counter = m.methods.entry(parts.method).or_default();
        *method_counter += 1;
    }

    let mut okay = hyper::Response::default();
    *okay.status_mut() = StatusCode::OK;

    let body_bytes = vec![];
    *okay.body_mut() = Body::from(body_bytes);
    Ok(okay)
}

enum ActiveListener {
    None,
    Http,
    Tcp,
}

/// Describes the currently running ducks test
struct TestState {
    listener: ActiveListener,
}

/// Tracks state for a ducks instance
pub struct DucksTarget {
    /// Stores state information for the current test.
    running_test: Mutex<TestState>,

    /// Shutdown channel. Send on this to exit the process immediately.
    shutdown_tx: mpsc::Sender<()>,
}

#[tonic::async_trait]
impl IntegrationTarget for DucksTarget {
    type GetLogsStream = Pin<Box<dyn Stream<Item = Result<LogMessage, Status>> + Send>>;

    #[tracing::instrument(skip(self))]
    async fn shutdown(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<integration_api::Empty>, Status> {
        self.shutdown_tx
            .send(())
            .await
            .expect("failed to send shutdown signal");
        Ok(tonic::Response::new(Empty {}))
    }

    #[tracing::instrument(skip(self))]
    async fn get_logs(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::GetLogsStream>, Status> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    async fn start_test(
        &self,
        config: tonic::Request<TestConfig>,
    ) -> Result<tonic::Response<integration_api::ListenInfo>, Status> {
        let config: DucksConfig = config.into_inner().into();

        match config.listen {
            shared::ListenConfig::Http => {
                // bind to a random open TCP port
                let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0));
                let addr = AddrIncoming::bind(&bind_addr)
                    .map_err(|_e| Status::internal("unable to bind a port"))?;
                let port = addr.local_addr().port() as u32;
                tokio::spawn(Self::http_listen(config, addr));

                let mut state = self.running_test.lock().await;
                state.listener = ActiveListener::Http;
                Ok(tonic::Response::new(ListenInfo { port }))
            }
            shared::ListenConfig::None => {
                let mut state = self.running_test.lock().await;
                state.listener = ActiveListener::None;
                Ok(tonic::Response::new(ListenInfo { port: 0 }))
            }
            shared::ListenConfig::Tcp => {
                let listener = TcpListener::bind("0.0.0.0:0").await?;
                let port = listener.local_addr()?.port();
                tokio::spawn(Self::tcp_listen(config, listener));

                let mut state = self.running_test.lock().await;
                state.listener = ActiveListener::Tcp;
                Ok(tonic::Response::new(ListenInfo { port: port as u32 }))
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_metrics(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<integration_api::Metrics>, Status> {
        let http_metric = {
            if let Some(metric) = HTTP_COUNTERS.get() {
                let m = metric.lock().await;
                Some((&*m).into())
            } else {
                None
            }
        };
        let tcp_metric = {
            if let Some(metric) = TCP_COUNTERS.get() {
                let m = metric.lock().await;
                Some((&*m).into())
            } else {
                None
            }
        };
        Ok(tonic::Response::new(Metrics {
            http: http_metric,
            tcp: tcp_metric,
        }))
    }
}

impl DucksTarget {
    async fn http_listen(_config: DucksConfig, addr: AddrIncoming) -> Result<(), anyhow::Error> {
        debug!("HTTP listener active");
        HTTP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(HttpCounters::default())));

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

    async fn tcp_handler(mut socket: TcpStream) -> Result<(), anyhow::Error> {
        // 500KB input buffer per connection (this can probably be smaller)
        let mut buffer = BytesMut::with_capacity(524_288);
        trace!("TCP connection received");

        // Read & count metrics until connection closes (closes on any error)
        loop {
            let _ = socket.readable().await?;
            socket.read_buf(&mut buffer).await?;

            {
                let metric = TCP_COUNTERS.get().unwrap();
                let mut m = metric.lock().await;
                m.read_count += 1;
                m.total_bytes += buffer.len() as u64;
                m.entropy.add(entropy::metric_entropy(&buffer) as f64);

                // TODO: data rate metrics & a way to measure consistency
            }

            buffer.clear();
        }
    }

    async fn tcp_listen(_config: DucksConfig, incoming: TcpListener) -> Result<(), anyhow::Error> {
        debug!("TCP listener active");
        TCP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(TcpCounters::default())));

        loop {
            let (socket, _remote) = incoming.accept().await?;
            tokio::spawn(Self::tcp_handler(socket));
        }
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
        running_test: Mutex::new(TestState {
            listener: ActiveListener::None,
        }),
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
