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
use bytes::Bytes;
use bytes::BytesMut;
use http_body_util::Full;
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::{Method, Request, StatusCode, service::service_fn};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use once_cell::sync::OnceCell;
use shared::{
    DucksConfig,
    integration_api::{
        self, Empty, HttpMetrics, ListenInfo, LogMessage, Metrics, SocketMetrics, TestConfig,
        integration_target_server::{IntegrationTarget, IntegrationTargetServer},
    },
};
use sketches_ddsketch::DDSketch;
use std::{collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio::{
    io::AsyncReadExt,
    net::{self, TcpListener, TcpStream, UdpSocket, UnixListener},
    sync::{Mutex, mpsc},
};
use tokio_stream::{Stream, wrappers::UnixListenerStream};
use tonic::Status;
use tonic::transport;
use tracing::error;
use tracing::{debug, trace, warn};

static HTTP_COUNTERS: OnceCell<Arc<Mutex<HttpCounters>>> = OnceCell::new();
static TCP_COUNTERS: OnceCell<Arc<Mutex<SocketCounters>>> = OnceCell::new();
static UDP_COUNTERS: OnceCell<Arc<Mutex<SocketCounters>>> = OnceCell::new();

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

impl From<&HttpCounters> for HttpMetrics {
    fn from(val: &HttpCounters) -> Self {
        HttpMetrics {
            request_count: val.request_count,
            total_bytes: val.total_bytes,
            median_entropy: val
                .entropy
                .quantile(0.5)
                .expect("quantile argument must be between 0.0 and 1.0 inclusive")
                .unwrap_or_default(),
            median_size: val
                .body_size
                .quantile(0.5)
                .expect("quantile argument must be between 0.0 and 1.0 inclusive")
                .unwrap_or_default(),
        }
    }
}

struct SocketCounters {
    entropy: DDSketch,
    read_count: u64,
    total_bytes: u64,
}

impl Default for SocketCounters {
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

impl From<&SocketCounters> for SocketMetrics {
    fn from(val: &SocketCounters) -> Self {
        SocketMetrics {
            read_count: val.read_count,
            total_bytes: val.total_bytes,
            median_entropy: val
                .entropy
                .quantile(0.5)
                .expect("quantile argument must be between 0.0 and 1.0 inclusive")
                .unwrap_or_default(),
        }
    }
}

#[tracing::instrument(level = "trace")]
async fn http_req_handler(
    req: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    // Split into parts
    let (parts, body) = req.into_parts();

    // Convert the `Body` into `Bytes`
    let body = body.boxed().collect().await?.to_bytes();

    {
        let metric = HTTP_COUNTERS.get().expect("HTTP_COUNTERS not initialized");
        let mut m = metric.lock().await;
        m.request_count += 1;

        m.total_bytes = body.len() as u64;
        m.entropy.add(entropy::metric_entropy(&body) as f64);

        m.body_size.add(body.len() as f64);

        let method_counter = m.methods.entry(parts.method).or_default();
        *method_counter += 1;
    }

    // Create a simple OK response
    let mut resp = hyper::Response::default();
    *resp.status_mut() = StatusCode::OK;
    *resp.body_mut() = full(body);
    Ok(resp)
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

/// Tracks state for a ducks instance
pub struct DucksTarget {
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
                let addr = SocketAddr::from(([127, 0, 0, 1], 0));
                let listener = net::TcpListener::bind(addr).await?;
                let port = listener.local_addr()?.port() as u32;
                tokio::spawn(Self::http_listen(config, listener));

                Ok(tonic::Response::new(ListenInfo { port }))
            }
            shared::ListenConfig::None => Ok(tonic::Response::new(ListenInfo { port: 0 })),
            shared::ListenConfig::Tcp => {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let port = listener.local_addr()?.port() as u32;
                tokio::spawn(Self::tcp_listen(config, listener));

                Ok(tonic::Response::new(ListenInfo { port }))
            }
            shared::ListenConfig::Udp => {
                let listener = UdpSocket::bind("127.0.0.1:0").await?;
                let port = listener.local_addr()?.port() as u32;
                tokio::spawn(Self::udp_listen(config, listener));

                Ok(tonic::Response::new(ListenInfo { port }))
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
        let udp_metric = {
            if let Some(metric) = UDP_COUNTERS.get() {
                let m = metric.lock().await;
                Some((&*m).into())
            } else {
                None
            }
        };
        Ok(tonic::Response::new(Metrics {
            http: http_metric,
            tcp: tcp_metric,
            udp: udp_metric,
        }))
    }
}

impl DucksTarget {
    async fn http_listen(
        _config: DucksConfig,
        listener: tokio::net::TcpListener,
    ) -> Result<(), anyhow::Error> {
        debug!("HTTP listener active");
        HTTP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(HttpCounters::default())));

        let mut join_set = JoinSet::new();
        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    error!("failed to accept connection: {e}");
                    break;
                }
            };

            let serve_connection = async move {
                let result = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service_fn(http_req_handler))
                    .await;

                if let Err(e) = result {
                    error!("error serving {addr}: {e}");
                }
            };

            join_set.spawn(serve_connection);
        }
        while (join_set.join_next().await).is_some() {}

        Ok(())
    }

    async fn tcp_handler(mut socket: TcpStream) -> Result<(), anyhow::Error> {
        // 500KiB input buffer per connection (this can probably be smaller)
        let mut buffer = BytesMut::with_capacity(524_288);
        trace!("TCP connection received");

        // Read & count metrics until connection closes (closes on any error)
        loop {
            socket.readable().await?;
            socket.read_buf(&mut buffer).await?;

            {
                let metric = TCP_COUNTERS
                    .get()
                    .ok_or(anyhow::anyhow!("TCP_COUNTERS not initialized"))?;
                let mut m = metric.lock().await;
                m.read_count += 1;
                m.total_bytes += buffer.len() as u64;
                m.entropy.add(entropy::metric_entropy(&buffer) as f64);
            }

            buffer.clear();
        }
    }

    async fn tcp_listen(_config: DucksConfig, incoming: TcpListener) -> Result<(), anyhow::Error> {
        debug!("TCP listener active on {}", incoming.local_addr()?);
        TCP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(SocketCounters::default())));

        loop {
            let (socket, _remote) = incoming.accept().await?;
            tokio::spawn(Self::tcp_handler(socket));
        }
    }

    async fn udp_listen(_config: DucksConfig, incoming: UdpSocket) -> Result<(), anyhow::Error> {
        debug!("UDP listener active on {}", incoming.local_addr()?);
        UDP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(SocketCounters::default())));
        let mut buf = [0u8; 1500];

        loop {
            trace!("Waiting for incoming data");
            let (count, _remote) = incoming.recv_from(&mut buf).await?;
            trace!("Got {} B from {}", count, _remote);

            {
                let metric = UDP_COUNTERS
                    .get()
                    .ok_or(anyhow::anyhow!("UDP_COUNTERS not initialized"))?;
                let mut m = metric.lock().await;
                m.read_count += 1;
                m.total_bytes += count as u64;
                m.entropy.add(entropy::metric_entropy(buf) as f64);
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    debug!("hello from ducks");

    // Every ducks-sheepdog pair is connected by a unique socket file
    let ducks_comm_file = std::env::args()
        .nth(1)
        .ok_or(anyhow::anyhow!("ducks socket file argument missing"))?;
    let ducks_comm =
        UnixListener::bind(&ducks_comm_file).context("ducks failed to bind to RPC socket")?;
    let ducks_comm = UnixListenerStream::new(ducks_comm);

    let timeout_seconds = std::env::args()
        .nth(2)
        .ok_or(anyhow::anyhow!("ducks timeout argument missing"))?;
    let timeout_seconds: u64 = timeout_seconds
        .parse()
        .context("ducks timeout argument must be a number")?;

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let (internal_shutdown_tx, mut internal_shutdown_rx) = mpsc::channel(1);

    let server = DucksTarget {
        shutdown_tx: shutdown_tx.clone(),
    };

    let rpc_server = transport::Server::builder()
        .add_service(IntegrationTargetServer::new(server))
        .serve_with_incoming_shutdown(ducks_comm, async move {
            internal_shutdown_rx.recv().await;
        });

    tokio::select! {
        result = rpc_server => {
            if let Err(e) = result {
                panic!("Server error: {e}");
            }
        },
        _ = shutdown_rx.recv() => {
            // Forward the shutdown signal to the internal channel
            let _ = internal_shutdown_tx.send(()).await;
        },
        _ = tokio::time::sleep(Duration::from_secs(timeout_seconds)) => {
            warn!("timed out");
            // Also send shutdown on timeout
            let _ = internal_shutdown_tx.send(()).await;
        }
    }
    debug!("shutting down");

    Ok(())
}
