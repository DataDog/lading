//! TCP request/response (`tcp_rr`) generator — the client side.
//! Based on <https://github.com/google/neper>
//!
//! Implements neper's `tcp_rr` protocol: each flow sends a fixed-size request,
//! waits for a fixed-size response, and repeats. Flows are distributed across
//! OS threads and multiplexed via mio.
//!
//! ## Metrics
//!
//! `requests_sent`: Completed request writes
//! `responses_received`: Completed response reads
//! `bytes_written`: Request bytes sent
//! `bytes_read`: Response bytes received
//! `connections_failed`: Failed connection attempts

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use serde::{Deserialize, Serialize};
use tracing::{info, trace, warn};

use super::General;
use crate::generator::common::MetricsBuilder;
use crate::neper::flow::{self, Action, Flow, FlowMap};
use crate::neper::metrics::{self, ThreadMetrics};
use crate::neper::thread;

const fn default_nonzero_u16() -> NonZeroU16 {
    // Safety: 1 != 0
    unsafe { NonZeroU16::new_unchecked(1) }
}

const fn default_nonzero_usize() -> NonZeroUsize {
    // Safety: 1 != 0
    unsafe { NonZeroUsize::new_unchecked(1) }
}

const fn default_true() -> bool {
    true
}

fn default_control_port() -> u16 {
    12866
}

fn default_data_port() -> u16 {
    12867
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration for the `tcp_rr` generator.
pub struct Config {
    /// The IP address of the `tcp_rr` server.
    pub addr: String,
    /// Data port for flow connections. Default 12867.
    #[serde(default = "default_data_port")]
    pub data_port: u16,
    /// Control port for startup synchronization with the blackhole. Default 12866.
    #[serde(default = "default_control_port")]
    pub control_port: u16,
    /// Number of OS threads (neper -T). Default 1.
    #[serde(default = "default_nonzero_u16")]
    pub threads: NonZeroU16,
    /// Total number of TCP flows/connections (neper -F). Default 1.
    #[serde(default = "default_nonzero_u16")]
    pub flows: NonZeroU16,
    /// Bytes per request. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub request_size: NonZeroUsize,
    /// Bytes per response to read back. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub response_size: NonZeroUsize,
    /// Whether to set `TCP_NODELAY` on connections. Default true.
    #[serde(default = "default_true")]
    pub no_delay: bool,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`TcpRr`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Worker thread panicked
    #[error("Worker thread panicked")]
    ThreadPanicked,
}

#[derive(Debug)]
/// The `tcp_rr` generator (client side).
pub struct TcpRr {
    config: Config,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

enum ClientState {
    SendRequest,
    RecvResponse,
}

impl TcpRr {
    /// Create a new [`TcpRr`] generator instance.
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let metric_labels = MetricsBuilder::new("tcp_rr").with_id(general.id).build();
        Self {
            config: config.clone(),
            metric_labels,
            shutdown,
        }
    }

    /// Run the generator to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Returns an error if a worker thread panics.
    ///
    /// # Panics
    ///
    /// Panics if `addr` cannot be resolved to a socket address.
    pub async fn spin(self) -> Result<(), Error> {
        let ip: std::net::IpAddr = self.config.addr.parse().expect("invalid addr");
        let data_addr = SocketAddr::new(ip, self.config.data_port);
        let control_addr = SocketAddr::new(ip, self.config.control_port);

        let shutdown_flag = thread::new_shutdown_flag();

        // Wait for the blackhole to be ready by connecting to its control port.
        info!("waiting for blackhole control port at {control_addr}");
        let start = std::time::Instant::now();
        let mut next_log = start + Duration::from_secs(60);
        {
            let flag = Arc::clone(&shutdown_flag);
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                shutdown.recv().await;
                flag.store(true, Relaxed);
            });
        }
        loop {
            if shutdown_flag.load(Relaxed) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!(
                        "shutdown before blackhole control port {control_addr} became reachable"
                    ),
                )));
            }
            if std::net::TcpStream::connect(control_addr).is_ok() {
                info!("blackhole ready, starting flows");
                break;
            }
            let now = std::time::Instant::now();
            if now >= next_log {
                let elapsed = now.duration_since(start).as_secs();
                warn!(
                    "still waiting for blackhole control port at {control_addr} ({elapsed}s elapsed)"
                );
                next_log = now + Duration::from_secs(60);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let num_threads = self.config.threads.get();
        let num_flows = self.config.flows.get();
        let request_size = self.config.request_size.get();
        let response_size = self.config.response_size.get();

        let flow_dist = thread::distribute_flows(num_flows, num_threads);

        let thread_metrics = Arc::new(
            (0..num_threads)
                .map(|_| ThreadMetrics::new())
                .collect::<Vec<_>>(),
        );

        let metrics_handle = {
            let tm = Arc::clone(&thread_metrics);
            let labels = self.metric_labels.clone();
            let flag = Arc::clone(&shutdown_flag);
            thread::spawn_named("tcp_rr-metrics", move || {
                metrics::run_metrics_thread(&tm, &labels, &flag);
            })
        };

        let mut worker_handles = Vec::with_capacity(num_threads as usize);
        for i in 0..num_threads {
            let thread_flows = flow_dist[i as usize];
            let flag = Arc::clone(&shutdown_flag);
            let tm = Arc::clone(&thread_metrics);
            let no_delay = self.config.no_delay;
            let handle = thread::spawn_named(&format!("tcp_rr-client-{i}"), move || {
                client_thread_main(
                    data_addr,
                    thread_flows,
                    request_size,
                    response_size,
                    no_delay,
                    &flag,
                    &tm[i as usize],
                );
            });
            worker_handles.push(handle);
        }

        self.shutdown.recv().await;
        info!("shutdown signal received");
        shutdown_flag.store(true, Relaxed);

        worker_handles.push(metrics_handle);
        thread::join_all(worker_handles).map_err(|()| Error::ThreadPanicked)?;

        Ok(())
    }
}

fn client_thread_main(
    addr: SocketAddr,
    num_flows: u16,
    request_size: usize,
    response_size: usize,
    no_delay: bool,
    shutdown_flag: &std::sync::atomic::AtomicBool,
    metrics: &ThreadMetrics,
) {
    let mut poll = Poll::new().expect("failed to create mio::Poll");
    let mut events = Events::with_capacity(num_flows as usize);
    let request_buf = vec![0u8; request_size];
    let mut response_buf = vec![0u8; response_size];
    let mut flows: FlowMap<ClientState> = FlowMap::new();
    let mut next_token: usize = 0;

    for _ in 0..num_flows {
        match std::net::TcpStream::connect(addr) {
            Ok(std_stream) => {
                let _ = std_stream.set_nodelay(no_delay);
                std_stream
                    .set_nonblocking(true)
                    .expect("failed to set nonblocking");
                let mut stream = TcpStream::from_std(std_stream);
                let token = Token(next_token);
                next_token += 1;
                poll.registry()
                    .register(&mut stream, token, Interest::WRITABLE)
                    .expect("failed to register flow");
                flows.insert(Flow {
                    stream,
                    token,
                    state: ClientState::SendRequest,
                    xfer: request_size,
                });
            }
            Err(e) => {
                trace!("connection to {addr} failed: {e}");
                metrics.connections_failed.add(1);
            }
        }
    }

    loop {
        let _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        if shutdown_flag.load(Relaxed) {
            break;
        }
        for event in &events {
            let token = event.token();
            let Some(fl) = flows.get_mut(token) else {
                continue;
            };
            let action = handle_client_event(fl, &request_buf, &mut response_buf, metrics);
            flow::apply_action(action, token, &mut flows, poll.registry());
        }
    }
}

fn handle_client_event(
    flow: &mut Flow<ClientState>,
    request_buf: &[u8],
    response_buf: &mut [u8],
    metrics: &ThreadMetrics,
) -> Action {
    match flow.state {
        ClientState::SendRequest => {
            let offset = request_buf.len() - flow.xfer;
            match flow.stream.write(&request_buf[offset..]) {
                Ok(n) => {
                    flow.xfer -= n;
                    if flow.xfer == 0 {
                        flow.xfer = response_buf.len();
                        flow.state = ClientState::RecvResponse;
                        metrics.requests_sent.add(1);
                        metrics.bytes_written.add(request_buf.len() as u64);
                        Action::Reregister(Interest::READABLE)
                    } else {
                        Action::Continue
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Action::Continue,
                Err(e) => {
                    trace!("write error: {e}");
                    Action::Remove
                }
            }
        }
        ClientState::RecvResponse => {
            let offset = response_buf.len() - flow.xfer;
            match flow.stream.read(&mut response_buf[offset..]) {
                Ok(0) => Action::Remove,
                Ok(n) => {
                    flow.xfer -= n;
                    if flow.xfer == 0 {
                        flow.xfer = request_buf.len();
                        flow.state = ClientState::SendRequest;
                        metrics.responses_received.add(1);
                        metrics.bytes_read.add(response_buf.len() as u64);
                        Action::Reregister(Interest::WRITABLE)
                    } else {
                        Action::Continue
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Action::Continue,
                Err(e) => {
                    trace!("read error: {e}");
                    Action::Remove
                }
            }
        }
    }
}
