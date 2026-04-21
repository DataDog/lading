//! TCP request/response (`tcp_rr`) blackhole — the server side.
//! Based on <https://github.com/google/neper>
//!
//! Listens for incoming connections and, for each flow, reads a fixed-size
//! request then writes a fixed-size response, repeating until the flow closes
//! or lading shuts down.
//!
//! ## Metrics
//!
//! `connections_accepted`: Incoming connections accepted
//! `requests_received`: Completed request reads
//! `responses_sent`: Completed response writes
//! `bytes_received`: Request bytes read
//! `bytes_written`: Response bytes sent

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroUsize};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use serde::{Deserialize, Serialize};
use tracing::{info, trace, warn};

use super::General;
use crate::neper::bpf;
use crate::neper::flow::{self, Action, Flow, FlowMap};
use crate::neper::metrics::{self, ThreadMetrics};
use crate::neper::thread;

fn default_nonzero_u16() -> NonZeroU16 {
    NonZeroU16::new(1).expect("1 is nonzero")
}

fn default_nonzero_usize() -> NonZeroUsize {
    NonZeroUsize::new(1).expect("1 is nonzero")
}

fn default_control_port() -> u16 {
    12866
}

fn default_data_port() -> u16 {
    12867
}

fn default_backlog() -> i32 {
    1024
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for the `tcp_rr` blackhole.
pub struct Config {
    /// IP address to bind on.
    pub addr: std::net::IpAddr,
    /// Data port for flow connections. Default 12867.
    #[serde(default = "default_data_port")]
    pub data_port: u16,
    /// Control port for startup synchronization with the generator. Default 12866.
    #[serde(default = "default_control_port")]
    pub control_port: u16,
    /// Number of OS server threads. Default 1. When > 1, uses `SO_REUSEPORT`
    /// with an eBPF program for load balancing
    #[serde(default = "default_nonzero_u16")]
    pub threads: NonZeroU16,
    /// Bytes to read per request. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub request_size: NonZeroUsize,
    /// Bytes to send per response. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub response_size: NonZeroUsize,
    /// Whether to set `TCP_NODELAY` on accepted connections. Default true.
    #[serde(default = "default_true")]
    pub no_delay: bool,
    /// Listener backlog (pending-connection queue length) passed to `listen(2)`.
    /// Default 1024.
    #[serde(default = "default_backlog")]
    pub backlog: i32,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`TcpRr`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Error binding TCP listener
    #[error("Failed to bind TCP listener to {addr}: {source}")]
    Bind {
        /// Binding address
        addr: SocketAddr,
        /// Underlying IO error
        #[source]
        source: Box<std::io::Error>,
    },
    /// Worker thread panicked
    #[error("Worker thread panicked")]
    ThreadPanicked,
}

#[derive(Debug)]
/// The `tcp_rr` blackhole (server side).
pub struct TcpRr {
    config: Config,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

enum ServerState {
    RecvRequest,
    SendResponse,
}

const LISTENER_TOKEN: Token = Token(0);

impl TcpRr {
    /// Create a new [`TcpRr`] blackhole instance.
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "tcp_rr".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }
        Self {
            config: *config,
            metric_labels,
            shutdown,
        }
    }

    /// Run the blackhole to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Returns an error if binding fails or a worker thread panics.
    ///
    /// # Panics
    ///
    /// Panics if the ready-barrier tokio task is cancelled.
    #[allow(clippy::too_many_lines)]
    pub async fn run(self) -> Result<(), Error> {
        let shutdown_flag = thread::new_shutdown_flag();
        let num_threads = self.config.threads.get();

        let thread_metrics = Arc::new(
            (0..num_threads)
                .map(|_| ThreadMetrics::new())
                .collect::<Vec<_>>(),
        );

        let metrics_handle = {
            let tm = Arc::clone(&thread_metrics);
            let labels = self.metric_labels.clone();
            let flag = Arc::clone(&shutdown_flag);
            thread::spawn_named("tcp_rr-bh-metrics", move || {
                metrics::run_metrics_thread(&tm, &labels, &flag);
            })
        };

        // Pre-build thread 0's listener here so the BPF program is attached
        // to the reuseport group before any other thread calls bind(). This
        // removes the need for a cross-thread BPF barrier — if the bind fails
        // or panics, it propagates as an error directly from this task.
        let binding_addr = SocketAddr::new(self.config.addr, self.config.data_port);
        let thread0_listener = if num_threads > 1 {
            Some(create_listener(0, num_threads, binding_addr, self.config.backlog))
        } else {
            None
        };

        // Each thread sends a ready signal via this channel after binding.
        // If a thread panics before signaling, its sender drops; once all
        // senders are gone, recv() returns None and we detect the failure
        // instead of hanging forever.
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut handles = Vec::with_capacity(num_threads as usize);
        let mut thread0_listener = thread0_listener;
        for i in 0..num_threads {
            let request_size = self.config.request_size.get();
            let response_size = self.config.response_size.get();
            let no_delay = self.config.no_delay;
            let backlog = self.config.backlog;
            let flag = Arc::clone(&shutdown_flag);
            let tm = Arc::clone(&thread_metrics);
            let prebuilt = if i == 0 {
                thread0_listener.take()
            } else {
                None
            };
            let tx = ready_tx.clone();
            let handle = thread::spawn_named(&format!("tcp_rr-server-{i}"), move || {
                server_thread_main(
                    i,
                    num_threads,
                    binding_addr,
                    prebuilt,
                    backlog,
                    request_size,
                    response_size,
                    no_delay,
                    &flag,
                    &tm[i as usize],
                    tx,
                );
            });
            handles.push(handle);
        }
        // Drop our own copy so the channel closes when all worker threads exit.
        drop(ready_tx);

        // Wait for each thread to signal ready. If a sender drops without
        // signaling (thread panicked), recv() eventually returns None.
        for _ in 0..num_threads {
            if ready_rx.recv().await.is_none() {
                shutdown_flag.store(true, Relaxed);
                thread::join_all(handles).map_err(|()| Error::ThreadPanicked)?;
                return Err(Error::ThreadPanicked);
            }
        }

        // All data listeners are up. Open control port so the generator
        // can connect and know we're ready.
        let control_addr = SocketAddr::new(self.config.addr, self.config.control_port);
        let control_listener =
            std::net::TcpListener::bind(control_addr).map_err(|source| Error::Bind {
                addr: control_addr,
                source: Box::new(source),
            })?;
        control_listener
            .set_nonblocking(true)
            .expect("failed to set control listener nonblocking");
        info!("control port listening on {control_addr}, waiting for generator");

        handles.push(metrics_handle);

        // Accept with shutdown awareness: poll accept in a loop.
        let flag = Arc::clone(&shutdown_flag);
        let shutdown_clone = self.shutdown.clone();
        tokio::spawn(async move {
            shutdown_clone.recv().await;
            flag.store(true, Relaxed);
        });
        let mut generator_connected = false;
        loop {
            if shutdown_flag.load(Relaxed) {
                info!("shutdown before generator connected");
                break;
            }
            match control_listener.accept() {
                Ok((_conn, peer)) => {
                    info!("generator connected from {peer}, data threads running");
                    generator_connected = true;
                    break;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    return Err(Error::Bind {
                        addr: control_addr,
                        source: Box::new(e),
                    });
                }
            }
        }
        drop(control_listener);

        if generator_connected {
            self.shutdown.recv().await;
            info!("shutdown signal received");
        }
        shutdown_flag.store(true, Relaxed);

        thread::join_all(handles).map_err(|()| Error::ThreadPanicked)?;

        Ok(())
    }
}

/// Create a listener socket. When `num_threads` > 1, sets `SO_REUSEPORT`
/// and (for thread 0) attaches the reuseport eBPF program.
fn create_listener(
    thread_index: u16,
    num_threads: u16,
    binding_addr: SocketAddr,
    backlog: i32,
) -> std::net::TcpListener {
    let domain = if binding_addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
        .expect("failed to create socket");
    socket.set_nonblocking(true).expect("failed to set nonblocking");
    socket.set_cloexec(true).expect("failed to set close-on-exec");
    socket.set_reuse_address(true).expect("failed to set SO_REUSEADDR");

    if num_threads > 1 {
        socket
            .set_reuse_port(true)
            .expect("failed to set SO_REUSEPORT");

        if thread_index == 0 {
            match bpf::load_reuseport_ebpf(u32::from(num_threads)) {
                Ok(prog) => {
                    if let Err(e) = bpf::attach_reuseport_ebpf(socket.as_raw_fd(), &prog) {
                        warn!("failed to attach reuseport eBPF: {e}, falling back to kernel hash");
                    }
                }
                Err(e) => {
                    warn!("failed to load reuseport eBPF: {e}, falling back to kernel hash");
                }
            }
        }
    }

    socket
        .bind(&binding_addr.into())
        .unwrap_or_else(|e| panic!("failed to bind to {binding_addr}: {e}"));
    socket.listen(backlog).expect("failed to listen");

    socket.into()
}

#[allow(clippy::too_many_arguments)]
fn server_thread_main(
    thread_index: u16,
    num_threads: u16,
    binding_addr: SocketAddr,
    prebuilt_listener: Option<std::net::TcpListener>,
    backlog: i32,
    request_size: usize,
    response_size: usize,
    no_delay: bool,
    shutdown_flag: &std::sync::atomic::AtomicBool,
    metrics: &ThreadMetrics,
    ready_tx: tokio::sync::mpsc::UnboundedSender<()>,
) {
    // Thread 0 uses the pre-built listener (with BPF already attached);
    // others bind their own sockets that join the existing reuseport group.
    let std_listener = prebuilt_listener
        .unwrap_or_else(|| create_listener(thread_index, num_threads, binding_addr, backlog));

    // Signal that this thread's listener is bound and ready. If this send
    // fails the receiver has gone away (blackhole is shutting down).
    let _ = ready_tx.send(());
    drop(ready_tx);

    let mut listener = mio::net::TcpListener::from_std(std_listener);
    let mut poll = Poll::new().expect("failed to create mio::Poll");
    let mut events = Events::with_capacity(256);

    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)
        .expect("failed to register listener");

    let mut request_buf = vec![0u8; request_size];
    let response_buf = vec![0u8; response_size];
    let mut flows: FlowMap<ServerState> = FlowMap::new();
    let mut next_token: usize = 1;

    loop {
        let _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        if shutdown_flag.load(Relaxed) {
            break;
        }
        for event in &events {
            if event.token() == LISTENER_TOKEN {
                loop {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            set_nodelay_mio(&stream, no_delay);
                            let token = Token(next_token);
                            next_token += 1;
                            let mut mio_stream = stream;
                            poll.registry()
                                .register(&mut mio_stream, token, Interest::READABLE)
                                .expect("failed to register flow");
                            flows.insert(Flow {
                                stream: mio_stream,
                                token,
                                state: ServerState::RecvRequest,
                                xfer: request_size,
                            });
                            metrics.connections_accepted.add(1);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            trace!("accept error: {e}");
                        }
                    }
                }
            } else {
                let token = event.token();
                let Some(fl) = flows.get_mut(token) else {
                    continue;
                };
                let action = handle_server_event(fl, &mut request_buf, &response_buf, metrics);
                flow::apply_action(action, token, &mut flows, poll.registry());
            }
        }
    }
}

/// Set `TCP_NODELAY` on a mio [`TcpStream`] via a borrowed `socket2::SockRef`.
fn set_nodelay_mio(stream: &TcpStream, no_delay: bool) {
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_tcp_nodelay(no_delay) {
        trace!("failed to set TCP_NODELAY: {e}");
    }
}

fn handle_server_event(
    flow: &mut Flow<ServerState>,
    request_buf: &mut [u8],
    response_buf: &[u8],
    metrics: &ThreadMetrics,
) -> Action {
    match flow.state {
        ServerState::RecvRequest => {
            let offset = request_buf.len() - flow.xfer;
            match flow.stream.read(&mut request_buf[offset..]) {
                Ok(0) => Action::Remove,
                Ok(n) => {
                    flow.xfer -= n;
                    if flow.xfer == 0 {
                        flow.xfer = response_buf.len();
                        flow.state = ServerState::SendResponse;
                        metrics.requests_received_count.add(1);
                        metrics.bytes_received.add(request_buf.len() as u64);
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
        ServerState::SendResponse => {
            let offset = response_buf.len() - flow.xfer;
            match flow.stream.write(&response_buf[offset..]) {
                Ok(n) => {
                    flow.xfer -= n;
                    if flow.xfer == 0 {
                        flow.xfer = request_buf.len();
                        flow.state = ServerState::RecvRequest;
                        metrics.responses_sent.add(1);
                        metrics.bytes_written.add(response_buf.len() as u64);
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
    }
}
