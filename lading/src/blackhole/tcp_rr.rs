//! TCP request/response (`tcp_rr`) blackhole — the server side.
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
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Barrier};
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

fn default_one() -> u16 {
    1
}

fn default_one_usize() -> usize {
    1
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for the `tcp_rr` blackhole.
pub struct Config {
    /// Address to bind the listener on.
    pub binding_addr: SocketAddr,
    /// Number of OS server threads. Default 1. When > 1, uses `SO_REUSEPORT`
    /// with an eBPF program for load balancing
    #[serde(default = "default_one")]
    pub threads: u16,
    /// Bytes to read per request. Default 1.
    #[serde(default = "default_one_usize")]
    pub request_size: usize,
    /// Bytes to send per response. Default 1.
    #[serde(default = "default_one_usize")]
    pub response_size: usize,
    /// Whether to set `TCP_NODELAY` on accepted connections. Default true.
    #[serde(default = "default_true")]
    pub no_delay: bool,
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
    sample_period: Duration,
}

enum ServerState {
    RecvRequest,
    SendResponse,
}

const LISTENER_TOKEN: Token = Token(0);

impl TcpRr {
    /// Create a new [`TcpRr`] blackhole instance.
    #[must_use]
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
        sample_period: Duration,
    ) -> Self {
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
            sample_period,
        }
    }

    /// Run the blackhole to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Returns an error if binding fails or a worker thread panics.
    pub async fn run(self) -> Result<(), Error> {
        let shutdown_flag = thread::new_shutdown_flag();
        let num_threads = self.config.threads;

        let thread_metrics = Arc::new(
            (0..num_threads)
                .map(|_| ThreadMetrics::new())
                .collect::<Vec<_>>(),
        );

        let metrics_handle = {
            let tm = Arc::clone(&thread_metrics);
            let labels = self.metric_labels.clone();
            let interval = self.sample_period;
            let flag = Arc::clone(&shutdown_flag);
            thread::spawn_named("tcp_rr-bh-metrics", move || {
                metrics::run_metrics_thread(&tm, &labels, interval, &flag);
            })
        };

        // When threads > 1, thread 0 must bind first to attach BPF before
        // other threads join the SO_REUSEPORT group. The barrier ensures
        // ordering: thread 0 binds + attaches, then signals the barrier,
        // then all other threads proceed to bind.
        let barrier = if num_threads > 1 {
            Some(Arc::new(Barrier::new(num_threads as usize)))
        } else {
            None
        };

        let mut handles = Vec::with_capacity(num_threads as usize);
        for i in 0..num_threads {
            let binding_addr = self.config.binding_addr;
            let request_size = self.config.request_size;
            let response_size = self.config.response_size;
            let no_delay = self.config.no_delay;
            let flag = Arc::clone(&shutdown_flag);
            let tm = Arc::clone(&thread_metrics);
            let barrier = barrier.clone();
            let handle = thread::spawn_named(&format!("tcp_rr-server-{i}"), move || {
                server_thread_main(
                    i,
                    num_threads,
                    binding_addr,
                    request_size,
                    response_size,
                    no_delay,
                    &flag,
                    &tm[i as usize],
                    barrier.as_deref(),
                );
            });
            handles.push(handle);
        }

        self.shutdown.recv().await;
        info!("shutdown signal received");
        shutdown_flag.store(true, Relaxed);

        handles.push(metrics_handle);
        thread::join_all(handles).map_err(|()| Error::ThreadPanicked)?;

        Ok(())
    }
}

/// Create a listener socket. When `num_threads` > 1, sets `SO_REUSEPORT`
/// and (for thread 0) attaches the reuseport eBPF program.
#[allow(clippy::cast_possible_truncation)]
fn create_listener(
    thread_index: u16,
    num_threads: u16,
    binding_addr: SocketAddr,
) -> std::net::TcpListener {
    // Create socket manually to set SO_REUSEPORT before bind.
    let domain = if binding_addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0) };
    assert!(fd >= 0, "failed to create socket");

    set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1);

    if num_threads > 1 {
        set_sock_opt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, 1);

        if thread_index == 0 {
            match bpf::load_reuseport_ebpf(u32::from(num_threads)) {
                Ok(prog) => {
                    if let Err(e) = bpf::attach_reuseport_ebpf(fd, &prog) {
                        warn!("failed to attach reuseport eBPF: {e}, falling back to kernel hash");
                    }
                }
                Err(e) => {
                    warn!("failed to load reuseport eBPF: {e}, falling back to kernel hash");
                }
            }
        }
    }

    let (sockaddr, socklen) = socket_addr_to_raw(&binding_addr);
    let ret = unsafe {
        libc::bind(
            fd,
            (&raw const sockaddr).cast::<libc::sockaddr>(),
            socklen,
        )
    };
    assert!(
        ret == 0,
        "failed to bind to {binding_addr}: {}",
        std::io::Error::last_os_error()
    );

    let ret = unsafe { libc::listen(fd, 1024) };
    assert!(
        ret == 0,
        "failed to listen: {}",
        std::io::Error::last_os_error()
    );

    unsafe { std::net::TcpListener::from_raw_fd(fd) }
}

#[allow(clippy::cast_possible_truncation)]
fn set_sock_opt(fd: libc::c_int, level: libc::c_int, optname: libc::c_int, val: libc::c_int) {
    unsafe {
        libc::setsockopt(
            fd,
            level,
            optname,
            (&raw const val).cast::<libc::c_void>(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[allow(clippy::cast_possible_truncation)]
fn socket_addr_to_raw(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = unsafe { &mut *(&raw mut storage).cast::<libc::sockaddr_in>() };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = a.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(a.ip().octets()),
            };
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(a) => {
            let sin6 = unsafe { &mut *(&raw mut storage).cast::<libc::sockaddr_in6>() };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: a.ip().octets(),
            };
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_scope_id = a.scope_id();
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    };
    (storage, len)
}

#[allow(clippy::too_many_arguments)]
fn server_thread_main(
    thread_index: u16,
    num_threads: u16,
    binding_addr: SocketAddr,
    request_size: usize,
    response_size: usize,
    no_delay: bool,
    shutdown_flag: &std::sync::atomic::AtomicBool,
    metrics: &ThreadMetrics,
    barrier: Option<&Barrier>,
) {
    // Thread 0 must bind first (to attach BPF before others join the
    // reuseport group). Non-zero threads wait at the barrier first.
    if let Some(barrier) = barrier
        && thread_index != 0
    {
        barrier.wait();
    }

    let std_listener = create_listener(thread_index, num_threads, binding_addr);

    // Thread 0 signals the barrier after binding so others can proceed.
    if let Some(barrier) = barrier
        && thread_index == 0
    {
        barrier.wait();
    }

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

/// Set `TCP_NODELAY` on a mio [`TcpStream`] using the raw fd.
fn set_nodelay_mio(stream: &TcpStream, no_delay: bool) {
    let fd = stream.as_raw_fd();
    set_sock_opt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, i32::from(no_delay));
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
