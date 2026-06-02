//! Shared client/server machinery for neper-style request/response workloads.
//!
//! Provides [`run_client`] and [`run_server`] entry points used by `tcp_rr`
//! (and forthcoming `tcp_crr`). The shared code owns the mio event loops, flow
//! lifecycle, control-port synchronization, and per-thread metrics plumbing;
//! per-variant modules build the [`ClientParams`] / [`ServerParams`] and
//! call in.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{self, SocketAddr};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::neper::bpf;
use crate::neper::flow::{self, Action, Flow, FlowMap};
use crate::neper::metrics::{self, ThreadMetrics};
use crate::neper::thread;

/// Errors produced by [`run_client`] and [`run_server`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// IO error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Failed to bind a listener.
    #[error("Failed to bind TCP listener to {addr}: {source}")]
    Bind {
        /// Binding address.
        addr: SocketAddr,
        /// Underlying IO error.
        #[source]
        source: Box<std::io::Error>,
    },
    /// A worker thread panicked.
    #[error("Worker thread panicked")]
    ThreadPanicked,
    /// Invalid configuration.
    #[error("invalid config: {0}")]
    Config(String),
}

/// Which neper-style protocol the client is driving.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Mode {
    /// `tcp_rr`: persistent connection, request/response loop forever.
    Rr,
    /// `tcp_crr`: connect, request/response, close, reconnect, repeat.
    Crr,
}

/// Parameters for [`run_client`].
///
/// Flow count is *not* a client parameter - it is owned by the server and
/// communicated to the client over the control connection during startup.
pub(crate) struct ClientParams {
    /// Address of the server's data port.
    pub(crate) data_addr: SocketAddr,
    /// Address of the server's control port.
    pub(crate) control_addr: SocketAddr,
    /// Number of OS threads.
    pub(crate) threads: u16,
    /// Bytes per request.
    pub(crate) request_size: usize,
    /// Bytes per response.
    pub(crate) response_size: usize,
    /// Whether to set `TCP_NODELAY`.
    pub(crate) no_delay: bool,
    /// RR or CRR.
    pub(crate) mode: Mode,
}

/// Parameters for [`run_server`].
pub(crate) struct ServerParams {
    /// Address to bind the data listener on.
    pub(crate) data_addr: SocketAddr,
    /// Address to bind the control listener on.
    pub(crate) control_addr: SocketAddr,
    /// Number of OS server threads.
    pub(crate) threads: u16,
    /// Total number of TCP flows the client should open. Sent to the client
    /// over the control connection during startup.
    pub(crate) flows: u16,
    /// Bytes to read per request.
    pub(crate) request_size: usize,
    /// Bytes to send per response.
    pub(crate) response_size: usize,
    /// Whether to set `TCP_NODELAY` on accepted connections.
    pub(crate) no_delay: bool,
    /// Listener backlog.
    pub(crate) backlog: i32,
    /// RR or CRR.
    pub(crate) mode: Mode,
}

enum ClientState {
    /// CRR only: waiting for a non-blocking `connect(2)` to complete. The
    /// `WRITABLE` readiness event signals connect completion; `take_error()`
    /// then tells us if it succeeded.
    Connecting,
    SendRequest,
    RecvResponse,
}

/// Actions returned by [`handle_client_event`]. Supersets [`flow::Action`]
/// with [`ClientAction::Reconnect`] for CRR's per-transaction reconnect.
#[derive(Clone, Copy)]
enum ClientAction {
    Continue,
    Reregister(Interest),
    /// CRR: response complete - close this socket and open a new one with the
    /// same `Token`. Handled by [`apply_client_action`].
    Reconnect,
    Remove,
}

enum ServerState {
    RecvRequest,
    SendResponse,
    CloseStream,
}

const LISTENER_TOKEN: Token = Token(0);

/// Control-channel handshake: server writes `flows` to the accepted control
/// connection as a 2-byte big-endian `u16` and closes; client reads the same
/// 2 bytes after connecting. Internal protocol - no magic / version byte.
const HANDSHAKE_LEN: usize = 2;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Run the neper-style client (generator side).
///
/// Connects `flows` TCP flows distributed across `threads` OS threads, then
/// runs a request/response loop on each until `shutdown` fires.
///
/// `thread_prefix` is used to name OS threads (`{prefix}-metrics`,
/// `{prefix}-client-{i}`) so multiple variants can coexist in `top -H`.
///
/// # Errors
///
/// Returns an error if configuration is invalid, the blackhole control port
/// is never reachable, or a worker thread panics.
pub(crate) async fn run_client(
    params: ClientParams,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
    thread_prefix: &'static str,
) -> Result<(), Error> {
    let shutdown_flag = thread::new_shutdown_flag();

    // Wait for the blackhole to be ready by connecting to its control port,
    // then read the flow count over that connection.
    info!(
        "waiting for blackhole control port at {}",
        params.control_addr
    );
    let deadline = Instant::now() + Duration::from_secs(300);
    {
        let flag = Arc::clone(&shutdown_flag);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown.recv().await;
            flag.store(true, Relaxed);
        });
    }
    let flows: u16 = loop {
        if shutdown_flag.load(Relaxed) {
            return Err(Error::Io(io::Error::new(
                ErrorKind::ConnectionRefused,
                format!(
                    "shutdown before blackhole control port {} became reachable",
                    params.control_addr
                ),
            )));
        }
        match generator_connect_blocking(params.control_addr) {
            Ok(mut conn) => {
                conn.set_read_timeout(Some(HANDSHAKE_TIMEOUT))
                    .expect("set_read_timeout on connected TcpStream must succeed");
                let mut buf = [0u8; HANDSHAKE_LEN];
                conn.read_exact(&mut buf)?;
                let received = u16::from_be_bytes(buf);
                info!("blackhole ready, {received} flows to open");
                break received;
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(Error::Io(io::Error::new(
                        ErrorKind::TimedOut,
                        format!(
                            "blackhole control port {} not reachable after 5 minutes: {e}",
                            params.control_addr
                        ),
                    )));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };

    if params.threads > flows {
        return Err(Error::Config(format!(
            "threads ({}) must be <= flows received from blackhole ({flows})",
            params.threads
        )));
    }

    let flow_dist = thread::distribute_flows(flows, params.threads);

    let thread_metrics = Arc::new(
        (0..params.threads)
            .map(|_| ThreadMetrics::new())
            .collect::<Vec<_>>(),
    );

    let metrics_handle = {
        let tm = Arc::clone(&thread_metrics);
        let labels = metric_labels.clone();
        let flag = Arc::clone(&shutdown_flag);
        thread::spawn_named(&format!("{thread_prefix}-metrics"), move || {
            metrics::run_metrics_thread(&tm, &labels, &flag);
        })
    };

    let data_addr = params.data_addr;
    let request_size = params.request_size;
    let response_size = params.response_size;
    let no_delay = params.no_delay;
    let mode = params.mode;
    let mut worker_handles = Vec::with_capacity(params.threads as usize);
    for i in 0..params.threads {
        let thread_flows = flow_dist[i as usize];
        let flag = Arc::clone(&shutdown_flag);
        let tm = Arc::clone(&thread_metrics);
        let handle = thread::spawn_named(&format!("{thread_prefix}-client-{i}"), move || {
            client_thread_main(
                data_addr,
                thread_flows,
                request_size,
                response_size,
                no_delay,
                mode,
                &flag,
                &tm[i as usize],
            );
        });
        worker_handles.push(handle);
    }

    shutdown.recv().await;
    info!("shutdown signal received");
    shutdown_flag.store(true, Relaxed);

    worker_handles.push(metrics_handle);
    thread::join_all(worker_handles).map_err(|()| Error::ThreadPanicked)?;

    Ok(())
}

/// `IP_LOCAL_PORT_RANGE` socket option (Linux >= 6.3). Not yet exposed by the
/// `libc` crate, so it is defined here from `<linux/in.h>`.
const IP_LOCAL_PORT_RANGE: libc::c_int = 51;
/// Lowest ephemeral local port the generator may use for its sockets.
const LOCAL_PORT_LOW: u16 = 1024;
/// Highest ephemeral local port the generator may use for its sockets.
const LOCAL_PORT_HIGH: u16 = 60999;

/// Set once the kernel is found not to support `IP_LOCAL_PORT_RANGE`, so the
/// "unsupported, falling back" warning is logged a single time rather than on
/// every socket the generator opens.
static PORT_RANGE_UNSUPPORTED: AtomicBool = AtomicBool::new(false);

/// Increase `socket`'s automatic source-port selection to
/// `[LOCAL_PORT_LOW, LOCAL_PORT_HIGH]` via `IP_LOCAL_PORT_RANGE`. The option
/// value packs the high port in the upper 16 bits and the low port in the
/// lower 16 bits.
///
/// This is done to reduce `EADDRNOTAVAIL` errors when a large number of flows are
/// created especially for `tcp_crr` workload.
/// Since port ranges are specific to network namespaces, this should not cause issues
/// for other daemons coming online on lower port ranges when lading is launched in its own
/// namespace.
fn set_local_port_range(socket: &socket2::Socket) -> io::Result<()> {
    let value: u32 = (u32::from(LOCAL_PORT_HIGH) << 16) | u32::from(LOCAL_PORT_LOW);
    // SAFETY: `socket` owns a valid fd for the duration of the borrow, and we
    // pass a pointer to a correctly sized `u32` as the option value, exactly as
    // `IP_LOCAL_PORT_RANGE` expects.
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            IP_LOCAL_PORT_RANGE,
            std::ptr::addr_of!(value).cast::<libc::c_void>(),
            std::mem::size_of::<u32>()
                .try_into()
                .expect("u32 size fits in socklen_t"),
        )
    };
    if ret != 0 {
        let err = io::Error::last_os_error();
        // ENOPROTOOPT / EOPNOTSUPP means the running kernel predates
        // IP_LOCAL_PORT_RANGE (< 6.3). Degrade gracefully: fall back to the
        // system-wide ephemeral range rather than failing the connection.
        if matches!(
            err.raw_os_error(),
            Some(libc::ENOPROTOOPT | libc::EOPNOTSUPP)
        ) {
            if !PORT_RANGE_UNSUPPORTED.swap(true, Relaxed) {
                warn!(
                    "IP_LOCAL_PORT_RANGE not supported by this kernel; \
                     falling back to the system ephemeral port range"
                );
            }
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

/// Create a TCP socket for the generator with its local port range constrained
/// to `[LOCAL_PORT_LOW, LOCAL_PORT_HIGH]`.
fn new_generator_socket(addr: SocketAddr) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    set_local_port_range(&socket)?;
    Ok(socket)
}

/// Blocking connect to `addr` using a port-range-constrained generator socket.
fn generator_connect_blocking(addr: SocketAddr) -> io::Result<net::TcpStream> {
    let socket = new_generator_socket(addr)?;
    socket.connect(&addr.into())?;
    Ok(net::TcpStream::from(socket))
}

/// Non-blocking connect to `addr` using a port-range-constrained generator
/// socket, returning a mio stream whose connect is in progress (completion is
/// signalled by a `WRITABLE` readiness event).
fn generator_connect_nonblocking(addr: SocketAddr) -> io::Result<TcpStream> {
    let socket = new_generator_socket(addr)?;
    socket.set_nonblocking(true)?;
    // A non-blocking connect reports in-progress as EINPROGRESS / WouldBlock;
    // that is expected and not an error.
    match socket.connect(&addr.into()) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) if e.kind() == ErrorKind::WouldBlock => {}
        Err(e) => return Err(e),
    }
    Ok(TcpStream::from_std(net::TcpStream::from(socket)))
}

#[allow(clippy::too_many_arguments)]
fn client_thread_main(
    addr: SocketAddr,
    num_flows: u16,
    request_size: usize,
    response_size: usize,
    no_delay: bool,
    mode: Mode,
    shutdown_flag: &AtomicBool,
    metrics: &ThreadMetrics,
) {
    let mut poll = Poll::new().expect("failed to create mio::Poll");
    let mut events = Events::with_capacity(num_flows as usize);
    let request_buf = vec![0u8; request_size];
    let mut response_buf = vec![0u8; response_size];
    let mut flows: FlowMap<ClientState> = FlowMap::new(num_flows as usize);
    let mut next_token: usize = 0;

    for _ in 0..num_flows {
        match generator_connect_blocking(addr) {
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
                flows
                    .insert(Flow {
                        stream,
                        token,
                        state: ClientState::SendRequest,
                        xfer: request_size,
                    })
                    .expect("client should never be able to exceed FlowMap capacity");
                metrics.connections_initiated.add(1);
            }
            Err(e) => {
                warn!("connection to {addr} failed: {e}");
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
            let action = handle_client_event(fl, mode, &request_buf, &mut response_buf, metrics);
            apply_client_action(action, token, &mut flows, &poll, addr, no_delay, metrics);
        }
    }
}

/// Apply a [`ClientAction`] to the flow map. Handles the CRR
/// reconnect transition (deregister old stream, open a new
/// non-blocking connect, reregister with the same token).
fn apply_client_action(
    action: ClientAction,
    token: Token,
    flows: &mut FlowMap<ClientState>,
    poll: &Poll,
    addr: SocketAddr,
    no_delay: bool,
    metrics: &ThreadMetrics,
) {
    let registry = poll.registry();
    match action {
        ClientAction::Continue => {}
        ClientAction::Reregister(interest) => {
            if let Some(flow) = flows.get_mut(token) {
                let _ = registry.reregister(&mut flow.stream, flow.token, interest);
            }
        }
        ClientAction::Reconnect => {
            let Some(flow) = flows.get_mut(token) else {
                return;
            };
            let _ = registry.deregister(&mut flow.stream);
            match generator_connect_nonblocking(addr) {
                Ok(mut new_stream) => {
                    {
                        let sock = socket2::SockRef::from(&new_stream);
                        if let Err(e) = sock.set_tcp_nodelay(no_delay) {
                            warn!("failed to set TCP_NODELAY on reconnect: {e}");
                        }
                    }
                    if let Err(e) =
                        registry.register(&mut new_stream, flow.token, Interest::WRITABLE)
                    {
                        warn!("reconnect register failed: {e}");
                        metrics.connections_failed.add(1);
                        let _ = flows.remove(token);
                    } else {
                        flow.stream = new_stream;
                        flow.state = ClientState::Connecting;
                        flow.xfer = 0;
                    }
                }
                Err(e) => {
                    warn!("reconnect to {addr} failed: {e}");
                    metrics.connections_failed.add(1);
                    let _ = flows.remove(token);
                }
            }
        }
        ClientAction::Remove => {
            metrics.connections_closed.add(1);
            if let Some(mut flow) = flows.remove(token) {
                let _ = registry.deregister(&mut flow.stream);
            }
        }
    }
}

fn handle_client_event(
    flow: &mut Flow<ClientState>,
    mode: Mode,
    request_buf: &[u8],
    response_buf: &mut [u8],
    metrics: &ThreadMetrics,
) -> ClientAction {
    // Connecting -> SendRequest transition: mio is edge-triggered, so the
    // single WRITABLE event that signaled connect completion is also the
    // event that must drive the first write. Transition state and fall
    // through to SendRequest in the same call.
    if matches!(flow.state, ClientState::Connecting) {
        match flow.stream.take_error() {
            Ok(None) => {
                flow.state = ClientState::SendRequest;
                flow.xfer = request_buf.len();
                metrics.connections_initiated.add(1);
                // fall through
            }
            Ok(Some(e)) => {
                warn!("connect failed: {e}");
                metrics.connections_failed.add(1);
                return ClientAction::Reconnect;
            }
            Err(e) => {
                warn!("take_error failed: {e}");
                metrics.connections_failed.add(1);
                return ClientAction::Reconnect;
            }
        }
    }

    match flow.state {
        ClientState::Connecting => unreachable!("transitioned out of Connecting above"),
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
                        ClientAction::Reregister(Interest::READABLE)
                    } else {
                        ClientAction::Continue
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => ClientAction::Continue,
                Err(e) => {
                    warn!("write error: {e}");
                    ClientAction::Remove
                }
            }
        }
        ClientState::RecvResponse => {
            let offset = response_buf.len() - flow.xfer;
            match flow.stream.read(&mut response_buf[offset..]) {
                Ok(0) => ClientAction::Remove,
                Ok(n) => {
                    flow.xfer -= n;
                    if flow.xfer == 0 {
                        flow.xfer = request_buf.len();
                        flow.state = ClientState::SendRequest;
                        metrics.responses_received.add(1);
                        metrics.bytes_read.add(response_buf.len() as u64);
                        match mode {
                            Mode::Rr => {
                                flow.xfer = request_buf.len();
                                flow.state = ClientState::SendRequest;
                                ClientAction::Reregister(Interest::WRITABLE)
                            }
                            Mode::Crr => ClientAction::Reconnect,
                        }
                    } else {
                        ClientAction::Continue
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => ClientAction::Continue,
                Err(e) => {
                    warn!("read error: {e}");
                    ClientAction::Remove
                }
            }
        }
    }
}

/// Run the neper-style server (blackhole side).
///
/// Binds a data listener (with `SO_REUSEPORT` + reuseport eBPF when
/// `threads > 1`), then accepts and services request/response flows until
/// `shutdown` fires.
///
/// `thread_prefix` is used to name OS threads (`{prefix}-bh-metrics`,
/// `{prefix}-server-{i}`).
///
/// # Errors
///
/// Returns an error if binding fails or a worker thread panics.
///
/// # Panics
///
/// Panics if the ready-barrier tokio task is cancelled.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_server(
    params: ServerParams,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
    thread_prefix: &'static str,
) -> Result<(), Error> {
    let shutdown_flag = thread::new_shutdown_flag();
    let num_threads = params.threads;

    let thread_metrics = Arc::new(
        (0..num_threads)
            .map(|_| ThreadMetrics::new())
            .collect::<Vec<_>>(),
    );

    let metrics_handle = {
        let tm = Arc::clone(&thread_metrics);
        let labels = metric_labels.clone();
        let flag = Arc::clone(&shutdown_flag);
        thread::spawn_named(&format!("{thread_prefix}-bh-metrics"), move || {
            metrics::run_metrics_thread(&tm, &labels, &flag);
        })
    };

    // Pre-build thread 0's listener here so the BPF program is attached to the
    // reuseport group before any other thread calls bind(). This removes the
    // need for a cross-thread BPF barrier - if bind fails or panics, it
    // propagates as an error directly from this task.
    let binding_addr = params.data_addr;
    let thread0_listener = if num_threads > 1 {
        Some(create_listener(
            0,
            num_threads,
            binding_addr,
            params.backlog,
        ))
    } else {
        None
    };

    // Each thread sends a ready signal via this channel after binding. If a
    // thread panics before signaling, its sender drops; once all senders are
    // gone, recv() returns None and we detect the failure instead of hanging
    // forever.
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<()>();

    let mut handles = Vec::with_capacity(num_threads as usize);
    let mut thread0_listener = thread0_listener;
    let flows = params.flows;
    for i in 0..num_threads {
        let request_size = params.request_size;
        let response_size = params.response_size;
        let no_delay = params.no_delay;
        let backlog = params.backlog;
        let flag = Arc::clone(&shutdown_flag);
        let tm = Arc::clone(&thread_metrics);
        let prebuilt = if i == 0 {
            thread0_listener.take()
        } else {
            None
        };
        let tx = ready_tx.clone();
        let handle = thread::spawn_named(&format!("{thread_prefix}-server-{i}"), move || {
            server_thread_main(
                i,
                num_threads,
                binding_addr,
                prebuilt,
                backlog,
                flows,
                request_size,
                response_size,
                no_delay,
                &flag,
                &tm[i as usize],
                tx,
                params.mode,
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

    // All data listeners are up. Open control port so the generator can
    // connect and know we're ready.
    let control_addr = params.control_addr;
    let control_listener = net::TcpListener::bind(control_addr).map_err(|source| Error::Bind {
        addr: control_addr,
        source: Box::new(source),
    })?;
    control_listener
        .set_nonblocking(true)
        .expect("failed to set control listener nonblocking");
    info!("control port listening on {control_addr}, waiting for generator");

    handles.push(metrics_handle);

    let flag = Arc::clone(&shutdown_flag);
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_clone.recv().await;
        flag.store(true, Relaxed);
    });
    let mut generator_connected = false;
    let flows_bytes = params.flows.to_be_bytes();
    loop {
        if shutdown_flag.load(Relaxed) {
            info!("shutdown before generator connected");
            break;
        }
        match control_listener.accept() {
            Ok((mut conn, peer)) => {
                // accept(2) on Linux returns a blocking socket regardless of
                // the listener's O_NONBLOCK; a small write_timeout guards
                // against a generator that connects but never reads.
                conn.set_write_timeout(Some(HANDSHAKE_TIMEOUT))
                    .expect("set_write_timeout on accepted TcpStream must succeed");
                conn.write_all(&flows_bytes)?;
                info!(
                    "generator connected from {peer}, sent flows={}, data threads running",
                    params.flows
                );
                generator_connected = true;
                break;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(100)).await;
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
        shutdown.recv().await;
        info!("shutdown signal received");
    }
    shutdown_flag.store(true, Relaxed);

    thread::join_all(handles).map_err(|()| Error::ThreadPanicked)?;

    Ok(())
}

/// Create a listener socket. When `num_threads` > 1, sets `SO_REUSEPORT`
/// and (for thread 0) attaches the reuseport eBPF program.
fn create_listener(
    thread_index: u16,
    num_threads: u16,
    binding_addr: SocketAddr,
    backlog: i32,
) -> net::TcpListener {
    let domain = if binding_addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
        .expect("failed to create socket");
    socket
        .set_nonblocking(true)
        .expect("failed to set nonblocking");
    socket
        .set_cloexec(true)
        .expect("failed to set close-on-exec");
    socket
        .set_reuse_address(true)
        .expect("failed to set SO_REUSEADDR");

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
    prebuilt_listener: Option<net::TcpListener>,
    backlog: i32,
    num_flows: u16,
    request_size: usize,
    response_size: usize,
    no_delay: bool,
    shutdown_flag: &AtomicBool,
    metrics: &ThreadMetrics,
    ready_tx: mpsc::UnboundedSender<()>,
    mode: Mode,
) {
    // Thread 0 uses the pre-built listener (with BPF already attached); others
    // bind their own sockets that join the existing reuseport group.
    let std_listener = prebuilt_listener
        .unwrap_or_else(|| create_listener(thread_index, num_threads, binding_addr, backlog));

    // Signal that this thread's listener is bound and ready. If this send
    // fails the receiver has gone away (blackhole is shutting down).
    let _ = ready_tx.send(());
    drop(ready_tx);

    let mut listener = TcpListener::from_std(std_listener);
    let mut poll = Poll::new().expect("failed to create mio::Poll");
    // Worst case under SO_REUSEPORT: every flow lands on this thread, so size
    // for the total flow count plus the listener token.
    let mut events = Events::with_capacity(num_flows as usize + 1);

    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)
        .expect("failed to register listener");

    let mut request_buf = vec![0u8; request_size];
    let response_buf = vec![0u8; response_size];
    let mut flows: FlowMap<ServerState> = FlowMap::new(num_flows as usize);
    let mut next_token: usize = 1;

    loop {
        let _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        if shutdown_flag.load(Relaxed) {
            break;
        }

        let mut attempts = 0;
        for event in &events {
            if event.token() == LISTENER_TOKEN {
                loop {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            set_nodelay_mio(&stream, no_delay);
                            let token = Token(next_token);
                            next_token += 1;
                            // Insert first: `insert` takes the flow by value and
                            // drops it (closing the fd) on failure, so there is
                            // nothing registered to clean up on the error path.
                            if let Err(err) = flows.insert(Flow {
                                stream,
                                token,
                                state: ServerState::RecvRequest,
                                xfer: request_size,
                            }) {
                                warn!("failed to insert flow in server FlowMap: {err}");
                                break;
                            }
                            let flow = flows.get_mut(token).expect("flow was just inserted");
                            poll.registry()
                                .register(&mut flow.stream, token, Interest::READABLE)
                                .expect("failed to register flow");
                            metrics.connections_accepted.add(1);
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => {
                            if attempts > 2 {
                                break;
                            }
                            warn!("accept error: {e}");
                            attempts += 1;
                            std::thread::sleep(Duration::from_millis(1000));
                        }
                    }
                }
            } else {
                let token = event.token();
                let Some(fl) = flows.get_mut(token) else {
                    continue;
                };
                let action =
                    handle_server_event(fl, &mut request_buf, &response_buf, metrics, mode);
                flow::apply_action(action, token, &mut flows, poll.registry());
            }
        }
    }
}

/// Set `TCP_NODELAY` on a mio [`TcpStream`] via a borrowed `socket2::SockRef`.
fn set_nodelay_mio(stream: &TcpStream, no_delay: bool) {
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_tcp_nodelay(no_delay) {
        warn!("failed to set TCP_NODELAY: {e}");
    }
}

fn handle_server_event(
    flow: &mut Flow<ServerState>,
    request_buf: &mut [u8],
    response_buf: &[u8],
    metrics: &ThreadMetrics,
    mode: Mode,
) -> Action {
    match flow.state {
        ServerState::RecvRequest => {
            let offset = request_buf.len() - flow.xfer;
            match flow.stream.read(&mut request_buf[offset..]) {
                Ok(0) => {
                    metrics.connections_closed.add(1);
                    Action::Remove
                }
                Ok(n) => {
                    flow.xfer -= n;
                    if flow.xfer == 0 {
                        flow.xfer = response_buf.len();
                        flow.state = ServerState::SendResponse;
                        metrics.requests_received.add(1);
                        metrics.bytes_received.add(request_buf.len() as u64);
                        Action::Reregister(Interest::WRITABLE)
                    } else {
                        Action::Continue
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => Action::Continue,
                Err(e) => {
                    warn!("read error: {e}");
                    metrics.connections_closed.add(1);
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
                        match mode {
                            Mode::Rr => flow.state = ServerState::RecvRequest,
                            Mode::Crr => flow.state = ServerState::CloseStream,
                        }
                        metrics.responses_sent.add(1);
                        metrics.bytes_written.add(response_buf.len() as u64);
                        Action::Reregister(Interest::READABLE)
                    } else {
                        Action::Continue
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => Action::Continue,
                Err(e) => {
                    warn!("write error: {e}");
                    metrics.connections_closed.add(1);
                    Action::Remove
                }
            }
        }
        ServerState::CloseStream => Action::Remove,
    }
}
