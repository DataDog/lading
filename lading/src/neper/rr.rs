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
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

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
}

enum ClientState {
    SendRequest,
    RecvResponse,
}

enum ServerState {
    RecvRequest,
    SendResponse,
}

const LISTENER_TOKEN: Token = Token(0);

/// Control-channel handshake: server writes `flows` to the accepted control
/// connection as a 2-byte big-endian `u16` and closes; client reads the same
/// 2 bytes after connecting. Internal protocol - no magic / version byte.
const HANDSHAKE_LEN: usize = 2;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the generator waits for the blackhole's control port to appear.
const CONTROL_CONNECT_TIMEOUT: Duration = Duration::from_secs(300);

/// Handle to a worker or metrics OS thread. Threads report fatal errors by
/// returning them; [`join_workers`] surfaces the first one.
type WorkerHandle = JoinHandle<Result<(), Error>>;

/// Join every handle, then report the first failure.
///
/// All handles are joined before returning, so no thread is left detached
/// even when an earlier one failed. A panic takes precedence over a returned
/// error because it means the thread's state is unknown.
fn join_workers(handles: Vec<WorkerHandle>) -> Result<(), Error> {
    let results = thread::join_all(handles).map_err(|()| Error::ThreadPanicked)?;
    for result in results {
        result?;
    }
    Ok(())
}

/// Signal shutdown, join every handle, and return `err`.
///
/// Used on error paths so a failure in the async task never leaves worker
/// threads detached. `err` is the root cause and is what gets returned; any
/// error surfaced while unwinding is logged instead, since it is almost
/// always a consequence of the first.
fn shutdown_and_join(err: Error, shutdown_flag: &AtomicBool, handles: Vec<WorkerHandle>) -> Error {
    shutdown_flag.store(true, Relaxed);
    if let Err(join_err) = join_workers(handles) {
        warn!("worker error while shutting down after \"{err}\": {join_err}");
    }
    err
}

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
/// is never reachable, or a worker thread reports a fatal error or panics.
pub(crate) async fn run_client(
    params: ClientParams,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
    thread_prefix: &'static str,
) -> Result<(), Error> {
    let shutdown_flag = thread::new_shutdown_flag();
    {
        let flag = Arc::clone(&shutdown_flag);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown.recv().await;
            flag.store(true, Relaxed);
        });
    }

    let flows = wait_for_blackhole(params.control_addr, &shutdown_flag)?;

    if params.threads > flows {
        return Err(Error::Config(format!(
            "threads ({}) must be <= flows received from blackhole ({flows})",
            params.threads
        )));
    }

    let flow_dist = thread::distribute_flows(flows, params.threads);

    let thread_metrics: Arc<[ThreadMetrics]> =
        (0..params.threads).map(|_| ThreadMetrics::new()).collect();

    // The metrics handle goes in first so every later error path joins it
    // along with the workers.
    let mut worker_handles: Vec<WorkerHandle> = Vec::with_capacity(params.threads as usize + 1);
    worker_handles.push({
        let tm = Arc::clone(&thread_metrics);
        let labels = metric_labels.clone();
        let flag = Arc::clone(&shutdown_flag);
        thread::spawn_named(&format!("{thread_prefix}-metrics"), move || {
            metrics::run_metrics_thread(&tm, &labels, &flag);
            Ok(())
        })?
    });

    let data_addr = params.data_addr;
    let request_size = params.request_size;
    let response_size = params.response_size;
    let no_delay = params.no_delay;
    for i in 0..params.threads {
        let thread_flows = flow_dist[i as usize];
        let flag = Arc::clone(&shutdown_flag);
        let tm = Arc::clone(&thread_metrics);
        let spawned = thread::spawn_named(&format!("{thread_prefix}-client-{i}"), move || {
            let result = client_thread_main(
                data_addr,
                thread_flows,
                request_size,
                response_size,
                no_delay,
                &flag,
                &tm[i as usize],
            );
            if let Err(ref e) = result {
                error!("client thread {i} failed: {e}");
            }
            result
        });
        match spawned {
            Ok(handle) => worker_handles.push(handle),
            Err(e) => {
                return Err(shutdown_and_join(
                    Error::Io(e),
                    &shutdown_flag,
                    worker_handles,
                ));
            }
        }
    }

    shutdown.recv().await;
    info!("shutdown signal received");
    shutdown_flag.store(true, Relaxed);

    join_workers(worker_handles)?;

    Ok(())
}

/// Wait for the blackhole to be ready by connecting to its control port, then
/// read the flow count over that connection.
///
/// Retries the connect until the blackhole appears, giving up after five
/// minutes or as soon as shutdown fires.
///
/// # Errors
///
/// Returns [`Error::Io`] if the control port never becomes reachable, if
/// shutdown fires first, or if the handshake read fails.
fn wait_for_blackhole(control_addr: SocketAddr, shutdown_flag: &AtomicBool) -> Result<u16, Error> {
    info!("waiting for blackhole control port at {control_addr}");
    let deadline = Instant::now() + CONTROL_CONNECT_TIMEOUT;
    loop {
        if shutdown_flag.load(Relaxed) {
            return Err(Error::Io(io::Error::new(
                ErrorKind::ConnectionRefused,
                format!("shutdown before blackhole control port {control_addr} became reachable"),
            )));
        }
        match net::TcpStream::connect(control_addr) {
            Ok(mut conn) => {
                conn.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
                let mut buf = [0u8; HANDSHAKE_LEN];
                conn.read_exact(&mut buf)?;
                let flows = u16::from_be_bytes(buf);
                info!("blackhole ready, {flows} flows to open");
                return Ok(flows);
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(Error::Io(io::Error::new(
                        ErrorKind::TimedOut,
                        format!(
                            "blackhole control port {control_addr} not reachable after {}s: {e}",
                            CONTROL_CONNECT_TIMEOUT.as_secs()
                        ),
                    )));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn client_thread_main(
    addr: SocketAddr,
    num_flows: u16,
    request_size: usize,
    response_size: usize,
    no_delay: bool,
    shutdown_flag: &AtomicBool,
    metrics: &ThreadMetrics,
) -> Result<(), Error> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(num_flows as usize);
    let request_buf = vec![0u8; request_size];
    let mut response_buf = vec![0u8; response_size];
    let mut flows: FlowMap<ClientState> = FlowMap::new();
    let mut next_token: usize = 0;

    for _ in 0..num_flows {
        match net::TcpStream::connect(addr) {
            Ok(std_stream) => {
                let _ = std_stream.set_nodelay(no_delay);
                std_stream.set_nonblocking(true)?;
                let mut stream = TcpStream::from_std(std_stream);
                let token = Token(next_token);
                next_token += 1;
                poll.registry()
                    .register(&mut stream, token, Interest::WRITABLE)?;
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
            flow::apply_action(action, token, &mut flows, poll.registry())?;
        }
    }

    Ok(())
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
                Err(e) if e.kind() == ErrorKind::WouldBlock => Action::Continue,
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
                Err(e) if e.kind() == ErrorKind::WouldBlock => Action::Continue,
                Err(e) => {
                    trace!("read error: {e}");
                    Action::Remove
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
/// Returns an error if binding fails, if a worker thread reports a fatal
/// error, or if a worker thread panics.
pub(crate) async fn run_server(
    params: ServerParams,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
    thread_prefix: &'static str,
) -> Result<(), Error> {
    let shutdown_flag = thread::new_shutdown_flag();
    let num_threads = params.threads;

    let thread_metrics: Arc<[ThreadMetrics]> =
        (0..num_threads).map(|_| ThreadMetrics::new()).collect();

    // Listeners first: until they are up there is nothing to report on, and
    // nothing to unwind if binding fails.
    let mut handles =
        prepare_data_listeners(&params, &shutdown_flag, &thread_metrics, thread_prefix).await?;

    let metrics_spawn = {
        let tm = Arc::clone(&thread_metrics);
        let labels = metric_labels.clone();
        let flag = Arc::clone(&shutdown_flag);
        thread::spawn_named(&format!("{thread_prefix}-bh-metrics"), move || {
            metrics::run_metrics_thread(&tm, &labels, &flag);
            Ok(())
        })
    };
    match metrics_spawn {
        Ok(handle) => handles.push(handle),
        Err(e) => return Err(shutdown_and_join(Error::Io(e), &shutdown_flag, handles)),
    }

    // All data listeners are up. Open control port so the generator can
    // connect and know we're ready. From here on the workers are running, so
    // every error path has to signal and join them before returning.
    let control_addr = params.control_addr;
    let control_listener = match net::TcpListener::bind(control_addr) {
        Ok(listener) => listener,
        Err(source) => {
            let err = Error::Bind {
                addr: control_addr,
                source: Box::new(source),
            };
            return Err(shutdown_and_join(err, &shutdown_flag, handles));
        }
    };
    if let Err(e) = control_listener.set_nonblocking(true) {
        return Err(shutdown_and_join(Error::Io(e), &shutdown_flag, handles));
    }
    info!("control port listening on {control_addr}, waiting for generator");

    let flag = Arc::clone(&shutdown_flag);
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_clone.recv().await;
        flag.store(true, Relaxed);
    });

    let generator_connected = match wait_for_generator(
        &control_listener,
        control_addr,
        params.flows,
        &shutdown_flag,
    )
    .await
    {
        Ok(connected) => connected,
        Err(e) => return Err(shutdown_and_join(e, &shutdown_flag, handles)),
    };
    drop(control_listener);

    if generator_connected {
        shutdown.recv().await;
        info!("shutdown signal received");
    }
    shutdown_flag.store(true, Relaxed);

    join_workers(handles)?;

    Ok(())
}

/// Spawn the data-listener worker threads and wait until every one has bound
/// its listener.
///
/// Thread 0's listener is built here, before any worker starts, so the
/// reuseport eBPF program is attached to the group before any other thread
/// calls `bind()`. That removes the need for a cross-thread BPF barrier - if
/// bind fails or panics it propagates as an error directly from this task.
///
/// # Errors
///
/// Returns [`Error::ThreadPanicked`] if a worker dies before signalling
/// ready. The remaining workers are signalled and joined before returning.
async fn prepare_data_listeners(
    params: &ServerParams,
    shutdown_flag: &thread::ShutdownFlag,
    thread_metrics: &Arc<[ThreadMetrics]>,
    thread_prefix: &'static str,
) -> Result<Vec<WorkerHandle>, Error> {
    let num_threads = params.threads;
    let binding_addr = params.data_addr;
    let flows = params.flows;

    let mut thread0_listener = if num_threads > 1 {
        Some(create_listener(
            0,
            num_threads,
            binding_addr,
            params.backlog,
        )?)
    } else {
        None
    };

    // Each thread sends a ready signal via this channel after binding. If a
    // thread panics before signaling, its sender drops; once all senders are
    // gone, recv() returns None and we detect the failure instead of hanging
    // forever.
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<()>();

    let mut handles = Vec::with_capacity(num_threads as usize);
    for i in 0..num_threads {
        let request_size = params.request_size;
        let response_size = params.response_size;
        let no_delay = params.no_delay;
        let backlog = params.backlog;
        let flag = Arc::clone(shutdown_flag);
        let tm = Arc::clone(thread_metrics);
        let prebuilt = if i == 0 {
            thread0_listener.take()
        } else {
            None
        };
        let tx = ready_tx.clone();
        let spawned = thread::spawn_named(&format!("{thread_prefix}-server-{i}"), move || {
            let result = server_thread_main(
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
            );
            if let Err(ref e) = result {
                error!("server thread {i} failed: {e}");
            }
            result
        });
        match spawned {
            Ok(handle) => handles.push(handle),
            Err(e) => {
                drop(ready_tx);
                return Err(shutdown_and_join(Error::Io(e), shutdown_flag, handles));
            }
        }
    }
    // Drop our own copy so the channel closes when all worker threads exit.
    drop(ready_tx);

    // Wait for each thread to signal ready. A sender that drops without
    // signaling means its thread exited early, so recv() returns None instead
    // of hanging. Joining then recovers why it exited.
    for _ in 0..num_threads {
        if ready_rx.recv().await.is_none() {
            shutdown_flag.store(true, Relaxed);
            join_workers(handles)?;
            return Err(Error::ThreadPanicked);
        }
    }

    Ok(handles)
}

/// Wait for the generator to connect to the control port, then hand it the
/// flow count over that connection.
///
/// Returns `true` once the generator has connected and received the count,
/// `false` if shutdown fired before any generator showed up.
///
/// # Errors
///
/// Returns an error if the handshake write fails or `accept` fails for a
/// reason other than `WouldBlock`.
async fn wait_for_generator(
    control_listener: &net::TcpListener,
    control_addr: SocketAddr,
    flows: u16,
    shutdown_flag: &AtomicBool,
) -> Result<bool, Error> {
    let flows_bytes = flows.to_be_bytes();
    loop {
        if shutdown_flag.load(Relaxed) {
            info!("shutdown before generator connected");
            return Ok(false);
        }
        match control_listener.accept() {
            Ok((mut conn, peer)) => {
                // accept(2) on Linux returns a blocking socket regardless of
                // the listener's O_NONBLOCK; a small write_timeout guards
                // against a generator that connects but never reads.
                conn.set_write_timeout(Some(HANDSHAKE_TIMEOUT))?;
                conn.write_all(&flows_bytes)?;
                info!("generator connected from {peer}, sent flows={flows}, data threads running");
                return Ok(true);
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
}

/// Create a listener socket. When `num_threads` > 1, sets `SO_REUSEPORT`
/// and (for thread 0) attaches the reuseport eBPF program.
///
/// # Errors
///
/// Returns [`Error::Bind`] if `binding_addr` cannot be bound, or
/// [`Error::Io`] if any of the socket options or `listen` fail.
fn create_listener(
    thread_index: u16,
    num_threads: u16,
    binding_addr: SocketAddr,
    backlog: i32,
) -> Result<net::TcpListener, Error> {
    let domain = if binding_addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    socket.set_cloexec(true)?;
    socket.set_reuse_address(true)?;

    if num_threads > 1 {
        socket.set_reuse_port(true)?;

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
        .map_err(|source| Error::Bind {
            addr: binding_addr,
            source: Box::new(source),
        })?;
    socket.listen(backlog)?;

    Ok(socket.into())
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
) -> Result<(), Error> {
    // Thread 0 uses the pre-built listener (with BPF already attached); others
    // bind their own sockets that join the existing reuseport group.
    let std_listener = match prebuilt_listener {
        Some(listener) => listener,
        None => create_listener(thread_index, num_threads, binding_addr, backlog)?,
    };

    let mut listener = TcpListener::from_std(std_listener);
    let mut poll = Poll::new()?;
    // Worst case under SO_REUSEPORT: every flow lands on this thread, so size
    // for the total flow count plus the listener token.
    let mut events = Events::with_capacity(num_flows as usize + 1);

    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

    // Signal that this thread's listener is bound and ready. If this send
    // fails the receiver has gone away (blackhole is shutting down).
    let _ = ready_tx.send(());
    drop(ready_tx);

    let mut request_buf = vec![0u8; request_size];
    let response_buf = vec![0u8; response_size];
    let mut flows: FlowMap<ServerState> = FlowMap::new();
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
                            let mut mio_stream = stream;
                            poll.registry()
                                .register(&mut mio_stream, token, Interest::READABLE)?;
                            flows.insert(Flow {
                                stream: mio_stream,
                                token,
                                state: ServerState::RecvRequest,
                                xfer: request_size,
                            });
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
                let action = handle_server_event(fl, &mut request_buf, &response_buf, metrics);
                flow::apply_action(action, token, &mut flows, poll.registry())?;
            }
        }
    }

    Ok(())
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
                        metrics.requests_received.add(1);
                        metrics.bytes_received.add(request_buf.len() as u64);
                        Action::Reregister(Interest::WRITABLE)
                    } else {
                        Action::Continue
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => Action::Continue,
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
                Err(e) if e.kind() == ErrorKind::WouldBlock => Action::Continue,
                Err(e) => {
                    trace!("write error: {e}");
                    Action::Remove
                }
            }
        }
    }
}
