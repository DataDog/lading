use std::{
    convert::Infallible,
    error,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Response, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use lading_payload::block::CacheMethod;
use proptest::{prelude::*, test_runner::TestCaseError};
use tokio::{net::TcpListener, runtime::Builder, sync::Semaphore, task::JoinHandle, time::timeout};

use super::{Config, General, Http, Method};

type TestError = Box<dyn error::Error + Send + Sync>;

/// Concurrency observations recorded by a test server.
#[derive(Default)]
struct Observed {
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    requests: AtomicUsize,
}

/// Serve every connection concurrently, holding each request open briefly so
/// the generator's connection limit is the only bound on in-flight requests.
async fn serve(listener: TcpListener, observed: Arc<Observed>) -> Result<(), TestError> {
    loop {
        let (socket, _) = listener.accept().await?;
        let observed = Arc::clone(&observed);
        tokio::spawn(async move {
            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let observed = Arc::clone(&observed);
                async move {
                    let now = observed.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    observed.max_in_flight.fetch_max(now, Ordering::SeqCst);
                    let _ = request.into_body().collect().await;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    observed.in_flight.fetch_sub(1, Ordering::SeqCst);
                    observed.requests.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
                }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
    }
}

fn config(addr: SocketAddr, seed: u8, parallel_connections: u16) -> Result<Config, TestError> {
    Ok(Config {
        seed: [seed; 32],
        target_uri: format!("http://{addr}/").parse()?,
        method: Method::Post {
            variant: lading_payload::Config::Ascii,
            maximum_prebuild_cache_size_bytes: byte_unit::Byte::from_u64(1_048_576),
            block_cache_method: CacheMethod::Fixed,
        },
        headers: hyper::HeaderMap::new(),
        bytes_per_second: Some(byte_unit::Byte::from_u64(104_857_600)),
        maximum_block_size: byte_unit::Byte::from_u64(1_024),
        parallel_connections,
        throttle: None,
    })
}

/// Run `f` to completion on a fresh multi-threaded runtime, converting its
/// error into a proptest failure.
fn run<F>(f: F) -> Result<(), TestCaseError>
where
    F: Future<Output = Result<(), TestError>>,
{
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|err| TestCaseError::fail(err.to_string()))?;
    runtime
        .block_on(f)
        .map_err(|err| TestCaseError::fail(err.to_string()))
}

/// Poll `done` every few milliseconds until it holds, failing after ten
/// seconds.
async fn wait_for(mut done: impl FnMut() -> bool) -> Result<(), TestError> {
    timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?;
    Ok(())
}

/// Spin two generators with limits `limits` against their own targets until
/// each target has seen its generator's full limit in flight, then check that
/// no target ever saw more.
async fn independent_limits(limits: [u16; 2], seed: u8) -> Result<(), TestError> {
    let (shutdown_watcher, shutdown_broadcaster) = lading_signal::signal();

    let mut servers = Vec::new();
    let mut generators = Vec::new();
    for (index, limit) in limits.iter().enumerate() {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let observed = Arc::new(Observed::default());
        servers.push((
            tokio::spawn(serve(listener, Arc::clone(&observed))),
            observed,
        ));
        let seed = seed.wrapping_add(u8::try_from(index)?);
        generators.push(Http::new(
            General {
                id: Some(format!("http-{index}")),
            },
            config(addr, seed, *limit)?,
            shutdown_watcher.clone(),
        )?);
    }

    let handles: Vec<_> = generators
        .into_iter()
        .map(|generator| tokio::spawn(generator.spin()))
        .collect();

    wait_for(|| {
        servers.iter().zip(limits).all(|((_, observed), limit)| {
            observed.max_in_flight.load(Ordering::SeqCst) >= usize::from(limit)
        })
    })
    .await?;
    // Keep load on a little longer so an over-limit generator would show it.
    tokio::time::sleep(Duration::from_millis(40)).await;
    shutdown_broadcaster.signal();

    for handle in handles {
        timeout(Duration::from_secs(10), handle).await???;
    }

    for ((server, observed), limit) in servers.iter().zip(limits) {
        server.abort();
        let max = observed.max_in_flight.load(Ordering::SeqCst);
        if max != usize::from(limit) {
            return Err(format!("target saw {max} in flight, limit {limit}").into());
        }
    }
    Ok(())
}

proptest! {
    /// For all limits L1, L2 in [1, 4] with L1 != L2 and all seeds s: two HTTP
    /// generators g1, g2 built in one process with limits L1, L2 both construct
    /// successfully, and max in-flight requests observed at gi's target equals
    /// Li.
    ///
    /// A process-global semaphore would either panic on the second
    /// construction or make both generators share one limit.
    #[test]
    fn two_generators_enforce_independent_limits(
        first in 1..=4_u16,
        offset in 1..=3_u16,
        seed: u8,
    ) {
        let second = (first - 1 + offset) % 4 + 1;
        run(independent_limits([first, second], seed))?;
    }
}

/// Server state for the shutdown test: the first request is held open until
/// `gate` is released, later requests are answered immediately.
struct Gated {
    requests: AtomicUsize,
    held: AtomicUsize,
    gate: Semaphore,
}

async fn serve_gated(listener: TcpListener, gated: Arc<Gated>) -> Result<(), TestError> {
    loop {
        let (socket, _) = listener.accept().await?;
        let gated = Arc::clone(&gated);
        tokio::spawn(async move {
            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let gated = Arc::clone(&gated);
                async move {
                    let _ = request.into_body().collect().await;
                    if gated.requests.fetch_add(1, Ordering::SeqCst) == 0 {
                        gated.held.store(1, Ordering::SeqCst);
                        if let Ok(permit) = gated.gate.acquire().await {
                            permit.forget();
                        }
                        gated.held.store(0, Ordering::SeqCst);
                    }
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
                }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
    }
}

/// Run the shutdown scenario with per-generator limits `limits`, each at least
/// 2 so a generator with one held request still has spare permits.
async fn shutdown_independent(limits: [u16; 2], seed: u8) -> Result<(), TestError> {
    let mut targets = Vec::new();
    let mut broadcasters = Vec::new();
    let mut handles: Vec<JoinHandle<Result<(), super::Error>>> = Vec::new();
    let mut servers = Vec::new();
    for (index, limit) in limits.iter().enumerate() {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let gated = Arc::new(Gated {
            requests: AtomicUsize::new(0),
            held: AtomicUsize::new(0),
            gate: Semaphore::new(0),
        });
        servers.push(tokio::spawn(serve_gated(listener, Arc::clone(&gated))));
        let (watcher, broadcaster) = lading_signal::signal();
        let generator = Http::new(
            General {
                id: Some(format!("http-{index}")),
            },
            config(addr, seed.wrapping_add(u8::try_from(index)?), *limit)?,
            watcher,
        )?;
        handles.push(tokio::spawn(generator.spin()));
        targets.push(gated);
        broadcasters.push(broadcaster);
    }
    for gated in &targets {
        wait_for(|| gated.held.load(Ordering::SeqCst) == 1).await?;
    }

    let mut broadcasters = broadcasters.into_iter();
    let first_shutdown = broadcasters.next().ok_or("missing broadcaster")?;
    let second_shutdown = broadcasters.next().ok_or("missing broadcaster")?;

    first_shutdown.signal();
    tokio::time::sleep(Duration::from_millis(50)).await;
    if handles[0].is_finished() {
        return Err("first generator exited with its own request in flight".into());
    }
    if targets[0].held.load(Ordering::SeqCst) != 1 {
        return Err("first target released its request early".into());
    }

    targets[0].gate.add_permits(1);
    let first = handles.remove(0);
    timeout(Duration::from_secs(10), first).await???;

    if handles[0].is_finished() {
        return Err("second generator exited without its own shutdown signal".into());
    }
    if targets[1].held.load(Ordering::SeqCst) != 1 {
        return Err("second target released its request early".into());
    }
    let sent_before = targets[1].requests.load(Ordering::SeqCst);
    wait_for(|| targets[1].requests.load(Ordering::SeqCst) > sent_before).await?;

    second_shutdown.signal();
    targets[1].gate.add_permits(1);
    let second = handles.remove(0);
    timeout(Duration::from_secs(10), second).await???;

    for server in servers {
        server.abort();
    }
    Ok(())
}

proptest! {
    /// For all limits L1, L2 in [2, 4] and all seeds s, given generators g1, g2
    /// with independent shutdown signals whose targets each hold the first
    /// request open: after g1 is signalled, g1 does not exit while its own
    /// request is outstanding, g1 exits once that request completes, and g2
    /// keeps running and sending with its own request still held.
    ///
    /// Li >= 2 leaves each generator spare permits, so it idles in its main
    /// loop rather than blocking on a permit. Exiting before the held response
    /// would mean shutdown no longer waits on in-flight requests.
    #[test]
    fn shutdown_waits_only_on_own_requests(
        first in 2..=4_u16,
        second in 2..=4_u16,
        seed: u8,
    ) {
        run(shutdown_independent([first, second], seed))?;
    }
}
