use std::{
    convert::Infallible,
    error,
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
use tokio::{net::TcpListener, sync::Semaphore, task::JoinHandle, time::timeout};

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

/// For all pairs of HTTP generators g1, g2 in one process with limits L1 != L2:
/// construction succeeds, and the maximum number of concurrent in-flight
/// requests observed at gi's target equals Li.
///
/// A process-global semaphore would either panic on the second construction
/// or make both generators share one limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_generators_enforce_independent_limits() -> Result<(), TestError> {
    let limits: [u16; 2] = [1, 3];
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
        let seed = u8::try_from(index)?;
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

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown_broadcaster.signal();

    for handle in handles {
        timeout(Duration::from_secs(10), handle).await???;
    }

    for ((server, observed), limit) in servers.iter().zip(limits) {
        server.abort();
        assert!(observed.requests.load(Ordering::SeqCst) > 0);
        assert_eq!(
            observed.max_in_flight.load(Ordering::SeqCst),
            usize::from(limit)
        );
    }
    Ok(())
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

async fn wait_until_held(gated: &Gated) -> Result<(), TestError> {
    timeout(Duration::from_secs(10), async {
        while gated.held.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

/// For generators g1, g2 with independent shutdown signals, where each target
/// holds g1's and g2's first request open: after g1 is signalled, g1 does not
/// exit while its own request is outstanding, g1 exits once that request
/// completes, and g2 keeps running with its own request still held.
///
/// Each generator has spare permits (limit 2, one request held), so it is idle
/// in its main loop rather than blocked acquiring a permit. Exiting before the
/// held response would mean shutdown no longer waits on in-flight requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_only_on_own_requests() -> Result<(), TestError> {
    let mut targets = Vec::new();
    let mut broadcasters = Vec::new();
    let mut handles: Vec<JoinHandle<Result<(), super::Error>>> = Vec::new();
    let mut servers = Vec::new();
    for index in 0..2u8 {
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
            config(addr, index, 2)?,
            watcher,
        )?;
        handles.push(tokio::spawn(generator.spin()));
        targets.push(gated);
        broadcasters.push(broadcaster);
    }
    for gated in &targets {
        wait_until_held(gated).await?;
    }

    let mut broadcasters = broadcasters.into_iter();
    let first_shutdown = broadcasters.next().ok_or("missing broadcaster")?;
    let second_shutdown = broadcasters.next().ok_or("missing broadcaster")?;

    first_shutdown.signal();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !handles[0].is_finished(),
        "first generator exited with its own request in flight"
    );
    assert_eq!(targets[0].held.load(Ordering::SeqCst), 1);

    targets[0].gate.add_permits(1);
    let first = handles.remove(0);
    timeout(Duration::from_secs(10), first).await???;

    assert!(
        !handles[0].is_finished(),
        "second generator exited without its own shutdown signal"
    );
    assert_eq!(targets[1].held.load(Ordering::SeqCst), 1);
    let sent_before = targets[1].requests.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        targets[1].requests.load(Ordering::SeqCst) > sent_before,
        "second generator stopped sending requests"
    );

    second_shutdown.signal();
    targets[1].gate.add_permits(1);
    let second = handles.remove(0);
    timeout(Duration::from_secs(10), second).await???;

    for server in servers {
        server.abort();
    }
    Ok(())
}
