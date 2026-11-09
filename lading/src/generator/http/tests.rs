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
use tokio::{net::TcpListener, time::timeout};

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
