//! Ducks is an integration testing target for lading.
//!
//! Ducks exists to enable correctness testing on lading. Any high-level
//! validation tasks are in-scope, but preference should be given to
//! unit-testing within lading over testing with ducks where possible.
//!
//! Currently, ducks can perform these tasks:
//! - Receive data
//! - Emit metrics with relevant counters
//!
//! Upcoming goals:
//! - Validate some forms of received data
//! - Send data

use hyper::{
    body,
    server::conn::{AddrIncoming, AddrStream},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use once_cell::sync::OnceCell;
use shared::{DucksConfig, GenericHttpMetrics};
use sketches_ddsketch::DDSketch;
use std::{
    io::{Read, Write},
    net::SocketAddr,
    os::unix::net::UnixStream,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::runtime::Builder;
use tower::ServiceBuilder;
use tracing::{debug, info};

#[derive(Debug)]
enum Error {
    Hyper(hyper::Error),
}

static GENERIC_HTTP_COUNTERS: OnceCell<Arc<Mutex<GenericHttpCounters>>> = OnceCell::new();

/// Corresponds to MeasurementConfig::GenericHttp
struct GenericHttpCounters {
    body_size: DDSketch,
    entropy: DDSketch,
    request_count: u64,
    total_bytes: u64,
    // record http methods received
}

impl Default for GenericHttpCounters {
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
        }
    }
}

#[tracing::instrument]
async fn http_req_handler(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let (_parts, body) = req.into_parts();
    let body = body::to_bytes(body).await?;

    {
        let metric = GENERIC_HTTP_COUNTERS.get().unwrap().clone();
        let mut m = metric.lock().unwrap();
        m.request_count += 1;

        m.total_bytes = body.len() as u64;
        m.entropy.add(entropy::metric_entropy(&body) as f64);

        m.body_size.add(body.len() as f64);
    }

    let mut okay = Response::default();
    *okay.status_mut() = StatusCode::OK;

    let body_bytes = vec![];
    *okay.body_mut() = Body::from(body_bytes);
    Ok(okay)
}

#[tracing::instrument]
async fn http_server(addr: AddrIncoming) -> Result<(), Error> {
    GENERIC_HTTP_COUNTERS.get_or_init(|| Arc::new(Mutex::new(GenericHttpCounters::default())));

    let service = make_service_fn(|_: &AddrStream| async move {
        Ok::<_, hyper::Error>(service_fn(move |request: Request<Body>| {
            debug!("REQUEST: {:?}", request);

            http_req_handler(request)
        }))
    });
    let svc = ServiceBuilder::new()
        .load_shed()
        .concurrency_limit(1_000)
        .timeout(Duration::from_secs(1))
        .service(service);

    let server = Server::builder(addr).serve(svc);
    server.await.map_err(Error::Hyper)
}

fn http_listen(mut sock: UnixStream, config: DucksConfig) -> Result<(), anyhow::Error> {
    let runtime = Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    // bind to a random open TCP port
    let addr = runtime.block_on(async {
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let addr = AddrIncoming::bind(&bind_addr).map(|addr| {
            // addr.set_keepalive(Some(Duration::from_secs(60)));
            addr
        })?;
        Result::<_, hyper::Error>::Ok(addr)
    })?;
    let port = addr.local_addr().port();

    // let the harness know what port this duck is listening on
    sock.write_all(serde_json::to_string(&shared::DucksMessage::OpenPort(port))?.as_bytes())?;

    // start serving requests
    runtime.spawn(http_server(addr));

    // runtime shuts down on drop. Spin until we receive a shutdown message.
    // TODO: actually deserialize the shutdown message.
    runtime.block_on(async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;

            let mut buf = [0; 16];
            let read = sock.read(&mut buf);
            if read.is_ok() {
                info!("Message received");
                break;
            }
        }
    });
    drop(runtime);

    info!("Ducks is finishing up");

    // Report interesting metrics / test results

    let metric = GENERIC_HTTP_COUNTERS.get().unwrap();
    let m = {
        let metric = metric.clone();
        let m = metric.lock().unwrap();

        GenericHttpMetrics {
            request_count: m.request_count,
            total_bytes: m.total_bytes,
            median_entropy: m.entropy.quantile(0.5).unwrap().unwrap(),
            median_size: m.body_size.quantile(0.5).unwrap().unwrap(),
        }
    };

    sock.write_all(
        serde_json::to_string(&shared::DucksMessage::MetricsReport(m))
            .unwrap()
            .as_bytes(),
    )
    .unwrap();

    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    debug!("Hello from ducks");

    let comms = std::env::args().nth(1).unwrap();
    let sock = UnixStream::connect(comms).unwrap();
    sock.set_nonblocking(true)?;
    debug!("Ducks is connected");

    // todo Wait for a DucksConfig

    let config = DucksConfig {
        listen: shared::ListenConfig::Http,
        measurements: shared::MeasurementConfig::GenericHttp,
        emit: shared::EmitConfig::None,
        assertions: shared::AssertionConfig::None,
    };

    match config.listen {
        shared::ListenConfig::Http => http_listen(sock, config)?,
    }

    Ok(())
}
