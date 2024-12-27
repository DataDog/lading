use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use hyper::service::Service;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use lading_signal::Watcher;
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, pin, sync::Semaphore, task::JoinSet};
use tracing::{debug, error, info};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error("IO error: {0}")]
    Io(std::io::Error),
}

pub(crate) async fn run_httpd<SF, S>(
    addr: SocketAddr,
    concurrency_limit: usize,
    shutdown: Watcher,
    make_service: SF,
) -> Result<(), Error>
where
    // "service factory"
    SF: Send + Sync + 'static + Clone + Fn() -> S,
    // The bounds on `S` per
    // https://docs.rs/hyper/latest/hyper/service/trait.Service.html and then
    // made concrete per
    // https://docs.rs/hyper-util/latest/hyper_util/server/conn/auto/struct.Builder.html#method.serve_connection.
    S: Service<
            hyper::Request<hyper::body::Incoming>,
            Response = hyper::Response<BoxBody<Bytes, hyper::Error>>,
            Error = hyper::Error,
        > + Send
        + 'static,

    S::Future: Send + 'static,
{
    let listener = TcpListener::bind(addr).await.map_err(Error::Io)?;
    let sem = Arc::new(Semaphore::new(concurrency_limit));
    let mut join_set = JoinSet::new();

    let shutdown_fut = shutdown.recv();
    pin!(shutdown_fut);
    loop {
        tokio::select! {
            () = &mut shutdown_fut => {
                info!("Shutdown signal received, stopping accept loop.");
                break;
            }

            incoming = listener.accept() => {
                let (stream, addr) = match incoming {
                    Ok(sa) => sa,
                    Err(e) => {
                        error!("Error accepting connection: {e}");
                        continue;
                    }
                };

                let sem = Arc::clone(&sem);
                let service_factory = make_service.clone();

                join_set.spawn(async move {
                    debug!("Accepted connection from {addr}");
                    let permit = match sem.acquire_owned().await {
                        Ok(p) => p,
                        Err(e) => {
                            error!("Semaphore closed: {e}");
                            return;
                        }
                    };

                    let builder = auto::Builder::new(TokioExecutor::new());
                    let serve_future = builder.serve_connection_with_upgrades(
                        TokioIo::new(stream),
                        service_factory(),
                    );

                    if let Err(e) = serve_future.await {
                        error!("Error serving {addr}: {e}");
                    }
                    drop(permit);
                });
            }
        }
    }

    drop(listener);
    while join_set.join_next().await.is_some() {}
    Ok(())
}
