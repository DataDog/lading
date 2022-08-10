//! Sheepdog is an integration testing harness for lading.
//!
//! Sheepdog performs correctness testing on lading by running it against ducks.
//! Ducks exhibits testable behavior and exposes measurements for sheepdog to
//! assert against.
//!
//! Currently, sheepdog performs these tasks:
//! - Via ducks, verify that lading produces data
//!
//! Upcoming goals:
//! - Via ducks, verify that lading sinks data
//! - Verify that lading capture outputs are parseable
//! - Attempt to validate lading's data rate consistency
//! - Verify that lading generates meaningful data (is entropy a good enough signal?)

use std::{io::Write, path::PathBuf, process::Stdio, time::Duration};

use anyhow::Context;
use assert_fs::TempDir;
use shared::integration_api::{
    integration_server::{Integration, IntegrationServer},
    Empty, ListenInfo, MetricsReport,
};
use tokio::{
    net::UnixListener,
    process::Command,
    sync::{mpsc, oneshot, Mutex},
};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Response, Status};
use tracing::{debug, info};

/// Run a cargo build of ducks and return the path of the output binary
pub fn build_ducks() -> Result<PathBuf, anyhow::Error> {
    let bin = escargot::CargoBuild::new()
        .bin("ducks")
        .package("ducks")
        .current_release()
        .current_target()
        .manifest_path("Cargo.toml")
        .run()?
        .path()
        .to_owned();
    Ok(bin)
}

/// Run a cargo build of lading and return the path of the output binary
// tbh not sure if we should be building lading or accepting a binary
pub fn build_lading() -> Result<PathBuf, anyhow::Error> {
    let bin = escargot::CargoBuild::new()
        .bin("lading")
        .package("lading")
        .current_release()
        .current_target()
        .manifest_path("Cargo.toml")
        .run()?
        .path()
        .to_owned();
    Ok(bin)
}

/// Thin shim between tonic RPCs and the async test task
pub struct IntegrationServerImpl {
    listen_tx: Mutex<Option<oneshot::Sender<u16>>>,
    shutdown_rx: Mutex<Option<oneshot::Receiver<()>>>,
    results_tx: mpsc::Sender<MetricsReport>,
}

#[tonic::async_trait]
impl Integration for IntegrationServerImpl {
    #[tracing::instrument(skip(self))]
    async fn listening(
        &self,
        req: tonic::Request<ListenInfo>,
    ) -> Result<tonic::Response<shared::integration_api::Empty>, Status> {
        let sender = self.listen_tx.lock().await.take().ok_or_else(|| {
            Status::internal(
                "`Listening` has already been called. It may only be called a single time.",
            )
        })?;
        debug!(
            "Integration target is listening on port {}",
            req.get_ref().port
        );
        sender.send(req.get_ref().port as u16).unwrap();
        Ok(Response::new(Empty {}))
    }

    #[tracing::instrument(skip(self))]
    async fn await_shutdown(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<shared::integration_api::Empty>, Status> {
        let rx = self.shutdown_rx.lock().await.take().ok_or_else(|| {
            Status::internal(
                "`AwaitShutdown` has already been called. It may only be called a single time.",
            )
        })?;
        rx.await.unwrap();
        Ok(Response::new(Empty {}))
    }

    #[tracing::instrument(skip(self))]
    async fn report_metrics(
        &self,
        req: tonic::Request<MetricsReport>,
    ) -> Result<tonic::Response<shared::integration_api::Empty>, Status> {
        self.results_tx
            .send(req.into_inner())
            .await
            .map_err(|_| Status::internal("unable to accept metrics report"))?;
        Ok(Response::new(Empty {}))
    }
}

pub struct IntegrationTest {
    lading_config_template: String,
    tempdir: TempDir,
    experiment_duration: Duration,
    experiment_warmup: Duration,
}

impl IntegrationTest {
    pub fn new<S: ToString>(lading_config: S) -> Result<Self, anyhow::Error> {
        let _ = tracing_subscriber::fmt::try_init();
        let tempdir = TempDir::new().context("create tempdir")?;

        Ok(Self {
            lading_config_template: lading_config.to_string(),
            tempdir,
            experiment_duration: Duration::from_secs(1),
            experiment_warmup: Duration::from_secs(0),
        })
    }

    pub async fn run(self) -> Result<MetricsReport, anyhow::Error> {
        // Build and launch ducks. Cargo's locking is sufficient for this to
        // work correctly when called in parallel. It would be more efficient to
        // only run this a single time though.
        let ducks = build_ducks()?;

        // Every ducks-sheepdog pair is connected by a unique socket file
        let ducks_comm_file = self.tempdir.join("ducks_socket");
        let ducks_comm = UnixListener::bind(&ducks_comm_file).context("bind to ducks_socket")?;
        let ducks_comm = UnixListenerStream::new(ducks_comm);

        let (port_tx, port_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (results_tx, mut results_rx) = mpsc::channel(1);

        let server = IntegrationServerImpl {
            listen_tx: Mutex::new(Some(port_tx)),
            shutdown_rx: Mutex::new(Some(shutdown_rx)),
            results_tx,
        };

        let _server_task = tokio::task::spawn(async move {
            let result = Server::builder()
                .add_service(IntegrationServer::new(server))
                .serve_with_incoming(ducks_comm)
                .await;
            if let Err(e) = result {
                panic!("Server error: {}", e);
            }
        });

        let mut ducks = Command::new(ducks)
            // switch Stdio to `inherit()` to see ducks logs in your terminal
            .stdout(Stdio::piped())
            .env("RUST_LOG", "ducks=debug,info")
            .arg(ducks_comm_file.to_str().unwrap())
            .spawn()
            .context("launch ducks")?;
        let port = port_rx.await?;

        let lading_config_file = self.tempdir.join("lading.yaml");
        let mut file =
            std::fs::File::create(&lading_config_file).context("create lading config file")?;
        let lading_config = self
            .lading_config_template
            .replace("{{port_number}}", &port.to_string());
        file.write_all(lading_config.as_bytes())
            .context("write lading config")?;

        let lading = build_lading()?;

        // Run lading against the ducks process that was started above
        let captures_file = self.tempdir.join("captures");
        let mut lading = Command::new(lading)
            // switch Stdio to `inherit()` to see lading logs in your terminal
            .stdout(Stdio::piped())
            .env("RUST_LOG", "info")
            .arg("--target-pid")
            .arg(ducks.id().unwrap().to_string())
            .arg("--config-path")
            .arg(lading_config_file.to_str().unwrap())
            .arg("--experiment-duration-seconds")
            .arg(self.experiment_duration.as_secs().to_string())
            .arg("--warmup-duration-seconds")
            .arg(self.experiment_warmup.as_secs().to_string())
            .arg("--capture-path")
            .arg(captures_file.to_str().unwrap())
            .spawn()
            .unwrap();

        // Wait for lading to push some load. It will exit on its own.
        debug!("lading running");
        let _status = lading.wait().await?;

        // Ask ducks to shutdown
        debug!("send shutdown command");
        shutdown_tx
            .send(())
            .expect("failed to send shutdown command");

        // Ducks will report its metrics and then exit
        // todo: kill process after timeout
        debug!("lading done");
        ducks.wait().await.unwrap();

        // todo: report captures file / provide some utilities for asserting against it
        let results = results_rx.recv().await.unwrap();
        info!("Test result: {:?}", results);
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn http_apache_common() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            r#"
generator:
  http:
    seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
      59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
    headers: {}
    target_uri: "http://localhost:{{port_number}}/"
    bytes_per_second: "100 Mb"
    parallel_connections: 5
    method:
      post:
        maximum_prebuild_cache_size_bytes: "64 Mb"
        variant: "apache_common"
        "#,
        )?;

        let reqs = test.run().await?;

        assert!(reqs.request_count > 10);
        Ok(())
    }

    #[tokio::test]
    async fn http_ascii() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            r#"
generator:
  http:
    seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
      59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
    headers: {}
    target_uri: "http://localhost:{{port_number}}/"
    bytes_per_second: "100 Mb"
    parallel_connections: 5
    method:
      post:
        maximum_prebuild_cache_size_bytes: "64 Mb"
        variant: "ascii"
        "#,
        )?;

        let reqs = test.run().await?;

        assert!(reqs.request_count > 10);
        Ok(())
    }
}
