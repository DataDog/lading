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
use shared::integration_api::{MetricsReport, TestConfig, integration_target_client::IntegrationTargetClient};
use tokio::{net::UnixStream, process::Command};
use tonic::transport::Endpoint;
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
        let ducks_binary = build_ducks()?;

        // Every ducks-sheepdog pair is connected by a unique socket file
        let ducks_comm_file = self.tempdir.join("ducks_socket");

        let mut ducks_process = Command::new(ducks_binary)
            // switch Stdio to `inherit()` to see ducks logs in your terminal
            .stdout(Stdio::piped())
            .env("RUST_LOG", "ducks=debug,info")
            .arg(ducks_comm_file.to_str().unwrap())
            .spawn()
            .context("launch ducks")?;

        // wait for ducks to bring up its RPC server
        while !std::path::Path::exists(&ducks_comm_file) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let channel = Endpoint::try_from("http://127.0.0.1/this-is-not-used")?
            .connect_with_connector(tower::service_fn(move |_| {
                UnixStream::connect(ducks_comm_file.clone())
            }))
            .await?;
        let mut ducks_rpc = IntegrationTargetClient::new(channel);
        debug!("Connetcted to ducks");

        let test = ducks_rpc.start_test(TestConfig {}).await?.into_inner();
        let port = test.port as u16;

        // template & write lading config
        let lading_config_file = self.tempdir.join("lading.yaml");
        let mut file =
            std::fs::File::create(&lading_config_file).context("create lading config file")?;
        let lading_config = self
            .lading_config_template
            .replace("{{port_number}}", &port.to_string());
        file.write_all(lading_config.as_bytes())
            .context("write lading config")?;

        // Run lading against the ducks process that was started above
        let lading = build_lading()?;
        let captures_file = self.tempdir.join("captures");
        let mut lading = Command::new(lading)
            // switch Stdio to `inherit()` to see lading logs in your terminal
            .stdout(Stdio::piped())
            .env("RUST_LOG", "info")
            .arg("--target-pid")
            .arg(ducks_process.id().unwrap().to_string())
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

        // wait for lading to push some load. It will exit on its own.
        debug!("lading running");
        let _lading_exit_status = lading.wait().await?;

        // get metrics from ducks
        let test_rpc = ducks_rpc.get_test_results(());
        let results = test_rpc.await?.into_inner();

        // ask ducks to shutdown
        debug!("send shutdown command");
        ducks_rpc.shutdown(()).await?;
        drop(ducks_rpc);
        ducks_process.wait().await.unwrap();

        // todo: report captures file / provide some utilities for asserting against it
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
