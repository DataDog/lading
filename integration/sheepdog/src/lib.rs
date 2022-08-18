//! Sheepdog is an integration testing harness for lading.
//!
//! Sheepdog performs integration and correctness testing on lading by running
//! it against ducks. Ducks exhibits testable behavior and maintains
//! measurements for sheepdog to assert against.
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
use shared::{
    integration_api::{self, integration_target_client::IntegrationTargetClient},
    DucksConfig,
};
use tokio::{net::UnixStream, process::Command};
use tonic::transport::Endpoint;
use tracing::debug;

pub struct Metrics {
    pub http: integration_api::HttpMetrics,
    pub tcp: integration_api::TcpMetrics,
}

impl From<integration_api::Metrics> for Metrics {
    fn from(metrics: integration_api::Metrics) -> Self {
        Self {
            tcp: metrics.tcp.unwrap_or_default(),
            http: metrics.http.unwrap_or_default(),
        }
    }
}

/// Build ducks and return the path of the output binary
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

/// Build lading and return the path of the output binary
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

/// Defines an individual integration test
pub struct IntegrationTest {
    lading_config_template: String,
    experiment_duration: Duration,
    experiment_warmup: Duration,
    ducks_config: DucksConfig,

    tempdir: TempDir,
}

impl IntegrationTest {
    /// Create an integration test for the given lading configuration
    pub fn new<S: ToString>(
        ducks_config: DucksConfig,
        lading_config: S,
    ) -> Result<Self, anyhow::Error> {
        let _ = tracing_subscriber::fmt::try_init();
        let tempdir = TempDir::new().context("create tempdir")?;

        Ok(Self {
            lading_config_template: lading_config.to_string(),
            ducks_config,
            tempdir,
            experiment_duration: Duration::from_secs(1),
            experiment_warmup: Duration::from_secs(0),
        })
    }

    pub async fn run(self) -> Result<Metrics, anyhow::Error> {
        // Build ducks and lading. Cargo's locking is sufficient for this to
        // work correctly when called in parallel. It would be more efficient to
        // only run this a single time though.
        let ducks_binary = build_ducks()?;
        let lading_binary = build_lading()?;

        // Every ducks-sheepdog pair is connected by a unique socket file
        let ducks_comm_file = self.tempdir.join("ducks_socket");

        let ducks_process = Command::new(ducks_binary)
            .stdout(Stdio::piped())
            .env("RUST_LOG", "ducks=trace,info")
            .arg(ducks_comm_file.to_str().unwrap())
            .spawn()
            .context("launch ducks")?;

        // wait for ducks to bring up its RPC server and then connect
        while !std::path::Path::exists(&ducks_comm_file) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = Endpoint::try_from("http://127.0.0.1/this-is-not-used")?
            .connect_with_connector(tower::service_fn(move |_| {
                UnixStream::connect(ducks_comm_file.clone())
            }))
            .await?;
        let mut ducks_rpc = IntegrationTargetClient::new(channel);
        debug!("connected to ducks");

        // instruct ducks to start the test (this is currently hardcoded to a
        // http sink test but will be configurable in the future)
        let test = ducks_rpc.start_test(self.ducks_config).await?.into_inner();
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

        // run lading against the ducks process that was started above
        let captures_file = self.tempdir.join("captures");
        let lading = Command::new(lading_binary)
            .stdout(Stdio::piped())
            .env("RUST_LOG", "trace")
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
        debug!("lading is running");
        let lading_output = lading.wait_with_output().await?;

        // get test results from ducks
        let metrics = ducks_rpc.get_metrics(());
        let metrics = metrics.await?.into_inner();

        let metrics = Metrics::from(metrics);

        // ask ducks to shutdown and wait for its process to exit
        debug!("send shutdown command");
        ducks_rpc.shutdown(()).await?;
        drop(ducks_rpc);
        let ducks_output = ducks_process.wait_with_output().await?;

        let ducks_stdout = ducks_output.stdout;
        let ducks_stdout = String::from_utf8(ducks_stdout)?;
        println!("ducks output:\n{}", ducks_stdout);

        let lading_stdout = lading_output.stdout;
        let lading_stdout = String::from_utf8(lading_stdout)?;
        println!("lading output:\n{}", lading_stdout);

        println!("test result: (todo)");

        // todo: report captures file & provide some utilities for asserting against it
        // info!("test result: {:?}", metrics);
        Ok(metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn http_apache_common() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Http,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
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
        maximum_prebuild_cache_size_bytes: "8 Mb"
        variant: "apache_common"
        "#,
        )?;

        let reqs = test.run().await?;

        assert!(reqs.http.request_count > 10);
        Ok(())
    }

    #[tokio::test]
    async fn http_ascii() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Http,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
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
        maximum_prebuild_cache_size_bytes: "8 Mb"
        variant: "ascii"
        "#,
        )?;

        let reqs = test.run().await?;

        assert!(reqs.http.request_count > 10);
        Ok(())
    }

    #[tokio::test]
    async fn tcp_fluent() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Tcp,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
            r#"
generator:
  tcp:
    seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
      59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
    addr: "localhost:{{port_number}}"
    bytes_per_second: "100 Mb"
    block_sizes: ["1Mb", "0.5Mb", "0.25Mb", "0.125Mb", "128Kb"]
    variant: fluent
    maximum_prebuild_cache_size_bytes: "8 Mb"
        "#,
        )?;

        let reqs = test.run().await?;

        assert!(reqs.tcp.total_bytes > 0);
        assert!(reqs.tcp.total_bytes > 100_000);
        Ok(())
    }
}
