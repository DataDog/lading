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
//!
//! # Development Notes
//!
//! Turn up logging when debugging the integration test rig:
//! `RUST_LOG=trace cargo test -p sheepdog`
//!

use std::{
    io::{Read, Write},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use anyhow::Context;
use hyper_util::rt::TokioIo;
use shared::{
    DucksConfig,
    integration_api::{self, integration_target_client::IntegrationTargetClient},
};
use tempfile::TempDir;
use tokio::{net::UnixStream, process::Command};
use tonic::transport::Endpoint;
use tracing::{debug, warn};

#[derive(Debug)]
pub struct Metrics {
    pub http: integration_api::HttpMetrics,
    pub tcp: integration_api::SocketMetrics,
    pub udp: integration_api::SocketMetrics,
}

impl From<integration_api::Metrics> for Metrics {
    fn from(metrics: integration_api::Metrics) -> Self {
        Self {
            tcp: metrics.tcp.unwrap_or_default(),
            udp: metrics.udp.unwrap_or_default(),
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

enum TakeableTempDir {
    Owned(TempDir),
    Borrowed(std::path::PathBuf),
}

impl TakeableTempDir {
    fn new() -> Result<Self, anyhow::Error> {
        Ok(Self::Owned(TempDir::new()?))
    }

    fn path(&self) -> &std::path::Path {
        match self {
            TakeableTempDir::Owned(tempdir) => tempdir.path(),
            TakeableTempDir::Borrowed(path) => path.as_ref(),
        }
    }

    fn take(&mut self) -> Result<TakeableTempDir, anyhow::Error> {
        match self {
            TakeableTempDir::Owned(inner) => {
                let dir = inner.path().to_owned();
                let tempdir = std::mem::replace(self, TakeableTempDir::Borrowed(dir));
                Ok(tempdir)
            }
            TakeableTempDir::Borrowed(_) => anyhow::bail!("cannot take a borrowed tempdir"),
        }
    }
}

/// Defines an individual integration test
pub struct IntegrationTest {
    lading_config_template: String,
    experiment_duration: Duration,
    experiment_warmup: Duration,
    ducks_config: DucksConfig,

    tempdir: TakeableTempDir,
}

impl IntegrationTest {
    /// Create an integration test for the given lading configuration
    pub fn new<S: ToString>(
        ducks_config: DucksConfig,
        lading_config: S,
    ) -> Result<Self, anyhow::Error> {
        let _ = tracing_subscriber::fmt::try_init();
        let tempdir = TakeableTempDir::new()?;

        Ok(Self {
            lading_config_template: lading_config.to_string(),
            ducks_config,
            tempdir,
            experiment_duration: Duration::from_secs(30),
            experiment_warmup: Duration::from_secs(5),
        })
    }

    async fn run_inner(self) -> Result<Metrics, anyhow::Error> {
        // Build ducks and lading. Cargo's locking is sufficient for this to
        // work correctly when called in parallel. It would be more efficient to
        // only run this a single time though.
        let ducks_binary = build_ducks()?;
        let lading_binary = build_lading()?;

        // Every ducks-sheepdog pair is connected by a unique socket file
        let ducks_comm_file = self.tempdir.path().join("ducks_socket");

        let ducks_timeout =
            self.experiment_warmup + self.experiment_duration + Duration::from_secs(60);
        let ducks_timeout = ducks_timeout.as_secs().to_string();

        let ducks_process = Command::new(ducks_binary)
            .stdout(Stdio::from(std::fs::File::create(
                self.tempdir.path().join("ducks.stdout"),
            )?))
            .stderr(Stdio::from(std::fs::File::create(
                self.tempdir.path().join("ducks.stderr"),
            )?))
            .env("RUST_LOG", "ducks=debug,info")
            .arg(
                ducks_comm_file
                    .to_str()
                    .ok_or(anyhow::anyhow!("path is invalid unicode"))?,
            )
            .arg(ducks_timeout)
            .spawn()
            .context("launch ducks")?;

        // wait for ducks to bring up its RPC server and then connect
        while !std::path::Path::exists(&ducks_comm_file) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = Endpoint::try_from("http://127.0.0.1/this-is-not-used")?
            .connect_with_connector(tower::service_fn(move |_| {
                let ducks_comm_file = ducks_comm_file.clone();
                async move {
                    let sock = UnixStream::connect(ducks_comm_file).await?;
                    // Wrap the raw UnixStream in a TokioIo wrapper
                    Ok::<_, std::io::Error>(TokioIo::new(sock))
                }
            }))
            .await
            .context("Failed to connect to `ducks` integration test target binary")?;
        let mut ducks_rpc = IntegrationTargetClient::new(channel);
        debug!("connected to ducks");

        // instruct ducks to start the test (this is currently hardcoded to a
        // http sink test but will be configurable in the future)
        let test = ducks_rpc
            .start_test(self.ducks_config)
            .await
            .context("sending start test signal")?
            .into_inner();
        let port = test.port as u16;

        // template & write lading config
        let lading_config_file = self.tempdir.path().join("lading.yaml");
        let mut file =
            std::fs::File::create(&lading_config_file).context("create lading config file")?;
        let lading_config = self
            .lading_config_template
            .replace("{{port_number}}", &port.to_string());
        file.write_all(lading_config.as_bytes())
            .context("write lading config")?;

        // run lading against the ducks process that was started above
        let captures_file = self.tempdir.path().join("captures");
        let lading = Command::new(lading_binary)
            .stdout(Stdio::from(std::fs::File::create(
                self.tempdir.path().join("lading.stdout"),
            )?))
            .stderr(Stdio::from(std::fs::File::create(
                self.tempdir.path().join("lading.stderr"),
            )?))
            .env("RUST_LOG", "lading=debug,info")
            .arg("--target-pid")
            .arg(
                ducks_process
                    .id()
                    .ok_or(anyhow::anyhow!(
                        "child has already been polled to completion"
                    ))?
                    .to_string(),
            )
            .arg("--config-path")
            .arg(
                lading_config_file
                    .to_str()
                    .ok_or(anyhow::anyhow!("path is invalid unicode"))?,
            )
            .arg("--experiment-duration-seconds")
            .arg(self.experiment_duration.as_secs().to_string())
            .arg("--warmup-duration-seconds")
            .arg(self.experiment_warmup.as_secs().to_string())
            .arg("--capture-path")
            .arg(
                captures_file
                    .to_str()
                    .ok_or(anyhow::anyhow!("path is invalid unicode"))?,
            )
            .spawn()
            .context("spawn lading")?;

        // wait for lading to push some load. It will exit on its own.
        debug!("lading is running");
        let lading_output = lading
            .wait_with_output()
            .await
            .context("wait for lading to exit")?;

        if lading_output.status.success() {
            debug!("lading exited successfully");
        } else {
            warn!("lading exited with an error: {:?}", lading_output);
        }

        // get test results from ducks
        let metrics = ducks_rpc.get_metrics(());
        let metrics = metrics
            .await
            .context("get metrics from ducks")?
            .into_inner();

        let metrics = Metrics::from(metrics);

        // ask ducks to shutdown and wait for its process to exit
        debug!("send shutdown command");
        ducks_rpc
            .shutdown(())
            .await
            .context("requesting ducks shutdown")?;
        drop(ducks_rpc);
        let ducks_output = ducks_process
            .wait_with_output()
            .await
            .context("wait for ducks to exit")?;

        if ducks_output.status.success() {
            debug!("ducks exited successfully");
        } else {
            warn!("ducks exited with an error: {:?}", ducks_output);
        }

        IntegrationTest::print_stdio(&self.tempdir);

        // todo: report captures file & provide some utilities for asserting against it
        println!("test result: {metrics:?}");
        Ok(metrics)
    }

    fn print_stdio(tempdir: &TakeableTempDir) {
        let logs = vec![
            ("ducks.stdout", tempdir.path().join("ducks.stdout")),
            ("ducks.stderr", tempdir.path().join("ducks.stderr")),
            ("lading.stdout", tempdir.path().join("lading.stdout")),
            ("lading.stderr", tempdir.path().join("lading.stderr")),
        ];

        fn try_print_file(name: &str, path: &std::path::Path) -> Result<(), anyhow::Error> {
            let mut contents = String::new();
            std::fs::File::open(path)?.read_to_string(&mut contents)?;
            if contents.is_empty() {
                println!("{name}: <empty>");
            } else {
                println!("{name}:\n{contents}");
            }
            Ok(())
        }

        for (name, path) in logs {
            let _ = try_print_file(name, &path);
        }
    }

    pub async fn run(mut self) -> Result<Metrics, anyhow::Error> {
        let test_tempdir = self.tempdir.take()?;

        let res = tokio::select! {
            res = self.run_inner() => { res }
            _ = tokio::time::sleep(Duration::from_secs(30 * 60)) => { panic!("test timed out") }
        };

        if res.is_err() {
            IntegrationTest::print_stdio(&test_tempdir);
        }

        res
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
  - http:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        headers: {}
        target_uri: "http://localhost:{{port_number}}/"
        bytes_per_second: "100 MiB"
        parallel_connections: 5
        method:
          post:
            maximum_prebuild_cache_size_bytes: "8 MiB"
            variant: "apache_common"
        "#,
        )?;

        let reqs = test.run().await?;

        debug_assert!(
            reqs.http.request_count > 10,
            "Request count: {request_count}",
            request_count = reqs.http.request_count
        );
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
  - http:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        headers: {}
        target_uri: "http://localhost:{{port_number}}/"
        bytes_per_second: "100 MiB"
        parallel_connections: 5
        method:
          post:
            maximum_prebuild_cache_size_bytes: "8 MiB"
            variant: "ascii"
        "#,
        )?;

        let reqs = test.run().await?;

        debug_assert!(
            reqs.http.request_count > 10,
            "Request count: {request_count}",
            request_count = reqs.http.request_count
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_otel_logs() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Http,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
            r#"
generator:
  - http:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        target_uri: "http://localhost:{{port_number}}/v1/logs"
        bytes_per_second: "100 MiB"
        parallel_connections: 5
        method:
          post:
            maximum_prebuild_cache_size_bytes: "8 MiB"
            variant:
              opentelemetry_logs: {}
        headers:
            Content-Type: "application/x-protobuf"
        "#,
        )?;

        let reqs = test.run().await?;

        debug_assert!(
            reqs.http.request_count > 10,
            "Request count: {request_count}",
            request_count = reqs.http.request_count
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_otel_traces() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Http,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
            r#"
generator:
  - http:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        target_uri: "http://localhost:{{port_number}}/v1/traces"
        bytes_per_second: "100 MiB"
        parallel_connections: 5
        method:
          post:
            maximum_prebuild_cache_size_bytes: "8 MiB"
            variant: "opentelemetry_traces"
        headers:
            Content-Type: "application/x-protobuf"
        "#,
        )?;

        let reqs = test.run().await?;

        debug_assert!(
            reqs.http.request_count > 10,
            "Request count: {request_count}",
            request_count = reqs.http.request_count
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_otel_metrics() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Http,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
            r#"
generator:
  - http:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        target_uri: "http://localhost:{{port_number}}/v1/metrics"
        bytes_per_second: "100 MiB"
        parallel_connections: 5
        method:
          post:
            maximum_prebuild_cache_size_bytes: "8 MiB"
            variant:
              opentelemetry_metrics:
                metric_weights:
                  gauge: 50
                  sum: 50
        headers:
            Content-Type: "application/x-protobuf"
        "#,
        )?;

        let reqs = test.run().await?;

        debug_assert!(
            reqs.http.request_count > 10,
            "Request count: {request_count}",
            request_count = reqs.http.request_count
        );
        Ok(())
    }

    #[ignore]
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
  - tcp:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        addr: "127.0.0.1:{{port_number}}"
        bytes_per_second: "100 MiB"
        variant: fluent
        maximum_prebuild_cache_size_bytes: "8 MiB"
        "#,
        )?;

        let reqs = test.run().await?;

        // TODO: Verify that lading's bytes out == ducks bytes in

        assert!(reqs.tcp.total_bytes > 0);
        assert!(reqs.tcp.total_bytes > 100_000);
        Ok(())
    }

    #[tokio::test]
    async fn udp_ascii() -> Result<(), anyhow::Error> {
        let test = IntegrationTest::new(
            DucksConfig {
                listen: shared::ListenConfig::Udp,
                emit: shared::EmitConfig::None,
                assertions: shared::AssertionConfig::None,
            },
            r#"
generator:
  - udp:
        seed: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
          59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131]
        addr: "127.0.0.1:{{port_number}}"
        bytes_per_second: "100 MiB"
        variant: ascii
        maximum_prebuild_cache_size_bytes: "8 MiB"
        "#,
        )?;

        let reqs = test.run().await?;

        assert!(reqs.udp.total_bytes > 0);
        assert!(reqs.udp.total_bytes > 100_000);
        Ok(())
    }
}
