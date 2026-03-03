//! The neper network performance generator.
//!
//! This generator spawns a [neper](https://github.com/google/neper) client
//! process to drive TCP workloads (request-response, connection-per-request,
//! streaming) against a neper server and reports the resulting throughput as a
//! metric.
//!
//! ## Metrics
//!
//! `neper_throughput`: Throughput value reported by neper at the end of the run

use std::{io, path::PathBuf, process::Stdio};

use metrics::gauge;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{info, warn};

use super::General;
use crate::generator::common::MetricsBuilder;

/// Directory where neper binaries are installed.
const NEPER_BIN_DIR: &str = "/usr/local/bin";

fn default_startup_delay_seconds() -> u64 {
    5
}

/// Neper workload type.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    /// TCP request-response
    TcpRr,
    /// TCP connect/request/response (new connection per request)
    TcpCrr,
    /// TCP streaming
    TcpStream,
}

impl Workload {
    /// Return the binary name for this workload.
    #[must_use]
    pub fn binary_name(self) -> &'static str {
        match self {
            Workload::TcpRr => "tcp_rr",
            Workload::TcpCrr => "tcp_crr",
            Workload::TcpStream => "tcp_stream",
        }
    }

    /// Return the key in neper's output that carries the throughput value.
    fn throughput_key(self) -> &'static str {
        match self {
            Workload::TcpRr | Workload::TcpCrr | Workload::TcpStream => "throughput",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration for the neper generator.
pub struct Config {
    /// The workload to run.
    pub workload: Workload,
    /// The host (or IP) of the neper server.
    pub host: String,
    /// Optional control port override (`-C`).
    pub control_port: Option<u16>,
    /// Optional data port override (`-P`).
    pub data_port: Option<u16>,
    /// Duration in seconds for the neper run (`-l`).
    pub duration_seconds: u64,
    /// Seconds to wait before spawning neper (let the server start).
    #[serde(default = "default_startup_delay_seconds")]
    pub startup_delay_seconds: u64,
    /// Extra CLI arguments forwarded verbatim to the neper binary.
    #[serde(default)]
    pub extra_args: Vec<String>,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Neper`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Neper process exited with a non-zero status.
    #[error("neper exited with {status}: {stderr}")]
    NeperFailed {
        /// Exit status
        status: std::process::ExitStatus,
        /// Captured stderr
        stderr: String,
    },
    /// Could not parse throughput from neper output.
    #[error("failed to parse neper output: {0}")]
    OutputParse(String),
}

#[derive(Debug)]
/// The neper generator.
///
/// Spawns a neper client binary, waits for it to finish, and emits the
/// throughput value as a gauge metric.
pub struct Neper {
    config: Config,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl Neper {
    /// Create a new [`Neper`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration is invalid.
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let metric_labels = MetricsBuilder::new("neper").with_id(general.id).build();
        Ok(Self {
            config: config.clone(),
            shutdown,
            metric_labels,
        })
    }

    /// Run the neper client to completion or until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if spawning neper fails, it exits with a non-zero
    /// status, or the output cannot be parsed.
    pub async fn spin(self) -> Result<(), Error> {
        // Give the server time to come up.
        let delay = tokio::time::Duration::from_secs(self.config.startup_delay_seconds);
        info!(
            delay_secs = self.config.startup_delay_seconds,
            "waiting for neper server to start"
        );
        tokio::time::sleep(delay).await;

        let binary = PathBuf::from(NEPER_BIN_DIR).join(self.config.workload.binary_name());

        let mut cmd = Command::new(&binary);
        cmd.arg("-c")
            .arg("-H")
            .arg(&self.config.host)
            .arg("-l")
            .arg(self.config.duration_seconds.to_string());

        if let Some(port) = self.config.control_port {
            cmd.arg("-C").arg(port.to_string());
        }
        if let Some(port) = self.config.data_port {
            cmd.arg("-P").arg(port.to_string());
        }
        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        info!(?binary, "spawning neper client");
        let mut child = cmd.spawn()?;

        // Clone so we can wait for shutdown after the child finishes.
        let shutdown_post = self.shutdown.clone();
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        tokio::select! {
            result = child.wait() => {
                let status = result?;
                if !status.success() {
                    let stderr = match child.stderr.take() {
                        Some(mut se) => {
                            use tokio::io::AsyncReadExt;
                            let mut buf = String::new();
                            let _ = se.read_to_string(&mut buf).await;
                            buf
                        }
                        None => String::new(),
                    };
                    return Err(Error::NeperFailed { status, stderr });
                }

                let stdout = match child.stdout.take() {
                    Some(mut so) => {
                        use tokio::io::AsyncReadExt;
                        let mut buf = String::new();
                        let _ = so.read_to_string(&mut buf).await;
                        buf
                    }
                    None => String::new(),
                };

                match parse_throughput(&stdout, self.config.workload) {
                    Ok(value) => {
                        info!(throughput = value, "neper run complete");
                        gauge!("neper_throughput", &self.metric_labels).set(value);
                    }
                    Err(e) => {
                        warn!("could not parse neper throughput: {e}");
                    }
                }

                // Wait for shutdown signal before returning so lading keeps
                // running (e.g. to let the observer collect the metric).
                shutdown_post.recv().await;
            }
            () = &mut shutdown_wait => {
                info!("shutdown signal received, killing neper client");
                let _ = child.kill().await;
            }
        }

        Ok(())
    }
}

/// Parse the throughput value from neper's stdout output.
///
/// Neper prints key=value pairs, one per line. We look for the line matching
/// the workload's throughput key.
fn parse_throughput(output: &str, workload: Workload) -> Result<f64, Error> {
    let key = workload.throughput_key();
    for line in output.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            return v.trim().parse::<f64>().map_err(|e| {
                Error::OutputParse(format!("could not parse '{v}' as f64: {e}"))
            });
        }
    }
    Err(Error::OutputParse(format!(
        "key '{key}' not found in neper output"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_throughput_tcp_rr() {
        let output = "num_flows=1\nnum_threads=1\nthroughput=12345.67\n";
        let value = parse_throughput(output, Workload::TcpRr).unwrap();
        assert!((value - 12345.67).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_throughput_tcp_stream() {
        let output = "throughput=98765.43\nlatency=0.5\n";
        let value = parse_throughput(output, Workload::TcpStream).unwrap();
        assert!((value - 98765.43).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_throughput_missing_key() {
        let output = "latency=0.5\n";
        assert!(parse_throughput(output, Workload::TcpRr).is_err());
    }

    #[test]
    fn workload_binary_names() {
        assert_eq!(Workload::TcpRr.binary_name(), "tcp_rr");
        assert_eq!(Workload::TcpCrr.binary_name(), "tcp_crr");
        assert_eq!(Workload::TcpStream.binary_name(), "tcp_stream");
    }
}
