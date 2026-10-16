//! The neper network performance blackhole.
//!
//! This blackhole spawns a [neper](https://github.com/google/neper) server
//! process that listens for incoming connections from a neper client. When
//! the server exits (after the client disconnects), its stdout is parsed for
//! the throughput value and emitted as a gauge metric.
//!
//! ## Metrics
//!
//! `neper_throughput`: Throughput value reported by the neper server

use std::{io, path::PathBuf, process::Stdio};

use metrics::gauge;
use nix::sys::resource::{Resource, getrlimit, setrlimit};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{info, warn};

use super::General;
use crate::generator::neper::Workload;

/// Directory where neper binaries are installed.
const NEPER_BIN_DIR: &str = "/usr/local/bin";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for the neper blackhole.
pub struct Config {
    /// The workload to serve.
    pub workload: Workload,
    /// Optional control port override (`-C`).
    pub control_port: Option<u16>,
    /// Optional data port override (`-P`).
    pub data_port: Option<u16>,
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
    /// Failed to set resource limits.
    #[error("failed to set rlimit: {0}")]
    Rlimit(#[from] nix::errno::Errno),
    /// Neper process exited with a non-zero status.
    #[error("neper server exited with {status}: {stderr}")]
    NeperFailed {
        /// Exit status
        status: std::process::ExitStatus,
        /// Captured stderr
        stderr: String,
    },
    /// Neper server was still running when shutdown arrived and the grace
    /// period expired.
    #[error("neper server did not exit within the grace period after shutdown")]
    ShutdownTimeout,
}

#[derive(Debug)]
/// The neper blackhole.
///
/// Spawns a neper server binary and keeps it running until shutdown.
pub struct Neper {
    config: Config,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl Neper {
    /// Create a new [`Neper`] blackhole instance.
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "neper".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            config: config.clone(),
            shutdown,
            metric_labels,
        }
    }

    /// Run the neper server until the child exits.
    ///
    /// The blackhole always waits for the neper server to finish so that its
    /// stdout can be parsed for the throughput metric. If a shutdown signal
    /// arrives while the server is still running, a short grace period is
    /// given; if the server does not exit in time it is killed and an error
    /// is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if spawning neper fails, it exits with a non-zero
    /// status, or it is still running when the shutdown grace period expires.
    pub async fn run(self) -> Result<(), Error> {
        // Raise the open file descriptor limit to the hard limit. Neper opens
        // many sockets and can easily exceed the default soft limit.
        let (_, hard) = getrlimit(Resource::RLIMIT_NOFILE)?;
        setrlimit(Resource::RLIMIT_NOFILE, hard, hard)?;
        info!(nofile_limit = hard, "raised RLIMIT_NOFILE to hard limit");

        let binary = PathBuf::from(NEPER_BIN_DIR).join(self.config.workload.binary_name());

        let mut cmd = Command::new(&binary);

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

        info!(?binary, "spawning neper server");
        let mut child = cmd.spawn()?;

        // Always wait for the child. If shutdown arrives first, give the
        // server a short grace period to finish on its own before killing it.
        let mut shutdown_wait = std::pin::pin!(self.shutdown.recv());
        let exited_cleanly = tokio::select! {
            result = child.wait() => {
                let status = result?;
                if !status.success() {
                    let stderr = read_child_stderr(&mut child).await;
                    warn!(%status, "neper server exited unexpectedly");
                    return Err(Error::NeperFailed { status, stderr });
                }
                true
            }
            () = &mut shutdown_wait => {
                info!("shutdown signal received, waiting briefly for neper server to exit");
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(5),
                    child.wait(),
                ).await {
                    Ok(Ok(status)) if status.success() => true,
                    Ok(Ok(status)) => {
                        let stderr = read_child_stderr(&mut child).await;
                        warn!(%status, "neper server exited with error during grace period");
                        return Err(Error::NeperFailed { status, stderr });
                    }
                    Ok(Err(e)) => return Err(Error::Io(e)),
                    Err(_) => {
                        warn!("grace period expired, killing neper server");
                        let _ = child.kill().await;
                        false
                    }
                }
            }
        };

        if exited_cleanly {
            let stdout = read_child_stdout(&mut child).await;
            if let Some(value) = parse_throughput(&stdout, self.config.workload) {
                info!(throughput = value, "neper server run complete");
                gauge!("neper_throughput", &self.metric_labels).set(value);
            } else {
                warn!("could not parse neper throughput from server output");
            }
            Ok(())
        } else {
            Err(Error::ShutdownTimeout)
        }
    }
}

async fn read_child_stdout(child: &mut tokio::process::Child) -> String {
    match child.stdout.take() {
        Some(mut so) => {
            use tokio::io::AsyncReadExt;
            let mut buf = String::new();
            let _ = so.read_to_string(&mut buf).await;
            buf
        }
        None => String::new(),
    }
}

async fn read_child_stderr(child: &mut tokio::process::Child) -> String {
    match child.stderr.take() {
        Some(mut se) => {
            use tokio::io::AsyncReadExt;
            let mut buf = String::new();
            let _ = se.read_to_string(&mut buf).await;
            buf
        }
        None => String::new(),
    }
}

/// Parse the throughput value from neper's stdout output.
///
/// Neper prints key=value pairs, one per line. We look for the line matching
/// the workload's throughput key.
fn parse_throughput(output: &str, workload: Workload) -> Option<f64> {
    let key = workload.throughput_key();
    for line in output.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            return v.trim().parse::<f64>().ok();
        }
    }
    None
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
        assert!(parse_throughput(output, Workload::TcpRr).is_none());
    }
}
