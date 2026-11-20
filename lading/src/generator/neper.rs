//! The neper network performance generator.
//!
//! This generator spawns a [neper](https://github.com/google/neper) client
//! process to drive TCP workloads (request-response, connection-per-request,
//! streaming) against a neper server. Throughput metrics are emitted by the
//! corresponding neper blackhole (server side).

use std::{io, path::PathBuf, process::Stdio};

use nix::sys::resource::{Resource, getrlimit, setrlimit};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::info;

use super::General;

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
    #[must_use]
    pub fn throughput_key(self) -> &'static str {
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
    /// Failed to set resource limits.
    #[error("failed to set rlimit: {0}")]
    Rlimit(#[from] nix::errno::Errno),
    /// Neper process exited with a non-zero status.
    #[error("neper exited with {status}: {stderr}")]
    NeperFailed {
        /// Exit status
        status: std::process::ExitStatus,
        /// Captured stderr
        stderr: String,
    },
}

#[derive(Debug)]
/// The neper generator.
///
/// Spawns a neper client binary, waits for it to finish, and emits the
/// throughput value as a gauge metric.
pub struct Neper {
    config: Config,
    shutdown: lading_signal::Watcher,
}

impl Neper {
    /// Create a new [`Neper`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration is invalid.
    pub fn new(
        _general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            shutdown,
        })
    }

    /// Run the neper client to completion or until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if spawning neper fails or it exits with a non-zero
    /// status.
    pub async fn spin(self) -> Result<(), Error> {
        // Raise the open file descriptor limit to the hard limit. Neper opens
        // many sockets and can easily exceed the default soft limit.
        let (_, hard) = getrlimit(Resource::RLIMIT_NOFILE)?;
        setrlimit(Resource::RLIMIT_NOFILE, hard, hard)?;
        info!(nofile_limit = hard, "raised RLIMIT_NOFILE to hard limit");

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

        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        info!(?binary, "spawning neper client");
        let mut child = cmd.spawn()?;

        let shutdown_post = self.shutdown.clone();
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        tokio::select! {
            biased;

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
                info!("neper client finished");

                // Wait for shutdown so lading keeps running while the
                // blackhole collects and emits the throughput metric.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_binary_names() {
        assert_eq!(Workload::TcpRr.binary_name(), "tcp_rr");
        assert_eq!(Workload::TcpCrr.binary_name(), "tcp_crr");
        assert_eq!(Workload::TcpStream.binary_name(), "tcp_stream");
    }
}
