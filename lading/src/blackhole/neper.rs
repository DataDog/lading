//! The neper network performance blackhole.
//!
//! This blackhole spawns a [neper](https://github.com/google/neper) server
//! process that listens for incoming connections from a neper client. The
//! server is kept alive until a shutdown signal is received.

use std::{io, path::PathBuf, process::Stdio};

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

    /// Run the neper server until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if spawning neper fails or it exits with a non-zero
    /// status before shutdown.
    pub async fn run(self) -> Result<(), Error> {
        let _ = &self.metric_labels; // reserved for future metrics

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

        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        info!(?binary, "spawning neper server");
        let mut child = cmd.spawn()?;

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
                    warn!(%status, "neper server exited unexpectedly");
                    return Err(Error::NeperFailed { status, stderr });
                }
                // Server exited cleanly (e.g. after the client disconnected).
                warn!("neper server exited on its own");
            }
            () = &mut shutdown_wait => {
                info!("shutdown signal received, killing neper server");
                let _ = child.kill().await;
            }
        }

        Ok(())
    }
}
