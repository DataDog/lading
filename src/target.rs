//! Manages lading's target
//!
//! The lading 'target' is the process that lading inspects by pushing load
//! into from a [`crate::generator::Server`] and possibly into a
//! [`crate::blackhole::Server`]. A [`crate::inspector::Server`] is intended to
//! read operating system details about the target sub-process.
//!
//! Lading supports two types of targets, binary and process ID (PID) targets.
//! In binary target mode, lading will launch a child process and shut it down
//! cleanly by signaling SIGTERM to it. If the target crashes this is also
//! detected and lading does a controlled shutdown.
//!
//! In PID target mode, lading will follow along with a running process. This is
//! intended to enable containerized targets. In this mode, the target process
//! should run until lading has exited. Lading will exit with an error if the
//! watched process terminates early.

use std::{
    collections::HashMap,
    io,
    num::NonZeroU32,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

use metrics::gauge;
use nix::{
    errno::Errno,
    sys::signal::{kill, SIGTERM},
    unistd::Pid,
};
use tokio::{process::Command, sync::broadcast::Sender};
use tracing::{error, info};

pub use crate::common::{Behavior, Output};
use crate::{common::stdio, observer::RSS_BYTES, signals::Shutdown};

/// Expose the process' current RSS consumption, allowing abstractions to be
/// built on top in the Target implementation.
pub(crate) static RSS_BYTES_LIMIT: AtomicU64 = AtomicU64::new(u64::MAX);

/// Errors produced by [`Meta`]
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum MetaError {
    /// Unable to support byte limits greater than u64::MAX
    #[error("unable to support bytes greater than u64::MAX")]
    ByteLimitTooLarge,
}

/// Source for live metadata about the running target.
#[derive(Debug, Clone, Copy)]
pub struct Meta {}

impl Meta {
    /// Set the maximum RSS bytes limit
    ///
    /// # Errors
    ///
    /// Will fail if the `byte_unit::Byte` value given is larger than `u64::MAX`
    /// bytes.
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn set_rss_bytes_limit(limit: byte_unit::Byte) -> Result<(), MetaError> {
        let raw_limit: u128 = limit.get_bytes();
        if raw_limit > u128::from(u64::MAX) {
            return Err(MetaError::ByteLimitTooLarge);
        }

        gauge!("rss_bytes_limit", raw_limit as f64);

        RSS_BYTES_LIMIT.store(raw_limit as u64, Ordering::Relaxed);
        Ok(())
    }

    #[inline]
    pub(crate) fn rss_bytes_limit_exceeded() -> bool {
        let limit: u64 = RSS_BYTES_LIMIT.load(Ordering::Relaxed);
        let current: u64 = RSS_BYTES.load(Ordering::Relaxed);

        gauge!(
            "rss_bytes_limit_overage",
            current.saturating_sub(limit) as f64
        );

        current > limit
    }
}

/// Errors produced by [`Server`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Unable to spawn target
    #[error("unable to spawn target: {0}")]
    TargetSpawn(io::Error),
    /// Unable to await target exit
    #[error("unable to wait for target exit: {0}")]
    TargetWait(io::Error),
    /// Unable to create PidFd from raw PID
    #[error("unable to create PidFd: {0}")]
    PidConversion(io::Error),
    /// SIGTERM error
    #[error("unable to terminate target process: {0}")]
    SigTerm(Errno),
    /// The target PID does not exist or is invalid
    #[error("PID not found: {0}")]
    PidNotFound(u32),
    /// The target process exited unexpectedly
    #[error("target exited unexpectedly: {0:?}")]
    TargetExited(Option<ExitStatus>),
}

/// Configuration for PID target mode
#[allow(missing_copy_implementations)]
#[derive(Debug, PartialEq, Eq)]
pub struct PidConfig {
    /// PID to watch
    pub pid: NonZeroU32,
}

/// Configuration for binary launch mode
#[derive(Debug, PartialEq, Eq)]
pub struct BinaryConfig {
    /// The path to the target executable.
    pub command: PathBuf,
    /// Arguments for the target sub-process.
    pub arguments: Vec<String>,
    /// Inherit the environment variables from lading's environment.
    pub inherit_environment: bool,
    /// Environment variables to set for the target sub-process. Lading's own
    /// environment variables are only propagated to the target sub-process if
    /// `inherit_environment` is set.
    pub environment_variables: HashMap<String, String>,
    /// Manages stderr, stdout of the target sub-process.
    pub output: Output,
}

/// Configuration for [`Server`]
#[derive(Debug, PartialEq, Eq)]
pub enum Config {
    /// An existing process that is managed externally
    Pid(PidConfig),
    /// A binary that will be launched and managed directly
    Binary(BinaryConfig),
}

#[derive(Debug)]
/// The target server.
///
/// This struct manages the target under examination by lading. No action is
/// taken until [`Server::run`] is called. It is assumed that only one
/// instance of this struct will ever exist at a time, although there are no
/// protections for that.
pub struct Server {
    config: Config,
    shutdown: Shutdown,
}

impl Server {
    /// Create a new [`Server`] instance
    #[must_use]
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self { config, shutdown }
    }

    /// Run this [`Server`] to completion
    ///
    /// Target server will use the `broadcast::Sender` passed here to transmit
    /// the PID of the target process.
    ///
    /// This function waits for either a shutdown signal or (in PID-watch mode)
    /// the exit of the watched process. Child exit status does not currently
    /// propagate. This is less than ideal.
    ///
    /// # Binary launch mode
    ///
    /// This function runs the user supplied program to its completion, or until
    /// a shutdown signal is received.
    ///
    /// ## Errors
    ///
    /// Function will return an error if the underlying program cannot be waited
    /// on or will not shutdown when signaled to.
    ///
    /// # PID watch mode
    ///
    /// Function will return an error if the target PID does not exist or if the
    /// target process exits.
    ///
    /// ## Errors
    ///
    /// Function will return an error if no process with the given PID exists
    /// or if the process terminates while being watched.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(self, pid_snd: Sender<u32>) -> Result<(), Error> {
        let config = self.config;

        // Note that each target mode has different expectations around target
        // exit. PID mode expects the target to continue running; any exit is
        // a critical error. Binary mode expects the target to run until
        // signalled to exit.
        match config {
            Config::Pid(config) => {
                Self::watch(config, pid_snd, self.shutdown).await?;
            }
            Config::Binary(config) => {
                Self::execute_binary(config, pid_snd, self.shutdown).await?;
            }
        }

        Ok(())
    }

    /// Watch a process running elsewhere on the system. lading will report an
    /// error if the process ends before the test completes.
    async fn watch(
        config: PidConfig,
        pid_snd: Sender<u32>,
        mut shutdown: Shutdown,
    ) -> Result<(), Error> {
        // Convert pid config value to a plain i32 (no truncation concerns;
        // PID_MAX_LIMIT is 2^22)
        let raw_pid: i32 = config
            .pid
            .get()
            .try_into()
            .map_err(|_| Error::PidNotFound(config.pid.get()))?;
        let pid = Pid::from_raw(raw_pid);

        // Verify that the given PID is valid
        let ret = kill(pid, None);
        if ret.is_err() {
            return Err(Error::PidNotFound(config.pid.get()));
        }

        pid_snd
            .send(config.pid.get())
            .expect("target server unable to transmit PID, catastrophic failure");
        drop(pid_snd);

        // Use PIDfd to watch the target process (linux kernel 5.3 and up)
        #[cfg(target_os = "linux")]
        let target_wait = {
            use async_pidfd::AsyncPidFd;
            let pidfd = AsyncPidFd::from_pid(raw_pid).map_err(Error::PidConversion)?;
            async move {
                let exit_info = pidfd.wait().await;
                exit_info.map(|info| info.status()).ok()
            }
        };

        // Watch the process by polling the PID. This works across unices but
        // does not give access to the exit code on early termination.
        #[cfg(not(target_os = "linux"))]
        let target_wait = async move {
            use std::time::Duration;
            use tokio::time::sleep;
            loop {
                let ret = kill(pid, None);
                if ret.is_err() {
                    break;
                }
                sleep(Duration::from_secs(1)).await;
            }
            Option::<ExitStatus>::None
        };

        tokio::select! {
            target_exit = target_wait => {
                if let Some(code) = target_exit{
                    error!("target exited unexpectedly with code {}", code);
                    Err(Error::TargetExited(Some(code)))
                } else {
                    error!("target exited unexpectedly; exit code unavailable");
                    Err(Error::TargetExited(None))
                }
            },
            _ = shutdown.recv() => {
                info!("shutdown signal received");
                Ok(())
            }
        }
    }

    /// Execute a binary target. lading will attempt to gracefully terminate the
    /// process after the test has completed.
    async fn execute_binary(
        config: BinaryConfig,
        pid_snd: Sender<u32>,
        mut shutdown: Shutdown,
    ) -> Result<ExitStatus, Error> {
        let mut target_cmd = Command::new(config.command);
        target_cmd
            .stdin(Stdio::null())
            .stdout(stdio(&config.output.stdout))
            .stderr(stdio(&config.output.stderr));
        if !config.inherit_environment {
            target_cmd.env_clear();
        }
        target_cmd
            .kill_on_drop(true)
            .args(config.arguments)
            .envs(config.environment_variables.iter());
        let mut target_child = target_cmd.spawn().map_err(Error::TargetSpawn)?;
        let target_id = target_child.id().expect("target must have PID");
        pid_snd
            .send(target_id)
            .expect("target server unable to transmit PID, catastrophic failure");
        drop(pid_snd);

        tokio::select! {
            res = target_child.wait() => {
                match res {
                    Ok(res) => {
                        error!("target exited unexpectedly with code {}", res);
                        Err(Error::TargetExited(Some(res)))
                    },
                    Err(e) => {
                        error!("target exited unexpectedly; exit code unavailable ({})", e);
                        Err(Error::TargetExited(None))
                    },
                }
            },
            _ = shutdown.recv() => {
                info!("shutdown signal received");
                // Note that `Child::kill` sends SIGKILL which is not what we
                // want. We instead send SIGTERM so that the child has a chance
                // to clean up.
                let pid: Pid = Pid::from_raw(target_id.try_into().unwrap());
                kill(pid, SIGTERM).map_err(Error::SigTerm)?;
                let res = target_child.wait().await.map_err(Error::TargetWait)?;
                Ok(res)
            }
        }
    }
}
