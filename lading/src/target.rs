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
    io,
    num::NonZeroI32,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use bollard::Docker;
use bollard::query_parameters::InspectContainerOptionsBuilder;
use lading_signal::Broadcaster;
use metrics::gauge;
use nix::{
    errno::Errno,
    sys::signal::{SIGTERM, kill},
    unistd::Pid,
};
use rustc_hash::FxHashMap;
use tokio::{process::Command, time};
use tracing::{error, info};

use crate::common::stdio;
pub use crate::common::{Behavior, Output};

/// Type used to receive the target PID once it is running.
#[allow(clippy::module_name_repetitions)]
pub type TargetPidReceiver = tokio::sync::broadcast::Receiver<Option<i32>>;

#[allow(clippy::module_name_repetitions)]
type TargetPidSender = tokio::sync::broadcast::Sender<Option<i32>>;

/// Errors produced by [`Server`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Unable to spawn target
    #[error("unable to spawn target: {0}")]
    TargetSpawn(io::Error),
    /// Unable to await target exit
    #[error("unable to wait for target exit: {0}")]
    TargetWait(io::Error),
    /// Unable to create `PidFd` from raw PID
    #[error("unable to create PidFd: {0}")]
    PidConversion(io::Error),
    /// SIGTERM error
    #[error("unable to terminate target process: {0}")]
    SigTerm(Errno),
    /// The target PID does not exist or is invalid
    #[error("PID not found: {0}")]
    PidNotFound(i32),
    /// The target process exited unexpectedly
    #[error("target exited unexpectedly: {0:?}")]
    TargetExited(Option<ExitStatus>),
    /// See [`SendError`]
    #[error(transparent)]
    Send(#[from] tokio::sync::broadcast::error::SendError<Option<i32>>),
    /// Process already finished error
    #[error("Child has already been polled to completion")]
    ProcessFinished,
    /// Container does not exist
    #[error(transparent)]
    Bollard(#[from] bollard::errors::Error),
}

/// Configuration for Docker target mode
#[allow(missing_copy_implementations)]
#[derive(Debug, PartialEq, Eq)]
pub struct DockerConfig {
    /// Container name to watch
    pub name: String,
}

/// Configuration for PID target mode
#[allow(missing_copy_implementations)]
#[derive(Debug, PartialEq, Eq)]
pub struct PidConfig {
    /// PID to watch
    pub pid: NonZeroI32,
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
    pub environment_variables: FxHashMap<String, String>,
    /// Manages stderr, stdout of the target sub-process.
    pub output: Output,
}

/// Configuration for [`Server`]
#[derive(Debug, PartialEq, Eq)]
pub enum Config {
    /// A docker managed process
    Docker(DockerConfig),
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
    shutdown: lading_signal::Watcher,
}

impl Server {
    /// Create a new [`Server`] instance
    #[must_use]
    pub fn new(config: Config, shutdown: lading_signal::Watcher) -> Self {
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
    pub async fn run(
        self,
        pid_snd: TargetPidSender,
        target_running: Broadcaster,
    ) -> Result<(), Error> {
        let config = self.config;

        // Note that each target mode has different expectations around target
        // exit. PID mode expects the target to continue running; any exit is
        // a critical error. Binary mode expects the target to run until
        // signalled to exit.
        match config {
            Config::Pid(config) => {
                Self::watch(config, pid_snd, target_running, self.shutdown).await?;
            }
            Config::Docker(config) => {
                Self::watch_container(config, pid_snd, target_running, self.shutdown).await?;
            }
            Config::Binary(config) => {
                Self::execute_binary(config, pid_snd, target_running, self.shutdown).await?;
            }
        }

        Ok(())
    }

    /// Watch a container running elsewhere on the system. lading will report an
    /// error if the container exits before the test completes.
    async fn watch_container(
        config: DockerConfig,
        pid_snd: TargetPidSender,
        target_running: Broadcaster,
        shutdown: lading_signal::Watcher,
    ) -> Result<(), Error> {
        let docker = Docker::connect_with_socket_defaults()?;

        let pid: i64 = loop {
            let inspect_options = InspectContainerOptionsBuilder::default().build();
            if let Ok(container) = docker.inspect_container(&config.name, Some(inspect_options)).await {
                if let Some(pid) = container.state.and_then(|state| state.pid) {
                    // In some cases docker will report pid 0 as the pid for the
                    // polled container. This is not usable by us and we believe
                    // a race condition.
                    if pid == 0 {
                        info!(
                            "Found container with name {name} but with pid {pid}. Polling.",
                            name = config.name
                        );
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    break pid;
                }
            } else {
                info!(
                    "Could not find container with name {name}, polling.",
                    name = config.name
                );
                time::sleep(Duration::from_secs(1)).await;
            }
        };
        pid_snd.send(Some(
            pid.try_into().expect("cannot convert pid to 32 bit type"),
        ))?;
        drop(pid_snd);
        target_running.signal();

        // Use PIDfd to watch the target process (linux kernel 5.3 and up)
        #[cfg(target_os = "linux")]
        let target_wait = {
            use async_pidfd::AsyncPidFd;
            let pidfd =
                AsyncPidFd::from_pid(pid.try_into().expect("cannot convert pid to 32 bit type"))
                    .map_err(Error::PidConversion)?;
            async move {
                let exit_info = pidfd.wait().await;
                exit_info.map(|info| info.status()).ok()
            }
        };

        // Watch the process by polling the PID. This works across unices but
        // does not give access to the exit code on early termination.
        #[cfg(not(target_os = "linux"))]
        let target_wait = async move {
            let pid = Pid::from_raw(pid.try_into().expect("cannot convert pid to 32 bit type"));
            loop {
                let ret = kill(pid, None);
                if ret.is_err() {
                    break;
                }
                time::sleep(Duration::from_secs(1)).await;
            }
            Option::<ExitStatus>::None
        };

        let mut interval = time::interval(Duration::from_millis(400));
        tokio::pin!(target_wait);
        let shutdown_wait = shutdown.recv();
        tokio::pin!(shutdown_wait);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    gauge!("target.running").set( 1.0);
                },
                target_exit = &mut target_wait => {
                    if let Some(code) = target_exit{
                        error!("target exited unexpectedly with code {}", code);
                        break Err(Error::TargetExited(Some(code)));
                    }
                    error!("target exited unexpectedly; exit code unavailable");
                    break Err(Error::TargetExited(None));
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    break Ok(());
                }
            }
        }
    }

    /// Watch a process running elsewhere on the system. lading will report an
    /// error if the process ends before the test completes.
    async fn watch(
        config: PidConfig,
        pid_snd: TargetPidSender,
        target_running: Broadcaster,
        shutdown: lading_signal::Watcher,
    ) -> Result<(), Error> {
        // Convert pid config value to a plain i32 (no truncation concerns;
        // PID_MAX_LIMIT is 2^22)
        let raw_pid: i32 = config.pid.get();
        let pid = Pid::from_raw(raw_pid);

        // Verify that the given PID is valid
        let ret = kill(pid, None);
        if ret.is_err() {
            return Err(Error::PidNotFound(config.pid.get()));
        }

        pid_snd.send(Some(config.pid.get()))?;
        drop(pid_snd);
        target_running.signal();

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

        let mut interval = time::interval(Duration::from_millis(400));
        tokio::pin!(target_wait);
        let shutdown_wait = shutdown.recv();
        tokio::pin!(shutdown_wait);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    gauge!("target.running").set(1.0);
                },
                target_exit = &mut target_wait => {
                    if let Some(code) = target_exit{
                        error!("target exited unexpectedly with code {}", code);
                        break Err(Error::TargetExited(Some(code)));
                    }
                    error!("target exited unexpectedly; exit code unavailable");
                    break Err(Error::TargetExited(None));
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    break Ok(());
                }
            }
        }
    }

    /// Execute a binary target. lading will attempt to gracefully terminate the
    /// process after the test has completed.
    async fn execute_binary(
        config: BinaryConfig,
        pid_snd: TargetPidSender,
        target_running: Broadcaster,
        shutdown: lading_signal::Watcher,
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
        let target_id = target_child.id().ok_or(Error::ProcessFinished)?;
        pid_snd.send(Some(
            target_id
                .try_into()
                .expect("could not convert target pid to i32"),
        ))?;
        drop(pid_snd);
        target_running.signal();

        let mut interval = time::interval(Duration::from_secs(400));
        let shutdown_wait = shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    gauge!("target.running").set( 1.0);
                },
                res = target_child.wait() => {
                    match res {
                        Ok(res) => {
                            error!("target exited unexpectedly with code {}", res);
                            break Err(Error::TargetExited(Some(res)))
                        },
                        Err(e) => {
                            error!("target exited unexpectedly; exit code unavailable ({})", e);
                            break Err(Error::TargetExited(None))
                        },
                    }
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // Note that `Child::kill` sends SIGKILL which is not what we
                    // want. We instead send SIGTERM so that the child has a chance
                    // to clean up.
                    let pid: Pid = Pid::from_raw(target_id.try_into().expect("Failed to convert into valid PID"));
                    kill(pid, SIGTERM).map_err(Error::SigTerm)?;
                    let res = target_child.wait().await.map_err(Error::TargetWait)?;
                    break Ok(res)
                }
            }
        }
    }
}
