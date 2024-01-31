//! Manage the target inspector sub-process
//!
//! The interogation that lading does of the target sub-process is intentionally
//! limited to in-process concerns. For instance, lading is able to measure the
//! bytes written per second to a target becase lading itself is writing the
//! bytes. It's valuable to have further information about the target
//! sub-process and that's the responsibility of the inspector. Consider that
//! you can get a Linux `perf` sample of the target by means of having inspector
//! run an appropriate shell script, or take samples of the target's CPU use.

use std::{
    io,
    path::PathBuf,
    process::{ExitStatus, Stdio},
};

use nix::{
    errno::Errno,
    sys::signal::{kill, SIGTERM},
    unistd::Pid,
};
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tokio::process::Command;
use tracing::{error, info};

use crate::{
    common::{stdio, Output},
    signals::Phase,
    target::TargetPidReceiver,
};

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Server`]
pub enum Error {
    /// Wrapper for [`nix::errno::Errno`]
    #[error(transparent)]
    Errno(Errno),
    /// Wrapper for [`std::io::Error`]
    #[error(transparent)]
    Io(io::Error),
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configuration for [`Server`]
pub struct Config {
    /// The path to the inspector executable.
    pub command: PathBuf,
    /// Arguments for the inspector sub-process.
    pub arguments: Vec<String>,
    /// Environment variables to set for the inspector sub-process. Lading's own
    /// environment variables are not propagated to the sub-process.
    pub environment_variables: FxHashMap<String, String>,
    /// Manages stderr, stdout of the inspector sub-process.
    pub output: Output,
}

#[derive(Debug)]
/// The inspector sub-process server.
///
/// This struct manages a sub-process that can be used to do further examination
/// of the [`crate::target::Server`] by means of operating system facilities. The
/// sub-process is not created until [`Server::run`] is called. It is assumed
/// that only one instance of this struct will ever exist at a time, although
/// there are no protections for that.
pub struct Server {
    config: Config,
    shutdown: Phase,
}

impl Server {
    /// Create a new [`Server`] instance
    ///
    /// The inspector `Server` is responsible for investigating the
    /// [`crate::target::Server`] sub-process.
    ///
    /// # Errors
    ///
    /// Function will error if the path to the sub-process is not valid or if
    /// the path is valid but is not to file executable by this program.
    pub fn new(config: Config, shutdown: Phase) -> Result<Self, Error> {
        Ok(Self { config, shutdown })
    }

    /// Run this [`Server`] to completion
    ///
    /// This function runs the user supplied program to its completion, or until
    /// a shutdown signal is received. Child exit status does not currently
    /// propagate. This is less than ideal.
    ///
    /// Target server will use the `TargetPidReceiver` passed here to transmit
    /// its PID. This PID is passed to the sub-process in the `TARGET_PID`
    /// environment variable. This variable is not set in no-target mode.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying program cannot be waited
    /// on or will not shutdown when signaled to.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(mut self, mut pid_snd: TargetPidReceiver) -> Result<ExitStatus, Error> {
        let target_pid = pid_snd
            .recv()
            .await
            .expect("target failed to transmit PID, catastrophic failure");
        drop(pid_snd);

        let config = self.config;

        let mut target_cmd = Command::new(config.command);
        let mut environment_variables = config.environment_variables.clone();
        if let Some(pid) = target_pid {
            environment_variables.insert(String::from("TARGET_PID"), pid.to_string());
        } else {
            environment_variables.insert(String::from("NO_TARGET"), String::from("1"));
        }

        target_cmd
            .stdin(Stdio::null())
            .stdout(stdio(&config.output.stdout))
            .stderr(stdio(&config.output.stderr))
            .env_clear()
            .kill_on_drop(true)
            .args(config.arguments)
            .envs(environment_variables.iter());
        let mut target_child = target_cmd.spawn().map_err(Error::Io)?;

        let target_wait = target_child.wait();
        tokio::select! {
            res = target_wait => {
                match res {
                    Ok(status) => {
                        error!("child exited with status: {}", status);
                        Ok(status)
                    }
                    Err(err) => {
                        error!("child exited with error: {}", err);
                        Err(Error::Io(err))
                    }
                }
            },
            () = self.shutdown.recv() => {
                info!("shutdown signal received");
                // Note that `Child::kill` sends SIGKILL which is not what we
                // want. We instead send SIGTERM so that the child has a chance
                // to clean up.
                let pid: Pid = Pid::from_raw(target_child.id().expect("Process ID provided is already done").try_into().expect("Failed to convert into valid PID"));
                kill(pid, SIGTERM).map_err(Error::Errno)?;
                let res = target_child.wait().await.map_err(Error::Io)?;
                Ok(res)
            }
        }
    }
}
