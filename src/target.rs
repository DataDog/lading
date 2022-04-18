//! Manage the target sub-process
//!
//! The lading 'target' is the sub-process that lading inspects by pushing load
//! into from a [`crate::generator::Server`] and possibly into a
//! [`crate::blackhole::Server`]. A [`crate::inspector::Server`] is intended to
//! read operating system details about the target sub-process.
//!
//! It is lading's responsibility to start the target sub-process and shut it
//! down cleanly by signaling SIGTERM to it. If the target crashes this is also
//! detected and lading does a controlled shutdown.

use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    process::{ExitStatus, Stdio},
};

use nix::{
    errno::Errno,
    sys::signal::{kill, SIGTERM},
    unistd::Pid,
};
use tokio::process::Command;
use tracing::{error, info};

pub use crate::common::{Behavior, Output};
use crate::{common::stdio, signals::Shutdown};

#[derive(Debug)]
/// Errors produced by [`Server`]
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    Io(io::Error),
    /// Wrapper for [`nix::errno::Errno`]
    Errno(Errno),
}

#[derive(Debug)]
/// Configuration for [`Server`]
pub struct Config {
    /// The path to the target executable.
    pub command: PathBuf,
    /// Arguments for the target sub-process.
    pub arguments: Vec<String>,
    /// Environment variables to set for the target sub-process. Lading's own
    /// environment variables are not propagated to the target sub-process.
    pub environment_variables: HashMap<String, String>,
    /// Manages stderr, stdout of the target sub-process.
    pub output: Output,
}

#[derive(Debug)]
/// The target sub-process server.
///
/// This struct manages the sub-process under examination by lading. The
/// sub-process is not created until [`Server::run`] is called. It is assumed
/// that only one instance of this struct will ever exist at a time, although
/// there are no protections for that.
pub struct Server {
    config: Config,
    shutdown: Shutdown,
}

impl Server {
    /// Create a new [`Server`] instance
    ///
    /// The target `Server` is responsible for managing the sub-process under
    /// examination.
    ///
    /// # Errors
    ///
    /// Function will error if the path to the sub-process is not valid or if
    /// the path is valid but is not to file executable by this program.
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        Ok(Self { config, shutdown })
    }

    /// Run this [`Server`] to completion
    ///
    /// This function runs the user supplied program to its completion, or until
    /// a shutdown signal is received. Child exit status does not currently
    /// propagate. This is less than ideal.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying program cannot be waited
    /// on or will not shutdown when signaled to.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(mut self) -> Result<ExitStatus, Error> {
        let config = self.config;

        let mut target_cmd = Command::new(config.command);
        target_cmd
            .stdin(Stdio::null())
            .stdout(stdio(&config.output.stdout))
            .stderr(stdio(&config.output.stderr))
            .env_clear()
            .kill_on_drop(true)
            .args(config.arguments)
            .envs(config.environment_variables.iter());
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
            _ = self.shutdown.recv() => {
                info!("shutdown signal received");
                // Note that `Child::kill` sends SIGKILL which is not what we
                // want. We instead send SIGTERM so that the child has a chance
                // to clean up.
                let pid: Pid = Pid::from_raw(target_child.id().unwrap().try_into().unwrap());
                kill(pid, SIGTERM).map_err(Error::Errno)?;
                let res = target_child.wait().await.map_err(Error::Io)?;
                Ok(res)
            }
        }
    }
}
