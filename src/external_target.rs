//! Watch an externally-launched process
//!
//! Push load to a process while inspecting it. The target process is managed
//! externally. This is intended to be used to measure processes running in
//! containers.
//!
//! An alternative to the lading-managed binary target,
//! [`crate::target::Server`].

use std::{io, num::NonZeroU32, time::Duration};

use nix::{errno::Errno, libc::kill};
use tokio::{sync::broadcast::Sender, time::sleep};
use tracing::{error, info};

pub use crate::common::{Behavior, Output};
use crate::signals::Shutdown;

#[derive(Debug)]
/// Errors produced by [`Server`]
pub enum Error {
    /// Wrapper for [`std::io::Error`]
    Io(io::Error),
    /// Wrapper for [`nix::errno::Errno`]
    Errno(Errno),
    /// The target PID does not exist or is invalid
    PIDNotFound,
    /// The target process exited unexpectedly
    TargetExited,
}

#[allow(missing_copy_implementations)]
#[derive(Debug)]
/// Configuration for [`Server`]
pub struct Config {
    /// PID to watch
    pub pid: NonZeroU32,
}

#[derive(Debug)]
/// The external target sub-process server.
pub struct Server {
    config: Config,
    shutdown: Shutdown,
}

impl Server {
    /// Create a new [`Server`] instance
    ///
    /// The target `Server` is responsible for watching the external process
    /// under observation.
    #[must_use]
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        Self { config, shutdown }
    }

    /// Run this [`Server`] to completion
    ///
    /// This function waits for either a shutdown signal or the exit of the
    /// externally-launched process. Child exit status is not currently
    /// recorded.
    ///
    /// # Errors
    ///
    /// Function will return an error if the target PID does not exist or if the
    /// target process exits.
    ///
    /// # Panics
    ///
    /// None are known.
    pub async fn run(mut self, pid_snd: Sender<u32>) -> Result<(), Error> {
        let config = self.config;

        let pid = config
            .pid
            .get()
            .try_into()
            .map_err(|_| Error::PIDNotFound)?; // Invalid PID: PID_MAX_LIMIT is 2^22

        // Safety: no safety concerns
        let ret = unsafe { kill(pid, 0) };
        if ret != 0 {
            return Err(Error::PIDNotFound);
        }

        pid_snd
            .send(config.pid.get())
            .expect("target server unable to transmit PID, catastrophic failure");
        drop(pid_snd);

        // TODO: Wait for PID to exit and receive exit code on linux.
        // Try https://crates.io/crates/async-pidfd
        let target_wait = async {
            loop {
                // Safety: no safety concerns
                let ret = unsafe { kill(pid, 0) };
                if ret != 0 {
                    break;
                }
                sleep(Duration::from_secs(1)).await;
            }
        };

        tokio::select! {
            _ = target_wait => {
                error!("child exited unexpectedly");
                Err(Error::TargetExited)
            },
            _ = self.shutdown.recv() => {
                info!("shutdown signal received");
                Ok(())
            }
        }
    }
}
