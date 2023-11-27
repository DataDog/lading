//! Manage the target observer
//!
//! The interogation that lading does of the target sub-process is intentionally
//! limited to in-process concerns, for the most part. The 'inspector' does
//! allow for a sub-process to do out-of-band inspection of the target but
//! cannot incorporate whatever it's doing into the capture data that lading
//! produces. This observer, on Linux, looks up the target process in procfs and
//! writes out key details about memory and CPU consumption into the capture
//! data. On non-Linux systems the observer, if enabled, will emit a warning.

use std::{io, sync::atomic::AtomicU64};

use crate::target::TargetPidReceiver;
use serde::Deserialize;

use crate::signals::Shutdown;

#[cfg(target_os = "linux")]
mod linux;

#[allow(dead_code)] // used on Linux
/// Expose the process' current RSS consumption, allowing abstractions to be
/// built on top in the Target implementation.
pub(crate) static RSS_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Server`]
pub enum Error {
    /// Wrapper for [`nix::errno::Errno`]
    #[error("erno: {0}")]
    Errno(#[from] nix::errno::Errno),
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[cfg(target_os = "linux")]
    /// Wrapper for [`linux::Error`]
    #[error("Linux error: {0}")]
    Linux(#[from] linux::Error),
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Configuration for [`Server`]
pub struct Config {}

#[derive(Debug)]
/// The inspector sub-process server.
///
/// This struct manages a sub-process that can be used to do further examination
/// of the [`crate::target::Server`] by means of operating system facilities. The
/// sub-process is not created until [`Server::run`] is called. It is assumed
/// that only one instance of this struct will ever exist at a time, although
/// there are no protections for that.
pub struct Server {
    #[allow(dead_code)] // config is not actively used, left as a stub
    config: Config,
    #[allow(dead_code)] // this field is unused when target_os is not "linux"
    shutdown: Shutdown,
}

impl Server {
    /// Create a new [`Server`] instance
    ///
    /// The observer `Server` is responsible for investigating the
    /// [`crate::target::Server`] sub-process.
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
    /// Target server will use the `TargetPidReceiver` passed here to transmit
    /// its PID. This PID is passed to the sub-process as the first argument.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying program cannot be waited
    /// on or will not shutdown when signaled to.
    ///
    /// # Panics
    ///
    /// None are known.
    #[allow(
        clippy::similar_names,
        clippy::too_many_lines,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    #[cfg(target_os = "linux")]
    pub async fn run(mut self, mut pid_snd: TargetPidReceiver) -> Result<(), Error> {
        use std::time::Duration;

        use crate::observer::linux::Sampler;

        let target_pid = pid_snd
            .recv()
            .await
            .expect("target failed to transmit PID, catastrophic failure");
        drop(pid_snd);

        let target_pid = target_pid.expect("observer cannot be used in no-target mode");

        let mut sample_delay = tokio::time::interval(Duration::from_secs(1));
        let mut sampler = Sampler::new(target_pid)?;

        loop {
            tokio::select! {
                _ = sample_delay.tick() => {
                    sampler.sample()?;
                }
                _ = self.shutdown.recv() => {
                    tracing::info!("shutdown signal received");
                    return Ok(());
                }
            }
        }
    }

    /// "Run" this [`Server`] to completion
    ///
    /// On non-Linux systems, this function is a no-op that logs a warning
    /// indicating observer capabilities are unavailable on these systems.
    ///
    /// # Errors
    ///
    /// None are known.
    ///
    /// # Panics
    ///
    /// None are known.
    #[allow(clippy::unused_async)]
    #[cfg(not(target_os = "linux"))]
    pub async fn run(self, _pid_snd: TargetPidReceiver) -> Result<(), Error> {
        tracing::warn!("observer unavailable on non-Linux system");
        Ok(())
    }
}
