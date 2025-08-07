//! Manage the target observer
//!
//! The interogation that lading does of the target sub-process is intentionally
//! limited to in-process concerns, for the most part. The 'inspector' does
//! allow for a sub-process to do out-of-band inspection of the target but
//! cannot incorporate whatever it's doing into the capture data that lading
//! produces. This observer, on Linux, looks up the target process in procfs and
//! writes out key details about memory and CPU consumption into the capture
//! data. On non-Linux systems the observer, if enabled, will emit a warning.

use std::io;

use crate::target::TargetPidReceiver;
use serde::Deserialize;

#[cfg(target_os = "linux")]
mod linux;

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
    shutdown: lading_signal::Watcher,
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
    pub fn new(config: Config, shutdown: lading_signal::Watcher) -> Result<Self, Error> {
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
    pub async fn run(
        self,
        mut pid_snd: TargetPidReceiver,
        sample_period: std::time::Duration,
    ) -> Result<(), Error> {
        use crate::observer::linux::Sampler;

        let target_pid = match pid_snd.recv().await {
            Ok(pid) => pid,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                tracing::warn!("Observer lagged behind target PID broadcast, but target should be running");
                // In lag case, we don't have the PID but target should be running
                // This is not ideal but we'll continue - observer functionality will be limited
                None
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err(Error::from(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "target PID channel closed before PID was received",
                )));
            }
        };
        drop(pid_snd);

        let target_pid = match target_pid {
            Some(pid) => pid,
            None => {
                // This can happen in no-target mode or if we lagged behind the PID broadcast
                tracing::error!("Observer cannot function without target PID");
                return Err(Error::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "observer cannot be used when target PID is unavailable",
                )));
            }
        };

        let mut sample_delay = tokio::time::interval(sample_period);
        let mut sampler = Sampler::new(
            target_pid,
            vec![(String::from("focus"), String::from("target"))],
        )?;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                _ = sample_delay.tick() => {
                    sampler.sample().await?;
                }
                () = &mut shutdown_wait => {
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
    pub async fn run(
        self,
        _pid_snd: TargetPidReceiver,
        _sample_period: std::time::Duration,
    ) -> Result<(), Error> {
        tracing::warn!("observer unavailable on non-Linux system");
        Ok(())
    }
}
