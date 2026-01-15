//! Observe target resource usage for lading.
//!
//! This library supports the lading binary found elsewhere in this project. It
//! samples target resource usage, primarily on Linux via procfs, and is not
//! intended for external use.

#![deny(clippy::cargo)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

use std::{io, time::Duration};

#[cfg(target_os = "linux")]
use crate::linux::Sampler;
use serde::Deserialize;
use tokio::sync::broadcast;

#[cfg(target_os = "linux")]
/// Linux-specific observer functionality for cgroups and procfs.
pub mod linux;

/// Type used to receive the target PID once it is running.
pub type TargetPidReceiver = broadcast::Receiver<Option<i32>>;

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
/// The observer sub-process server.
///
/// This struct manages a sub-process that can be used to do further examination
/// of a target process by means of operating system facilities. The sub-process
/// is not created until [`Server::run`] is called. It is assumed that only one
/// instance of this struct will ever exist at a time, although there are no
/// protections for that.
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
        sample_period: Duration,
    ) -> Result<(), Error> {
        let target_pid = pid_snd
            .recv()
            .await
            .expect("target failed to transmit PID, catastrophic failure");
        drop(pid_snd);

        let target_pid = target_pid.expect("observer cannot be used in no-target mode");

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
        _sample_period: Duration,
    ) -> Result<(), Error> {
        tracing::warn!("observer unavailable on non-Linux system");
        Ok(())
    }
}
