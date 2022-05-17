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

use nix::errno::Errno;
use serde::Deserialize;
use tokio::{sync::broadcast::Receiver, time};
use tracing::info;

use crate::signals::Shutdown;

#[cfg(target_os = "linux")]
use procfs::process::Process;

#[derive(Debug)]
/// Errors produced by [`Server`]
pub enum Error {
    /// Wrapper for [`nix::errno::Errno`]
    Errno(Errno),
    /// Wrapper for [`std::io::Error`]
    Io(io::Error),
    #[cfg(target_os = "linux")]
    /// Wrapper for [`procfs::ProcError`]
    ProcError(procfs::ProcError),
}

#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
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
    /// Target server will use the `broadcast::Sender` passed here to transmit
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
    #[cfg(target_os = "linux")]
    pub async fn run(mut self, mut pid_snd: Receiver<u32>) -> Result<(), Error> {
        use std::time::Duration;

        use metrics::{counter, gauge};

        let target_pid = pid_snd
            .recv()
            .await
            .expect("target failed to transmit PID, catastrophic failure");
        drop(pid_snd);

        let process = Process::new(target_pid.try_into().expect("PID coercion failed"))
            .map_err(Error::ProcError)?;

        let ticks_per_second: u64 = procfs::ticks_per_second()
            .expect("cannot determine ticks per second")
            .try_into()
            .unwrap();
        let page_size: u64 = procfs::page_size()
            .expect("cannot determinte page size")
            .try_into()
            .unwrap();

        gauge!("page_size", page_size as f64);
        gauge!("ticks_per_second", ticks_per_second as f64);

        let mut procfs_delay = time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = procfs_delay.tick() => {
                    if let Ok(stat) = process.stat() {
                        // Number of pages that the process has in real memory.
                        gauge!("rss_pages", stat.rss as f64);
                        // Soft limit on RSS bytes, see RLIMIT_RSS in getrlimit(2).
                        gauge!("rsslim_bytes", stat.rsslim as f64);
                        // Number of threads this process has active.
                        gauge!("num_threads", stat.num_threads as f64);
                        // The number of ticks -- reference ticks_per_second -- that the
                        // process has spent scheduled in user-mode.
                        counter!("utime_ticks", stat.utime);
                        // The number of ticks -- reference ticks_per_second -- that the
                        // process has spent scheduled in kernel-mode.
                        counter!("stime_ticks", stat.stime);
                        // The size in bytes of the process in virtual memory.
                        counter!("vsize_bytes", stat.vsize);
                    }
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(());
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    pub async fn run(mut self, _pid_snd: Receiver<u32>) -> Result<ExitStatus, Error> {
        warn!("observer unavailable on non-Linux system");
    }
}
