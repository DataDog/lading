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

use nix::errno::Errno;
use serde::Deserialize;
use tokio::{self, sync::broadcast::Receiver};
use tracing;

use crate::signals::Shutdown;

#[cfg(target_os = "linux")]
use procfs::process::Process;

/// Expose the process' current RSS consumption, allowing abstractions to be
/// built on top in the Target implementation.
pub(crate) static RSS_BYTES: AtomicU64 = AtomicU64::new(0);

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

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
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

    /// Get all children of the specified process.
    ///
    /// This ignores most errors in favor of creating a best-effort list of
    /// children.
    #[cfg(target_os = "linux")]
    fn get_all_children(process: Process) -> Result<Vec<Process>, Error> {
        let tree = process
            .tasks()
            .map_err(Error::ProcError)?
            .flatten()
            .flat_map(|t| t.children())
            .flatten()
            .flat_map(TryInto::try_into)
            .flat_map(Process::new)
            .flat_map(Self::get_all_children)
            .flatten()
            .chain(std::iter::once(process))
            .collect();
        Ok(tree)
    }

    /// Get process stats for the given process and all of its children.
    #[cfg(target_os = "linux")]
    fn get_proc_stats(process: &Process) -> Result<Vec<procfs::process::Stat>, Error> {
        let target_process = Process::new(process.pid()).map_err(Error::ProcError)?;
        let target_and_children = Self::get_all_children(target_process)?;
        let stats = target_and_children
            .into_iter()
            .map(|p| p.stat())
            .collect::<Result<Vec<_>, _>>()
            .map_err(Error::ProcError)?;
        Ok(stats)
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
    #[allow(clippy::similar_names)]
    #[cfg(target_os = "linux")]
    pub async fn run(mut self, mut pid_snd: Receiver<u32>) -> Result<(), Error> {
        use std::{sync::atomic::Ordering, time::Duration};

        use metrics::gauge;
        use procfs::Uptime;

        let target_pid = pid_snd
            .recv()
            .await
            .expect("target failed to transmit PID, catastrophic failure");
        drop(pid_snd);

        let process = Process::new(target_pid.try_into().expect("PID coercion failed"))
            .map_err(Error::ProcError)?;

        let ticks_per_second: f64 = procfs::ticks_per_second() as f64;
        let page_size = procfs::page_size();

        gauge!("ticks_per_second", ticks_per_second);

        let mut procfs_delay = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = procfs_delay.tick() => {
                    if let (Ok(parent_stat), Ok(all_stats)) = (process.stat(), Self::get_proc_stats(&process)) {
                        // Calculate process uptime. We have two pieces of
                        // information from the kernel: computer uptime and
                        // process starttime relative to power-on of the
                        // computer.
                        let process_starttime_ticks: u64 = parent_stat.starttime;
                        let process_starttime_seconds: f64 = process_starttime_ticks as f64 / ticks_per_second;
                        let uptime_seconds: f64 = Uptime::new().expect("could not query uptime").uptime;
                        let process_uptime_seconds = uptime_seconds - process_starttime_seconds;

                        let cutime: u64 = all_stats.iter().map(|stat| <i64 as std::convert::TryInto<u64>>::try_into(stat.cutime).unwrap()).sum();
                        let cstime: u64 = all_stats.iter().map(|stat| <i64 as std::convert::TryInto<u64>>::try_into(stat.cstime).unwrap()).sum();
                        let utime: u64 = all_stats.iter().map(|stat| stat.utime).sum();
                        let stime: u64 = all_stats.iter().map(|stat| stat.stime).sum();

                        let kernel_time_seconds = (cstime + stime) as f64 / ticks_per_second;
                        let user_time_seconds = (cutime + utime) as f64 / ticks_per_second;

                        // The time spent in kernel-space in seconds.
                        gauge!("kernel_time_seconds", kernel_time_seconds);
                        // The time spent in user-space in seconds.
                        gauge!("user_time_seconds", user_time_seconds);
                        // The uptime of the process in fractional seconds.
                        gauge!("uptime_seconds", process_uptime_seconds);

                        let rss: u64 = all_stats.iter().fold(0, |val, stat| val.saturating_add(stat.rss));
                        let rsslim: u64 = all_stats.iter().fold(0, |val, stat| val.saturating_add(stat.rsslim));
                        let vsize: u64 = all_stats.iter().fold(0, |val, stat| val.saturating_add(stat.vsize));
                        let num_threads: u64 = all_stats.iter().map(|stat| <i64 as std::convert::TryInto<u64>>::try_into(stat.num_threads).unwrap()).sum();

                        let rss_bytes: u64 = rss*page_size;
                        RSS_BYTES.store(rss_bytes, Ordering::Relaxed);

                        // Number of pages that the process has in real memory.
                        gauge!("rss_bytes", rss_bytes as f64);
                        // Soft limit on RSS bytes, see RLIMIT_RSS in getrlimit(2).
                        gauge!("rsslim_bytes", rsslim as f64);
                        // The size in bytes of the process in virtual memory.
                        gauge!("vsize_bytes", vsize as f64);
                        // Number of threads this process has active.
                        gauge!("num_threads", num_threads as f64);

                        // Number of processes this target has active
                        gauge!("num_processes", all_stats.len() as f64);
                    }
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
    pub async fn run(self, _pid_snd: Receiver<u32>) -> Result<(), Error> {
        tracing::warn!("observer unavailable on non-Linux system");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_os = "linux")]
    fn observer_observes_process_hierarchy() {
        use super::*;
        use std::{process::Command, time::Duration};

        let mut test_proc = Command::new("/bin/sh")
            .args(["-c", "sleep 1"])
            .spawn()
            .expect("launch child process");

        // wait for `sh` to launch `sleep`
        std::thread::sleep(Duration::from_millis(250));

        let proc =
            Process::new(test_proc.id().try_into().unwrap()).expect("create Process from PID");
        let stats = Server::get_proc_stats(&proc).expect("get proc stat hierarchy");

        test_proc.kill().unwrap();

        let mut bins = stats.iter().map(|s| s.comm.clone()).collect::<Vec<_>>();
        bins.sort();

        assert_eq!(&bins, &[String::from("sh"), String::from("sleep")]);
    }
}
