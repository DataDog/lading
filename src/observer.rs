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
use nix::errno::Errno;
use serde::Deserialize;

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
    fn get_proc_stats(
        process: &Process,
    ) -> Result<Vec<(procfs::process::Stat, procfs::process::MemoryMaps)>, Error> {
        let target_process = Process::new(process.pid()).map_err(Error::ProcError)?;
        let target_and_children = Self::get_all_children(target_process)?;
        let stats = target_and_children
            .into_iter()
            .map(|p| Ok((p.stat()?, p.smaps()?)))
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
        use std::{sync::atomic::Ordering, time::Duration};

        use metrics::{gauge, register_counter, register_gauge};
        use procfs::{
            process::{Limit, LimitValue},
            Uptime,
        };

        let target_pid = pid_snd
            .recv()
            .await
            .expect("target failed to transmit PID, catastrophic failure");
        drop(pid_snd);

        let target_pid = target_pid.expect("observer cannot be used in no-target mode");

        let process = Process::new(target_pid.try_into().expect("PID coercion failed"))
            .map_err(Error::ProcError)?;

        let num_cores = procfs::CpuInfo::new()
            .map_err(Error::ProcError)?
            .num_cores() as u64; // Cores

        let ticks_per_second: u64 = procfs::ticks_per_second(); // CPU-ticks / second
        let page_size = procfs::page_size();

        gauge!("core_total", num_cores as f64);
        gauge!("ticks_per_second", ticks_per_second as f64);

        let mut procfs_delay = tokio::time::interval(Duration::from_secs(1));

        let mut prev_kernel_time_ticks = 0;
        let mut prev_user_time_ticks = 0;
        let mut prev_process_uptime_ticks = 0;

        let kernel_ticks_counter = register_counter!("kernel_ticks");
        let user_ticks_counter = register_counter!("user_ticks");
        let target_uptime_ticks_counter = register_counter!("target_uptime_ticks");
        let cpu_utilization_gauge = register_gauge!("cpu_utilization");
        let kernel_cpu_utilization_gauge = register_gauge!("kernel_cpu_utilization");
        let user_cpu_utilization_gauge = register_gauge!("user_cpu_utilization");

        loop {
            tokio::select! {
                _ = procfs_delay.tick() => {
                    if let (Ok(parent_stat), Ok(all_stats)) = (process.stat(), Self::get_proc_stats(&process)) {
                        // Calculate process uptime. We have two pieces of
                        // information from the kernel: computer uptime and
                        // process starttime relative to power-on of the
                        // computer.
                        let process_starttime_ticks: u64 = parent_stat.starttime; // ticks after system boot
                        let uptime_seconds: f64 = Uptime::new().expect("could not query uptime").uptime; // seconds since boot
                        let uptime_ticks: u64 = uptime_seconds.round() as u64 * ticks_per_second; // CPU-ticks since boot
                        let process_uptime_ticks: u64 = uptime_ticks - process_starttime_ticks;

                        // Child process wait time
                        let cutime: i64 = all_stats.iter().map(|stat| stat.0.cutime).sum();
                        let cstime: i64 = all_stats.iter().map(|stat| stat.0.cstime).sum();
                        // Parent process wait time
                        let utime: u64 = all_stats.iter().map(|stat| stat.0.utime).sum();
                        let stime: u64 = all_stats.iter().map(|stat| stat.0.stime).sum();

                        let kernel_time_ticks: u64 = cstime.unsigned_abs() + stime; // CPU-ticks
                        let user_time_ticks: u64 = cutime.unsigned_abs() + utime; // CPU-ticks

                        let process_uptime_ticks_diff = process_uptime_ticks - prev_process_uptime_ticks; // CPU-ticks
                        let kernel_time_ticks_diff = kernel_time_ticks - prev_kernel_time_ticks; // CPU-ticks
                        let user_time_ticks_diff = user_time_ticks - prev_user_time_ticks; // CPU-ticks
                        let time_ticks_diff = (kernel_time_ticks + user_time_ticks) - (prev_kernel_time_ticks + prev_user_time_ticks); // CPU-ticks

                        let user_utilization = (user_time_ticks_diff * num_cores) as f64 / process_uptime_ticks_diff as f64; // Cores
                        let kernel_utilization = (kernel_time_ticks_diff * num_cores) as f64 / process_uptime_ticks_diff as f64; // Cores
                        let cpu_utilization = (time_ticks_diff * num_cores) as f64 / process_uptime_ticks_diff as f64; // Cores

                        // The time spent in kernel-space in ticks.
                        kernel_ticks_counter.absolute(kernel_time_ticks);
                        // The time spent in user-space in ticks.
                        user_ticks_counter.absolute(user_time_ticks);
                        // The uptime of the process in CPU ticks.
                        target_uptime_ticks_counter.absolute(process_uptime_ticks);
                        // The utilization of available CPU cores in user and kernel space.
                        cpu_utilization_gauge.set(cpu_utilization);
                        // The utilization of available CPU cores in user space.
                        user_cpu_utilization_gauge.set(user_utilization);
                        // The utilization of available CPU cores in kernel space.
                        kernel_cpu_utilization_gauge.set(kernel_utilization);

                        prev_kernel_time_ticks = kernel_time_ticks;
                        prev_user_time_ticks = user_time_ticks;
                        prev_process_uptime_ticks = process_uptime_ticks;

                        let rss: u64 = all_stats.iter().fold(0, |val, stat| val.saturating_add(stat.0.rss));
                        let pss: u64 = all_stats.iter().fold(0, |val, stat| {
                            let one_proc = stat.1.iter().fold(0u64, |one_map, stat| {
                                one_map.saturating_add(stat.extension.map.get("Pss").copied().unwrap_or_default())
                            });
                            val.saturating_add(one_proc)
                        });

                        let rsslim: u64 = all_stats.iter().fold(0, |val, stat| val.saturating_add(stat.0.rsslim));
                        let vsize: u64 = all_stats.iter().fold(0, |val, stat| val.saturating_add(stat.0.vsize));
                        let num_threads: u64 = all_stats.iter().map(|stat| stat.0.num_threads).sum::<i64>().unsigned_abs();

                        let rss_bytes: u64 = rss*page_size;
                        RSS_BYTES.store(rss_bytes, Ordering::Relaxed); // stored for the purposes of throttling

                        // Number of pages that the process has in real memory.
                        gauge!("rss_bytes", rss_bytes as f64);
                        // Proportional share of bytes owned by this process and its children.
                        gauge!("pss_bytes", pss as f64);
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
    pub async fn run(self, _pid_snd: TargetPidReceiver) -> Result<(), Error> {
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

        let mut bins = stats.iter().map(|s| s.0.comm.clone()).collect::<Vec<_>>();
        bins.sort();

        assert_eq!(&bins, &[String::from("sh"), String::from("sleep")]);
    }
}
