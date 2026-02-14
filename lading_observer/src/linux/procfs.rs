/// Sampler implementation for procfs filesystems
pub mod memory;
mod stat;
mod uptime;
mod vmstat;

use std::io;

use memory::{smaps, smaps_rollup};

use metrics::{counter, gauge};
use nix::errno::Errno;
use procfs::process::Process;
use rustc_hash::FxHashMap;
use tracing::{error, warn};

use crate::linux::utils::process_descendents::ProcessDescendantsIterator;

const BYTES_PER_KIBIBYTE: u64 = 1024;

/// Determines if a child process is forked but not exec'd by comparing with its
/// parent.
///
/// When a process forks, the child initially shares memory with the parent
/// until it calls `exec` to replace its memory space with a new program. During
/// this fork-but-not-exec state, both processes have identical exe paths and
/// command lines.
///
/// This heuristic is critical for accurate memory accounting in lading. Without
/// it, we double-count memory usage because the forked child and its parent:
/// the child appears to have its own memory in `/proc/<pid>/smaps` but it's
/// actually sharing pages with the parent.
///
/// Returns true if the child is forked but not exec'd, false otherwise.
#[inline]
fn forked_but_not_execd(child: &ProcessInfo, parent: &ProcessInfo) -> bool {
    child.exe == parent.exe && child.cmdline == parent.cmdline
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by functions in this module
pub enum Error {
    /// Wrapper for [`nix::errno::Errno`]
    #[error("erno: {0}")]
    Errno(#[from] Errno),
    /// Wrapper for [`std::io::Error`]
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[cfg(target_os = "linux")]
    /// Wrapper for [`procfs::ProcError`]
    #[error("Unable to read procfs: {0}")]
    Proc(#[from] procfs::ProcError),
    /// Wrapper for [`stat::Error`]
    #[error("Unable to read stat: {0}")]
    Stat(#[from] stat::Error),
    /// Wrapper for [`uptime::Error`]
    #[error("Unable to read uptime: {0}")]
    Uptime(#[from] uptime::Error),
    /// Wrapper for [`vmstat::Error`]
    #[error("Unable to read vmstat: {0}")]
    Vmstat(#[from] vmstat::Error),
}

macro_rules! report_status_field {
    ($status:expr, $labels:expr, $field:tt) => {
        if let Some(value) = $status.$field {
            gauge!(concat!("status.", stringify!($field), "_bytes"), &$labels)
                .set((value * BYTES_PER_KIBIBYTE) as f64);
        }
    };
}

#[derive(Debug)]
struct ProcessInfo {
    cmdline: String,
    exe: String,
    comm: String,
    pid_s: String,
    stat_sampler: stat::Sampler,
}

#[derive(Debug)]
pub(crate) struct Sampler {
    parent: Process,
    process_info: FxHashMap<i32, ProcessInfo>,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32) -> Result<Self, Error> {
        let parent = Process::new(parent_pid)?;
        let process_info = FxHashMap::default();

        Ok(Self {
            parent,
            process_info,
        })
    }

    #[allow(
        clippy::similar_names,
        clippy::too_many_lines,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_lossless
    )]
    pub(crate) async fn poll(&mut self, include_smaps: bool) -> Result<(), Error> {
        // A tally of the total RSS and PSS consumed by the parent process and
        // its children.
        let mut aggr = smaps_rollup::Aggregator::default();
        let mut processes_found: u32 = 0;
        let mut processes_skipped: u32 = 0;

        // Clear process_info at the start of each poll. A process is capable of
        // changing its details in key ways between polls.
        self.process_info.clear();

        for process in ProcessDescendantsIterator::new(self.parent.pid) {
            let pid = process.pid();
            let process_info = match initialize_process_info(pid).await {
                Ok(Some(info)) => info,
                Ok(None) => {
                    warn!("Could not initialize process info, will retry.");
                    return Ok(());
                }
                Err(e) => {
                    warn!("Error initializing process info: {:?}", e);
                    return Ok(());
                }
            };

            if let Ok(stat) = process.stat() {
                let parent_pid = stat.ppid;
                if let Some(parent_info) = self.process_info.get(&parent_pid)
                    && forked_but_not_execd(&process_info, parent_info)
                {
                    counter!("process_skipped").increment(1);
                    processes_skipped += 1;
                    continue;
                }
            }

            self.process_info.insert(pid, process_info);

            processes_found += 1;
            match self.handle_process(process, &mut aggr, include_smaps).await {
                Ok(true) => {
                    // handled successfully
                }
                Ok(false) => {
                    processes_skipped += 1;
                }
                Err(e) => {
                    warn!("Encountered uncaught error when handling `/proc/{pid}/`: {e}");
                }
            }
        }

        gauge!("total_rss_bytes").set(aggr.rss as f64);
        gauge!("total_pss_bytes").set(aggr.pss as f64);
        gauge!("processes_found").set(processes_found as f64);

        // Collect system-wide virtual memory statistics
        if let Err(e) = vmstat::poll().await {
            warn!("Couldn't process `/proc/vmstat`: {e}");
        }

        // If we skipped any processes, log a warning.
        if processes_skipped > 0 {
            warn!("Skipped {} processes", processes_skipped);
        }

        Ok(())
    }

    /// Handle a process. Returns true if the process was handled successfully,
    /// false if it was skipped for any reason.
    #[allow(
        clippy::similar_names,
        clippy::too_many_lines,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap
    )]
    async fn handle_process(
        &mut self,
        process: Process,
        aggr: &mut smaps_rollup::Aggregator,
        include_smaps: bool,
    ) -> Result<bool, Error> {
        let pid = process.pid();

        // `/proc/{pid}/status`
        let status = match process.status() {
            Ok(status) => status,
            Err(e) => {
                warn!("Couldn't read status: {:?}", e);
                // The pid may have exited since we scanned it or we may not
                // have sufficient permission.
                return Ok(false);
            }
        };
        if status.tgid != pid {
            // This is a thread, not a process and we do not wish to scan it.
            return Ok(false);
        }

        // SAFETY: We've inserted process info into this map when polling for
        // child processes.
        let pinfo = self
            .process_info
            .get_mut(&pid)
            .expect("catastrophic programming error");

        let labels: [(&'static str, String); 4] = [
            ("pid", pinfo.pid_s.clone()),
            ("exe", pinfo.exe.clone()),
            ("cmdline", pinfo.cmdline.clone()),
            ("comm", pinfo.comm.clone()),
        ];

        // `/proc/{pid}/status`
        report_status_field!(status, labels, vmrss);
        report_status_field!(status, labels, rssanon);
        report_status_field!(status, labels, rssfile);
        report_status_field!(status, labels, rssshmem);
        report_status_field!(status, labels, vmdata);
        report_status_field!(status, labels, vmstk);
        report_status_field!(status, labels, vmexe);
        report_status_field!(status, labels, vmlib);

        let uptime = uptime::poll().await?;

        // `/proc/{pid}/stat`, most especially per-process CPU data.
        if let Err(e) = pinfo.stat_sampler.poll(pid, uptime, &labels).await {
            // We don't want to bail out entirely if we can't read stats
            // which will happen if we don't have permissions or, more
            // likely, the process has exited.
            warn!("Couldn't process `/proc/{pid}/stat`: {e}");
            return Ok(false);
        }

        if include_smaps {
            // `/proc/{pid}/smaps`
            match smaps::Regions::from_pid(pid) {
                Ok(memory_regions) => {
                    for (pathname, measures) in memory_regions.aggregate_by_pathname() {
                        let labels: [(&'static str, String); 5] = [
                            ("pid", pinfo.pid_s.clone()),
                            ("exe", pinfo.exe.clone()),
                            ("cmdline", pinfo.cmdline.clone()),
                            ("comm", pinfo.comm.clone()),
                            ("pathname", pathname),
                        ];
                        gauge!("smaps.rss.by_pathname", &labels).set(measures.rss as f64);
                        gauge!("smaps.pss.by_pathname", &labels).set(measures.pss as f64);
                        gauge!("smaps.swap.by_pathname", &labels).set(measures.swap as f64);
                        gauge!("smaps.size.by_pathname", &labels).set(measures.size as f64);

                        if let Some(m) = measures.private_clean {
                            gauge!("smaps.private_clean.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.private_dirty {
                            gauge!("smaps.private_dirty.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.shared_clean {
                            gauge!("smaps.shared_clean.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.shared_dirty {
                            gauge!("smaps.shared_dirty.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.referenced {
                            gauge!("smaps.referenced.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.anonymous {
                            gauge!("smaps.anonymous.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.lazy_free {
                            gauge!("smaps.lazy_free.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.anon_huge_pages {
                            gauge!("smaps.anon_huge_pages.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.shmem_pmd_mapped {
                            gauge!("smaps.shmem_pmd_mapped.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.shared_hugetlb {
                            gauge!("smaps.shared_hugetlb.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.private_hugetlb {
                            gauge!("smaps.private_hugetlb.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.file_pmd_mapped {
                            gauge!("smaps.file_pmd_mapped.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.locked {
                            gauge!("smaps.locked.by_pathname", &labels).set(m as f64);
                        }
                        if let Some(m) = measures.swap_pss {
                            gauge!("smaps.swap_pss.by_pathname", &labels).set(m as f64);
                        }
                    }
                }
                Err(err) => {
                    // We don't want to bail out entirely if we can't read stats
                    // which will happen if we don't have permissions or, more
                    // likely, the process has exited.
                    warn!("Couldn't process `/proc/{pid}/smaps`: {err}");
                }
            }
        }

        // `/proc/{pid}/smaps_rollup`
        if let Err(err) = smaps_rollup::poll(pid, &labels, aggr).await {
            // We don't want to bail out entirely if we can't read smap rollup
            // which will happen if we don't have permissions or, more
            // likely, the process has exited.
            warn!("Couldn't process `/proc/{pid}/smaps_rollup`: {err}");
            return Ok(false);
        }

        Ok(true)
    }
}

/// Initialize [`ProcessInfo`] for a process. Returns `None` if the process should be skipped.
async fn initialize_process_info(pid: i32) -> Result<Option<ProcessInfo>, Error> {
    let exe = match proc_exe(pid).await {
        Ok(exe) => exe,
        Err(e) => {
            warn!("Couldn't read exe for pid {}: {:?}", pid, e);
            return Ok(None);
        }
    };
    let comm = match proc_comm(pid).await {
        Ok(comm) => comm,
        Err(e) => {
            warn!("Couldn't read comm for pid {}: {:?}", pid, e);
            return Ok(None);
        }
    };
    let cmdline = match proc_cmdline(pid).await {
        Ok(cmdline) => cmdline,
        Err(e) => {
            warn!("Couldn't read cmdline for pid {}: {:?}", pid, e);
            return Ok(None);
        }
    };

    let pid_s = format!("{pid}");
    let stat_sampler = stat::Sampler::new();
    let info = ProcessInfo {
        cmdline,
        exe,
        comm,
        pid_s,
        stat_sampler,
    };

    Ok(Some(info))
}

/// Read `/proc/{pid}/comm`
async fn proc_comm(pid: i32) -> Result<String, Error> {
    let comm_path = format!("/proc/{pid}/comm");
    let buf = tokio::fs::read_to_string(&comm_path).await?;
    Ok(buf.trim().to_string())
}

/// Collect the 'name' of the process. This is pulled from `/proc/<pid>/exe` and
/// we take the last part of that, like posix `top` does.
async fn proc_exe(pid: i32) -> Result<String, Error> {
    let exe_path = format!("/proc/{pid}/exe");
    let exe = tokio::fs::read_link(&exe_path).await?;
    if let Some(basename) = exe.file_name() {
        Ok(String::from(
            basename
                .to_str()
                .expect("could not convert basename to str"),
        ))
    } else {
        // It's possible to have a process with no named exe. On
        // Linux systems with functional security setups it's not
        // clear _when_ this would be the case but, hey.
        Ok(String::new())
    }
}

/// Read `/proc/{pid}/cmdline`
async fn proc_cmdline(pid: i32) -> Result<String, Error> {
    let cmdline_path = format!("/proc/{pid}/cmdline");
    let buf = tokio::fs::read_to_string(&cmdline_path).await?;
    let parts: Vec<String> = buf
        .split('\0')
        .filter_map(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect();
    let res = if parts.is_empty() {
        String::from("zombie")
    } else {
        parts.join(" ")
    };
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        /// Generate a valid executable path
        fn arb_exe_path()(
            components in prop::collection::vec("[a-zA-Z0-9_-]+", 1..5),
        ) -> String {
            format!("/usr/bin/{}", components.join("/"))
        }
    }

    prop_compose! {
        /// Generate a command line with arguments
        fn arb_cmdline()(
            cmd in "[a-zA-Z0-9_-]+",
            args in prop::collection::vec("[a-zA-Z0-9_=-]+", 0..5),
        ) -> String {
            if args.is_empty() {
                cmd
            } else {
                format!("{} {}", cmd, args.join(" "))
            }
        }
    }

    prop_compose! {
        /// Generate process info for testing
        fn arb_process_info()(
            exe in arb_exe_path(),
            cmdline in arb_cmdline(),
            comm in "[a-zA-Z0-9_-]+",
            pid in 1..100000i32,
        ) -> ProcessInfo {
            ProcessInfo {
                exe,
                cmdline,
                comm,
                pid_s: pid.to_string(),
                stat_sampler: stat::Sampler::new(),
            }
        }
    }

    proptest! {
        #[test]
        fn identical_processes_are_forked_but_not_execed(
            exe in arb_exe_path(),
            cmdline in arb_cmdline(),
            comm1 in "[a-zA-Z0-9_-]+",
            comm2 in "[a-zA-Z0-9_-]+",
            pid1 in 1..100000i32,
            pid2 in 1..100000i32,
        ) {
            let parent = ProcessInfo {
                exe: exe.clone(),
                cmdline: cmdline.clone(),
                comm: comm1,
                pid_s: pid1.to_string(),
                stat_sampler: stat::Sampler::new(),
            };
            let child = ProcessInfo {
                exe,
                cmdline,
                comm: comm2,
                pid_s: pid2.to_string(),
                stat_sampler: stat::Sampler::new(),
            };

            assert!(forked_but_not_execd(&child, &parent),
                "Processes with identical exe and cmdline should be detected as forked-but-not-execed");
        }

        #[test]
        fn different_exe_means_execed(
            parent in arb_process_info(),
            mut child in arb_process_info(),
        ) {
            // Ensure child has different exe
            child.exe = format!("{}_different", parent.exe);

            assert!(!forked_but_not_execd(&child, &parent),
                "Processes with different exe paths should NOT be detected as forked-but-not-execed");
        }

        #[test]
        fn different_cmdline_means_execed(
            parent in arb_process_info(),
            mut child in arb_process_info(),
        ) {
            // Ensure child has same exe but different cmdline
            child.exe = parent.exe.clone();
            child.cmdline = format!("{} --extra-arg", parent.cmdline);

            assert!(!forked_but_not_execd(&child, &parent),
                "Processes with different cmdlines should NOT be detected as forked-but-not-execed");
        }

        #[test]
        fn empty_strings_handled_correctly(
            has_exe in prop::bool::ANY,
            has_cmdline in prop::bool::ANY,
        ) {
            let parent = ProcessInfo {
                exe: if has_exe { "/bin/test".to_string() } else { String::new() },
                cmdline: if has_cmdline { "test arg".to_string() } else { String::new() },
                comm: "test".to_string(),
                pid_s: "1".to_string(),
                stat_sampler: stat::Sampler::new(),
            };
            let child = ProcessInfo {
                exe: if has_exe { "/bin/test".to_string() } else { String::new() },
                cmdline: if has_cmdline { "test arg".to_string() } else { String::new() },
                comm: "test".to_string(),
                pid_s: "2".to_string(),
                stat_sampler: stat::Sampler::new(),
            };

            // Both have same exe and cmdline (even if empty), so should be detected
            assert!(forked_but_not_execd(&child, &parent));
        }

        #[test]
        fn whitespace_sensitivity(
            base_cmdline in arb_cmdline(),
            extra_spaces in prop::collection::vec(prop::bool::ANY, 0..3),
        ) {
            let parent = ProcessInfo {
                exe: "/bin/test".to_string(),
                cmdline: base_cmdline.clone(),
                comm: "test".to_string(),
                pid_s: "1".to_string(),
                stat_sampler: stat::Sampler::new(),
            };

            // Add extra spaces based on the bool vector
            let mut modified_cmdline = base_cmdline.clone();
            for add_space in extra_spaces {
                if add_space {
                    modified_cmdline.push(' ');
                }
            }

            let child = ProcessInfo {
                exe: "/bin/test".to_string(),
                cmdline: modified_cmdline.clone(),
                comm: "test".to_string(),
                pid_s: "2".to_string(),
                stat_sampler: stat::Sampler::new(),
            };

            // If cmdlines differ (even by whitespace), should NOT be detected as forked-but-not-execed
            if base_cmdline != modified_cmdline {
                assert!(!forked_but_not_execd(&child, &parent),
                    "Even whitespace differences should mean the process has exec'd");
            } else {
                assert!(forked_but_not_execd(&child, &parent));
            }
        }
    }
}
