/// Sampler implementation for procfs filesystems
mod memory;
mod stat;
mod uptime;

use std::{collections::VecDeque, io};

use metrics::gauge;
use nix::errno::Errno;
use procfs::process::Process;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{error, warn};

const BYTES_PER_KIBIBYTE: u64 = 1024;

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
        let mut aggr = memory::smaps_rollup::Aggregator::default();
        let mut processes_found: i32 = 0;
        let mut pids_skipped: FxHashSet<i32> = FxHashSet::default();

        // Clear process_info at the start of each poll to ensure fresh data
        self.process_info.clear();

        // Every sample run we collect all the child processes rooted at the
        // parent. As noted by the procfs documentation is this done by
        // dereferencing the `/proc/<pid>/root` symlink.
        // We must be sure to initialize the parent process info.
        let parent_pid = self.parent.pid();
        let parent_info = match initialize_process_info(parent_pid).await {
            Ok(Some(info)) => info,
            Ok(None) => {
                warn!("Could not initialize parent process info");
                return Ok(());
            }
            Err(e) => {
                warn!("Error initializing parent process info: {:?}", e);
                return Ok(());
            }
        };
        self.process_info.insert(parent_pid, parent_info);

        let mut processes: VecDeque<Process> = VecDeque::with_capacity(16); // an arbitrary smallish number
        processes.push_back(Process::new(parent_pid)?);

        // We need to track the pids of processes we've already seen to avoid
        // duplicate reads of process info. A child pid may be listed multiple
        // times, see below loop for details.
        let mut pids: FxHashSet<i32> = FxHashSet::default();
        pids.insert(parent_pid);

        while let Some(process) = processes.pop_back() {
            // Search for child processes. This is done by querying for every
            // thread of `process` and inspecting each child of the thread. Note
            // that processes on linux are those threads that have their group
            // id equal to their pid. It's also possible for a pid to list
            // itself as a child so we reference the pid hashset above to avoid
            // infinite loops.
            //
            // We take special care to avoid processes that are forked but not
            // exec'd. We do this because unexeced processes register the heap
            // memory of the parent process in their smaps, leading to a double
            // counting.
            if let Ok(tasks) = process.tasks() {
                for task in tasks.flatten() {
                    if let Ok(mut children) = task.children() {
                        for child in children
                            .drain(..)
                            .filter_map(|c| Process::new(c as i32).ok())
                        {
                            let pid = child.pid();
                            if !pids.contains(&pid) {
                                // This is a new process. We initialize its
                                // process info and then determine by
                                // examination of the exe/cmdline if the process
                                // is not exec'd -- meaning we will not poll it
                                // -- or if it's a process in good standing and
                                // we'll attempt to poll it.
                                let child_info = match initialize_process_info(pid).await {
                                    Ok(Some(info)) => info,
                                    Ok(None) => continue,
                                    Err(e) => {
                                        warn!(
                                            "Couldn't initialize process info for pid {}: {:?}",
                                            pid, e
                                        );
                                        continue;
                                    }
                                };

                                // Check if this is a forked but not execed process
                                let parent_info = self
                                    .process_info
                                    .get(&process.pid())
                                    .expect("parent process info should exist");

                                if child_info.exe == parent_info.exe
                                    && child_info.cmdline == parent_info.cmdline
                                {
                                    // This is a forked but not execed process.
                                    continue;
                                }

                                // Store the initialized info before adding to processes queue
                                self.process_info.insert(pid, child_info);
                                processes.push_back(child);
                                pids.insert(pid);
                            }
                        }
                    }
                }
            }

            processes_found += 1;
            let pid = process.pid();
            match self.handle_process(process, &mut aggr, include_smaps).await {
                Ok(true) => {
                    // handled successfully
                }
                Ok(false) => {
                    pids_skipped.insert(pid);
                }
                Err(e) => {
                    warn!("Encountered uncaught error when handling `/proc/{pid}/`: {e}");
                }
            }
        }

        gauge!("total_rss_bytes").set(aggr.rss as f64);
        gauge!("total_pss_bytes").set(aggr.pss as f64);
        gauge!("processes_found").set(processes_found as f64);

        // If we skipped any processes, log a warning.
        if !pids_skipped.is_empty() {
            warn!(
                "Skipped {} processes: {:?}",
                pids_skipped.len(),
                pids_skipped
            );
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
        aggr: &mut memory::smaps_rollup::Aggregator,
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
            match memory::smaps::Regions::from_pid(pid) {
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
        if let Err(err) = memory::smaps_rollup::poll(pid, &labels, aggr).await {
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
