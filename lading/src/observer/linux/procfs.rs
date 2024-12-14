/// Sampler implementation for procfs filesystems
mod memory;
mod stat;

use std::{
    collections::{hash_map::Entry, VecDeque},
    io,
};

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
    /// Unable to parse /proc/uptime
    #[error("/proc/uptime malformed: {0}")]
    MalformedUptime(&'static str),
    /// Unable to parse floating point
    #[error("Float Parsing: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
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
        clippy::cast_possible_wrap
    )]
    pub(crate) async fn poll(&mut self) -> Result<(), Error> {
        // A tally of the total RSS and PSS consumed by the parent process and
        // its children.
        let mut aggr = memory::smaps_rollup::Aggregator::default();

        // Every sample run we collect all the child processes rooted at the
        // parent. As noted by the procfs documentation is this done by
        // dereferencing the `/proc/<pid>/root` symlink.
        let mut pids: FxHashSet<i32> = FxHashSet::default();
        let mut processes: VecDeque<Process> = VecDeque::with_capacity(16); // an arbitrary smallish number
        processes.push_back(Process::new(self.parent.pid())?);

        // BEGIN pid loop
        while let Some(process) = processes.pop_back() {
            // Search for child processes. This is done by querying for every
            // thread of `process` and inspecting each child of the thread. Note
            // that processes on linux are those threads that have their group
            // id equal to their pid. It's also possible for a pid to list
            // itself as a child so we reference the pid hashset above to avoid
            // infinite loops.
            if let Ok(tasks) = process.tasks() {
                for task in tasks.flatten() {
                    if let Ok(mut children) = task.children() {
                        for child in children
                            .drain(..)
                            .filter_map(|c| Process::new(c as i32).ok())
                        {
                            let pid = child.pid();
                            if !pids.contains(&pid) {
                                // We have not seen this process and do need to
                                // record it for child scanning and sampling if
                                // it proves to be a process.
                                processes.push_back(child);
                                pids.insert(pid);
                            }
                        }
                    }
                }
            }

            let pid = process.pid();

            // `/proc/{pid}/status`
            let status = match process.status() {
                Ok(status) => status,
                Err(e) => {
                    warn!("Couldn't read status: {:?}", e);
                    // The pid may have exited since we scanned it or we may not
                    // have sufficient permission.
                    continue;
                }
            };
            if status.tgid != pid {
                // This is a thread, not a process and we do not wish to scan it.
                continue;
            }

            // If we haven't seen this process before, initialize its ProcessInfo.
            match self.process_info.entry(pid) {
                Entry::Occupied(_) => { /* Already initialized */ }
                Entry::Vacant(entry) => {
                    let exe = proc_exe(pid).await?;
                    let comm = proc_comm(pid).await?;
                    let cmdline = proc_cmdline(pid).await?;
                    let pid_s = format!("{pid}");
                    let stat_sampler = stat::Sampler::new();

                    entry.insert(ProcessInfo {
                        cmdline,
                        exe,
                        comm,
                        pid_s,
                        stat_sampler,
                    });
                }
            }

            // SAFETY: We've just inserted this pid into the map.
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

            let uptime = proc_uptime().await?;

            // `/proc/{pid}/stat`, most especially per-process CPU data.
            if let Err(e) = pinfo.stat_sampler.poll(pid, uptime, &labels).await {
                // We don't want to bail out entirely if we can't read stats
                // which will happen if we don't have permissions or, more
                // likely, the process has exited.
                warn!("Couldn't process `/proc/{pid}/stat`: {e}");
                continue;
            }

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

            // `/proc/{pid}/smaps_rollup`
            if let Err(err) = memory::smaps_rollup::poll(pid, &labels, &mut aggr).await {
                // We don't want to bail out entirely if we can't read smap rollup
                // which will happen if we don't have permissions or, more
                // likely, the process has exited.
                warn!("Couldn't process `/proc/{pid}/smaps_rollup`: {err}");
            }
        }
        // END pid loop

        gauge!("total_rss_bytes").set(aggr.rss as f64);
        gauge!("total_pss_bytes").set(aggr.pss as f64);

        Ok(())
    }
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

/// Read `/proc/uptime`
async fn proc_uptime() -> Result<f64, Error> {
    let buf = tokio::fs::read_to_string("/proc/uptime").await?;
    let uptime_secs = proc_uptime_inner(&buf)?;
    Ok(uptime_secs)
}

/// Parse `/proc/uptime` to extract total uptime in seconds.
///
/// # Errors
///
/// Function errors if the file is malformed.
#[inline]
fn proc_uptime_inner(contents: &str) -> Result<f64, Error> {
    // TODO this should probably be scooted up to procfs.rs. Implies the
    // `proc_*` functions there need a test component, making this an inner
    // function eventually.

    let fields: Vec<&str> = contents.split_whitespace().collect();
    if fields.is_empty() {
        return Err(Error::MalformedUptime("/proc/uptime empty"));
    }
    let uptime_secs = fields[0].parse::<f64>()?;
    Ok(uptime_secs)
}

#[cfg(test)]
mod test {
    use super::proc_uptime_inner;

    #[test]
    fn parse_uptime_basic() {
        let line = "12345.67 4321.00\n";
        let uptime = proc_uptime_inner(line).unwrap();
        assert!((uptime - 12345.67).abs() < f64::EPSILON);
    }
}
