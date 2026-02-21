/// Code to read cgroup information.
pub mod v2;

use std::{collections::VecDeque, io, path::PathBuf};

use v2::cpu;

use nix::errno::Errno;
use procfs::process::Process;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, error, trace};

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
    /// Wrapper for [`v2::Error`]
    #[error("Unable to read cgroup: {0}")]
    CGroup(#[from] v2::Error),
}

#[derive(Debug)]
struct CgroupInfo {
    cpu_sampler: cpu::Sampler,
}

#[derive(Debug)]
pub(crate) struct Sampler {
    parent: Process,
    cgroup_info: FxHashMap<PathBuf, CgroupInfo>,
    labels: Vec<(String, String)>,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32, labels: Vec<(String, String)>) -> Result<Self, Error> {
        let parent = Process::new(parent_pid)?;
        let cgroup_info = FxHashMap::default();

        Ok(Self {
            parent,
            cgroup_info,
            labels,
        })
    }

    #[allow(clippy::cast_possible_wrap)]
    pub(crate) async fn poll(&mut self) -> Result<(), Error> {
        // Every sample run we collect all the child processes rooted at the
        // parent. As noted by the procfs documentation is this done by
        // dereferencing the `/proc/<pid>/root` symlink.
        debug!(
            "Polling procfs for child processes: {pid}",
            pid = self.parent.pid()
        );
        let mut pids: FxHashSet<i32> = FxHashSet::default();
        {
            let mut processes: VecDeque<Process> = VecDeque::with_capacity(16); // an arbitrary smallish number
            match Process::new(self.parent.pid()) {
                Ok(parent) => {
                    pids.insert(parent.pid());
                    processes.push_back(parent);
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
                    }
                }
                Err(err) => {
                    error!(
                        "Unable to read parent process {pid}: {err}",
                        pid = self.parent.pid()
                    );
                }
            }

            trace!("Found {count} processes", count = pids.len());
            // Now iterate the pids and collect the unique names of the cgroups associated.
            let mut cgroups = FxHashSet::default();
            for pid in pids {
                debug!("Polling cgroup for pid {pid}", pid = pid);
                match v2::get_path(pid).await {
                    Ok(cgroup_path) => {
                        cgroups.insert(cgroup_path);
                    }
                    Err(err) => {
                        error!("Unable to get cgroup path for pid {pid}: {err}");
                    }
                }
            }

            // Now iterate the cgroups and collect samples.
            for cgroup_path in cgroups {
                // If we haven't seen this cgroup before, initialize its CgroupInfo.
                let cinfo = self
                    .cgroup_info
                    .entry(cgroup_path.clone())
                    .or_insert_with(|| CgroupInfo {
                        cpu_sampler: cpu::Sampler::new(),
                    });

                if let Err(err) = v2::poll(&cgroup_path, &self.labels).await {
                    error!(
                        "Unable to poll cgroup memory metrics for {path}: {err}",
                        path = cgroup_path.to_string_lossy()
                    );
                }

                if let Err(err) = cinfo.cpu_sampler.poll(&cgroup_path, &self.labels).await {
                    error!(
                        "Unable to poll cgroup CPU metrics for {path}: {err}",
                        path = cgroup_path.to_string_lossy()
                    );
                }
            }
            Ok(())
        }
    }
}
