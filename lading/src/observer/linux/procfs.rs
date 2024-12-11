/// Sampler implementation for procfs filesystems
mod memory;

use std::{collections::VecDeque, io};

use metrics::gauge;
use nix::errno::Errno;
use procfs::ProcError::PermissionDenied;
use procfs::{process::Process, Current};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{error, info, warn};

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
}

macro_rules! report_status_field {
    ($status:expr, $labels:expr, $field:tt) => {
        if let Some(value) = $status.$field {
            gauge!(concat!("status.", stringify!($field), "_bytes"), &$labels)
                .set((value * BYTES_PER_KIBIBYTE) as f64);
        }
    };
}

#[derive(Debug, Default)]
struct Sample {
    utime: u64,
    stime: u64,
    uptime: u64,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ProcessIdentifier {
    pid: i32,
    exe: String,
    cmdline: String,
    comm: String,
}

#[derive(Debug)]
pub(crate) struct Sampler {
    parent: Process,
    ticks_per_second: u64,
    page_size: u64,
    previous_totals: Sample,
    have_logged_perms_err: bool,
}

impl Sampler {
    pub(crate) fn new(parent_pid: i32) -> Result<Self, Error> {
        let parent = Process::new(parent_pid)?;

        Ok(Self {
            parent,
            ticks_per_second: procfs::ticks_per_second(),
            page_size: procfs::page_size(),
            previous_totals: Sample::default(),
            have_logged_perms_err: false,
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
        // Key for this map is (pid, basename/exe, cmdline)
        let mut samples: FxHashMap<ProcessIdentifier, Sample> = FxHashMap::default();

        let mut total_processes: u64 = 0;
        // Calculate the ticks since machine uptime. This will be important
        // later for calculating per-process uptime. Because we capture this one
        // we will be slightly out of date with each subsequent iteration of the
        // loop. We do not believe this to be an issue.
        let uptime_seconds: f64 = procfs::Uptime::current()?.uptime; // seconds since boot
        let uptime_ticks: u64 =
            (uptime_seconds.round() as u64).saturating_mul(self.ticks_per_second); // CPU-ticks since boot

        // A tally of the total RSS and PSS consumed by the parent process and
        // its children.
        let mut aggr = memory::smaps_rollup::Aggregator::default();

        // Every sample run we collect all the child processes rooted at the
        // parent. As noted by the procfs documentation is this done by
        // dereferencing the `/proc/<pid>/root` symlink.
        let mut pids: FxHashSet<i32> = FxHashSet::default();
        let mut processes: VecDeque<Process> = VecDeque::with_capacity(16); // an arbitrary smallish number
        processes.push_back(Process::new(self.parent.pid())?);
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
            let mut has_ptrace_perm = true;
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

            // Collect the 'name' of the process. This is pulled from
            // /proc/<pid>/exe and we take the last part of that, like posix
            // `top` does. This will require us to label all data with both pid
            // and name, again like `top`.
            let basename: String = match process.exe() {
                Ok(exe) => {
                    if let Some(basename) = exe.file_name() {
                        String::from(
                            basename
                                .to_str()
                                .expect("could not convert basename to str"),
                        )
                    } else {
                        // It's possible to have a process with no named exe. On
                        // Linux systems with functional security setups it's not
                        // clear _when_ this would be the case but, hey.
                        String::new()
                    }
                }
                Err(PermissionDenied(_)) => {
                    // permission to dereference or read this symbolic link is governed
                    // by a ptrace(2) access mode PTRACE_MODE_READ_FSCREDS check
                    //
                    // In practice, this can occur when an unprivileged lading process
                    // is given a container to monitor as the container pids are owned by root
                    has_ptrace_perm = false;
                    if !self.have_logged_perms_err {
                        error!("lading lacks ptrace permissions, exe will be empty and smaps-related metrics will be missing.");
                        self.have_logged_perms_err = true;
                    }
                    String::new()
                }
                Err(NotFound(_)) => {
                    // The pid may have exited since we scanned it
                    info!("Failed to read exe symlink from a non-existent process. Skipping to the next process: {:?}", e);
                    continue;
                }
                Err(e) => {
                    warn!("Couldn't read exe symlink: {:?}", e);
                    // This is an unknown failure case
                    String::new()
                }
            };

            let cmdline: String = match process.cmdline() {
                Ok(cmdline) => cmdline.join(" "),
                Err(_) => "zombie".to_string(),
            };

            let stats = match process.stat() {
                Ok(stats) => stats,
                Err(e) => {
                    // We don't want to bail out entirely if we can't read stats
                    // which will happen if we don't have permissions or, more
                    // likely, the process has exited.
                    warn!("Got err when reading process stat: {e:?}");
                    continue;
                }
            };
            let comm = stats.comm.clone();

            // Calculate process uptime. We have two pieces of information from
            // the kernel: computer uptime and process starttime relative to
            // power-on of the computer.
            let uptime: u64 = uptime_ticks.saturating_sub(stats.starttime); // ticks

            // The times that the process and the processes' waited for children
            // have been scheduled in kernel and user space. We exclude cstime,
            // cutime because while the parent has waited it has not spent CPU
            // time, it's children have.
            let utime: u64 = stats.utime; // CPU-ticks
            let stime: u64 = stats.stime; // CPU-ticks

            let sample = Sample {
                utime,
                stime,
                uptime,
            };
            let id = ProcessIdentifier {
                pid,
                exe: basename.clone(),
                comm: comm.clone(),
                cmdline: cmdline.clone(),
            };
            samples.insert(id, sample);

            // Answering the question "How much memory is my program consuming?"
            // is not as straightforward as one might hope. Reside set size
            // (RSS) is the amount of memory held in memory measured in bytes,
            // Proportional Set Size (PSS) is the amount of memory held by the
            // program but unshared between processes (think data mmapped
            // multiple times), Virtual Size (vsize) is the amount of memory
            // held in pages, which may or may not be reflected in real memory.
            // VSize is often much, much larger than RSS.
            //
            // We currently do not pull PSS as it requires trawling the smaps
            // but we don't have call to do that, avoiding an allocation.
            //
            // Consider that Linux allocation is done in pages. If I allocate 1
            // byte, say, from the OS I will receive a page of memory back --
            // see `page_size` for the size of a page -- and the RSS and VSize
            // of my program is then a page worth of bytes. If I deallocate that
            // byte my RSS is 0 but _as an optimization_ Linux may not free the
            // page. My VSize remains one page. Allocators muddy this even
            // further by trying to account for the behavior of the operating
            // system. Anyway, good luck out there. You'll be fine.
            let rss: u64 = stats.rss * self.page_size;
            let rsslim: u64 = stats.rsslim;
            let vsize: u64 = stats.vsize;

            let labels = [
                (String::from("pid"), format!("{pid}")),
                (String::from("exe"), basename.clone()),
                (String::from("cmdline"), cmdline.clone()),
                (String::from("comm"), comm.clone()),
            ];

            // Number of pages that the process has in real memory.
            gauge!("rss_bytes", &labels).set(rss as f64);
            // Soft limit on RSS bytes, see RLIMIT_RSS in getrlimit(2).
            gauge!("rsslim_bytes", &labels).set(rsslim as f64);
            // The size in bytes of the process in virtual memory.
            gauge!("vsize_bytes", &labels).set(vsize as f64);
            // Number of threads this process has active.
            gauge!("num_threads", &labels).set(stats.num_threads as f64);

            total_processes = total_processes.saturating_add(1);

            // Also report memory data from `proc/status` as a reference point
            report_status_field!(status, labels, vmrss);
            report_status_field!(status, labels, rssanon);
            report_status_field!(status, labels, rssfile);
            report_status_field!(status, labels, rssshmem);
            report_status_field!(status, labels, vmdata);
            report_status_field!(status, labels, vmstk);
            report_status_field!(status, labels, vmexe);
            report_status_field!(status, labels, vmlib);

            // smaps and smaps_rollup are both governed by the same permission
            // restrictions as /proc/[pid]/maps. Per man 5 proc: Permission to
            // access this file is governed by a ptrace access mode
            // PTRACE_MODE_READ_FSCREDS check; see ptrace(2). If a previous call
            // to process.status() failed due to a lack of ptrace permissions,
            // then we assume smaps operations will fail as well.
            if has_ptrace_perm {
                // `/proc/{pid}/smaps`
                match memory::smaps::Regions::from_pid(pid) {
                    Ok(memory_regions) => {
                        for (pathname, measures) in memory_regions.aggregate_by_pathname() {
                            let labels = [
                                ("pid", format!("{pid}")),
                                ("exe", basename.clone()),
                                ("cmdline", cmdline.clone()),
                                ("comm", comm.clone()),
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
                        warn!(
                            "Couldn't process `/proc/{pid}/smaps`, skipping to next process: {err}"
                        );
                        continue;
                    }
                }

                // `/proc/{pid}/smaps_rollup`
                if let Err(err) = memory::smaps_rollup::poll(pid, &labels, &mut aggr).await {
                    // We don't want to bail out entirely if we can't read smap rollup
                    // which will happen if we don't have permissions or, more
                    // likely, the process has exited.

                    // For the later we expect to see one of two IO errors: The first being "No such process" (linux os error code 3)
                    // which in Rust's implementation is mapped to std::io::ErrorKind::Uncategorized. The second being
                    // "No such file or directory" (linux os error code 2) which is mapped to std::io::ErrorKind::NotFound.
                    // This explanation is justifiaction for checking the raw os error in the subsequent match rather than
                    // just filtering on ErrorKind::NotFound.
                    match err {
                        Error::Io(io_err) => {
                            if let Some(raw_os_error) = io_err.raw_os_error() {
                                match raw_os_error {
                                    2 | 3 => {
                                        // Handle specific linux OS errors: 2 (ENOENT) and 3 (ESRCH)
                                        warn!("Found linux OS error ({raw_os_error}) while processing `/proc/{pid}/smaps_rollup`, continuing to next pid: {io_err}");
                                        continue;
                                    }
                                    _ => {
                                        warn!("IO error while processing `/proc/{pid}/smaps_rollup`: {io_err}");
                                    }
                                }
                            } else {
                                warn!("Unhandled IO error without raw OS error code while processing `/proc/{pid}/smaps_rollup`: {io_err}");
                            }
                        }
                        _ => {
                            warn!("Couldn't process `/proc/{pid}/smaps_rollup`: {err}");
                        }
                    }
                }
            }
        }

        gauge!("num_processes").set(total_processes as f64);

        let total_sample = samples
            .iter()
            .fold(Sample::default(), |acc, (key, sample)| {
                let ProcessIdentifier {
                    pid,
                    exe: _,
                    cmdline: _,
                    comm: _,
                } = key;

                Sample {
                    utime: acc.utime.saturating_add(sample.utime),
                    stime: acc.stime.saturating_add(sample.stime),
                    // use parent process uptime
                    uptime: if *pid == self.parent.pid() {
                        sample.uptime
                    } else {
                        acc.uptime
                    },
                }
            });

        gauge!("total_rss_bytes").set(aggr.rss as f64);
        gauge!("total_pss_bytes").set(aggr.pss as f64);
        gauge!("total_utime").set(total_sample.utime as f64);
        gauge!("total_stime").set(total_sample.stime as f64);

        self.previous_totals = total_sample;

        Ok(())
    }
}
