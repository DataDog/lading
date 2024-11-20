use std::{collections::VecDeque, io, path::Path, sync::atomic::Ordering};

use cgroups_rs::cgroup::Cgroup;
use metrics::gauge;
use nix::errno::Errno;
use procfs::process::Process;
use procfs::ProcError::PermissionDenied;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{error, warn};

use crate::observer::memory::{Regions, Rollup};

use super::RSS_BYTES;

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
    #[error("Unable to read cgroups: {0}")]
    CGroups(#[from] cgroups_rs::error::Error),
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
    num_cores: usize,
    ticks_per_second: u64,
    page_size: u64,
    previous_samples: FxHashMap<ProcessIdentifier, Sample>,
    previous_totals: Sample,
    previous_gauges: Vec<Gauge>,
    have_logged_perms_err: bool,
}

struct Gauge(metrics::Gauge);

impl std::fmt::Debug for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gauge").finish_non_exhaustive()
    }
}

impl From<metrics::Gauge> for Gauge {
    fn from(gauge: metrics::Gauge) -> Self {
        Self(gauge)
    }
}

impl Gauge {
    #[inline]
    fn set(&self, value: f64) {
        self.0.set(value);
    }
}

impl Sampler {
    pub(crate) fn new(parent_pid: u32) -> Result<Self, Error> {
        let parent = Process::new(parent_pid.try_into().expect("PID coercion failed"))?;

        Ok(Self {
            parent,
            num_cores: num_cpus::get(), // Cores, logical on Linux, obeying cgroup limits if present
            ticks_per_second: procfs::ticks_per_second(),
            page_size: procfs::page_size(),
            previous_samples: FxHashMap::default(),
            previous_totals: Sample::default(),
            previous_gauges: Vec::default(),
            have_logged_perms_err: false,
        })
    }

    #[allow(
        clippy::similar_names,
        clippy::too_many_lines,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap
    )]
    pub(crate) async fn sample(&mut self) -> Result<(), Error> {
        let mut joinset = tokio::task::JoinSet::new();
        // Key for this map is (pid, basename/exe, cmdline)
        let mut samples: FxHashMap<ProcessIdentifier, Sample> = FxHashMap::default();

        let mut total_processes: u64 = 0;
        // We maintain a tally of the total RSS consumed by the parent process
        // and its children. This is used for analysis and for cooperation with
        // the throttle (through RSS_BYTES).
        let mut total_rss: u64 = 0;

        // Calculate the ticks since machine uptime. This will be important
        // later for calculating per-process uptime. Because we capture this one
        // we will be slightly out of date with each subsequent iteration of the
        // loop. We do not believe this to be an issue.
        let uptime_seconds: f64 = procfs::Uptime::new()
            .expect("could not query machine uptime")
            .uptime; // seconds since boot
        let uptime_ticks: u64 = uptime_seconds.round() as u64 * self.ticks_per_second; // CPU-ticks since boot

        // Clear values from previous sample run. This ensures that processes
        // that no longer exist will be reported with a 0 value.
        for gauge in self.previous_gauges.drain(..) {
            gauge.set(0.0);
        }

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
                ("pid", format!("{pid}")),
                ("exe", basename.clone()),
                ("cmdline", cmdline.clone()),
                ("comm", comm.clone()),
            ];

            // Number of pages that the process has in real memory.
            let rss_gauge = gauge!("rss_bytes", &labels);
            rss_gauge.set(rss as f64);
            self.previous_gauges.push(rss_gauge.into());
            // Soft limit on RSS bytes, see RLIMIT_RSS in getrlimit(2).
            let rsslim_gauge = gauge!("rsslim_bytes", &labels);
            rsslim_gauge.set(rsslim as f64);
            self.previous_gauges.push(rsslim_gauge.into());
            // The size in bytes of the process in virtual memory.
            let vsize_gauge = gauge!("vsize_bytes", &labels);
            vsize_gauge.set(vsize as f64);
            self.previous_gauges.push(vsize_gauge.into());
            // Number of threads this process has active.
            let num_threads_gauge = gauge!("num_threads", &labels);
            num_threads_gauge.set(stats.num_threads as f64);
            self.previous_gauges.push(num_threads_gauge.into());

            total_rss += rss;
            total_processes += 1;

            // Also report memory data from `proc/status` as a reference point
            report_status_field!(status, labels, vmrss);
            report_status_field!(status, labels, rssanon);
            report_status_field!(status, labels, rssfile);
            report_status_field!(status, labels, rssshmem);
            report_status_field!(status, labels, vmdata);
            report_status_field!(status, labels, vmstk);
            report_status_field!(status, labels, vmexe);
            report_status_field!(status, labels, vmlib);

            // smaps and smaps_rollup are both governed by the same permission restrictions
            // as /proc/[pid]/maps. Per man 5 proc:
            // Permission to access this file is governed by a ptrace access mode PTRACE_MODE_READ_FSCREDS check; see ptrace(2).
            // If a previous call to process.status() failed due to a lack of ptrace permissions,
            // then we assume smaps operations will fail as well.
            if has_ptrace_perm {
                joinset.spawn(async move {
                    let memory_regions = match Regions::from_pid(pid) {
                        Ok(memory_regions) => memory_regions,
                        Err(e) => {
                            // We don't want to bail out entirely if we can't read stats
                            // which will happen if we don't have permissions or, more
                            // likely, the process has exited.
                            warn!("Couldn't process `/proc/{pid}/smaps`: {}", e);
                            return;
                        }
                    };
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
                        gauge!("smaps.size.by_pathname", &labels).set(measures.size as f64);
                        gauge!("smaps.swap.by_pathname", &labels).set(measures.swap as f64);
                        gauge!("smaps.anon_huge_pages.by_pathname", &labels)
                            .set(measures.anon_huge_pages as f64);
                    }

                    let measures = memory_regions.aggregate();
                    let labels = [
                        ("pid", format!("{pid}")),
                        ("exe", basename.clone()),
                        ("comm", comm.clone()),
                        ("cmdline", cmdline.clone()),
                    ];

                    gauge!("smaps.rss.sum", &labels).set(measures.rss as f64);
                    gauge!("smaps.pss.sum", &labels).set(measures.pss as f64);
                    gauge!("smaps.size.sum", &labels).set(measures.size as f64);
                    gauge!("smaps.swap.sum", &labels).set(measures.swap as f64);

                    let rollup = match Rollup::from_pid(pid) {
                        Ok(rollup) => rollup,
                        Err(e) => {
                            // We don't want to bail out entirely if we can't read smap rollup
                            // which will happen if we don't have permissions or, more
                            // likely, the process has exited.
                            warn!("Couldn't process `/proc/{pid}/smaps_rollup`: {}", e);
                            return;
                        }
                    };

                    gauge!("smaps_rollup.rss", &labels).set(rollup.rss as f64);
                    gauge!("smaps_rollup.pss", &labels).set(rollup.pss as f64);
                    if let Some(v) = rollup.pss_dirty {
                        gauge!("smaps_rollup.pss_dirty", &labels).set(v as f64);
                    }
                    if let Some(v) = rollup.pss_anon {
                        gauge!("smaps_rollup.pss_anon", &labels).set(v as f64);
                    }
                    if let Some(v) = rollup.pss_file {
                        gauge!("smaps_rollup.pss_file", &labels).set(v as f64);
                    }
                    if let Some(v) = rollup.pss_shmem {
                        gauge!("smaps_rollup.pss_shmem", &labels).set(v as f64);
                    }
                    if let Some(v) = rollup.anon_huge_pages {
                        gauge!("smaps_rollup.anon_huge_pages", &labels).set(v as f64);
                    }
                });
            }

            // if possible we compute the working set of the cgroup
            // using the same heuristic as kubernetes:
            // total_usage - inactive_file
            let cgroup = get_cgroup(pid as _)?;
            if let Some(memory_controller) =
                cgroup.controller_of::<cgroups_rs::memory::MemController>()
            {
                let mem_stat = memory_controller.memory_stat();

                let inactive_file = if cgroup.v2() {
                    mem_stat.stat.inactive_file
                } else {
                    mem_stat.stat.total_inactive_file
                };
                let usage = mem_stat.usage_in_bytes;
                let working_set = if usage < inactive_file {
                    0
                } else {
                    usage - inactive_file
                };

                gauge!("working_set_bytes").set(working_set as f64);
                gauge!("memory.working_set_bytes").set(working_set as f64);

                gauge!("memory.fail_cnt").set(mem_stat.fail_cnt as f64);
                gauge!("memory.limit_bytes").set(mem_stat.limit_in_bytes as f64);
                gauge!("memory.usage_in_bytes").set(mem_stat.usage_in_bytes as f64);
                gauge!("memory.max_usage_in_bytes").set(mem_stat.max_usage_in_bytes as f64);
                gauge!("memory.soft_limit_in_bytes").set(mem_stat.soft_limit_in_bytes as f64);

                gauge!("memory.stat.cache").set(mem_stat.stat.cache as f64);
                gauge!("memory.stat.rss").set(mem_stat.stat.rss as f64);
                gauge!("memory.stat.rss_huge").set(mem_stat.stat.rss_huge as f64);
                gauge!("memory.stat.shmem").set(mem_stat.stat.shmem as f64);
                gauge!("memory.stat.mapped_file").set(mem_stat.stat.mapped_file as f64);
                gauge!("memory.stat.dirty").set(mem_stat.stat.dirty as f64);
                gauge!("memory.stat.writeback").set(mem_stat.stat.writeback as f64);
                gauge!("memory.stat.swap").set(mem_stat.stat.swap as f64);
                gauge!("memory.stat.pgpgin").set(mem_stat.stat.pgpgin as f64);
                gauge!("memory.stat.pgpgout").set(mem_stat.stat.pgpgout as f64);
                gauge!("memory.stat.pgfault").set(mem_stat.stat.pgfault as f64);
                gauge!("memory.stat.pgmajfault").set(mem_stat.stat.pgmajfault as f64);
                gauge!("memory.stat.inactive_anon").set(mem_stat.stat.inactive_anon as f64);
                gauge!("memory.stat.active_anon").set(mem_stat.stat.active_anon as f64);
                gauge!("memory.stat.inactive_file").set(mem_stat.stat.inactive_file as f64);
                gauge!("memory.stat.active_file").set(mem_stat.stat.active_file as f64);
                gauge!("memory.stat.unevictable").set(mem_stat.stat.unevictable as f64);
                gauge!("memory.stat.hierarchical_memory_limit")
                    .set(mem_stat.stat.hierarchical_memory_limit as f64);
                gauge!("memory.stat.hierarchical_memsw_limit")
                    .set(mem_stat.stat.hierarchical_memsw_limit as f64);
                gauge!("memory.stat.total_cache").set(mem_stat.stat.total_cache as f64);
                gauge!("memory.stat.total_rss").set(mem_stat.stat.total_rss as f64);
                gauge!("memory.stat.total_rss_huge").set(mem_stat.stat.total_rss_huge as f64);
                gauge!("memory.stat.total_shmem").set(mem_stat.stat.total_shmem as f64);
                gauge!("memory.stat.total_mapped_file").set(mem_stat.stat.total_mapped_file as f64);
                gauge!("memory.stat.total_dirty").set(mem_stat.stat.total_dirty as f64);
                gauge!("memory.stat.total_writeback").set(mem_stat.stat.total_writeback as f64);
                gauge!("memory.stat.total_swap").set(mem_stat.stat.total_swap as f64);
                gauge!("memory.stat.total_pgpgin").set(mem_stat.stat.total_pgpgin as f64);
                gauge!("memory.stat.total_pgpgout").set(mem_stat.stat.total_pgpgout as f64);
                gauge!("memory.stat.total_pgfault").set(mem_stat.stat.total_pgfault as f64);
                gauge!("memory.stat.total_pgmajfault").set(mem_stat.stat.total_pgmajfault as f64);
                gauge!("memory.stat.total_inactive_anon")
                    .set(mem_stat.stat.total_inactive_anon as f64);
                gauge!("memory.stat.total_active_anon").set(mem_stat.stat.total_active_anon as f64);
                gauge!("memory.stat.total_inactive_file")
                    .set(mem_stat.stat.total_inactive_file as f64);
                gauge!("memory.stat.total_active_file").set(mem_stat.stat.total_active_file as f64);
                gauge!("memory.stat.total_unevictable").set(mem_stat.stat.total_unevictable as f64);
            }
            // Load the CPU controller and get the cpu.stat String out of the
            // cgroup, parse whatever fields are present and report them back
            // out as metrics.
            if let Some(cpu_controller) = cgroup.controller_of::<cgroups_rs::cpu::CpuController>() {
                let cpu = cpu_controller.cpu();
                for line in cpu.stat.lines() {
                    let mut fields = line.split_whitespace();
                    let metric_name = fields.next().unwrap_or_default();
                    let value = fields.next().unwrap_or_default();
                    gauge!(format!("cpu.{metric_name}"))
                        .set(value.parse::<f64>().unwrap_or_default());
                }
                if let Ok(shares) = cpu_controller.shares() {
                    gauge!("cpu.shares").set(shares as f64);
                }
                if let Ok(cfs_period) = cpu_controller.cfs_period() {
                    gauge!("cpu.cfs_period").set(cfs_period as f64);
                }
                if let Ok(cfs_quota) = cpu_controller.cfs_quota() {
                    gauge!("cpu.cfs_quota").set(cfs_quota as f64);
                }
            }
        }

        gauge!("num_processes").set(total_processes as f64);
        RSS_BYTES.store(total_rss, Ordering::Relaxed); // stored for the purposes of throttling

        // Now we loop through our just collected samples and calculate CPU
        // utilization. This require memory and we will now reference -- and
        // update, when done -- the previous samples.
        for (key, sample) in &samples {
            let prev = self.previous_samples.remove(key).unwrap_or_default();

            let calc = calculate_cpu_percentage(sample, &prev, self.num_cores);

            let ProcessIdentifier {
                pid,
                exe,
                cmdline,
                comm,
            } = key;

            let labels = [
                ("pid", format!("{pid}", pid = pid.clone())),
                ("exe", exe.clone()),
                ("cmdline", cmdline.clone()),
                ("comm", comm.clone()),
            ];

            let cpu_gauge = gauge!("cpu_percentage", &labels);
            cpu_gauge.set(calc.cpu);
            self.previous_gauges.push(cpu_gauge.into());
            let kernel_gauge = gauge!("kernel_cpu_percentage", &labels);
            kernel_gauge.set(calc.kernel);
            self.previous_gauges.push(kernel_gauge.into());
            let user_cpu_gauge = gauge!("user_cpu_percentage", &labels);
            user_cpu_gauge.set(calc.user);
            self.previous_gauges.push(user_cpu_gauge.into());
        }

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
                    utime: acc.utime + sample.utime,
                    stime: acc.stime + sample.stime,
                    // use parent process uptime
                    uptime: if *pid == self.parent.pid() {
                        sample.uptime
                    } else {
                        acc.uptime
                    },
                }
            });

        let totals = calculate_cpu_percentage(&total_sample, &self.previous_totals, self.num_cores);

        gauge!("total_rss_bytes").set(total_rss as f64);
        gauge!("total_utime").set(total_sample.utime as f64);
        gauge!("total_stime").set(total_sample.stime as f64);

        gauge!("total_cpu_percentage").set(totals.cpu);
        gauge!("total_kernel_cpu_percentage").set(totals.kernel);
        gauge!("total_user_cpu_percentage").set(totals.user);

        self.previous_totals = total_sample;
        self.previous_samples = samples;

        // drain any tasks before returning
        while (joinset.join_next().await).is_some() {}
        Ok(())
    }
}

#[allow(clippy::struct_field_names)]
struct CpuPercentage {
    cpu: f64,
    kernel: f64,
    user: f64,
}

#[allow(clippy::similar_names)]
#[inline]
fn calculate_cpu_percentage(sample: &Sample, previous: &Sample, num_cores: usize) -> CpuPercentage {
    let uptime_diff = sample.uptime - previous.uptime; // CPU-ticks
    let stime_diff: u64 = sample.stime.saturating_sub(previous.stime); // CPU-ticks
    let utime_diff: u64 = sample.utime.saturating_sub(previous.utime); // CPU-ticks
    let time_diff: u64 =
        (sample.stime + sample.utime).saturating_sub(previous.stime + previous.utime); // CPU-ticks

    let user = percentage(utime_diff as f64, uptime_diff as f64, num_cores as f64);
    let kernel = percentage(stime_diff as f64, uptime_diff as f64, num_cores as f64);
    let cpu = percentage(time_diff as f64, uptime_diff as f64, num_cores as f64);

    CpuPercentage { cpu, kernel, user }
}

#[inline]
fn percentage(delta_ticks: f64, delta_time: f64, num_cores: f64) -> f64 {
    // Takes (heavy) inspiration from Datadog Agent, see
    // https://github.com/DataDog/datadog-agent/blob/8914a281cf6f9cfa867e0d72899c39afa51abce7/pkg/process/checks/process_nix.go
    if delta_time == 0.0 {
        return 0.0;
    }

    // `delta_time` is the number of scheduler ticks elapsed during this slice
    // of time. `delta_ticks` is the number of ticks spent across all cores
    // during this time.
    let overall_percentage = (delta_ticks / delta_time) * 100.0;

    overall_percentage.clamp(0.0, 100.0 * num_cores)
}

#[inline]
fn get_cgroup(pid: u32) -> Result<Cgroup, Error> {
    let hierarchies = cgroups_rs::hierarchies::auto();
    if hierarchies.v2() {
        // for cgroups v2, we parse `/proc/<pid>/cgroup` looking for the main cgroup
        // relative path. We then use this to load the correct cgroup.
        // For unknown reasons, the cgroups_rs lib is not able to do this on its own.
        // Heavily inspired by
        // https://github.com/containerd/rust-extensions/blob/3d4de340d83aa06dff24fbf73d7d584ebe77c7ec/crates/shim/src/cgroup.rs#L178

        let eof = || io::Error::from(io::ErrorKind::UnexpectedEof);
        let path = format!("/proc/{pid}/cgroup");
        let content = std::fs::read_to_string(path)?;

        let first_line = content.lines().next().ok_or_else(eof)?;
        let (_, path_part) = first_line.split_once("::").ok_or_else(eof)?;

        let mut path_parts = path_part.split('/').skip(1);
        let namespace = path_parts.next().ok_or_else(eof)?;
        let cgroup_name = path_parts.next().ok_or_else(eof)?;

        Ok(Cgroup::load(
            hierarchies,
            format!("/sys/fs/cgroup/{namespace}/{cgroup_name}").as_str(),
        ))
    } else {
        let relative_paths = cgroups_rs::cgroup::get_cgroups_relative_paths_by_pid(pid)?;
        Ok(Cgroup::load_with_relative_paths(
            hierarchies,
            Path::new("."),
            relative_paths,
        ))
    }
}
