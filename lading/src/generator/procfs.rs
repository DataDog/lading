//! procfs generator; emits /proc-like filesystem
//!
//! The procfs generator does not "connect" directly with the target, and
//! emulates a `/proc` filesystem, though it is not mounted at `/proc` in the
//! target container so as to avoid interfering in the container's OS.
//!
//! Data types for fields generally assume a 64-bit architecture, rather than
//! use Rust's C-compatible data types.

use ::std::{num::NonZeroU32, path::PathBuf};

use ::serde::{Deserialize, Serialize};

/// Maximum `pid` value, defined in the Linux kernel via a macro of the same
/// name in `include/linux/threads.h`. Assumes a 64-bit system; the value below
/// is 2^22. The value for a 32-bit system is at most 2^15 (32,768). Can be set
/// to a lower value by writing to `/proc/sys/kernel/pid_max`, but may not
/// exceed 2^22.
const PID_MAX_LIMIT: i32 = 4_194_304;

/// Maximum length of a process's `comm` value (in `/proc/{pid}/comm`)/
const TASK_COMM_LEN: usize = 16;

#[derive(::thiserror::Error, Debug)]
/// Errors emitted by ['ProcfsGen']
pub enum Error {
    /// Wrapper around [`std::io::Error`]
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
}

#[derive(Debug, Deserialize, PartialEq)]
/// Configuration of [`ProcfsGen`]
pub struct Config {
    /// Seed for random operations against this target
    pub seed: [u8; 32],
    /// Root path for procfs filesystem
    pub root: PathBuf,
    /// Upper bound on processes created
    pub max_processes: NonZeroU32,
}

mod task {
    use ::serde::Serialize;

    /// Models `/proc/{pid}/io` & the Linux kernel `task_io_accounting` struct.
    ///
    /// Fields take their names, types, and field comments from corresponding fields
    /// in the `task_io_accounting` struct in the Linux kernel. See also the [Linux
    /// kernel `/proc` filesystem
    /// documentation](https://docs.kernel.org/filesystems/proc.html), Section 3.3.
    #[derive(Debug, Serialize)]
    pub(super) struct Io {
        /// Bytes read (by process).
        rchar: u64,
        /// Bytes written (by process).
        wchar: u64,
        /// Number of read syscalls.
        syscr: u64,
        /// Number of write syscalls.
        syscw: u64,
        /// Number of bytes this task has caused to be read from storage.
        read_bytes: u64,
        /// Number of bytes this task has caused, or shall cause, to be written to
        /// disk.
        write_bytes: u64,
        /// Accounts for "negative" IO, e.g., due to truncating a dirty pagecache.
        /// See comments in the Linux `task_io_accounting` struct for details.
        cancelled_write_bytes: u64,
    }

    /// Corresponds to task states from Linux kernel `task_state_array`.
    ///
    /// See `linux/fs/array.c` for details. Assumes kernel version 4.14 or
    /// later. Earlier kernel versions may lack some of these states (e.g.,
    /// idle), have additional states (e.g., in kernel version 3.9, wakekill,
    /// waking), or have multiple representations of the same state (e.g., in
    /// kernel version 3.9, both "x" and "X" are used to represent a "dead"
    /// task). For scope reasons, kernel version 4.14 is chosen as a cutoff
    /// because it is the long-term release kernel with most recent end-of-life
    /// data (2024-01) as of time of writing (circa 2023-11/12).
    #[derive(Debug)]
    pub(super) enum State {
        Running,
        Sleeping,
        DiskSleep,
        Stopped,
        TracingStop,
        Dead,
        Zombie,
        Parked,
        Idle,
    }

    impl State {
        /// Emit single-letter status corresponding to task state.
        ///
        /// This letter corresponds to the task state in `/proc/{pid}/stat`,
        /// also found in the output of `ps` (which reads those files). Letters
        /// taken verbatim from `task_state_array`.
        pub(super) fn as_single_letter(&self) -> &str {
            match self {
                Self::Running => "R",
                Self::Sleeping => "S",
                Self::DiskSleep => "D",
                Self::Stopped => "T",
                Self::TracingStop => "t",
                Self::Dead => "X",
                Self::Zombie => "Z",
                Self::Parked => "P",
                Self::Idle => "I",
            }
        }

        /// Emit human-readable status corresponding to task state.
        ///
        /// This string view corresponds to the task state stored in
        /// `/proc/{pid}/status`. Strings taken verbatim from
        /// `task_state_array`.
        pub(super) fn as_human_readable(&self) -> &str {
            match self {
                Self::Running => "R (running)",
                Self::Sleeping => "S (sleeping)",
                Self::DiskSleep => "D (disk sleep)",
                Self::Stopped => "T (stopped)",
                Self::TracingStop => "t (tracing stop)",
                Self::Dead => "X (dead)",
                Self::Zombie => "Z (zombie)",
                Self::Parked => "P (parked)",
                Self::Idle => "I (idle)",
            }
        }
    }

    /// Models `/proc/{pid}/statm`; see `proc_pid_statm` in Linux kernel.
    ///
    /// All of the fields in this struct model `unsigned long` values from C.
    /// Assumes target is running Linux kernel 2.6.8-rc3 or later. See also the
    /// [Linux kernel `/proc` filesystem
    /// documentation](https://docs.kernel.org/filesystems/proc.html), Table 1-3.
    /// Field names and field documentation correspond to the fields mentioned in
    /// that table.
    #[derive(Debug)]
    pub(super) struct Statm {
        /// Total program size (pages). Same as VmSize in `/proc/{pid}/status`.
        size: u64,
        /// Size of memory portions (pages). Same as VmRSS in `/proc/{pid}/status`.
        resident: u64,
        /// Number of pages that are shared (i.e., backed by a file, same as
        /// RssFile+RssShmem in `/proc/{pid}/status`).
        shared: u64,
        /// Number of pages that are 'code' (not including libs; broken, includes
        /// data segment).
        trs: u64,
        /// Number of pages of library; always 0 as of Linux 2.6.
        lrs: u64,
        /// Number of pages of data/stack (includes libs; broken, includes library
        /// text)
        drs: u64,
        /// Number of dirty pages; always 0 as of Linux 2.6
        dt: u64,
    }
}

/// Models a process ID number, which is an `int` type in C.
///
/// This data is modeled by the `pid_t` type in the Linux kernel.
#[derive(Debug)]
struct Pid(i32);

/// Models a user ID number, which is an `unsigned int` type in C.
///
/// This data is modeled by the `uid_t` type in the Linux kernel.
#[derive(Debug)]
struct Uid(u32);

/// Models a group ID number, which is an `unsigned int` type in C.
///
/// This data is modeled by the `gid_t` type in the Linux kernel.
#[derive(Debug)]
struct Gid(u32);

/// Models SigQ field of `/proc/{pid}/status`.
#[derive(Debug)]
struct SigQ {
    signals_queued: u32,
    max_number_for_queue: u64,
}

/// Models signal mask fields in `/proc/{pid}/status`.
///
/// This mask is architecture-dependent and is modeled by the `sigset_t` type in
/// the Linux kernel. On x86 64-bit platforms and arm64 platforms, this type is
/// an `unsigned long [1]` in C. Since these two platforms are the platforms we
/// are most likely to support, this type is hardcoded to a Rust equivalent of
/// that representation. On most other platforms, this type can be punned to an
/// unsigned 64-bit integer (e.g., on 32-bit architectures, this type is an
/// `unsigned long[2]` in C), so hardcoding this type to a `u64` doesn't cost us
/// much in portability. One notable exception to this 64-bit representation is
/// MIPS, which uses a type punnable to `u128`, but supporting that architecture
/// seems unlikely.
#[derive(Debug)]
struct SignalMask(u64);

/// Models capability mask fields in `/proc/{pid}/status`.
///
/// This mask is a `u64` in the Linux kernel on all supported architectures; see
/// the definition of the `kernel_cap_t` type in the kernel source code for
/// details.
#[derive(Debug)]
struct CapabilityMask(u64);

/// Models entries of `{Cpus,Mems}_allowed_list` in `/proc/{pid}/status`.
///
/// Models entries of the `Cpus_allowed_list` and `Mems_allowed_list` fields in
/// `/proc/{pid}status`. The list format is a comma-separated list of CPU (node)
/// or memory-node numbers and ranges of numbers in ASCII decimal. Examples
/// include:
///
/// ```text
/// 0-6,14         # bits 0, 1, 2, 3, 4, 5, 6, and 14 set
/// 0-3,6,9-12     # bits 0, 1, 2, 3, 6, 9, 10, 11, and 12 set
/// ```
///
/// See `proc(5)` and `cpuset(7)` `man` pages for details on the "list format".
#[derive(Debug)]
enum ListEntry {
    Single(u64),
    RangeInclusive {
        /// First element of range.
        first: u64,
        /// Last element of range. Must be greater than `first`.
        last: u64,
    },
}

/// Models `Seccomp` field of `/proc/{pid}/status`, if it exists.
///
/// The `Seccomp` field of `/proc/{pid}/status`, if it exists (if
/// `CONFIG_SECCOMP` is set), stores the Seccomp mode of the process (since
/// Linux 3.8). This field can take one of three values: 0
/// (`SECCOMP_MODE_DISABLED`), 1 (`SECCOMP_MODE_STRICT`), or 2
/// (`SECCOMP_MODE_FILTER`), even though the underlying data itself is a C
/// `int`.
#[derive(Debug)]
enum SeccompMode {
    Disabled = 0,
    Strict = 1,
    Filter = 2,
}

/// Models entries of `{Cpus,Mems}_allowed` fields in `/proc/{pid}/status`.
///
/// Models entries of the `Cpus_allowed` and `Mems_allowed` fields in
/// `/proc/{pid}/status`. Both of these fields are collections of 32-bit words.
#[derive(Debug)]
struct MaskEntry(u32);

/// Models `Speculation_Store_Bypass` field of `/proc/{pid}/status`
#[derive(Debug)]
enum SpeculationStoreBypass {
    Unknown,
    NotVulnerable,
    ThreadForceMitigated,
    ThreadMitigated,
    ThreadVulnerable,
    GloballyMitigated,
    Vulnerable,
}

/// Models `SpeculationIndirectBranch` field of `/proc/{pid}/status`
#[derive(Debug)]
enum SpeculationIndirectBranch {
    Unsupported,
    NotAffected,
    ConditionalForceDisabled,
    ConditionalDisabled,
    ConditionalEnabled,
    AlwaysEnabled,
    AlwaysDisabled,
    Unknown,
}

/// Models `/proc/{pid}/status`.
///
/// See the [Linux kernel `/proc` filesystem
/// documentation](https://docs.kernel.org/filesystems/proc.html), Table 1-2.
///
/// This struct needs to be consistent with the information in
/// `/proc/[pid]/stat`. In many cases, the information in this struct has an
/// obvious analogue to the information in `/proc/{pid}/stat` (e.g., process pid
/// is the same, process name is printed without escapes, task state is
/// displayed as a single letter instead of a letter plus description). In other
/// cases, consistency is *not* as simple as "output a possibly different format
/// isomorphic to the format displayed by `status`". Examples include the
/// process's `task_struct` `flags`, which influence the `Kthread` field of
/// `status` and the `/proc/{pid}/comm` file.
///
/// It's also worth noting here that the `proc(5)` man page documentation for
/// this file is slightly out-of-date.
#[derive(Debug)]
struct Status {
    /// Filename of executable (with escapes, limited to 64 bytes)
    name: String,
    /// File mode creation mask (only printed if not zero)
    umask: ::nix::sys::stat::Mode,
    /// State of process
    state: task::State,
    /// Thread group ID
    tgid: Pid,
    /// NUMA group ID
    ngid: Pid,
    /// Process ID
    pid: Pid,
    /// Process ID of parent process
    ppid: Pid,
    /// PID of process tracing this process (0 if not, or the trace is outside
    /// of the current pid namespace)
    tracer_pid: Pid,
    /// Real, effective, saved set, and file system UIDs
    uid: [Uid; 4],
    /// Real, effective, saved set, and file system GIDs
    gid: [Gid; 4],
    /// Number of file descriptor slots currently allocated
    fd_size: u64,
    /// Supplementary group list
    groups: Vec<Gid>,
    // if kernel compiled with CONFIG_PID_NS
    /// Descendant namespace thread group ID hierarchy
    ns_tgid: Vec<Pid>,
    /// Descendant namespace process ID hierarchy
    ns_pid: Vec<Pid>,
    /// Descendant namespace process group ID hierarchy
    ns_pgid: Vec<Pid>,
    /// Descendant namespace session ID hierarchy
    ns_sid: Vec<Pid>,
    // endif
    /// Whether the process thread is a kernel thread
    kthread: bool,
    /// Peak virtual memory size (in bytes)
    vm_peak: u64,
    /// Total program size (in bytes)
    vm_size: u64,
    /// Locked memory size (in bytes)
    vm_lck: u64,
    /// Pinned memory size (in bytes)
    vm_pin: u64,
    /// Peak resident set size (in bytes)
    vm_hwm: u64,
    /// Resident set size (in bytes) = rss_anon + rss_file + rss_shmem
    vm_rss: u64,
    /// Resident anonymous memory size (in bytes)
    rss_anon: u64,
    /// Resident file mappings size (in bytes)
    rss_file: u64,
    /// Resident shmem memory size (in bytes; includes SysV shm, tmpfs mapping,
    /// shared anonymous mappings)
    rss_shmem: u64,
    /// Size of private data segments (in bytes)
    vm_data: u64,
    /// Size of stack segments (in bytes)
    vm_stk: u64,
    /// Size of text segment (in bytes)
    vm_exe: u64,
    /// Size of shared library code (in bytes)
    vm_lib: u64,
    /// Size of page table entries (in bytes)
    vm_pte: u64,
    /// Size of swap used by anonymous private data (in bytes; does not include
    /// shmem swap)
    vm_swap: u64,
    /// Size of huge translation lookaside buffer (in bytes)
    huge_tlb_pages: u64,
    /// Process's memory is currently being dumped ()
    core_dumping: bool,
    /// Process is allowed to use transparent hugepage support
    thp_enabled: bool,
    /// Mask for linear address masking (LAM) to support storing metadata
    /// in pointer addresses
    untag_mask: u64,
    /// Number of threads used by process
    threads: i32,
    /// Number of signals queued / max number for queue
    sigq: SigQ,
    /// Bitmap of pending signals for the thread
    sig_pnd: SignalMask,
    /// Bitmap of shared pending signals for the thread
    shd_pnd: SignalMask,
    /// Bitmap of blocked signals
    sig_blk: SignalMask,
    /// Bitmap of ignored signals
    sig_ign: SignalMask,
    /// Bitmap of caught signals
    sig_cgt: SignalMask,
    /// Bitmap of inheritable capabilities
    cap_inh: CapabilityMask,
    /// Bitmap of permitted capabilities
    cap_prm: CapabilityMask,
    /// Bitmap of effective capabilities
    cap_eff: CapabilityMask,
    /// Bitmap of capabilities bounding set
    cap_bnd: CapabilityMask,
    /// Value of the process's `no_new_privs` bit. If set to 1, the `execve`
    /// syscall promises not to grant the process any additional privileges to
    /// do anything that could not have been done without that syscall.
    no_new_privs: bool,
    // if CONFIG_SECCOMP set
    /// Seccomp mode of the process.
    seccomp: SeccompMode,
    // endif CONFIG_SECCOMP set
    // if CONFIG_SECCOMP_FILTER set
    seccomp_filters: i32,
    // end if CONFIG_SECCOMP_FILTER set
    /// Mask of CPUs on which this process may run; isomorphic to
    /// `cpus_allowed_list`
    cpus_allowed: Vec<MaskEntry>,
    /// List of CPUs on which this process may run; isomorphic to `cpus_allowed`.
    cpus_allowed_list: Vec<ListEntry>,
    /// Mask of memory nodes allowed to this process; isomorphic to
    /// `mems_allowed_list`.
    mems_allowed: Vec<MaskEntry>,
    /// List of memory nodes allowed to this process; isomorphic to
    /// `mems_allowed`.
    mems_allowed_list: Vec<ListEntry>,
    /// Indicates whether process may or may not be vulnerable to a Speculative
    /// Store Bypass attack (CVE-2018-3639)
    speculation_store_bypass: SpeculationStoreBypass,
    /// Indicates whether process may or may not be vulnerable to branch target
    /// injection attacks (Spectre variant 2)
    speculation_indirect_branch: SpeculationIndirectBranch,
    /// Number of times process has been context-switched voluntarily
    voluntary_ctxt_switches: u64,
    /// Number of times process has been context-switched involuntarily
    nonvoluntary_ctxt_switches: u64,
}

/// Models `/proc/{pid}/stat`.
///
/// All information in this struct taken from `proc(5)` man page; this
/// documentation appears to be up to date for `/proc/{pid}/stat`.
#[derive(Debug)]
struct Stat {
    /// The process ID.
    pid: Pid,
    /// File name of executable, in parentheses. Limited to `TASK_COMM_LEN`
    /// bytes.
    comm: String,
    /// Indicates process state.
    state: task::State,
    /// The PID of the parent of this process.
    ppid: Pid,
    /// The process group ID of the process.
    pgrp: Pid,
    /// The session ID of the process.
    session: Pid,
    /// The controlling terminal of the process.
    tty_nr: i32,
    /// The ID of the foreground process group of the controlling terminal of
    /// the process.
    tpgid: Pid,
    /// The kernel flags word of the prcoess. For bit meaninges, set the `PF_*`
    /// defines in the Linux kernel source file `include/linux/sched.h`.
    flags: u32,
    /// The number of minor faults the process has made that have not required
    /// loading a memory page from disk.
    minflt: u64,
    /// The number of minor faults that the process's waited-for children have
    /// made.
    cminflt: u64,
    /// The number of major faults the process has made that have required
    /// loading a memory page from disk.
    majflt: u64,
    /// The number of major faults that the process' waited-for children have
    /// made.
    cmajflt: u64,
    /// Amount of time that this process has been scheduled in user mode,
    /// measured in clock ticks.
    utime: u64,
    /// Amount of time that this process has been scheduled in kernel mode,
    /// measured in clock ticks.
    stime: u64,
    /// Amount of time that this process's waited-for children have been
    /// scheduled in user mode, measured in clock ticks.
    cutime: i64,
    /// Amount of time that this process's waited-for children have been
    /// scheduled in kernel mode, measured in clock ticks.
    cstime: i64,
    /// Scheduling priority. For processes running a real-time scheduling
    /// policy, this is the negated scheduling priority, minus on; that is, a
    /// number in the range -2 to -100, corresponding to real-time priorities 1
    /// to 99. For processes running under a non-real-time scheduling policy,
    /// this is the raw nice value as represented in the kernel. The kernel
    /// stores nice values as numbers in the range 0 (high) to 39 (low),
    /// corresponding to the user-visible nice range of -20 (high) to 19 (low).
    priority: i64,
    /// The nice value, a value in the range 19 (low priority) to -20 (high
    /// priority).
    nice: i64,
    /// Number of threads in this process.
    num_threads: i64,
    /// The time in jiffies before the next `SIGALRM` is sent to the process due
    /// to an interval timer. Since Linux 2.6.17, this field is no longer
    /// maintained, and is hard coded as 0.
    itrealvalue: i64,
    /// The time the process started after system boot, expressed in clock ticks.
    starttime: u128,
    /// Virtual memory size in bytes.
    vsize: u64,
    /// Resident set size: number of pages the process has in real memory. This
    /// value is inaccurate; see `/proc/{pid}/statm` for details.
    rss: i64,
    /// Current soft limit in bytes on the RSS of the process; see the
    /// description of `RLIMIT_RSS` in `getrlimit(2)`.
    rsslim: u64,
    /// The address above which program text can run.
    startcode: u64,
    /// The address below which program text can run.
    endcode: u64,
    /// The address of the start (i.e., bottom) of the stack.
    startstack: u64,
    /// The current value of ESP (stack pointer), as found in the kernel stack
    /// pages for the process.
    kstkesp: u64,
    /// The current EIP (instruction pointer).
    kstkeip: u64,
    /// The bitmap of pending signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    signal: u64,
    /// The bitmap of blocked signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    blocked: u64,
    /// The bitmap of ignored signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    sigignore: u64,
    /// The bitmap of caught signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    sigcatch: u64,
    /// This is the "channel" in which the process is waiting. It is the address
    /// of a location in the kernel where the process is sleeping. The
    /// corresponding symbolic name can be found in `/proc/{pid}/wchan`.
    wchan: u64,
    /// Number of pages swapped (not maintained). Hard coded as 0.
    nswap: u64,
    /// Cumulative `nswap` for child processes (not maintained). Hard coded as
    /// 0.
    cnswap: u64,
    /// Signal to be sent to parent when process dies.
    exit_signal: i32,
    /// CPU number last executed on.
    processor: i32,
    /// Real-time scheduling priority, a number in the range 1 to 99 for
    /// processes scheduled under a real-time policy, or 0, for non-real-time
    /// processes.
    rt_priority: u32,
    /// Scheduling policy. Decode using the `SCHED_*` constants in
    /// `linux/sched.h`.
    policy: u32,
    /// Aggregated block I/O delays, measured in clock ticks.
    delayacct_blkio_ticks: u128,
    /// Guest time of the process (time spend running a virtual CPU for a guest
    /// operating system), measured in clock ticks.
    guest_time: u128,
    /// Guest time of the process's children, measured in clock ticks.
    cguest_time: u128,
    /// Address above which program initialized and uninitialized (BSS) data are
    /// placed.
    start_data: u64,
    /// Address below which program initialized and uninitialized (BSS) data are
    /// placed.
    end_data: u64,
    /// Address above which program heap can be expanded with `brk(2)`.
    start_brk: u64,
    /// Address above which program command-line arguments (`argv`) are placed.
    arg_start: u64,
    /// Address below which program command-line arguments (`argv`) are placed.
    arg_end: u64,
    /// Address above which program environment is placed.
    env_start: u64,
    /// Address below which program environment is placed.
    env_end: u64,
    /// The thread's exit status in the form reported by `waitpid(2)`.
    exit_code: i32,
}

/// Models data associated with `/proc/{pid}/statm`.
///
/// Provides information about memory usage, measured in pages.
#[derive(Debug)]
struct Statm {
    /// Total program size (same as VmSize in `/proc/{pid}/status`).
    size: u64,
    /// Resident set size (inaccurate; same as VmRss in `/proc/{pid}/status`).
    resident: u64,
    /// Number of resident shared pages (i.e., backed by a file) (inaccurate;
    /// same as RssFile+RssShmem in `/proc/{pid}/status`).
    shared: u64,
    /// Text (code).
    text: u64,
    /// Library (unused since Linux 2.6; always 0).
    lib: u64,
    /// Data + stack.
    data: u64,
    /// Dirty pages (unused since Linux 2.6; always 0).
    dt: u64,
}

/// Models data associated with a process ID (pid).
///
/// `process-agent` currently only reads the following files in `/proc/{pid}`:
///
/// - cmdline (string containing command lin)
/// - comm (string of `TASK_COMM_LEN` characters or less; currently,
///   `TASK_COMM_LEN` is 16).
/// - io
/// - stat
/// - statm
/// - status
///
/// so this struct reflects that behavior.
#[derive(Debug)]
struct Process {
    /// Process ID number (`pid`).
    id: Pid,
    /// Command line for process (unless a zombie); corresponds to
    /// `/proc/{pid}/cmdline`.
    cmdline: String,
    /// Command name associated with process. Truncated to `TASK_COMM_LEN`
    /// bytes.
    comm: String,
    /// Corresponds to `/proc/{pid}/io`.
    io: task::Io,
    /// Corresponds to `/proc/{pid}/stat`.
    stat: Stat,
    /// Corresponds to `/proc/{pid}/statm`.
    statm: Statm,
    /// Corresponds to `/proc/{pid}/status`.
    status: Status,
}

// The procfs generator.
//
// Generates a fake procfs filesystem.
