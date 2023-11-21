//! procfs generator; emits /proc-like filesystem
//!
//! The procfs generator does not "connect" directly with the target, and
//! emulates a `/proc` filesystem, though it is not mounted at `/proc` in the
//! target container so as to avoid interfering in the container's OS.

use ::std::{num::NonZeroU32, path::PathBuf};

use ::serde::{Deserialize, Serialize};

/// Maximum `pid` value, defined in the Linux kernel via a macro of the same
/// name in `include/linux/threads.h`. Assumes a 64-bit system; the value below
/// is 2^22. The value for a 32-bit system is at most 2^15 (32,768). Can be set
/// to a lower value by writing to `/proc/sys/kernel/pid_max`, but may not
/// exceed 2^22.
const PID_MAX_LIMIT: u32 = 4_194_304;

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

/// Models `/proc/{pid}/status` and `/proc/{pid}/stat` as of Linux 4.19.
///
/// See the [Linux kernel `/proc` filesystem
/// documentation](https://docs.kernel.org/filesystems/proc.html), Table 1-2.
///
/// TODO(geoffrey.oxberry@datadoghq.com): Add remaining fields from Table 1-2
/// and figure out which fields are optional and which are not.
#[derive(Debug)]
struct Status {
    /// Filename of executable
    name: String,
    /// File mode creation mask
    umask: Option<::nix::sys::stat::Mode>,
    /// State of process
    state: task::State,
    /// Thread group ID
    tgid: u32,
    /// NUMA group ID
    ngid: u32,
    /// Process ID
    pid: u32,
    /// Process ID of parent process
    ppid: u32,
    /// PID of process tracing this process (0 if not, or the trace is outside
    /// of the current pid namespace)
    tracer_pid: u32,
    /// Real, effective, saved set, and file system UIDs
    uid: [u32; 4],
    /// Real, effective, saved set, and file system GIDs
    gid: [u32; 4],
    /// Number of file descriptor slots currently allocated
    fd_size: u64,
    /// Supplementary group list
    groups: Vec<u32>,
    /// Descendant namespace thread group ID hierarchy
    ns_tgid: Vec<u32>,
    /// Descendant namespace process ID hierarchy
    ns_pid: Vec<u32>,
    /// Descendant namespace process group ID hierarchy
    ns_pgid: Vec<u32>,
    /// Descendant namespace session ID hierarchy
    ns_sid: Vec<u32>,
    /// Kernel thread flag (true/false should be serialized as 1/0)
    kthread: Option<bool>,
    /// Peak virtual memory size (in bytes)
    vm_peak: u64,
    /// Total program size (in bytes)
    vm_size: u64,
    vm_lck: u64,
    vm_pin: u64,
    vm_hwm: u64,
    vm_rss: u64,
    rss_anon: u64,
    rss_file: u64,
    rss_shmem: u64,
    vm_data: u64,
    vm_stk: u64,
    vm_exe: u64,
    vm_lib: u64,
    vm_pte: u64,
    vm_swap: u64,
    huge_tlb_pages: u64,
    core_dumping: bool,
    thp_enabled: bool,
    threads: u32,
    sig_q: [u8; 8],
}

/// Models data associated with a process ID (pid).
///
/// TODO(geoffrey.oxberry@datadoghq.com): `process-agent` currently only tests
/// the following files in `/proc/{pid}`:
///
/// - cmdline (string containing command lin)
/// - comm (string of `TASK_COMM_LEN` characters or less; currently,
///   `TASK_COMM_LEN` is 16).
/// - io
/// - stat
/// - statm
/// - status
///
/// One or two test cases also use `/proc/{pid}/cwd` (a symlink).
///
/// The `process-agent` system probe also reads from `/proc/{pid}`:
///
/// - `exe` (a symlink to the executable referred to in cmdline)
/// - `fd` (a directory containing all file descriptors)
#[derive(Debug)]
struct Process {
    /// Process ID number (`pid`)
    id: NonZeroU32,
    /// Command line for process (unless a zombie); corresponds to
    /// `/proc/{pid}/cmdline`
    cmdline: String,
    /// Command name associated with process. Truncated to `TASK_COMM_LEN`
    /// bytes.
    comm: String,
}

// The procfs generator.
//
// Generates a fake procfs filesystem.
