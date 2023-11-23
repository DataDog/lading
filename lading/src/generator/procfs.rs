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
#[derive(Debug)]
struct Status {
    /// Filename of executable (with escapes, limited to 64 bytes)
    name: String,
    /// File mode creation mask
    umask: Option<::nix::sys::stat::Mode>,
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
    cpus_allowed: Vec<MaskEntry>,
    cpus_allowed_list: Vec<ListEntry>,
    mems_allowed: MaskEntry,
    mems_allowed_list: Vec<ListEntry>,
    speculation_store_bypass: SpeculationStoreBypass,
    speculation_indirect_branch: SpeculationIndirectBranch,
    voluntary_ctxt_switches: u64,
    nonvoluntary_ctxt_switches: u64,
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
