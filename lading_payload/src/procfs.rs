//! Procfs payload.

use core::fmt;
use std::{
    io::Write,
    num::{NonZeroU64, NonZeroU8},
};

use crate::{common::strings, Error, Generator};

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};
use serde::Deserialize;

/// Maximum `pid` value, defined in the Linux kernel via a macro of the same
/// name in `include/linux/threads.h`. Assumes a 64-bit system; the value below
/// is 2^22 so we can use this value in a non-inclusive range. The value for a
/// 32-bit system is at most 2^15 (32,768). Can be set to a lower value by
/// writing to `/proc/sys/kernel/pid_max`, but may not exceed 2^22.
const PID_MAX_LIMIT: i32 = 4_194_304;

/// Maximum length (in bytes) of a process's `comm` value (in
/// `/proc/{pid}/comm`)
const TASK_COMM_LEN: usize = 16;

/// Maximum length (in bytes) of process or task name. See `proc_task_name`
/// function in the Linux kernel for details.
const TASK_NAME_LEN: usize = 64;

/// Maximum decimal value of a `umask`. A `umask` consists of four octal digits,
/// so its maximum value can be at most 8^4 - 1 = 2^12 - 1 = 4095. The value below
/// is 2^12 so we can use this value in a non-inclusive range.
const UMASK_MAX: u16 = 4096;

/// Maximum number of supplemental groups a process can belong to. Corresponds
/// to macro of the same name in `include/uapi/linux/limits.h`
const NGROUPS_MAX: usize = 65536;

/// Maximum number of bytes in a path name, including a null byte. Rust does not
/// automatically terminate strings with a null byte, so we subtract one from the
/// macro of the same name in `include/uapi/linux/limits.h`.
const PATH_MAX: usize = 4095;

fn num_numa_nodes() -> NonZeroU8 {
    // SAFETY: Input to `NonZeroU8::new` is hardcoded to nonzero value.
    NonZeroU8::new(32).unwrap()
}

fn num_cpus() -> NonZeroU8 {
    // SAFETY: Input to `NonZeroU8::new` is hardcoded to nonzero value.
    NonZeroU8::new(8).unwrap()
}

fn max_groups() -> NonZeroU8 {
    // SAFETY: Input to `NonZeroU8::new` is hardcoded to nonzero value.
    NonZeroU8::new(10).unwrap()
}

fn max_pid_namespaces() -> NonZeroU8 {
    // SAFETY: Input to `NonZeroU8::new` is hardcoded to nonzero value.
    NonZeroU8::new(1).unwrap()
}

/// Configure the `Procfs` payload.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Config {
    /// Number of NUMA nodes exposed to processes. Sizes the `Mems_allowed` field
    /// in `/proc/{pid}/status`.
    #[serde(default = "num_numa_nodes")]
    pub num_numa_nodes: NonZeroU8,
    /// Number of CPUs exposed to processes. Sizes the `Cpus_allowed` field in
    /// `/proc/{pid}/status`.
    #[serde(default = "num_cpus")]
    pub num_cpus: NonZeroU8,
    /// Maximum number of groups to which a process can belong. Sizes the
    /// `Groups` field in `/proc/{pid}/status`.
    #[serde(default = "max_groups")]
    pub max_groups: NonZeroU8,
    /// Maximum number of process namespaces in process ID hierarchies. Sizes
    /// the `NStgid`, `NSpid`, `NSpgid`, and `NSsid` fields in
    /// `/proc/{pid}/status`.
    #[serde(default = "max_pid_namespaces")]
    pub max_pid_namespaces: NonZeroU8,
}

mod proc {
    use rand::{distributions::Standard, prelude::Distribution, Rng};

    #[derive(Debug, Clone, Copy)]
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

    impl Distribution<Io> for Standard {
        fn sample<R>(&self, rng: &mut R) -> Io
        where
            R: Rng + ?Sized,
        {
            Io {
                rchar: rng.gen(),
                wchar: rng.gen(),
                syscr: rng.gen(),
                syscw: rng.gen(),
                read_bytes: rng.gen(),
                write_bytes: rng.gen(),
                cancelled_write_bytes: rng.gen(),
            }
        }
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
    #[derive(Debug, Clone, Copy)]
    pub(super) enum State {
        /// Task state is "R (running)"
        Running,
        /// Task state is "S (sleeping)"
        Sleeping,
        /// Task state is "D (disk sleep)"
        DiskSleep,
        /// Task state is "T (stopped)"
        Stopped,
        /// Task state is "t (tracing stop)"
        TracingStop,
        /// Task state is "X (dead)"
        Dead,
        /// Task state is "Z (zombie)"
        Zombie,
        /// Task state is "P (parked)"
        Parked,
        /// Task state is "I (idle)"
        Idle,
    }

    impl Distribution<State> for Standard {
        fn sample<R>(&self, rng: &mut R) -> State
        where
            R: Rng + ?Sized,
        {
            match rng.gen_range(0..9) {
                0 => State::Running,
                1 => State::Sleeping,
                2 => State::DiskSleep,
                3 => State::Stopped,
                4 => State::TracingStop,
                5 => State::Dead,
                6 => State::Zombie,
                7 => State::Parked,
                8 => State::Idle,
                _ => unreachable!(),
            }
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
///
/// This type could probably be replaced by the `procfs::process:StatM` type;
/// compared to that type, this type has slightly more documentation.
///
/// Although in principle, `/proc/{pid}/statm` should be consistent with
/// `/proc/{pid}/stat` and `/proc/{pid}/status`, these consistency relationships
/// are ignored for ease of implementation.
#[derive(Debug, Clone, Copy)]
struct Statm {
    /// Total program size (pages). Same as VmSize in
    /// `/proc/{pid}/status`.
    size: u64,
    /// Size of memory portions (pages). Same as VmRSS in
    /// `/proc/{pid}/status`.
    resident: u64,
    /// Number of pages that are shared (i.e., backed by a file, same as
    /// `RssFile` + `RssShmem`` in `/proc/{pid}/status`).
    shared: u64,
    /// Number of pages that are 'code' (not including libs; broken,
    /// includes data segment). Looks to be the same as `VmExe` + `VmLib` in
    /// `/proc/{pid}/status` if `CONFIG_MMU` is set. (It is by default on
    /// `x86` and `arm64`.)
    trs: u64,
    /// Number of pages of library; always 0 as of Linux 2.6.
    lrs: u64,
    /// Number of pages of data/stack (includes libs; broken, includes
    /// library text). Looks to be the same as `VmData` + `VmStk` in
    /// `/proc/{pid}/status` if `CONFIG_MMU` is set.
    drs: u64,
    /// Number of dirty pages; always 0 as of Linux 2.6
    dt: u64,
}

impl Distribution<Statm> for Standard {
    /// Generates "uniformly random" instance of [`Statm`].
    ///
    /// Exists because our model of `/proc/{pid}/statm` currently ignores
    /// consistency with `/proc/{pid}/stat` and `/proc/{pid}/status`.
    fn sample<R>(&self, rng: &mut R) -> Statm
    where
        R: Rng + ?Sized,
    {
        Statm {
            size: rng.gen(),
            resident: rng.gen(),
            shared: rng.gen(),
            trs: rng.gen(),
            lrs: 0,
            drs: rng.gen(),
            dt: 0,
        }
    }
}

/// Models a process ID number, which is an `int` type in C.
///
/// This data is modeled by the `pid_t` type in the Linux kernel.
#[derive(Debug, Clone, Copy)]
struct Pid(i32);

impl Distribution<Pid> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Pid
    where
        R: Rng + ?Sized,
    {
        Pid(rng.gen_range(1..PID_MAX_LIMIT))
    }
}

/// Models a user ID number, which is an `unsigned int` type in C.
///
/// This data is modeled by the `uid_t` type in the Linux kernel.
#[derive(Debug)]
struct Uid(u32);

impl Distribution<Uid> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Uid
    where
        R: Rng + ?Sized,
    {
        Uid(rng.gen())
    }
}

/// Models a group ID number, which is an `unsigned int` type in C.
///
/// This data is modeled by the `gid_t` type in the Linux kernel.
#[derive(Debug)]
struct Gid(u32);

impl Distribution<Gid> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Gid
    where
        R: Rng + ?Sized,
    {
        Gid(rng.gen())
    }
}

/// Models SigQ field of `/proc/{pid}/status`.
#[derive(Debug)]
struct SigQ {
    signals_queued: u32,
    max_number_for_queue: u64,
}

impl Distribution<SigQ> for Standard {
    fn sample<R>(&self, rng: &mut R) -> SigQ
    where
        R: Rng + ?Sized,
    {
        // In theory, `SigQ` for a "real" process would be constrained such that
        //
        // - `signals_queued` is less than or equal to `max_number_for_queue`
        // - `max_number_for_queue` is equal to the value returned by `ulimit -i`.
        //
        // In practice, we ignore these constraints for simplicity.
        SigQ {
            signals_queued: rng.gen(),
            max_number_for_queue: rng.gen(),
        }
    }
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

impl Distribution<SignalMask> for Standard {
    fn sample<R>(&self, rng: &mut R) -> SignalMask
    where
        R: Rng + ?Sized,
    {
        SignalMask(rng.gen())
    }
}

/// Models capability mask fields in `/proc/{pid}/status`.
///
/// This mask is a `u64` in the Linux kernel on all supported architectures; see
/// the definition of the `kernel_cap_t` type in the kernel source code for
/// details.
#[derive(Debug)]
struct CapabilityMask(u64);

impl Distribution<CapabilityMask> for Standard {
    fn sample<R>(&self, rng: &mut R) -> CapabilityMask
    where
        R: Rng + ?Sized,
    {
        CapabilityMask(rng.gen())
    }
}

/// Models entries of `{Cpus,Mems}_allowed` fields in `/proc/{pid}/status`.
///
/// Models entries of the `Cpus_allowed` and `Mems_allowed` fields in
/// `/proc/{pid}/status`. Both of these fields are collections of 32-bit words.
#[derive(Debug)]
struct MaskEntry(u32);

impl Distribution<MaskEntry> for Standard {
    fn sample<R>(&self, rng: &mut R) -> MaskEntry
    where
        R: Rng + ?Sized,
    {
        MaskEntry(rng.gen())
    }
}

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

impl Distribution<SeccompMode> for Standard {
    fn sample<R>(&self, rng: &mut R) -> SeccompMode
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..3) {
            0 => SeccompMode::Disabled,
            1 => SeccompMode::Strict,
            2 => SeccompMode::Filter,
            _ => unreachable!(),
        }
    }
}

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

impl Distribution<SpeculationStoreBypass> for Standard {
    fn sample<R>(&self, rng: &mut R) -> SpeculationStoreBypass
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..7) {
            0 => SpeculationStoreBypass::Unknown,
            1 => SpeculationStoreBypass::NotVulnerable,
            2 => SpeculationStoreBypass::ThreadForceMitigated,
            3 => SpeculationStoreBypass::ThreadMitigated,
            4 => SpeculationStoreBypass::ThreadVulnerable,
            5 => SpeculationStoreBypass::GloballyMitigated,
            6 => SpeculationStoreBypass::Vulnerable,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for SpeculationStoreBypass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SpeculationStoreBypass::Unknown => "unknown",
            SpeculationStoreBypass::NotVulnerable => "not vulnerable",
            SpeculationStoreBypass::ThreadForceMitigated => "thread force mitigated",
            SpeculationStoreBypass::ThreadMitigated => "thread mitigated",
            SpeculationStoreBypass::ThreadVulnerable => "thread vulnerable",
            SpeculationStoreBypass::GloballyMitigated => "globally mitigated",
            SpeculationStoreBypass::Vulnerable => "vulnerable",
        };
        write!(f, "{s}")
    }
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

impl Distribution<SpeculationIndirectBranch> for Standard {
    fn sample<R>(&self, rng: &mut R) -> SpeculationIndirectBranch
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..8) {
            0 => SpeculationIndirectBranch::Unsupported,
            1 => SpeculationIndirectBranch::NotAffected,
            2 => SpeculationIndirectBranch::ConditionalForceDisabled,
            3 => SpeculationIndirectBranch::ConditionalDisabled,
            4 => SpeculationIndirectBranch::ConditionalEnabled,
            5 => SpeculationIndirectBranch::AlwaysEnabled,
            6 => SpeculationIndirectBranch::AlwaysDisabled,
            7 => SpeculationIndirectBranch::Unknown,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for SpeculationIndirectBranch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SpeculationIndirectBranch::Unsupported => "unsupported",
            SpeculationIndirectBranch::NotAffected => "not affected",
            SpeculationIndirectBranch::ConditionalForceDisabled => "conditional force disabled",
            SpeculationIndirectBranch::ConditionalDisabled => "conditional disabled",
            SpeculationIndirectBranch::ConditionalEnabled => "conditional enabled",
            SpeculationIndirectBranch::AlwaysEnabled => "always enabled",
            SpeculationIndirectBranch::AlwaysDisabled => "always disabled",
            SpeculationIndirectBranch::Unknown => "unknown",
        };
        write!(f, "{s}")
    }
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
///
/// We can't use the `procfs::Process::Status` type because it is marked
/// non-exhaustive.
#[derive(Debug)]
struct Status {
    /// Filename of executable (with escapes, limited to 64 bytes)
    name: String,
    /// File mode creation mask (only printed if not zero)
    umask: u32,
    /// State of process
    state: proc::State,
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
    /// Number of file descriptor slots currently allocated. Cannot exceed the
    /// value stored in `/proc/sys/fs/file-max`. A unprivileged user process may
    /// not exceed the output of `ulimit -n -H`.
    fd_size: u64,
    /// Supplementary group list
    groups: Vec<Gid>,
    /// Descendant namespace thread group ID hierarchy. Present only if kernel
    /// compiled with `CONFIG_PID_NS`.
    ns_tgid: Vec<Pid>,
    /// Descendant namespace process ID hierarchy. Present only if kernel
    /// compiled with `CONFIG_PID_NS`.
    ns_pid: Vec<Pid>,
    /// Descendant namespace process group ID hierarchy. Present only if kernel
    /// compiled with `CONFIG_PID_NS`.
    ns_pgid: Vec<Pid>,
    /// Descendant namespace session ID hierarchy. Present only if kernel
    /// compiled with `CONFIG_PID_NS`.
    ns_sid: Vec<Pid>,
    /// Whether the process thread is a kernel thread.
    kthread: Option<bool>,
    /// Peak virtual memory size (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_peak: u64,
    /// Total program size (in bytes). Present only if task has non-null memory
    /// management pointer.
    vm_size: u64,
    /// Locked memory size (in bytes). Present only if task has non-null memory
    /// management pointer.
    vm_lck: u64,
    /// Pinned memory size (in bytes). Present only if task has non-null memory
    /// management pointer.
    vm_pin: u64,
    /// Peak resident set size (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_hwm: u64,
    /// Resident set size (in bytes) = rss_anon + rss_file + rss_shmem. Present
    /// only if task has non-null memory management pointer.
    vm_rss: u64,
    /// Resident anonymous memory size (in bytes). Present only if task has
    /// non-null memory management pointer.
    rss_anon: u64,
    /// Resident file mappings size (in bytes). Present only if task has
    /// non-null memory management pointer.
    rss_file: u64,
    /// Resident shmem memory size (in bytes; includes SysV shm, tmpfs mapping,
    /// shared anonymous mappings). Present only if task has non-null memory
    /// management pointer.
    rss_shmem: u64,
    /// Size of private data segments (in bytes). Present only if task has
    /// non-null memory management pointer.
    vm_data: u64,
    /// Size of stack segments (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_stk: u64,
    /// Size of text segment (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_exe: u64,
    /// Size of shared library code (in bytes). Present only if task has
    /// non-null memory management pointer.
    vm_lib: u64,
    /// Size of page table entries (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_pte: u64,
    /// Size of swap used by anonymous private data (in bytes; does not include
    /// shmem swap). Present only if task has non-null memory management
    /// pointer.
    vm_swap: u64,
    /// Size of huge translation lookaside buffer (in bytes). Present only if
    /// task has non-null memory management pointer.
    huge_tlb_pages: u64,
    /// Process's memory is currently being dumped. Present only if task has
    /// non-null memory management pointer.
    core_dumping: bool,
    /// Process is allowed to use transparent hugepage support. Present only if
    /// task has non-null memory management pointer.
    thp_enabled: bool,
    /// Mask for linear address masking (LAM) to support storing metadata in
    /// pointer addresses. Present only if task has non-null memory management
    /// pointer.
    untag_mask: u64,
    /// Number of threads used by process.
    threads: i32,
    /// Number of signals queued / max number for queue.
    sigq: SigQ,
    /// Bitmap of pending signals for the thread.
    sig_pnd: SignalMask,
    /// Bitmap of shared pending signals for the thread.
    shd_pnd: SignalMask,
    /// Bitmap of blocked signals.
    sig_blk: SignalMask,
    /// Bitmap of ignored signals.
    sig_ign: SignalMask,
    /// Bitmap of caught signals.
    sig_cgt: SignalMask,
    /// Bitmap of inheritable capabilities.
    cap_inh: CapabilityMask,
    /// Bitmap of permitted capabilities.
    cap_prm: CapabilityMask,
    /// Bitmap of effective capabilities.
    cap_eff: CapabilityMask,
    /// Bitmap of capabilities bounding set.
    cap_bnd: CapabilityMask,
    /// Value of the process's `no_new_privs` bit. If set to 1, the `execve`
    /// syscall promises not to grant the process any additional privileges to
    /// do anything that could not have been done without that syscall.
    no_new_privs: bool,
    /// Seccomp mode of the process. Present only if kernel configured with
    /// `CONFIG_SECCOMP`.
    seccomp: SeccompMode,
    /// Number of Seccomp filters used by process. Present only if kernel
    /// configured with `CONFIG_SECCOMP_FILTER`.
    seccomp_filters: i32,
    /// Mask of CPUs on which this process may run; isomorphic to
    /// `cpus_allowed_list`.
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
    /// Store Bypass attack (CVE-2018-3639).
    speculation_store_bypass: SpeculationStoreBypass,
    /// Indicates whether process may or may not be vulnerable to branch target
    /// injection attacks (Spectre variant 2)
    speculation_indirect_branch: SpeculationIndirectBranch,
    /// Number of times process has been context-switched voluntarily
    voluntary_ctxt_switches: u64,
    /// Number of times process has been context-switched involuntarily
    nonvoluntary_ctxt_switches: u64,
}

/// Models scheduling policies used by the kernel.
///
/// Used in the `policy` field of `/proc/{pid}/stat`. Policy names can be found
/// in `man 2 sched_setscheduler`; their values may be found in
/// `include/uapi/linux/sched.h` in the Linux kernel.
///
/// These values are printed as `u32` (`std::ffi::c_uint`) values in
/// `/proc/{pid}/stat`.
///
/// These values have implications for the `priority` and `nice` fields of
/// `/proc/{pid}/stat` (see `man 7 sched`). That said, these implications are
/// largely ignored by hard-coding the policy to a non-real-time policy.
#[derive(Debug)]
enum SchedulingPolicy {
    /// `SCHED_OTHER` policy in POSIX, also called `SCHED_NORMAL` in kernel (see
    /// `man 7 sched`). Not a real-time policy. This policy is the "standard
    /// round-robin time-sharing policy" (see `man 2 sched_setscheduler`).
    Normal = 0,
    /// `SCHED_FIFO` policy: first-in, first-out. A real-time, POSIX-compliant
    /// policy.
    Fifo = 1,
    /// `SCHED_RR` policy: a round-robin policy. A real-time, POSIX-compliant
    /// policy, which is the main distinction between this policy and the
    /// `Normal` policy.
    RoundRobin = 2,
    /// `SCHED_BATCH` policy for "batch" style execution of processes. Not a
    /// real-time policy.
    Batch = 3,
    // A value for 4 is intentionally omitted; it is reserved in the kernel for
    // `SCHED_ISO`, but that policy has not yet been implemented.
    //
    /// `SCHED_IDLE` policy for running *very* low priority jobs. Not a
    /// real-time policy.
    Idle = 5,
    /// `SCHED_DEADLINE` policy based on Earliest Deadline First and Constant
    /// Bandwidth Server algorithms. A real-time policy.
    Deadline = 6,
}

impl fmt::Display for SchedulingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: u32 = match self {
            SchedulingPolicy::Normal => 0,
            SchedulingPolicy::Fifo => 1,
            SchedulingPolicy::RoundRobin => 2,
            SchedulingPolicy::Batch => 3,
            SchedulingPolicy::Idle => 5,
            SchedulingPolicy::Deadline => 6,
        };
        write!(f, "{s}")
    }
}

/// Models `/proc/{pid}/stat`.
///
/// All information in this struct taken from `proc(5)` man page; this
/// documentation appears to be up to date for `/proc/{pid}/stat`.
///
/// We can't use the `procfs::process::Stat` type because it is marked
/// non-exhaustive.
#[derive(Debug)]
struct Stat<'a> {
    /// (1) The process ID.
    pid: Pid,
    /// (2) File name of executable, in parentheses. Limited to `TASK_COMM_LEN`
    /// bytes.
    comm: &'a str,
    /// (3) Indicates process state.
    state: proc::State,
    /// (4) The PID of the parent of this process.
    ppid: Pid,
    /// (5) The process group ID of the process.
    pgrp: Pid,
    /// (6) The session ID of the process.
    session: Pid,
    /// (7) The controlling terminal of the process.
    tty_nr: i32,
    /// (8) The ID of the foreground process group of the controlling terminal of
    /// the process.
    tpgid: Pid,
    /// (9) The kernel flags word of the prcoess. For bit meanings, set the `PF_*`
    /// defines in the Linux kernel source file `include/linux/sched.h`.
    flags: u32,
    /// (10) The number of minor faults the process has made that have not required
    /// loading a memory page from disk.
    minflt: u64,
    /// (11) The number of minor faults that the process's waited-for children have
    /// made.
    cminflt: u64,
    /// (12) The number of major faults the process has made that have required
    /// loading a memory page from disk.
    majflt: u64,
    /// (13) The number of major faults that the process' waited-for children have
    /// made.
    cmajflt: u64,
    /// (14) Amount of time that this process has been scheduled in user mode,
    /// measured in clock ticks.
    utime: u64,
    /// (15) Amount of time that this process has been scheduled in kernel mode,
    /// measured in clock ticks.
    stime: u64,
    /// (16) Amount of time that this process's waited-for children have been
    /// scheduled in user mode, measured in clock ticks.
    cutime: i64,
    /// (17) Amount of time that this process's waited-for children have been
    /// scheduled in kernel mode, measured in clock ticks.
    cstime: i64,
    /// (18) Scheduling priority. For processes running a real-time scheduling
    /// policy, this is the negated scheduling priority, minus on; that is, a
    /// number in the range -2 to -100, corresponding to real-time priorities 1
    /// to 99. For processes running under a non-real-time scheduling policy,
    /// this is the raw nice value as represented in the kernel. The kernel
    /// stores nice values as numbers in the range 0 (high) to 39 (low),
    /// corresponding to the user-visible nice range of -20 (high) to 19 (low).
    priority: i64,
    /// (19) The nice value, a value in the range 19 (low priority) to -20 (high
    /// priority).
    nice: i64,
    /// (20) Number of threads in this process.
    num_threads: i64,
    /// (21) The time in jiffies before the next `SIGALRM` is sent to the process due
    /// to an interval timer. Since Linux 2.6.17, this field is no longer
    /// maintained, and is hard coded as 0.
    itrealvalue: i64,
    /// (22) The time the process started after system boot, expressed in clock ticks.
    starttime: u64,
    /// (23) Virtual memory size in bytes.
    vsize: u64,
    /// (24) Resident set size: number of pages the process has in real memory. This
    /// value is inaccurate; see `/proc/{pid}/statm` for details.
    rss: i64,
    /// (25) Current soft limit in bytes on the RSS of the process; see the
    /// description of `RLIMIT_RSS` in `getrlimit(2)`.
    rsslim: u64,
    /// (26) The address above which program text can run.
    startcode: u64,
    /// (27) The address below which program text can run.
    endcode: u64,
    /// (28) The address of the start (i.e., bottom) of the stack.
    startstack: u64,
    /// (29) The current value of ESP (stack pointer), as found in the kernel stack
    /// pages for the process.
    kstkesp: u64,
    /// (30) The current EIP (instruction pointer).
    kstkeip: u64,
    /// (31) The bitmap of pending signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    signal: u64,
    /// (32) The bitmap of blocked signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    blocked: u64,
    /// (33) The bitmap of ignored signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    sigignore: u64,
    /// (34) The bitmap of caught signals, displayed as a decimal number. Obsolete,
    /// because it does not provide information on real-time signals; use
    /// `/proc/{pid}/status` instead.
    sigcatch: u64,
    /// (35) This is the "channel" in which the process is waiting. It is the address
    /// of a location in the kernel where the process is sleeping. The
    /// corresponding symbolic name can be found in `/proc/{pid}/wchan`.
    wchan: u64,
    /// (36) Number of pages swapped (not maintained). Hard coded as 0.
    nswap: u64,
    /// (37) Cumulative `nswap` for child processes (not maintained). Hard coded as
    /// 0.
    cnswap: u64,
    /// (38) Signal to be sent to parent when process dies.
    exit_signal: i32,
    /// (39) CPU number last executed on.
    processor: i32,
    /// (40) Real-time scheduling priority, a number in the range 1 to 99 for
    /// processes scheduled under a real-time policy, or 0, for non-real-time
    /// processes.
    rt_priority: u32,
    /// (41) Scheduling policy. Decode using the `SCHED_*` constants in
    /// `linux/sched.h`.
    policy: SchedulingPolicy,
    /// (42) Aggregated block I/O delays, measured in clock ticks.
    delayacct_blkio_ticks: u64,
    /// (43) Guest time of the process (time spend running a virtual CPU for a guest
    /// operating system), measured in clock ticks.
    guest_time: u64,
    /// (44) Guest time of the process's children, measured in clock ticks.
    cguest_time: u64,
    /// (45) Address above which program initialized and uninitialized (BSS) data are
    /// placed.
    start_data: u64,
    /// (46) Address below which program initialized and uninitialized (BSS) data are
    /// placed.
    end_data: u64,
    /// (47) Address above which program heap can be expanded with `brk(2)`.
    start_brk: u64,
    /// (48) Address above which program command-line arguments (`argv`) are placed.
    arg_start: u64,
    /// (49) Address below which program command-line arguments (`argv`) are placed.
    arg_end: u64,
    /// (50) Address above which program environment is placed.
    env_start: u64,
    /// (51) Address below which program environment is placed.
    env_end: u64,
    /// (52) The thread's exit status in the form reported by `waitpid(2)`.
    exit_code: i32,
}

/// Generates [`Stat`].
struct StatGenerator {
    /// The process ID to assign to the `/proc/{pid}/stat` file.
    ///
    /// This datum is a field so that we can foce `/proc/{pid}/stat` and
    /// `/proc/{pid}/status` to have, at minimum, the same `pid`.
    pid: Pid,
    /// Pool used to generate strings for task's `comm` name.
    pool: strings::Pool,
}

impl StatGenerator {
    /// Construct new instance of [`StatGenerator`].
    fn new<R>(pid: Pid, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        Self {
            pid,
            pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for StatGenerator {
    type Output = Stat<'a>;

    /// Generates a [`Stat`] instance (modeling `/proc/{pid}/stat`) under the
    /// following assumptions:
    ///
    /// - `pid == pgrp == session`
    /// - `policy == SCHED_OTHER == 0` (which is not a real-time scheduling
    ///   policy)
    /// - process has at most 32 threads (`1 <= num_threads <= 32`)
    /// - process is assigned to processor index between 0 and 7 inclusive (so
    ///   machine is assumed to have 8 processors; `0 <= processor < 8`).
    /// - `rss` is strictly positive
    /// - `rss <= rsslim` may *not* be true
    /// - `startcode <= endcode` may *not* be true
    /// - `start_data <= end_data` may *not* be true
    /// - `arg_start <= arg_end` may *not* be true
    /// - `env_start <= env_end` may *not* be true
    /// - The ranges `startcode..endcode`, `start_data..end_data`,
    ///   `arg_start..arg_end`, `env_start..env_end` may *not* have pairwise
    ///   empty intersections.
    fn generate<R>(&'a self, mut rng: &mut R) -> Self::Output
    where
        R: Rng + ?Sized,
    {
        let pid: Pid = self.pid;

        // Assume task scheduling policy is `SCHED_OTHER` (i.e., not a real-time
        // scheduling policy)
        let priority = rng.gen_range(0..39);
        let nice = priority - 20;

        Stat {
            pid,
            comm: self.pool.of_size_range(&mut rng, 1..TASK_COMM_LEN).unwrap(),
            state: rng.gen(),
            ppid: rng.gen(),
            // Assume process group ID group and session ID are eqaul to PID
            // because this situation seems to be common.
            pgrp: pid,
            session: pid,
            tty_nr: rng.gen(),
            tpgid: rng.gen(),
            flags: rng.gen(),
            minflt: rng.gen(),
            cminflt: rng.gen(),
            majflt: rng.gen(),
            cmajflt: rng.gen(),
            utime: rng.gen(),
            stime: rng.gen(),
            cutime: rng.gen(),
            cstime: rng.gen(),
            priority,
            nice,
            // Assume up to 32 threads per process, arbitrarily.
            num_threads: rng.gen_range(1..=32),
            itrealvalue: rng.gen(),
            starttime: rng.gen(),
            vsize: rng.gen(),
            rss: rng.gen_range(1..i64::MAX),
            // For now, ignore that rss <= rsslim should hold
            rsslim: rng.gen(),
            startcode: rng.gen(),
            // For now, ignore that startcode <= endcode should hold
            endcode: rng.gen(),
            startstack: rng.gen(),
            // Assume `kstkesp` & `kstkeip` are both 0 because this property
            // seems to hold across a variety of processes
            kstkesp: 0,
            kstkeip: 0,
            signal: rng.gen(),
            blocked: rng.gen(),
            sigignore: rng.gen(),
            sigcatch: rng.gen(),
            wchan: rng.gen(),
            nswap: 0,
            cnswap: 0,
            exit_signal: rng.gen(),
            // Assume machine has 8 cores for now
            processor: rng.gen_range(0..8),
            // Due to asumptions above, this value must be 0; the process is
            // assumed not to be a real-time process. If this value is modified,
            // then `priority`, `nice`, and `policy` must also be modified.
            rt_priority: 0,
            // Hardcoded to a non-real-time scheduling policy; may be relaxed
            // later.
            policy: SchedulingPolicy::Normal,
            delayacct_blkio_ticks: rng.gen(),
            guest_time: rng.gen(),
            cguest_time: rng.gen(),
            start_data: rng.gen(),
            // Although end_data should probably satisfy the property
            // end_data >= start_data, this implementation ignores that
            // property for simplicity.
            end_data: rng.gen(),
            start_brk: rng.gen(),
            arg_start: rng.gen(),
            // Although arg_end should probably satisfy the property
            // arg_end >= arg_start, this implementation ignores that property
            // for simplicity.
            arg_end: rng.gen(),
            env_start: rng.gen(),
            // Although env_end should probably satisfy the property
            // env_end >= env_start, this implementation ignores that property
            // for simplicity.
            env_end: rng.gen(),
            // Exit code is assigned arbitrarily, for simplicity.
            exit_code: rng.gen(),
        }
    }
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
struct Process<'a> {
    /// Command line for process (unless a zombie); corresponds to
    /// `/proc/{pid}/cmdline`.
    cmdline: String,
    /// Command name associated with process. Truncated to `TASK_COMM_LEN`
    /// bytes.
    comm: String,
    /// Corresponds to `/proc/{pid}/io`.
    io: proc::Io,
    /// Corresponds to `/proc/{pid}/stat`.
    stat: Stat<'a>,
    /// Corresponds to `/proc/{pid}/statm`.
    statm: Statm,
    /// Corresponds to `/proc/{pid}/status`.
    status: Status,
}

/*

impl Process {
    /// Create a new [`Process`] modeling `/proc/{pid}` files.
    ///
    /// Very much a work-in-progress because some common setup work should be
    /// factored out into a separate type.
    fn new(rng: &mut rngs::StdRng, config: &Config) -> Self {
        // Generate length of executable name (could be length 0).

        // Generate executable name

        // Generate number of arguments (if executable name has positive length)

        // Generate each argument

        // Assemble into a command line, which will be assigned to the
        // `cmdline` field of `Self`.

        // Generate 7 random `u64` elements for `task::Io `struct.

        // --- start generating Status struct --- //

        // Generate task name (up to 64 bytes). This name is related to
        // cmdline, but could be different. I don't think this string can be
        // empty.

        // TODO(geoffrey.oxberry@datadoghq.com): Add remaining fields.
        //
        // The following fields should be pretty straightforward:
        //
        // - umask: choose any legel variant of that `enum` type
        // - state: choose any variant of `task::State`
        // - tgid, ngid, pid, ppid, tracer_pid: choose an i32.
        // - fd_size: may need to check `/proc/sys/fs/file-max` or `ulimit`
        //   before deciding on a `u64` range to use
        // - kthread: choose a random bool
        // - vm_, rss_*, huge_tlb_pages: pick `u64` values; the display value
        //   will need to be in kibibytes (but printed as "kB").
        // - core_dumping, thp_enabled: choose a random bool
        // - untag_mask: IIRC, could be 0xffffffffffffffff on architectures that
        //   don't support masking, and on those that do, need to know if
        //   addresses are 48 bits or 57 bits. In practice, maybe it could be a
        //   random `u64`; not sure it's important.
        //
        // The following fields are fields I need to look into:
        //
        // - uid: much of the time, real, effective, saved set, and filesystem
        //   UIDs should be the same, but not always.
        // - gid: much of the time, real, effective, saved set, and filesystem
        //   GIDs could be the same, but not always.
        // - ns_tgid: ?
        // - ns_pid: ?
        // - ns_sid: ?

        // --- end generating Status struct --- //

        // Statm struct is basically a view of the Status struct, assuming we're
        // running on a kernel configured with an MMU. (For our main cases of
        // interest, x86_64 and arm64 machines, this assumption is true.) How
        // this view should be constructed is discussed in the comments for the
        // `Statm` struct.

        // Truncate task name to 16 bytes & store in `comm` field of `Self`.
    }
}

*/