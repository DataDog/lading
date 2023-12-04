//! Procfs payload.

use core::fmt;

use crate::{common::strings, Generator};

use rand::{distributions::Standard, prelude::Distribution, Rng};

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
const UMASK_MAX: u32 = 4096;

/// Maximum number of supplemental groups a process can belong to. Corresponds
/// to macro of the same name in `include/uapi/linux/limits.h`.
const NGROUPS_MAX: usize = 65536;

/// Maximum number of bytes in any component of a path name, including a null
/// byte. Rust does not automatically terminate strings with a null byte, so we
/// subtract one from the macro of the same name in
/// `include/uapi/linux/limits.h`. To simplify the generation of
/// `/proc/{pid}/cmdline`, we assume the command line has one component and no
/// arguments.
const NAME_MAX: usize = 254;

/// Maximum number of Seccomp filters that can be attached to a given thread.
/// This number is calculated from information in `man 2 seccomp`. The maximum
/// number of such instructions that can be attached to a thread cannot exceed
/// `MAX_INSNS_PER_PATH` (32768). In computing this number, each filter program
/// incurs an overhead of 4 instructions, and filter programs must contain at
/// least one instruction (or an error code is returned). Consequently, the
/// maximum number of filters that can be attached to a given thread is
/// floor(32768 / 5) = 6553. In actual practice, the number of filters attached
/// to a process is likely considerably less.
const SECCOMP_FILTER_MAX: i32 = 6553;

/// Assumed number of processors, for simplicity. This number was chosen
/// arbitrarily to mimic a small server.
const ASSUMED_NPROC: i32 = 8;

/// Assumed number of max threads, for simplicity. This number was chosen
/// arbitrarily.
const ASSUMED_THREAD_MAX: i32 = 32;

/// Assumed maximum number of groups associated with a process, which must be
/// less than `NGROUPS_MAX`. This number was chosen arbitrarily, on the grounds
/// that most background processes on a developer box belong to at most a dozen
/// or so groups, allowing for systems that may have more groups due to more
/// service users.
const ASSUMED_NGROUPS_MAX: usize = 32;

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

    impl ::std::fmt::Display for self::Io {
        /// Formats [`Io`] as it would be seen in `/proc/{pid}/io`.
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            write!(
                f,
                concat!(
                    "rchar: {rchar}\n",
                    "wchar: {wchar}\n",
                    "syscr: {syscr}\n",
                    "syscw: {syscw}\n",
                    "read_bytes: {read_bytes}\n",
                    "write_bytes: {write_bytes}\n",
                    "cancelled_write_bytes: {cancelled_write_bytes}"
                ),
                rchar = self.rchar,
                wchar = self.wchar,
                syscr = self.syscr,
                syscw = self.syscw,
                read_bytes = self.read_bytes,
                write_bytes = self.write_bytes,
                cancelled_write_bytes = self.cancelled_write_bytes
            )
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

    impl State {
        /// Formats [`State`] as displayed in `/proc/{pid}/status`.
        pub(super) fn as_status_code(&self) -> &str {
            match self {
                State::Running => "R (running)",
                State::Sleeping => "S (sleeping)",
                State::DiskSleep => "D (disk sleep)",
                State::Stopped => "T (stopped)",
                State::TracingStop => "t (tracing stop)",
                State::Dead => "X (dead)",
                State::Zombie => "Z (zombie)",
                State::Parked => "P (parked)",
                State::Idle => "I (idle)",
                _ => unreachable!(),
            }
        }

        /// Formats [`State`] as displayed in `/proc/{pid}/stat`.
        pub(super) fn as_stat_code(&self) -> &str {
            match self {
                State::Running => "R",
                State::Sleeping => "S",
                State::DiskSleep => "D",
                State::Stopped => "T",
                State::TracingStop => "t",
                State::Dead => "X",
                State::Zombie => "Z",
                State::Parked => "P",
                State::Idle => "I",
                _ => unreachable!(),
            }
        }
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

impl fmt::Display for Statm {
    /// Outputs [`Statm`] as formatted for `/proc/{pid}/statm`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{size} {resident} {shared} {trs} {lrs} {drs} {dt}",
            size = self.size,
            resident = self.resident,
            shared = self.shared,
            trs = self.trs,
            lrs = self.lrs,
            drs = self.drs,
            dt = self.dt
        )
    }
}

/// Models a `umask`, equivalent to [`std::ffi::c_uint`].
#[derive(Debug)]
struct Umask(u32);

impl Distribution<Umask> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Umask
    where
        R: Rng + ?Sized,
    {
        Umask(rng.gen_range(0..UMASK_MAX))
    }
}

impl fmt::Display for Umask {
    /// Capability masks are displayed in [`fmt::Octal`] format by default.
    ///
    /// The format is left-zero-padded to four octal digits. There is no leading
    /// `0o` in the display representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{mask:04o}", mask = self.0)
    }
}

/// Models a device bit field, equivalent to [`std::ffi::c_int`].
///
/// This data is modeled by the `dev_t` type in the kernel, which is a
/// [`std::ffi::c_uint`] that is cast to a [`std::ffi::c_int`] and encodes a
/// bit field. The minor device number is contained in the combination of bits 31 to 20
/// and 7 to 0. The major device number is in bits 15 to 8. (Bits 16 to 19
/// are ignored.)
#[derive(Debug)]
struct DeviceMask(i32);

impl Distribution<DeviceMask> for Standard {
    /// Generates a valid [`DeviceMask`] bit field with uniform probability.
    fn sample<R>(&self, rng: &mut R) -> DeviceMask
    where
        R: Rng + ?Sized,
    {
        // This operation could likely be done more efficiently by unrolling
        // both loops, generating the low-order 16 bits all at once, then
        // generating the 12 high-order bits all at once. Even though this code
        // is performance-sensitive, the simple approach is used here to land a
        // simple implementation before later performance optimizations.
        let mut mask: i32 = 0;
        for index in 0..16 {
            let bit: i32 = rng.gen_range(0..=1);
            mask |= bit << index;
        }
        for index in 20..32 {
            let bit: i32 = rng.gen_range(0..=1);
            mask |= bit << index;
        }
        DeviceMask(mask)
    }
}

impl fmt::Display for DeviceMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{mask}", mask = self.0)
    }
}

/// Models a process ID number, equivalent to [`std::ffi::c_int`].
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

impl fmt::Display for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Models a user ID number, equivalent to [`std::ffi::c_uint`].
///
/// This data is modeled by the `uid_t` type in the Linux kernel.
#[derive(Debug, Clone, Copy)]
struct Uid(u32);

impl Distribution<Uid> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Uid
    where
        R: Rng + ?Sized,
    {
        Uid(rng.gen())
    }
}

impl fmt::Display for Uid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Models a group ID number, equivalent to [`std::ffi::c_uint`].
///
/// This data is modeled by the `gid_t` type in the Linux kernel.
#[derive(Debug, Clone, Copy)]
struct Gid(u32);

impl Distribution<Gid> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Gid
    where
        R: Rng + ?Sized,
    {
        Gid(rng.gen())
    }
}

impl fmt::Display for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Models memory size entries in `/proc/{pid}/status` *only*.
///
/// Models memory size entries (e.g., `VmPeak`, `VmSize`) in
/// `/proc/{pid}/status`. These entries are quirky because:
///
/// - the underlying memory size data is stored in units of bytes
/// - the output is displayed in units of "kB", which in this context means
///   *kibibyte* (i.e., 1024 bytes).
#[derive(Debug, Clone, Copy)]
struct MemSize(u64);

impl Distribution<MemSize> for Standard {
    fn sample<R>(&self, rng: &mut R) -> MemSize
    where
        R: Rng + ?Sized,
    {
        MemSize(rng.gen())
    }
}

impl fmt::Display for MemSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size_in_bytes = self.0;
        let size_in_kibibytes = size_in_bytes >> 10;
        write!(f, "{size_in_kibibytes} kB")
    }
}

/// Models SigQ field of `/proc/{pid}/status`.
///
/// Equivalent to `(std::ffi::c_uint, std::ffi::c_ulong)`.
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

impl fmt::Display for SignalMask {
    /// Signal masks are displayed in [`fmt::LowerHex`] format by default.
    ///
    /// In particular, a leading `0x` is used in the display representation, and
    /// the output is left-zero-padded to exactly 8 digits (excluding the
    /// leading `0x`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{mask:#08x}", mask = self.0)
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

impl fmt::Display for CapabilityMask {
    /// Capability masks are displayed in [`fmt::LowerHex`] format by default.
    ///
    /// In particular, a leading `0x` is used in the display representation, and
    /// the output is left-zero-padded to exactly 8 digits (excluding the
    /// leading `0x`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{mask:#08x}", mask = self.0)
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

impl fmt::Display for MaskEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{mask:x}", mask = self.0)
    }
}

/// Models `{Cpus,Mems}_allowed` fields of `/proc/{pid}/status`.
///
/// Models a vector of [`MaskEntry`] elements for formatting purposes.
#[derive(Debug)]
struct MaskCollection(Vec<MaskEntry>);

impl fmt::Display for MaskCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let string_vec: Vec<_> = self.0.iter().map(MaskEntry::to_string).collect();
        let output = &string_vec.join(",");
        write!(f, "{output}")
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

impl fmt::Display for ListEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ListEntry::Single(entry) => write!(f, "{entry}"),
            ListEntry::RangeInclusive { first, last } => write!(f, "{first}-{last}"),
        }
    }
}

/// Models `{Cpus,Mems}_allowed_list` fields of `/proc/{pid}/status`.
///
/// Models a vector of [`ListEntry`] elements for formatting purposes.
#[derive(Debug)]
struct ListCollection(Vec<ListEntry>);

impl fmt::Display for ListCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let string_vec: Vec<_> = self.0.iter().map(ListEntry::to_string).collect();
        let output = &string_vec.join(",");
        write!(f, "{output}")
    }
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
/// The `Kthread` field is omitted for the time being.
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
    /// Filename of executable (with escapes, limited to [`TASK_NAME_LEN`] bytes).
    name: String,
    /// File mode creation mask.
    umask: Umask,
    /// State of process.
    state: proc::State,
    /// Thread group ID.
    tgid: Pid,
    /// NUMA group ID.
    ngid: Pid,
    /// Process ID.
    pid: Pid,
    /// Process ID of parent process.
    ppid: Pid,
    /// PID of process tracing this process (0 if not, or the trace is outside
    /// of the current pid namespace).
    tracer_pid: Pid,
    /// Real UID.
    ruid: Uid,
    /// Effective UID.
    euid: Uid,
    /// Saved set UID.
    suid: Uid,
    /// Filesystem UID.
    fuid: Uid,
    /// Real GID.
    rgid: Gid,
    /// Effective GID.
    egid: Gid,
    /// Saved set GID.
    sgid: Gid,
    /// Filesystem GID.
    fgid: Gid,
    /// Number of file descriptor slots currently allocated. Cannot exceed the
    /// value stored in `/proc/sys/fs/file-max`. A unprivileged user process may
    /// not exceed the output of `ulimit -n -H`.
    fd_size: u64,
    /// Supplementary group list.
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
    /// Peak virtual memory size (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_peak: MemSize,
    /// Total program size (in bytes). Present only if task has non-null memory
    /// management pointer.
    vm_size: MemSize,
    /// Locked memory size (in bytes). Present only if task has non-null memory
    /// management pointer.
    vm_lck: MemSize,
    /// Pinned memory size (in bytes). Present only if task has non-null memory
    /// management pointer.
    vm_pin: MemSize,
    /// Peak resident set size (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_hwm: MemSize,
    /// Resident set size (in bytes) = rss_anon + rss_file + rss_shmem. Present
    /// only if task has non-null memory management pointer.
    vm_rss: MemSize,
    /// Resident anonymous memory size (in bytes). Present only if task has
    /// non-null memory management pointer.
    rss_anon: MemSize,
    /// Resident file mappings size (in bytes). Present only if task has
    /// non-null memory management pointer.
    rss_file: MemSize,
    /// Resident shmem memory size (in bytes; includes SysV shm, tmpfs mapping,
    /// shared anonymous mappings). Present only if task has non-null memory
    /// management pointer.
    rss_shmem: MemSize,
    /// Size of private data segments (in bytes). Present only if task has
    /// non-null memory management pointer.
    vm_data: MemSize,
    /// Size of stack segments (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_stk: MemSize,
    /// Size of text segment (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_exe: MemSize,
    /// Size of shared library code (in bytes). Present only if task has
    /// non-null memory management pointer.
    vm_lib: MemSize,
    /// Size of page table entries (in bytes). Present only if task has non-null
    /// memory management pointer.
    vm_pte: MemSize,
    /// Size of swap used by anonymous private data (in bytes; does not include
    /// shmem swap). Present only if task has non-null memory management
    /// pointer.
    vm_swap: MemSize,
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
    cpus_allowed: MaskCollection,
    /// List of CPUs on which this process may run; isomorphic to `cpus_allowed`.
    cpus_allowed_list: ListCollection,
    /// Mask of memory nodes allowed to this process; isomorphic to
    /// `mems_allowed_list`.
    mems_allowed: MaskCollection,
    /// List of memory nodes allowed to this process; isomorphic to
    /// `mems_allowed`.
    mems_allowed_list: ListCollection,
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

// TODO(geoffrey.oxberry@datadoghq.com): Implement `std::Display` for `Status`.
// Then, theoretically, I could use `format!` to stringify it and compute its
// length as a string for the process generator. I could try to figure out an
// upper bound on the length by hand, but if I have to serialize it to a string
// anyway, and Rust is going to compute the length of that string in bytes, I
// may as well have Rust compute the length for me because it will be more
// accurate.

/// Generates [`Status`].
struct StatusGenerator {
    /// The process ID to assign to the `/proc/{pid}/status` file.
    ///
    /// Its purpose is to enable clients to force `/proc/{pid}/stat` and
    /// `/proc/{pid}/status` to have, at minimum, the same `pid`.
    pid: Pid,
    /// Task's longer `comm` name; has length at most [`TASK_NAME_LEN`] bytes.
    /// Used to avoid the need for multiple string pools. The name of this field
    /// is a snake case version of the corresponding `Name` field in
    /// `/proc/{pid}/status`.
    name: String,
}

impl StatusGenerator {
    /// Construct new instance of [`StatusGenerator`].
    fn new<R>(pid: Pid, name: String, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        assert!(name.len() <= TASK_NAME_LEN);
        Self { pid, name }
    }
}

impl<'a> Generator<'a> for StatusGenerator {
    type Output = Status;

    fn generate<R>(&'a self, rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        let pid = self.pid;
        let uid: Uid = rng.gen();
        let gid = Gid(uid.0);
        let seccomp: SeccompMode = rng.gen();

        // Seccomp_filters is 0 unless the Seccomp mode is SECCOMP_MODE_FILTER.
        let seccomp_filters = match &seccomp {
            // If Seccomp mode is SECCOMP_MODE_FILTER, then Seccomp_filter is at
            // least 1.
            SeccompMode::Filter => rng.gen_range(1..SECCOMP_FILTER_MAX),
            SeccompMode::Disabled | SeccompMode::Strict => 0,
        };

        // For simplicity, assume each process uses all available CPUs.
        let mut cpu_mask = 0_u32;
        for i in 0..ASSUMED_NPROC {
            cpu_mask |= (1 << i);
        }
        let cpus_allowed = MaskCollection(vec![MaskEntry(cpu_mask)]);
        let cpus_allowed_list = ListCollection(vec![ListEntry::RangeInclusive {
            first: 0,
            last: (ASSUMED_NPROC - 1) as u64,
        }]);

        // Sanity check the assumed number of groups a process may belong to here,
        // because this location is the only place where that data is used.
        assert!(ASSUMED_NGROUPS_MAX <= NGROUPS_MAX);

        // The `Groups` field of `/proc/{pid}/status` is allowed to be *empty*.
        let ngroups = rng.gen_range(0..=ASSUMED_NGROUPS_MAX);
        let mut groups = Vec::with_capacity(ngroups);
        for _ in 0..ngroups {
            groups.push(Gid(rng.gen()));
        }

        Status {
            name: self.name.clone(),
            umask: rng.gen(),
            state: rng.gen(),
            tgid: pid,
            ngid: pid,
            pid,
            ppid: rng.gen(),
            // For simplicity, we assume arbitrarily that our generated
            // processes are not being traced, which corresponds to setting
            // tracer_pid to 0.
            tracer_pid: Pid(0),
            ruid: uid,
            euid: uid,
            suid: uid,
            fuid: uid,
            rgid: gid,
            egid: gid,
            sgid: gid,
            fgid: gid,
            fd_size: rng.gen(),
            groups,
            ns_tgid: vec![pid],
            ns_pid: vec![pid],
            ns_pgid: vec![pid],
            ns_sid: vec![pid],
            vm_peak: rng.gen(),
            vm_size: rng.gen(),
            vm_lck: rng.gen(),
            vm_pin: rng.gen(),
            vm_hwm: rng.gen(),
            vm_rss: rng.gen(),
            rss_anon: rng.gen(),
            rss_file: rng.gen(),
            rss_shmem: rng.gen(),
            vm_data: rng.gen(),
            vm_stk: rng.gen(),
            vm_exe: rng.gen(),
            vm_lib: rng.gen(),
            vm_pte: rng.gen(),
            vm_swap: rng.gen(),
            huge_tlb_pages: rng.gen(),
            core_dumping: rng.gen(),
            thp_enabled: rng.gen(),
            untag_mask: rng.gen(),
            threads: rng.gen_range(1..=ASSUMED_THREAD_MAX),
            sigq: SigQ {
                signals_queued: rng.gen(),
                max_number_for_queue: rng.gen(),
            },
            sig_pnd: rng.gen(),
            shd_pnd: rng.gen(),
            sig_blk: rng.gen(),
            sig_ign: rng.gen(),
            sig_cgt: rng.gen(),
            cap_inh: rng.gen(),
            cap_prm: rng.gen(),
            cap_eff: rng.gen(),
            cap_bnd: rng.gen(),
            no_new_privs: rng.gen(),
            seccomp,
            seccomp_filters,
            // Mask entries are displayed in hexadecimal, hence the use of
            // hexadecimal literals. For simplicity, assume each process uses
            // all available CPUs.
            cpus_allowed,
            cpus_allowed_list,
            // Assume a single NUMA node is available due to a lack of
            // real-world examples of processes that are allowed to allocate
            // memory on a NUMA node other than node 0
            mems_allowed: MaskCollection(vec![MaskEntry(0x0)]),
            mems_allowed_list: ListCollection(vec![ListEntry::Single(0)]),
            speculation_store_bypass: rng.gen(),
            speculation_indirect_branch: rng.gen(),
            voluntary_ctxt_switches: rng.gen(),
            nonvoluntary_ctxt_switches: rng.gen(),
        }
    }
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
struct Stat {
    /// (1) The process ID.
    pid: Pid,
    /// (2) File name of executable, in parentheses. Limited to [`TASK_COMM_LEN`]
    /// bytes.
    comm: String,
    /// (3) Indicates process state.
    state: proc::State,
    /// (4) The PID of the parent of this process.
    ppid: Pid,
    /// (5) The process group ID of the process.
    pgrp: Pid,
    /// (6) The session ID of the process.
    session: Pid,
    /// (7) The controlling terminal of the process. This number is a bit field.
    /// The minor device number is contained in the combination of bits 31 to 20
    /// and 7 to 0. The major device number is in bits 15 to 8. (Bits 16 to 19
    /// are zeros.)
    tty_nr: DeviceMask,
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

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The concat macro is used here in an attempt to avoid excessively long
        // lines of string literals. Literals are deliberately grouped into rows
        // of five for readability (except for the last line), but
        // `/proc/{pid}/stat` displays all of the corresponding fields on one
        // line.
        write!(
            f,
            concat!(
                "{pid} {comm} {state} {ppid} {pgrp} ",
                "{session} {tty_nr} {tpgid} {flags} {minflt} ",
                "{cminflt} {majflt} {cmajflt} {utime} {stime} ",
                "{cutime} {cstime} {priority} {nice} {num_threads} ",
                "{itrealvalue} {starttime} {vsize} {rss} {rsslim} ",
                "{startcode} {endcode} {startstack} {kstkesp} {kstkeip} ",
                "{signal} {blocked} {sigignore} {sigcatch} {wchan} ",
                "{nswap} {cnswap} {exit_signal} {processor} {rt_priority} ",
                "{policy} {delayacct_blkio_ticks} {guest_time} {cguest_time} {start_data}",
                "{end_data} {start_brk} {arg_start} {arg_end} {env_start} ",
                "{env_end} {exit_code}"
            ),
            pid = self.pid,
            comm = self.comm,
            state = self.state.as_stat_code(),
            ppid = self.ppid,
            pgrp = self.pgrp,
            session = self.session,
            tty_nr = self.tty_nr,
            tpgid = self.tpgid,
            flags = self.flags,
            minflt = self.minflt,
            cminflt = self.cminflt,
            majflt = self.majflt,
            cmajflt = self.cmajflt,
            utime = self.utime,
            stime = self.stime,
            cutime = self.cutime,
            cstime = self.cstime,
            priority = self.priority,
            nice = self.nice,
            num_threads = self.num_threads,
            itrealvalue = self.itrealvalue,
            starttime = self.starttime,
            vsize = self.vsize,
            rss = self.rss,
            rsslim = self.rsslim,
            startcode = self.startcode,
            endcode = self.endcode,
            startstack = self.startstack,
            kstkesp = self.kstkesp,
            kstkeip = self.kstkeip,
            signal = self.signal,
            blocked = self.blocked,
            sigignore = self.sigignore,
            sigcatch = self.sigcatch,
            wchan = self.wchan,
            nswap = self.nswap,
            cnswap = self.cnswap,
            exit_signal = self.exit_signal,
            processor = self.processor,
            rt_priority = self.rt_priority,
            policy = self.policy,
            delayacct_blkio_ticks = self.delayacct_blkio_ticks,
            guest_time = self.guest_time,
            cguest_time = self.cguest_time,
            start_data = self.start_data,
            end_data = self.end_data,
            start_brk = self.start_brk,
            arg_start = self.arg_start,
            arg_end = self.arg_end,
            env_start = self.env_start,
            env_end = self.env_end,
            exit_code = self.exit_code
        )
    }
}

/// Generates [`Stat`].
struct StatGenerator {
    /// The process ID to assign to the `/proc/{pid}/stat` file.
    ///
    /// Its purpose is to enable clients to force `/proc/{pid}/stat` and
    /// `/proc/{pid}/status` to have, at minimum, the same `pid`.
    pid: Pid,
    /// String for task's `comm` name. Used to avoid having multiple string
    /// pools, particularly because this string slice should be at most 16
    /// bytes.
    comm: String,
}

impl StatGenerator {
    /// Construct new instance of [`StatGenerator`].
    fn new<R>(pid: Pid, comm: String, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        assert!(comm.len() <= TASK_COMM_LEN);
        Self { pid, comm }
    }
}

impl<'a> Generator<'a> for StatGenerator {
    type Output = Stat;

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
        // scheduling policy). The range of task priorities in the kernel in this
        // scenario is 0 (high) to 39 (low), inclusive. (See `man 5 proc`.)
        let policy = SchedulingPolicy::Normal;
        let priority = rng.gen_range(0..40);
        let nice = priority - 20;

        // Due to asumptions above, this value must be 0; the process is
        // assumed not to be a real-time process. If this value is modified,
        // then `priority`, `nice`, and `policy` must also be modified.
        let rt_priority = 0;

        Stat {
            pid,
            comm: self.comm.clone(),
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
            num_threads: rng.gen_range(1..=(ASSUMED_THREAD_MAX as i64)),
            itrealvalue: 0,
            starttime: rng.gen(),
            vsize: rng.gen(),
            rss: rng.gen_range(1..i64::MAX),
            // For now, ignore that rss <= rsslim should hold.
            rsslim: rng.gen(),
            startcode: rng.gen(),
            // For now, ignore that startcode <= endcode should hold.
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
            processor: rng.gen_range(0..ASSUMED_NPROC),
            rt_priority,
            policy,
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
/// - cmdline (string containing command line)
/// - comm (string of [`TASK_COMM_LEN`] characters or less).
/// - io
/// - stat
/// - statm
/// - status
///
/// so this struct reflects that behavior.
#[derive(Debug)]
pub struct Process {
    /// Command line for process (unless a zombie); corresponds to
    /// `/proc/{pid}/cmdline`. Unlike other fields, must be serialized to a
    /// binary format. This field is basically the `argv` argument to `execve`,
    /// serialized in binary on a single line, including all of the null
    /// characters. For example, a command like `python3 -m pip freeze` would be
    /// serialized to a single, null-terminated line in binary in which each of
    /// the interior whitespace characters is replaced with a null character:
    /// `python3^@-m^@pip^@freeze`, where the digraph `^@` represents the "null
    /// character" grapheme. (This digraph was chosen because that is how `less`
    /// renders a null character.)
    cmdline: String,
    /// Command name associated with process. Truncated to [`TASK_COMM_LEN`]
    /// bytes.
    comm: String,
    /// Corresponds to `/proc/{pid}/io`.
    io: proc::Io,
    /// Corresponds to `/proc/{pid}/stat`.
    stat: Stat,
    /// Corresponds to `/proc/{pid}/statm`.
    statm: Statm,
    /// Corresponds to `/proc/{pid}/status`.
    status: Status,
}

/// Generates a [`Process`].
#[derive(Debug)]
pub struct ProcessGenerator {
    str_pool: strings::Pool,
}

impl ProcessGenerator {
    /// Construct a new instance of `ProcessGenerator`.
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for ProcessGenerator {
    type Output = Process;

    /// Generates a [`Process`].
    ///
    /// Generates a [`Process`] with a random the following caveats:
    ///
    /// - The `/proc/{pid}/stat`, `/proc/{pid}/status`, and `/proc/{pid}/statm`
    ///   files aren't necessarily consistent with each other, nor are any of
    ///   these files necessarily internally consistent. Notably, none of the
    ///   virtual memory fields will be consistent among these files; there may
    ///   be other inconsistencies.
    /// - The `/proc/{pid}/cmdline` file will consist of a single string of
    ///   length at most [`NAME_MAX`] bytes, which is the maximum length of a
    ///   single path element in Linux. For simplicity, this string models a
    ///   command with no arguments.
    /// - The `/proc/{pid}/comm` file, corresponding to the process's command
    ///   name, is the leading substring of at most [`TASK_COMM_LEN`] bytes from
    ///   `/proc/{pid}/cmdline`. If `/proc/{pid}/cmdline` is shorter than
    ///   [`TASK_COMM_LEN`] bytes, then `/proc/{pid}/comm` and
    ///   `/proc/{pid}/cmdline` are identical.
    fn generate<R>(&'a self, rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        // For simplicity, assume the command line is a single string consisting
        // of a single path component and no arguments.
        let cmdline_size: usize = rng.gen_range(1..NAME_MAX);

        // SAFETY: If this call fails, then execution should panic because an
        // inability to generate process command lines is a serious bug.
        let cmdline = String::from(self.str_pool.of_size(rng, cmdline_size).unwrap());

        // Assume the comm name and task name are derived from `cmdline`. Note
        // from `man 5 proc` that a thread may modify its `comm` (command name)
        // and/or its `cmdline` from what is executed in the terminal, so
        // there's nothing that forces the command name or task name to be a
        // subset of the command line.
        let comm_size = std::cmp::min(TASK_COMM_LEN, cmdline.len());
        let comm = String::from(&cmdline[..comm_size]);

        let name_size = std::cmp::min(TASK_NAME_LEN, cmdline.len());
        let name = String::from(&cmdline[..name_size]);

        let io: proc::Io = rng.gen();
        let statm: Statm = rng.gen();

        let pid: Pid = Pid(rng.gen_range(1..PID_MAX_LIMIT));

        let stat_gen = StatGenerator::new(pid, comm.clone(), rng);
        let stat = stat_gen.generate(rng);

        let status_gen = StatusGenerator::new(pid, name, rng);
        let status = status_gen.generate(rng);

        Process {
            cmdline,
            comm: comm.clone(),
            io,
            stat,
            statm,
            status,
        }
    }
}
