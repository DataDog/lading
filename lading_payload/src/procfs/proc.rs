use rand::{Rng, distr::StandardUniform, prelude::Distribution};
use std::fmt;

#[derive(Debug, Clone, Copy)]
pub struct Io {
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

impl Distribution<Io> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Io
    where
        R: Rng + ?Sized,
    {
        Io {
            rchar: rng.random(),
            wchar: rng.random(),
            syscr: rng.random(),
            syscw: rng.random(),
            read_bytes: rng.random(),
            write_bytes: rng.random(),
            cancelled_write_bytes: rng.random(),
        }
    }
}

impl fmt::Display for self::Io {
    /// Formats [`Io`] as it would be seen in `/proc/{pid}/io`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        }
    }
}

impl Distribution<State> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> State
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..9) {
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
