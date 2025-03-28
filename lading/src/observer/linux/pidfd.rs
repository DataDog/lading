use nix::Error;
use nix::errno::Errno;
use nix::unistd::{Pid, close};
use std::mem::MaybeUninit;
use std::os::unix::io::RawFd;

fn siginfo() -> libc::siginfo_t {
    // SAFETY: This function creates a zero-initialized siginfo_t struct which
    // is safe because all bit patterns are valid for the struct. The struct is
    // only used as an output parameter for waitid() which will properly
    // initialize all fields.
    unsafe {
        // Create zero-initialized siginfo_t. The libc crate exposes siginfo_t
        // with its union fields etc as private fields, so we need to create a
        // zeroed out instance and then set the platform specific fields.
        let mut siginfo = MaybeUninit::<libc::siginfo_t>::zeroed().assume_init();

        siginfo.si_signo = 0;
        siginfo.si_errno = 0;
        siginfo.si_code = 0;

        siginfo
    }
}

#[derive(Debug)]
pub(crate) struct PidFd {
    fd: RawFd,
}

impl PidFd {
    /// Open a pidfd for the specified PID via the `SYS_pidfd_open` syscall.
    ///
    /// # Errors
    /// Returns an error if the PID doesn't exist or if the kernel doesn't
    /// support pidfd.
    pub(crate) fn open(pid: Pid) -> Result<Self, Error> {
        // Perform the syscall directly using libc, but convert errors using nix::errno::Errno.
        let res = Errno::result(unsafe {
            libc::syscall(
                libc::SYS_pidfd_open, // numeric ID for pidfd_open
                libc::c_long::from(pid.as_raw()),
                0 as libc::c_long, // flags = 0
            )
        })?;

        let fd = res as RawFd;
        Ok(Self { fd })
    }

    /// Check if the process associated with this pidfd has exited yet.
    ///
    /// This function calls `waitid(P_PIDFD, fd, WEXITED|WNOHANG)`, so a wait
    /// without waiting. Returns true immediately if the process has exited,
    /// else false.
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn is_exited(&self) -> Result<bool, Error> {
        let mut info = siginfo();

        // waitid(P_PIDFD, <fd>, &mut info, WEXITED|WNOHANG)
        //
        // WEXITED -- Wait for processes that have exited.
        //
        // WNOHANG -- Do not hang if no status is available; return
        //
        // If waitid() returns because a child process was found that satisfied
        // the conditions indicated by the arguments idtype and options, then
        // the structure pointed to by infop shall be filled in by the system
        // with the status of the process; the si_signo member shall be set
        // equal to SIGCHLD.  If waitid() returns because WNOHANG was specified
        // and status is not available for any process specified by idtype and
        // id, then the si_signo and si_pid members of the structure pointed to
        // by infop shall be set to zero and the values of other members of the
        // structure are unspecified.
        //
        // The waitid() function shall fail if:
        //
        // ECHILD -- The calling process has no existing unwaited-for child
        // processes.
        //
        // EINTR -- The waitid() function was interrupted by a signal.
        //
        // EINVAL -- An invalid value was specified for options, or idtype and
        // id specify an invalid set of processes.
        //
        // With pidfd in play ECHILD will be returned if the target process has
        // exited, didn't exist to begin with or lading is not in the same pid
        // namespace.

        let res = Errno::result(unsafe {
            libc::waitid(
                libc::P_PIDFD,
                self.fd as libc::id_t,
                &raw mut info,
                libc::WEXITED | libc::WNOHANG,
            )
        });

        // If WNOHANG was specified and status is not available for any process
        // specified by idtype and id, 0 shall be returned. If waitid() returns
        // due to the change of state of one of its children, 0 shall be
        // returned. Otherwise, -1 shall be returned and errno set to indicate
        // the error.

        match res {
            Ok(0) => {
                let si_pid = unsafe { info.si_pid() };
                // NOTE `info` lacks a si_signo and it's sufficient to check
                // only si_pid.
                Ok(si_pid != 0)
            }
            Ok(_) => unreachable!("waitid() returned a value other than 0 or -1"),
            Err(Errno::ECHILD) => {
                // No such process existed or remains.
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }
}

impl Drop for PidFd {
    fn drop(&mut self) {
        let _ = close(self.fd);
    }
}
