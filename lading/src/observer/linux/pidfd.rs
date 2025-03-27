use nix::Error;
use nix::errno::Errno;
use nix::unistd::{Pid, close};
use std::mem::MaybeUninit;
use std::os::unix::io::RawFd;

fn siginfo() -> libc::siginfo_t {
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
    pub(crate) fn is_active(&self) -> Result<bool, Error> {
        let mut info = siginfo();

        // waitid(P_PIDFD, <fd>, &mut info, WEXITED|WNOHANG)
        Errno::result(unsafe {
            libc::waitid(
                libc::P_PIDFD,
                self.fd as libc::id_t, // 'pid' in waitid terms
                &raw mut info,
                libc::WEXITED | libc::WNOHANG,
            )
        })?;

        // If `info.si_pid() != 0`, that means the process has exited.
        let exited = unsafe { info.si_pid() } != 0;
        Ok(exited)
    }
}

impl Drop for PidFd {
    fn drop(&mut self) {
        let _ = close(self.fd);
    }
}
