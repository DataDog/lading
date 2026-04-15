//! Stub for non-Linux platforms where eBPF is unavailable.

use std::io;
use std::os::fd::{OwnedFd, RawFd};

pub(crate) fn load_reuseport_ebpf(_num_sockets: u32) -> io::Result<OwnedFd> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reuseport eBPF is only supported on Linux",
    ))
}

pub(crate) fn attach_reuseport_ebpf(_socket_fd: RawFd, _prog: &OwnedFd) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reuseport eBPF is only supported on Linux",
    ))
}
