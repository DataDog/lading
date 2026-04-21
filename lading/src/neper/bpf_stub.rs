//! Stub for non-Linux platforms where eBPF is unavailable.

use std::os::fd::{OwnedFd, RawFd};

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("bpf load failed: {reason}")]
    Load { reason: String },
    #[error("bpf attach failed: {reason}")]
    Attach { reason: String },
}

pub(crate) fn load_reuseport_ebpf(_num_sockets: u32) -> Result<OwnedFd, Error> {
    Err(Error::Load {
        reason: "reuseport eBPF is only supported on Linux".to_string(),
    })
}

pub(crate) fn attach_reuseport_ebpf(_socket_fd: RawFd, _prog: &OwnedFd) -> Result<(), Error> {
    Err(Error::Attach {
        reason: "reuseport eBPF is only supported on Linux".to_string(),
    })
}
