//! eBPF program for `SO_REUSEPORT` load balancing.
//!
//! Provides an eBPF program matching neper's `prog1`: select random
//! socket
//!
//! The program is loaded via the `bpf()` syscall and attached to a listener
//! socket with `SO_ATTACH_REUSEPORT_EBPF`. Only the first socket in the
//! reuseport group needs the program attached — the kernel applies it to the
//! entire group.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::ffi::CStr;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("bpf load failed: {reason}")]
    Load { reason: String },
    #[error("bpf attach failed: {reason}")]
    Attach { reason: String },
}

// eBPF instruction encoding: { code:u8, dst_reg:4|src_reg:4, off:i16, imm:i32 }
#[repr(C)]
struct BpfInsn {
    code: u8,
    regs: u8, // dst_reg:4 (low) | src_reg:4 (high)
    off: i16,
    imm: i32,
}

impl BpfInsn {
    const fn new(code: u8, dst: u8, src: u8, off: i16, imm: i32) -> Self {
        Self {
            code,
            regs: (src << 4) | (dst & 0x0f),
            off,
            imm,
        }
    }
}

// BPF instruction classes and opcodes (from linux/bpf.h).
const BPF_JMP: u8 = 0x05;
const BPF_ALU: u8 = 0x04;
const BPF_CALL: u8 = 0x80;
const BPF_EXIT: u8 = 0x90;
const BPF_MOD: u8 = 0x90;
const BPF_K: u8 = 0x00;

// BPF registers.
const BPF_REG_0: u8 = 0;

// BPF helper function IDs.
const BPF_FUNC_GET_PRANDOM_U32: i32 = 7;

// BPF program types and commands.
const BPF_PROG_LOAD: libc::c_int = 5;
const BPF_PROG_TYPE_SOCKET_FILTER: u32 = 1;

// bpf_attr union for BPF_PROG_LOAD — only the fields we need.
#[repr(C)]
struct BpfAttrProgLoad {
    prog_type: u32,
    insn_cnt: u32,
    insns: u64,
    license: u64,
    log_level: u32,
    log_size: u32,
    log_buf: u64,
    // Pad to cover the full bpf_attr union size the kernel expects.
    _pad: [u8; 96],
}

/// Build and load the reuseport eBPF program.
///
/// The program randomly selects a socket index for incoming connections:
/// ```text
/// r0 = get_prandom_u32() % num_sockets
/// return r0
/// ```
///
/// # Errors
///
/// Returns an error if the `bpf()` syscall fails (e.g. missing permissions,
/// kernel too old).
pub(crate) fn load_reuseport_ebpf(num_sockets: u32) -> Result<OwnedFd, Error> {
    let imm = num_sockets as i32;
    let prog = [
        BpfInsn::new(BPF_JMP | BPF_CALL, 0, 0, 0, BPF_FUNC_GET_PRANDOM_U32),
        BpfInsn::new(BPF_ALU | BPF_MOD | BPF_K, BPF_REG_0, 0, 0, imm),
        BpfInsn::new(BPF_JMP | BPF_EXIT, 0, 0, 0, 0),
    ];

    let license = b"GPL\0";
    let mut log_buf = [0u8; 4096];

    let attr = BpfAttrProgLoad {
        prog_type: BPF_PROG_TYPE_SOCKET_FILTER,
        insn_cnt: prog.len() as u32,
        insns: prog.as_ptr() as u64,
        license: license.as_ptr() as u64,
        log_level: 1,
        log_size: log_buf.len() as u32,
        log_buf: log_buf.as_mut_ptr() as u64,
        _pad: [0u8; 96],
    };

    let fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_PROG_LOAD,
            (&raw const attr).cast::<libc::c_void>(),
            std::mem::size_of::<BpfAttrProgLoad>() as libc::c_int,
        )
    };

    if fd < 0 {
        let os_err = io::Error::last_os_error();
        // Extract the verifier log (null-terminated string written by the kernel).
        let log = CStr::from_bytes_until_nul(&log_buf)
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let reason = if log.trim().is_empty() {
            os_err.to_string()
        } else {
            format!("{os_err}: {}", log.trim())
        };
        return Err(Error::Load { reason });
    }

    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

/// Attach a loaded reuseport eBPF program to a socket.
///
/// Must be called on the first socket in the `SO_REUSEPORT` group before
/// other sockets bind to the same address.
///
/// # Errors
///
/// Returns an error if `setsockopt` fails.
pub(crate) fn attach_reuseport_ebpf(socket_fd: RawFd, prog: &OwnedFd) -> Result<(), Error> {
    let prog_fd: libc::c_int = prog.as_raw_fd();
    let ret = unsafe {
        libc::setsockopt(
            socket_fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_EBPF,
            (&raw const prog_fd).cast::<libc::c_void>(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(Error::Attach {
            reason: io::Error::last_os_error().to_string(),
        });
    }
    Ok(())
}
