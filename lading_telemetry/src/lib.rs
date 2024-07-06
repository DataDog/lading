//! Implementation of a lading specific telemetry protocol
//!
//! We regularly have need to run lading in environments where low-overhead
//! telemetry across a unix socket is desirable. This crate allows for that. The
//! on-wire format is subject to change with no backward compatibility
//! guarantees. Do not mix versions of this crate.

pub mod protocol;

#[allow(clippy::empty_structs_with_brackets)]
pub mod proto;
