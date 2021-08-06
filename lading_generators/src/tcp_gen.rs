//! The `tcp_gen` library
//!
//! This crate is intended to back the `tcp_gen` executable and is
//! not considered useful otherwise.

pub use worker::Worker;
pub mod config;
mod worker;
