//! The `file_gen` library
//!
//! This sub-crate is intended to back the `file_gen` executable and is not
//! considered useful otherwise.

pub mod config;
mod worker;
pub use worker::Log;
