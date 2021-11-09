//! The `splunk_hec_gen` library
//!
//! This crate is intended to back the `splunk_hec_gen` executable and is
//! not considered useful otherwise.

pub use worker::Worker;
pub mod config;
mod worker;
mod acknowledgements;
