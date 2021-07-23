//! The `kafka_gen` library
//!
//! This crate is intended to back the `kafka_gen` executable and is
//! not considered useful otherwise.

pub use worker::Worker;
pub mod config;
mod worker;
