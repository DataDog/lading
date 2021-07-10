//! The `http_gen` library
//!
//! This crate is intended to back the `http_gen` executable and is
//! not considered useful otherwise.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

pub use worker::Worker;

pub mod config;
mod worker;
