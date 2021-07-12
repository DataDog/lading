//! The `file_gen` library
//!
//! This crate is intended to back the `file_gen` executable and is not
//! considered useful otherwise.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

pub mod config;
mod file;

pub use file::Log;
