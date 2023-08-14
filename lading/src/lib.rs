//! The lading daemon load generation and introspection tool.
//!
//! This library support the lading binary found elsewhere in this project. The
//! bits and pieces here are not intended to be used outside of supporting
//! lading, although if they are helpful in other domains that's a nice
//! surprise.

#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(unused_extern_crates)]
#![deny(unused_allocation)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![deny(missing_copy_implementations)]
#![deny(missing_debug_implementations)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

pub mod blackhole;
pub(crate) mod block;
pub mod captures;
pub(crate) mod codec;
mod common;
pub mod config;
pub mod generator;
pub mod inspector;
pub mod observer;
pub mod signals;
pub mod target;
pub mod target_metrics;
