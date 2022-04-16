#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![deny(unused_extern_crates)]
#![deny(unused_allocation)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![deny(unreachable_pub)]
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
pub(crate) mod payload;
pub mod signals;
pub mod target;
