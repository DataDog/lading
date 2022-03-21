#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

pub mod blackhole;
pub mod block;
pub mod captures;
pub mod codec;
pub mod config;
pub mod generator;
pub mod payload;
pub mod signals;
pub mod target;
