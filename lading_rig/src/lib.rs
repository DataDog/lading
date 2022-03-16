//! The `rig` library

#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![allow(clippy::cast_precision_loss)]

pub mod blackhole;
pub mod captures;
pub mod config;
pub mod generator;
pub mod signals;
pub mod target;
