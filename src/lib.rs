#![deny(clippy::all)]
#![deny(clippy::pedantic)]

pub mod config;
mod file;

pub use file::Log;
