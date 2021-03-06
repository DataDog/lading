#![deny(clippy::all)]
#![deny(clippy::pedantic)]

use metrics::{register_counter, Unit};

pub mod config;
mod file;

pub use file::Log;

pub fn init_metrics() {
    register_counter!(
        "global_bytes_written",
        Unit::Bytes,
        "total number of bytes written globally"
    );
}
