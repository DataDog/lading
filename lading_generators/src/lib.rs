//! The `generator` library

#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

pub mod file_gen;
pub mod http_gen;
pub mod kafka_gen;
pub mod tcp_gen;
pub mod splunk_hec_gen;