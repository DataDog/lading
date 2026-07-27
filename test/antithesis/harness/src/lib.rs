//! Shared Antithesis harness for lading scenarios.
//!
//! Holds the per-timeline config-variation mechanism every scenario reuses:
//! [`config::sample`] draws a lading generator config from a value menu, and the
//! `first_sample_config` command serializes it to the shared volume the
//! system-under-test boots from. The menu is built from lading's own
//! `tcp::Config`, so it cannot drift from the real config schema.

pub mod capture;
pub mod config;
