//! Datadog Trace Agent payload generators.
//!
//! This module provides payload generators for different versions of the Datadog Trace Agent
//! protocol. Each version has specific format requirements and encoding schemes.
//!
//! The implementation follows lading's core principles:
//! - Pre-computation of all strings at initialization
//! - Dynamic string pools instead of static hardcoded data
//! - Performance-optimized generation suitable for load testing
//! - Deterministic output for reproducible testing
//!
//! See the individual module documentation for version-specific details.

use serde::{Deserialize, Serialize};

pub mod v04;

/// Configuration for trace agent payload generation by version.
///
/// Each version has different format requirements and performance characteristics.
/// This allows users to specify which trace agent protocol version to target.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Config {
    /// Version 0.4: msgpack array of arrays of spans
    #[serde(rename = "v0.4")]
    V04(v04::Config),
}

impl Config {
    #[must_use]
    /// Return the stringy version number of the config.
    pub fn version(self) -> &'static str {
        match self {
            Self::V04(_) => "v0.4",
        }
    }

    #[must_use]
    /// Return the API endpoint path for this trace agent version.
    pub fn endpoint_path(self) -> &'static str {
        match self {
            Self::V04(_) => "/v0.4/traces",
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config::V04(v04::Config::default())
    }
}

impl Config {
    /// Validate the configuration
    /// # Errors
    /// Returns an error if the configuration is invalid
    pub fn valid(&self) -> Result<(), String> {
        match self {
            Config::V04(config) => config
                .valid()
                .map_err(|_| "invalid configuration".to_string()),
        }
    }
}
