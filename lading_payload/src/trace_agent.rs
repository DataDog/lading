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
pub mod v1;

/// Configuration for trace agent payload generation by version.
///
/// Each version has different format requirements and performance characteristics.
/// This allows users to specify which trace agent protocol version to target.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Config {
    /// Version 0.4: msgpack array of arrays of spans
    #[serde(rename = "v0.4")]
    V04(v04::Config),
    /// Version 1.0: msgpack `idx`/ETP tracer payload, with streaming string table
    #[serde(rename = "v1.0")]
    V1(v1::Config),
}

impl Config {
    #[must_use]
    /// Return the stringy version number of the config.
    pub fn version(&self) -> &'static str {
        match self {
            Self::V04(_) => "v0.4",
            Self::V1(_) => "v1.0",
        }
    }

    #[must_use]
    /// Return the API endpoint path for this trace agent version.
    pub fn endpoint_path(&self) -> &'static str {
        match self {
            Self::V04(_) => "/v0.4/traces",
            Self::V1(_) => "/v1.0/traces",
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
            Config::V1(config) => config
                .valid()
                .map_err(|e| format!("invalid configuration: {e}")),
        }
    }
}

/// A version-dispatched trace payload serializer.
///
/// The two protocol versions generate different payload types, so a single enum lets the block
/// cache hold either without monomorphizing its construction logic per version.
#[derive(Debug)]
pub enum Serializer {
    /// Version 0.4: msgpack array of arrays of spans.
    V04(v04::V04),
    /// Version 1.0: msgpack `idx`/ETP tracer payload.
    V1(v1::V1),
}

impl crate::Serialize for Serializer {
    fn to_bytes<W, R>(
        &mut self,
        rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        W: std::io::Write,
        R: rand::Rng + Sized,
    {
        match self {
            Serializer::V04(ser) => ser.to_bytes(rng, max_bytes, writer),
            Serializer::V1(ser) => ser.to_bytes(rng, max_bytes, writer),
        }
    }

    fn data_points_generated(&self) -> Option<u64> {
        match self {
            Serializer::V04(ser) => ser.data_points_generated(),
            Serializer::V1(ser) => ser.data_points_generated(),
        }
    }
}
