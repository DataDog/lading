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

pub mod v04;