//! Test utilities for `lading_capture`
//!
//! Common test infrastructure used across tests and fuzzing.

#![cfg(any(test, feature = "fuzz"))]

pub mod writer;
