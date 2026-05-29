//! Scenario framework for property-based testing.
//!
//! A scenario defines what kind of log data to generate, how to configure
//! the agent, and what properties to assert on the output.

pub mod adaptive_sampling;
pub mod json_multiline;
pub mod mixed_multiline;
pub mod multiline;
pub mod truncation;

use proptest::prelude::*;

use crate::config::LogSourceConfig;
use crate::log_format::LogFormat;
use crate::log_gen::LogBatch;
use crate::property::Property;

/// A test scenario defines the full test lifecycle for one category of
/// agent behavior.
///
/// Each scenario provides:
/// - A proptest strategy that generates parameters
/// - Agent configuration appropriate for the behavior under test
/// - Input log data generated from those parameters
/// - Properties to assert on the agent's output
pub trait Scenario: std::fmt::Debug {
    /// The proptest-generated parameters for this scenario.
    type Params: std::fmt::Debug + Clone;

    /// The proptest strategy that generates parameters.
    fn strategy() -> BoxedStrategy<Self::Params>;

    /// Build log source configuration for the agent.
    fn log_source_config(params: &Self::Params) -> LogSourceConfig;

    /// The log format used for this test case.
    fn log_format(params: &Self::Params) -> LogFormat;

    /// Generate the log batch (input data) from the parameters.
    fn generate_input(params: &Self::Params) -> LogBatch;

    /// Return the list of properties to assert on the output.
    fn properties(params: &Self::Params) -> Vec<Box<dyn Property>>;

    /// Optional: maximum message size override for the agent config.
    /// Returns `None` to use the agent's default (256 KB).
    fn max_message_size_bytes(_params: &Self::Params) -> Option<usize> {
        None
    }
}
