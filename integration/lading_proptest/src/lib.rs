//! Property-based integration tests for the Datadog Agent logs pipeline.
//!
//! This crate feeds generated log data to the DD Agent, collects what the
//! agent sends to a fake intake endpoint, and asserts properties on the output.
//!
//! # Running
//!
//! ```bash
//! # Single case, container mode (default)
//! PROPTEST_CASES=1 cargo test -p lading_proptest -- --nocapture --test-threads=1
//!
//! # Binary mode
//! DD_AGENT_BINARY=/path/to/agent PROPTEST_CASES=1 cargo test -p lading_proptest -- --nocapture --test-threads=1
//!
//! # Specific scenario
//! PROPTEST_CASES=1 cargo test -p lading_proptest truncation -- --nocapture --test-threads=1
//! ```
//!
//! These tests require a DD Agent binary or Docker image. Set the agent target
//! via environment variables:
//!
//! - `DD_AGENT_IMAGE=datadog/agent:latest` for container mode (default)
//! - `DD_AGENT_BINARY=/path/to/agent` for binary mode

pub mod agent;
pub mod config;
pub mod diagnostics;
pub mod intake;
pub mod log_format;
pub mod log_gen;
pub mod orchestrator;
pub mod property;
pub mod scenario;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::diagnostics;
    use crate::log_format::LogFormat;
    use crate::orchestrator::{self, OrchestratorConfig};
    use crate::scenario::Scenario;
    use crate::scenario::multiline::MultilineScenario;
    use crate::scenario::truncation::TruncationScenario;

    fn orchestrator_config() -> OrchestratorConfig {
        OrchestratorConfig::default()
    }

    /// Bridge proptest (sync) to tokio (async).
    fn run_async<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Runtime::new()
            .expect("failed to create tokio runtime")
            .block_on(f)
    }

    fn proptest_cases() -> u32 {
        std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(proptest_cases()))]

        #[test]
        fn multiline_timestamp_detection(
            params in crate::scenario::multiline::strategy_with_format(
                LogFormat::TimestampPrefixed,
            )
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<MultilineScenario>(&config, &params)
            ).expect("test infrastructure failure");

            for prop_result in &result.property_results {
                prop_assert!(
                    prop_result.is_ok(),
                    "{}",
                    diagnostics::format_failure(&result),
                );
            }
        }

        #[test]
        fn multiline_json_detection(
            params in crate::scenario::multiline::strategy_with_format(
                LogFormat::Json,
            )
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<MultilineScenario>(&config, &params)
            ).expect("test infrastructure failure");

            for prop_result in &result.property_results {
                prop_assert!(
                    prop_result.is_ok(),
                    "{}",
                    diagnostics::format_failure(&result),
                );
            }
        }

        #[test]
        fn multiline_all_formats(
            params in MultilineScenario::strategy()
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<MultilineScenario>(&config, &params)
            ).expect("test infrastructure failure");

            for prop_result in &result.property_results {
                prop_assert!(
                    prop_result.is_ok(),
                    "{}",
                    diagnostics::format_failure(&result),
                );
            }
        }

        #[test]
        fn truncation(
            params in TruncationScenario::strategy()
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<TruncationScenario>(&config, &params)
            ).expect("test infrastructure failure");

            for prop_result in &result.property_results {
                prop_assert!(
                    prop_result.is_ok(),
                    "{}",
                    diagnostics::format_failure(&result),
                );
            }
        }

        #[test]
        fn truncation_plain_text(
            params in crate::scenario::truncation::strategy_with_format(
                LogFormat::PlainText,
            )
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<TruncationScenario>(&config, &params)
            ).expect("test infrastructure failure");

            for prop_result in &result.property_results {
                prop_assert!(
                    prop_result.is_ok(),
                    "{}",
                    diagnostics::format_failure(&result),
                );
            }
        }
    }
}
