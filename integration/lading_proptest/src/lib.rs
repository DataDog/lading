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
    use crate::scenario::json_multiline::JsonMultilineScenario;
    use crate::scenario::mixed_multiline::MixedMultilineScenario;
    use crate::scenario::multiline::MultilineScenario;
    use crate::scenario::truncation::TruncationScenario;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn orchestrator_config() -> OrchestratorConfig {
        init_tracing();
        OrchestratorConfig::default()
    }

    /// Bridge proptest (sync) to tokio (async).
    fn run_async<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Runtime::new()
            .expect("failed to create tokio runtime")
            .block_on(f)
    }

    proptest! {
        // Default 1 case; override via PROPTEST_CASES env var (read by proptest natively).
        #![proptest_config(ProptestConfig::with_cases(1))]

        #[test]
        fn multiline_timestamp_prefixed(
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
        fn multiline_syslog(
            params in crate::scenario::multiline::strategy_with_format(
                LogFormat::Syslog5424,
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
        fn multiline_apache(
            params in crate::scenario::multiline::strategy_with_format(
                LogFormat::ApacheCommon,
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

        // PlainText multiline deferred — needs explicit regex patterns.

        #[test]
        fn json_multiline(
            params in JsonMultilineScenario::strategy()
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<JsonMultilineScenario>(&config, &params)
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
        fn mixed_multiline(
            params in MixedMultilineScenario::strategy()
        ) {
            let config = orchestrator_config();
            let result = run_async(
                orchestrator::run_case::<MixedMultilineScenario>(&config, &params)
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

        #[test]
        fn truncation_json(
            params in crate::scenario::truncation::strategy_with_format(
                LogFormat::Json,
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

        #[test]
        fn truncation_syslog(
            params in crate::scenario::truncation::strategy_with_format(
                LogFormat::Syslog5424,
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

        #[test]
        fn truncation_apache(
            params in crate::scenario::truncation::strategy_with_format(
                LogFormat::ApacheCommon,
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

        #[test]
        fn truncation_timestamp_prefixed(
            params in crate::scenario::truncation::strategy_with_format(
                LogFormat::TimestampPrefixed,
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

    // Adaptive sampling uses run_action_sequence (not run_case), so it
    // lives outside the proptest! macro as a regular proptest test.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1))]

        #[test]
        fn adaptive_sampling(
            params in crate::scenario::adaptive_sampling::strategy()
        ) {
            let config = orchestrator_config();
            let seq = crate::scenario::adaptive_sampling::build_actions(&params);
            let properties = crate::scenario::adaptive_sampling::build_properties(&seq);
            let log_source = crate::scenario::adaptive_sampling::log_source_config();

            let result = run_async(
                orchestrator::run_action_sequence(
                    &config,
                    log_source,
                    None,
                    &seq.actions,
                    properties,
                )
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
