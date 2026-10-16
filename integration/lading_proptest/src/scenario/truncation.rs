//! Truncation boundary scenario.
//!
//! Tests that the agent correctly truncates lines exceeding the configured
//! `max_message_size_bytes` while delivering lines under the limit intact.

use proptest::prelude::*;

use crate::config::LogSourceConfig;
use crate::log_format::{self, LogFormat};
use crate::log_gen::{self, LogBatch};
use crate::property::{self, Property};
use crate::scenario::Scenario;

/// The Datadog Agent's default max message size in bytes.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;

/// Parameters for truncation testing.
#[derive(Debug, Clone, Copy)]
pub struct TruncationParams {
    /// Number of log lines to generate.
    pub line_count: usize,
    /// The log format to use.
    pub format: LogFormat,
    /// The max message size the agent is configured with.
    pub max_message_bytes: usize,
}

/// Truncation scenario.
#[derive(Debug, Copy, Clone)]
pub struct TruncationScenario;

impl Scenario for TruncationScenario {
    type Params = TruncationParams;

    fn strategy() -> BoxedStrategy<Self::Params> {
        (
            5_usize..15,
            log_format::log_format_strategy(),
            // Test at different truncation limits
            prop_oneof![
                Just(1024_usize),        // 1 KB
                Just(64 * 1024),         // 64 KB
                Just(DEFAULT_MAX_MESSAGE_BYTES), // 256 KB (default)
            ],
        )
            .prop_map(|(line_count, format, max_message_bytes)| TruncationParams {
                line_count,
                format,
                max_message_bytes,
            })
            .boxed()
    }

    fn log_source_config(_params: &Self::Params) -> LogSourceConfig {
        LogSourceConfig::Simple
    }

    fn log_format(params: &Self::Params) -> LogFormat {
        params.format
    }

    fn generate_input(params: &Self::Params) -> LogBatch {
        let limit = params.max_message_bytes;
        let mut lines = Vec::with_capacity(params.line_count);

        for i in 0..params.line_count {
            let id = uuid::Uuid::new_v4().to_string();

            // Distribute lines across three size categories
            let target_len = match i % 3 {
                // Well under limit
                0 => 100 + (i * 37) % 900,
                // Near boundary (+/- 50 bytes)
                1 => limit.saturating_sub(50) + (i * 13) % 100,
                // Well over limit
                _ => limit + 1000 + (i * 71) % (limit / 2),
            };

            let overhead_estimate = params.format.format_line(&id, "").len();
            let filler_len = target_len.saturating_sub(overhead_estimate);
            let filler: String = (0..filler_len)
                .map(|j| char::from(b'a' + u8::try_from(j % 26).unwrap_or(0)))
                .collect();
            let content = params.format.format_line(&id, &filler);
            lines.push(log_gen::LogLine { id, content });
        }

        LogBatch {
            lines,
            format: params.format,
            expected_continuations: Vec::new(),
            expected_json: None,
        }
    }

    fn properties(params: &Self::Params) -> Vec<Box<dyn Property>> {
        vec![
            Box::new(property::AllLinesDelivered),
            Box::new(property::TruncationRespected {
                max_message_bytes: params.max_message_bytes,
            }),
            Box::new(property::ContentPreserved),
        ]
    }

    fn max_message_size_bytes(params: &Self::Params) -> Option<usize> {
        // Only override if not using the default
        if params.max_message_bytes == DEFAULT_MAX_MESSAGE_BYTES {
            None
        } else {
            Some(params.max_message_bytes)
        }
    }
}

/// Strategy for truncation scenario with a pinned format.
pub fn strategy_with_format(format: LogFormat) -> BoxedStrategy<TruncationParams> {
    (
        5_usize..15,
        prop_oneof![
            Just(1024_usize),
            Just(64 * 1024),
            Just(DEFAULT_MAX_MESSAGE_BYTES),
        ],
    )
        .prop_map(move |(line_count, max_message_bytes)| TruncationParams {
            line_count,
            format,
            max_message_bytes,
        })
        .boxed()
}
