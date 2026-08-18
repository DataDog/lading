//! Multiline aggregation scenario.
//!
//! Tests that the agent correctly aggregates continuation lines with their
//! header line based on format-specific timestamp detection.
//!
//! Only formats with timestamps work for auto multiline detection:
//! `TimestampPrefixed`, `Syslog5424`, `ApacheCommon`. `PlainText` has no
//! `startGroup` signal (all lines merge). `Json` complete objects get
//! `noAggregate` (flushed standalone). JSON multiline (incomplete objects
//! spanning lines) is deferred to a future scenario.

use proptest::prelude::*;

use crate::config::LogSourceConfig;
use crate::log_format::{self, LogFormat};
use crate::log_gen::{self, LogBatch};
use crate::property::{self, Property};
use crate::scenario::Scenario;

/// Parameters for multiline aggregation testing.
#[derive(Debug, Clone, Copy)]
pub struct MultilineParams {
    /// Number of logical log entries (each may span multiple lines).
    pub entry_count: usize,
    /// Maximum continuation lines per entry.
    pub max_continuations: usize,
    /// The log format to use.
    pub format: LogFormat,
}

/// Multiline aggregation scenario.
#[derive(Debug, Copy, Clone)]
pub struct MultilineScenario;

impl Scenario for MultilineScenario {
    type Params = MultilineParams;

    fn strategy() -> BoxedStrategy<Self::Params> {
        (
            3_usize..20,
            1_usize..5,
            log_format::multiline_format_strategy(),
        )
            .prop_map(|(entry_count, max_continuations, format)| MultilineParams {
                entry_count,
                max_continuations,
                format,
            })
            .boxed()
    }

    fn log_source_config(_params: &Self::Params) -> LogSourceConfig {
        LogSourceConfig::AutoMultiline
    }

    fn log_format(params: &Self::Params) -> LogFormat {
        params.format
    }

    fn generate_input(params: &Self::Params) -> LogBatch {
        let mut lines = Vec::new();
        let mut expected_continuations = Vec::new();

        for entry_idx in 0..params.entry_count {
            let header_id = uuid::Uuid::new_v4().to_string();

            // Header line with a distinct timestamp per entry so the agent
            // sees each as a new `startGroup`.
            let header_content = params
                .format
                .format_line_with_index(&header_id, entry_idx, "log entry header");
            lines.push(log_gen::LogLine {
                id: header_id.clone(),
                content: header_content,
            });

            // Vary continuation count deterministically per entry.
            let cont_count = entry_idx % (params.max_continuations + 1);
            expected_continuations.push((header_id.clone(), cont_count));

            for seq in 0..cont_count {
                let cont_content = params.format.format_continuation(
                    &header_id,
                    seq,
                    &format!("continuation line {seq}"),
                );
                lines.push(log_gen::LogLine {
                    id: format!("{header_id}:cont:{seq}"),
                    content: cont_content,
                });
            }
        }
        LogBatch {
            lines,
            format: params.format,
            expected_continuations,
            expected_json: None,
        }
    }

    fn properties(params: &Self::Params) -> Vec<Box<dyn Property>> {
        vec![
            Box::new(property::AllLinesDelivered),
            Box::new(property::MultilineAggregated),
            Box::new(property::ExpectedEntryCount {
                expected: params.entry_count,
            }),
        ]
    }
}

/// Strategy for multiline scenario with a pinned format.
pub fn strategy_with_format(format: LogFormat) -> BoxedStrategy<MultilineParams> {
    (3_usize..20, 1_usize..5)
        .prop_map(move |(entry_count, max_continuations)| MultilineParams {
            entry_count,
            max_continuations,
            format,
        })
        .boxed()
}
