//! JSON multiline aggregation scenario.
//!
//! Generates structurally diverse valid JSON (flat objects, nested objects,
//! objects with arrays, top-level arrays) split across multiple lines.
//! Asserts that the agent correctly compacts multi-line JSON objects and
//! preserves all fields.
//!
//! This scenario does not encode knowledge of the agent's internal behavior.
//! It generates valid JSON and asserts data integrity — if the agent fails
//! to handle a particular JSON structure, the test discovers it mechanically.

use proptest::prelude::*;

use crate::config::LogSourceConfig;
use crate::log_format::{self, JsonStructure, LogFormat};
use crate::log_gen::{self, LogBatch};
use crate::property::{self, Property};
use crate::scenario::Scenario;

/// Parameters for JSON multiline testing.
#[derive(Debug, Clone)]
pub struct JsonMultilineParams {
    /// The JSON structures to generate, one per logical entry.
    pub entries: Vec<JsonStructure>,
}

/// JSON multiline scenario.
#[derive(Debug, Copy, Clone)]
pub struct JsonMultilineScenario;

impl Scenario for JsonMultilineScenario {
    type Params = JsonMultilineParams;

    fn strategy() -> BoxedStrategy<Self::Params> {
        proptest::collection::vec(log_format::json_structure_strategy(), 3..10)
            .prop_map(|entries| JsonMultilineParams { entries })
            .boxed()
    }

    fn log_source_config(_params: &Self::Params) -> LogSourceConfig {
        LogSourceConfig::JsonMultiline
    }

    fn log_format(_params: &Self::Params) -> LogFormat {
        LogFormat::Json
    }

    fn generate_input(params: &Self::Params) -> LogBatch {
        let mut lines = Vec::new();
        let mut expected_json: Vec<(String, serde_json::Value)> = Vec::new();

        for structure in &params.entries {
            let id = uuid::Uuid::new_v4().to_string();

            // Store the expected JSON for property checking
            expected_json.push((id.clone(), structure.expected_json(&id)));

            // Split the JSON across multiple lines for the log file
            let entry_lines = structure.render_lines(&id);
            for (i, line) in entry_lines.into_iter().enumerate() {
                let line_id = if i == 0 {
                    // First line of each entry carries the UUID
                    id.clone()
                } else {
                    format!("{id}:json_line:{i}")
                };
                lines.push(log_gen::LogLine {
                    id: line_id,
                    content: line,
                });
            }
        }

        LogBatch {
            lines,
            format: LogFormat::Json,
            expected_continuations: Vec::new(),
            expected_json: Some(expected_json),
        }
    }

    fn properties(_params: &Self::Params) -> Vec<Box<dyn Property>> {
        vec![
            Box::new(property::JsonIntegrity),
        ]
    }
}
