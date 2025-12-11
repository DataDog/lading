//! Validation logic for capture files
//!
//! This module contains the canonical validation logic for lading capture
//! files. All validation - in captool, tests - must use this module to ensure
//! consistency.

pub mod jsonl;
pub mod parquet;

/// Result of validating capture invariants
#[derive(Debug)]
pub struct ValidationResult {
    /// Total number of lines validated
    pub line_count: u128,
    /// Number of unique series (`metric_name` + labels combinations)
    pub unique_series: usize,
    /// Number of unique `fetch_index` values
    pub unique_fetch_indices: usize,
    /// `fetch_index`/time mapping violations
    pub fetch_index_errors: u128,
    /// Per-series violations (time or `fetch_index` not strictly increasing)
    pub per_series_errors: u128,
    /// Minimum seconds violations (when `min_seconds` is specified)
    pub min_seconds_errors: u128,
    /// First error encountered (line number, series id, message)
    pub first_error: Option<(u128, String, String)>,
}

impl ValidationResult {
    /// Returns true if validation passed with no errors
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.fetch_index_errors == 0 && self.per_series_errors == 0 && self.min_seconds_errors == 0
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    use crate::formats::parquet;
    use crate::line::{Line, LineValue, MetricKind};
    use crate::validate::jsonl::validate_lines;
    use crate::validate::parquet::validate_parquet;

    /// Helper to create test lines with various patterns
    fn create_test_lines() -> Vec<Line> {
        let run_id = Uuid::new_v4();
        vec![
            Line {
                run_id,
                time: 1000,
                fetch_index: 0,
                metric_name: "test.counter".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(100),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 2000,
                fetch_index: 1,
                metric_name: "test.counter".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(200),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 3000,
                fetch_index: 2,
                metric_name: "test.gauge".to_string(),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(42.5),
                labels: {
                    let mut map = FxHashMap::default();
                    map.insert("env".to_string(), "prod".to_string());
                    map
                },
                value_histogram: Vec::new(),
            },
        ]
    }

    #[test]
    fn parquet_validation_passes_for_valid_data() {
        let lines = create_test_lines();

        // Write to parquet
        let parquet_file = NamedTempFile::new().expect("create temp parquet file");
        {
            let mut writer = parquet::Format::new(parquet_file.reopen().expect("reopen"), 3)
                .expect("create parquet writer");
            for line in &lines {
                writer.write_metric(line).expect("write to parquet");
            }
            writer.flush().expect("flush parquet");
            writer.close().expect("close parquet");
        }

        let result = validate_parquet(parquet_file.path(), None).expect("parquet validation");

        assert_eq!(result.line_count, 3);
        assert_eq!(result.unique_fetch_indices, 3);
        assert_eq!(result.fetch_index_errors, 0);
        assert_eq!(result.per_series_errors, 0);
        assert_eq!(result.min_seconds_errors, 0);
        assert!(result.is_valid());
    }

    #[test]
    fn jsonl_validation_passes_for_valid_data() {
        let lines = create_test_lines();
        let result = validate_lines(&lines, None);

        assert_eq!(result.line_count, 3);
        assert_eq!(result.unique_series, 2);
        assert_eq!(result.unique_fetch_indices, 3);
        assert_eq!(result.fetch_index_errors, 0);
        assert_eq!(result.per_series_errors, 0);
        assert_eq!(result.min_seconds_errors, 0);
        assert!(result.is_valid());
    }

    #[test]
    fn parquet_validation_detects_fetch_index_violation() {
        let run_id = Uuid::new_v4();
        let lines = vec![
            Line {
                run_id,
                time: 1000,
                fetch_index: 0,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(1),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 2000,     // Different time!
                fetch_index: 0, // Same fetch_index - VIOLATION
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(2),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        // Write to parquet
        let parquet_file = NamedTempFile::new().expect("create temp parquet file");
        {
            let mut writer = parquet::Format::new(parquet_file.reopen().expect("reopen"), 3)
                .expect("create parquet writer");
            for line in &lines {
                writer.write_metric(line).expect("write to parquet");
            }
            writer.flush().expect("flush parquet");
            writer.close().expect("close parquet");
        }

        let result = validate_parquet(parquet_file.path(), None).expect("parquet validation");

        // Parquet detects: mapping violation (fetch_index_errors) + per-series monotonicity violation (per_series_errors)
        assert_eq!(result.fetch_index_errors, 1);
        assert_eq!(result.per_series_errors, 1);
        assert!(!result.is_valid());
    }

    #[test]
    fn jsonl_validation_detects_fetch_index_violation() {
        let run_id = Uuid::new_v4();
        let lines = vec![
            Line {
                run_id,
                time: 1000,
                fetch_index: 0,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(1),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 2000,     // Different time!
                fetch_index: 0, // Same fetch_index - VIOLATION
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(2),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        let result = validate_lines(&lines, None);

        // JSONL detects: mapping violation (fetch_index_errors) + per-series monotonicity violation (per_series_errors)
        assert_eq!(result.fetch_index_errors, 1);
        assert_eq!(result.per_series_errors, 1);
        assert!(!result.is_valid());
    }

    #[test]
    fn parquet_validation_detects_time_not_strictly_increasing_per_series() {
        let run_id = Uuid::new_v4();
        let lines = vec![
            Line {
                run_id,
                time: 2000,
                fetch_index: 0,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(1),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 1000,     // Time goes backward within same series - VIOLATION
                fetch_index: 1, // fetch_index is strictly increasing
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(2),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        // Write to parquet
        let parquet_file = NamedTempFile::new().expect("create temp parquet file");
        {
            let mut writer = parquet::Format::new(parquet_file.reopen().expect("reopen"), 3)
                .expect("create parquet writer");
            for line in &lines {
                writer.write_metric(line).expect("write to parquet");
            }
            writer.flush().expect("flush parquet");
            writer.close().expect("close parquet");
        }

        let result = validate_parquet(parquet_file.path(), None).expect("parquet validation");

        // Parquet checks time is strictly increasing within each series
        assert_eq!(result.per_series_errors, 1);
        assert!(!result.is_valid());
    }

    #[test]
    fn parquet_validation_allows_time_reordering_across_different_series() {
        let run_id = Uuid::new_v4();
        let lines = vec![
            Line {
                run_id,
                time: 2000,
                fetch_index: 0,
                metric_name: "test.counter".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(1),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 1000, // Time goes backward but different series - OK
                fetch_index: 1,
                metric_name: "test.gauge".to_string(),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(42.0),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        // Write to parquet
        let parquet_file = NamedTempFile::new().expect("create temp parquet file");
        {
            let mut writer = parquet::Format::new(parquet_file.reopen().expect("reopen"), 3)
                .expect("create parquet writer");
            for line in &lines {
                writer.write_metric(line).expect("write to parquet");
            }
            writer.flush().expect("flush parquet");
            writer.close().expect("close parquet");
        }

        let result = validate_parquet(parquet_file.path(), None).expect("parquet validation");

        // Different series can have overlapping timestamps
        assert_eq!(result.per_series_errors, 0);
        assert_eq!(result.fetch_index_errors, 0);
        assert!(result.is_valid());
    }

    #[test]
    fn jsonl_validation_detects_time_not_increasing() {
        let run_id = Uuid::new_v4();
        let lines = vec![
            Line {
                run_id,
                time: 2000,
                fetch_index: 0,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(1),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 1000, // Time goes backward - VIOLATION
                fetch_index: 1,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(2),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        let result = validate_lines(&lines, None);

        assert_eq!(result.per_series_errors, 1);
        assert!(!result.is_valid());
    }

    #[test]
    fn parquet_validation_detects_min_seconds_violation() {
        let run_id = Uuid::new_v4();
        let mut lines = Vec::new();

        // Create 30 lines with unique timestamps (need 60)
        for i in 0..30 {
            lines.push(Line {
                run_id,
                time: (i * 1000) as u128,
                fetch_index: i,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(i),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            });
        }

        // Write to parquet
        let parquet_file = NamedTempFile::new().expect("create temp parquet file");
        {
            let mut writer = parquet::Format::new(parquet_file.reopen().expect("reopen"), 3)
                .expect("create parquet writer");
            for line in &lines {
                writer.write_metric(line).expect("write to parquet");
            }
            writer.flush().expect("flush parquet");
            writer.close().expect("close parquet");
        }

        let result = validate_parquet(parquet_file.path(), Some(60)).expect("parquet validation");

        assert_eq!(result.min_seconds_errors, 1);
        assert!(!result.is_valid());
    }

    #[test]
    fn jsonl_validation_detects_min_seconds_violation() {
        let run_id = Uuid::new_v4();
        let mut lines = Vec::new();

        // Create 30 lines with unique timestamps (need 60)
        for i in 0..30 {
            lines.push(Line {
                run_id,
                time: (i * 1000) as u128,
                fetch_index: i,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(i),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            });
        }

        let result = validate_lines(&lines, Some(60));

        assert_eq!(result.min_seconds_errors, 1);
        assert!(!result.is_valid());
    }

    #[test]
    fn jsonl_validation_passes_with_out_of_order_rows() {
        let run_id = Uuid::new_v4();
        // Rows are physically out of order by fetch_index, but logically valid when sorted
        let lines = vec![
            Line {
                run_id,
                time: 5000,
                fetch_index: 5,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(5),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 2000, // Earlier time, earlier fetch_index - out of physical order
                fetch_index: 2,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(2),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 8000,
                fetch_index: 8,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(8),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        let result = validate_lines(&lines, None);

        // When sorted by fetch_index: [2→2000, 5→5000, 8→8000] - valid!
        assert_eq!(result.fetch_index_errors, 0);
        assert_eq!(result.per_series_errors, 0);
        assert!(result.is_valid());
    }

    #[test]
    fn parquet_validation_passes_with_out_of_order_rows() {
        let run_id = Uuid::new_v4();
        // Rows are physically out of order by fetch_index, but logically valid when sorted
        let lines = vec![
            Line {
                run_id,
                time: 5000,
                fetch_index: 5,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(5),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 2000, // Earlier time, earlier fetch_index - out of physical order
                fetch_index: 2,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(2),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
            Line {
                run_id,
                time: 8000,
                fetch_index: 8,
                metric_name: "test".to_string(),
                metric_kind: MetricKind::Counter,
                value: LineValue::Int(8),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            },
        ];

        // Write to parquet
        let parquet_file = NamedTempFile::new().expect("create temp parquet file");
        {
            let mut writer = parquet::Format::new(parquet_file.reopen().expect("reopen"), 3)
                .expect("create parquet writer");
            for line in &lines {
                writer.write_metric(line).expect("write to parquet");
            }
            writer.flush().expect("flush parquet");
            writer.close().expect("close parquet");
        }

        let result = validate_parquet(parquet_file.path(), None).expect("parquet validation");

        // When sorted by fetch_index: [2→2000, 5→5000, 8→8000] - valid!
        assert_eq!(result.fetch_index_errors, 0);
        assert_eq!(result.per_series_errors, 0);
        assert!(result.is_valid());
    }
}
