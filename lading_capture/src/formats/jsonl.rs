//! JSONL format
//!
//! This format writes one JSON object per line. Each line representing a single
//! metric observation.

use std::io::Write;

use crate::line;

/// JSONL format errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// IO errors during write operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization errors
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// JSONL format writer
#[derive(Debug)]
pub struct Format<W: Write> {
    writer: W,
}

impl<W: Write> Format<W> {
    /// Create a new instance of `Format`
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write> Format<W> {
    /// Write a single metric line to the output
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails
    pub fn write_metric(&mut self, line: &line::Line) -> Result<(), Error> {
        let payload = serde_json::to_string(line)?;
        self.writer.write_all(payload.as_bytes())?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    /// Flush any buffered data to disk
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails
    pub fn flush(&mut self) -> Result<(), Error> {
        self.writer.flush()?;
        Ok(())
    }
}

impl<W: Write> crate::formats::OutputFormat for Format<W> {
    fn write_metric(&mut self, line: &line::Line) -> Result<(), crate::formats::Error> {
        self.write_metric(line).map_err(Into::into)
    }

    fn flush(&mut self) -> Result<(), crate::formats::Error> {
        self.flush().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::line::{Line, LineValue, MetricKind};
    use rustc_hash::FxHashMap;
    use uuid::Uuid;

    #[test]
    fn writes_valid_jsonl() {
        let mut buffer = Vec::new();
        let mut format = Format::new(&mut buffer);

        let line = Line {
            run_id: Uuid::new_v4(),
            time: 1000,
            fetch_index: 0,
            metric_name: "test_metric".into(),
            metric_kind: MetricKind::Counter,
            value: LineValue::Int(42),
            labels: FxHashMap::default(),
        };

        format.write_metric(&line).expect("write should succeed");

        let output = String::from_utf8(buffer).expect("should be valid UTF-8");
        assert!(output.ends_with('\n'), "should end with newline");

        let trimmed = output.trim();
        let parsed: Line = serde_json::from_str(trimmed).expect("should deserialize");

        assert_eq!(parsed.metric_name, "test_metric");
        assert_eq!(parsed.fetch_index, 0);
    }

    #[test]
    fn multiple_writes_produce_multiple_lines() {
        let mut buffer = Vec::new();
        let mut format = Format::new(&mut buffer);

        for i in 0..5 {
            let line = Line {
                run_id: Uuid::new_v4(),
                time: 1000 + (i as u128),
                fetch_index: i as u64,
                metric_name: format!("metric_{i}"),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(i as f64),
                labels: FxHashMap::default(),
            };

            format.write_metric(&line).expect("write should succeed");
        }

        let output = String::from_utf8(buffer).expect("should be valid UTF-8");
        let lines: Vec<&str> = output.lines().collect();

        assert_eq!(lines.len(), 5, "should have 5 lines");

        for (i, line_str) in lines.iter().enumerate() {
            let parsed: Line = serde_json::from_str(line_str).expect("should deserialize");
            assert_eq!(parsed.metric_name, format!("metric_{i}"));
        }
    }
}
