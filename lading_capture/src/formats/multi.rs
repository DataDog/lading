//! Multi-format output
//!
//! This format writes metrics to multiple output formats simultaneously. This
//! allows capture data to be available as both JSONL and Parquet.

use std::io::{Seek, Write};

use crate::line;

/// Multi-format errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// JSONL format errors
    #[error("JSONL format error: {0}")]
    Jsonl(#[from] super::jsonl::Error),
    /// Parquet format errors
    #[error("Parquet format error: {0}")]
    Parquet(#[from] super::parquet::Error),
}

/// Multi-format writer
///
/// Writes metrics to both JSONL and Parquet formats, uses fail-fast semantics on
/// errors from either format.
#[derive(Debug)]
pub struct Format<W1: Write, W2: Write + Seek + Send> {
    jsonl: super::jsonl::Format<W1>,
    parquet: super::parquet::Format<W2>,
}

impl<W1: Write, W2: Write + Seek + Send> Format<W1, W2> {
    /// Create a new instance of `Format`
    #[must_use]
    pub fn new(jsonl: super::jsonl::Format<W1>, parquet: super::parquet::Format<W2>) -> Self {
        Self { jsonl, parquet }
    }
}

impl<W1: Write, W2: Write + Seek + Send> Format<W1, W2> {
    /// Write a single metric line to both output formats
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails in either format
    pub fn write_metric(&mut self, line: &line::Line) -> Result<(), Error> {
        self.jsonl.write_metric(line)?;
        self.parquet.write_metric(line)?;
        Ok(())
    }

    /// Flush both formats' buffered data to disk
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails in either format
    pub fn flush(&mut self) -> Result<(), Error> {
        self.jsonl.flush()?;
        self.parquet.flush()?;
        Ok(())
    }

    /// Close and finalize both output formats
    ///
    /// This is critical for Parquet format which must write its file footer.
    ///
    /// # Errors
    ///
    /// Returns an error if closing fails in either format
    pub fn close(self) -> Result<(), Error> {
        self.jsonl.close()?;
        self.parquet.close()?;
        Ok(())
    }
}

impl<W1: Write, W2: Write + Seek + Send> crate::formats::OutputFormat for Format<W1, W2> {
    fn write_metric(&mut self, line: &line::Line) -> Result<(), crate::formats::Error> {
        self.write_metric(line).map_err(Into::into)
    }

    fn flush(&mut self) -> Result<(), crate::formats::Error> {
        self.flush().map_err(Into::into)
    }

    fn close(self) -> Result<(), crate::formats::Error> {
        self.close().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        formats::{jsonl, parquet},
        line::{Line, LineValue, MetricKind},
    };
    use datadog_protos::metrics::Dogsketch;
    use ddsketch_agent::DDSketch;
    use protobuf::Message;
    use rustc_hash::FxHashMap;
    use std::io::Cursor;
    use uuid::Uuid;

    #[test]
    fn writes_to_both_formats() {
        let mut jsonl_buffer = Vec::new();
        let mut parquet_buffer = Cursor::new(Vec::new());

        let jsonl_format = jsonl::Format::new(&mut jsonl_buffer);
        let parquet_format = parquet::Format::new(&mut parquet_buffer, 6)
            .expect("parquet format creation should succeed");

        let mut format = Format::new(jsonl_format, parquet_format);

        let line = Line {
            run_id: Uuid::new_v4(),
            time: 1000,
            fetch_index: 0,
            metric_name: "test_metric".into(),
            metric_kind: MetricKind::Counter,
            value: LineValue::Int(42),
            labels: FxHashMap::default(),
            value_histogram: Vec::new(),
        };

        format.write_metric(&line).expect("write should succeed");
        format.flush().expect("flush should succeed");
        format.close().expect("close should succeed");

        // JSONL should have written data
        assert!(!jsonl_buffer.is_empty(), "JSONL buffer should not be empty");
        let jsonl_output = String::from_utf8(jsonl_buffer).expect("should be valid UTF-8");
        assert!(
            jsonl_output.contains("test_metric"),
            "JSONL should contain metric name"
        );

        // Parquet should have written data
        assert!(
            !parquet_buffer.get_ref().is_empty(),
            "Parquet buffer should not be empty"
        );
    }

    #[test]
    fn multiple_writes_to_both_formats() {
        let mut jsonl_buffer = Vec::new();
        let mut parquet_buffer = Cursor::new(Vec::new());

        let jsonl_format = jsonl::Format::new(&mut jsonl_buffer);
        let parquet_format = parquet::Format::new(&mut parquet_buffer, 6)
            .expect("parquet format creation should succeed");

        let mut format = Format::new(jsonl_format, parquet_format);

        for i in 0..5 {
            let line = Line {
                run_id: Uuid::new_v4(),
                time: 1000 + (i as u128),
                fetch_index: i as u64,
                metric_name: format!("metric_{i}"),
                metric_kind: MetricKind::Gauge,
                value: LineValue::Float(i as f64),
                labels: FxHashMap::default(),
                value_histogram: Vec::new(),
            };

            format.write_metric(&line).expect("write should succeed");
        }

        format.flush().expect("flush should succeed");
        format.close().expect("close should succeed");

        // JSONL should have 5 lines
        let jsonl_output = String::from_utf8(jsonl_buffer).expect("should be valid UTF-8");
        let lines: Vec<&str> = jsonl_output.lines().collect();
        assert_eq!(lines.len(), 5, "should have 5 lines in JSONL");

        // Parquet should have data
        assert!(
            !parquet_buffer.get_ref().is_empty(),
            "Parquet buffer should not be empty"
        );
    }

    #[test]
    fn histogram_consistency_across_formats() {
        let mut jsonl_buffer = Vec::new();
        let mut parquet_buffer = Cursor::new(Vec::new());

        let jsonl_format = jsonl::Format::new(&mut jsonl_buffer);
        let parquet_format = parquet::Format::new(&mut parquet_buffer, 6)
            .expect("parquet format creation should succeed");

        let mut format = Format::new(jsonl_format, parquet_format);

        let mut sketch = DDSketch::default();
        sketch.insert(1.0);
        sketch.insert(2.0);
        sketch.insert(3.0);

        let mut dogsketch = Dogsketch::new();
        sketch.merge_to_dogsketch(&mut dogsketch);
        let protobuf_bytes = dogsketch.write_to_bytes().expect("protobuf");
        let original_sketch = DDSketch::try_from(dogsketch).expect("convert");

        let line = Line {
            run_id: Uuid::new_v4(),
            time: 1000,
            fetch_index: 0,
            metric_name: "histogram_metric".into(),
            metric_kind: MetricKind::Histogram,
            value: LineValue::Float(0.0),
            labels: FxHashMap::default(),
            value_histogram: protobuf_bytes.clone(),
        };

        format.write_metric(&line).expect("write should succeed");
        format.flush().expect("flush should succeed");
        format.close().expect("close should succeed");

        let jsonl_output = String::from_utf8(jsonl_buffer).expect("should be valid UTF-8");
        let jsonl_line: Line = serde_json::from_str(&jsonl_output).expect("parse JSON");

        assert_eq!(
            jsonl_line.value_histogram, protobuf_bytes,
            "JSONL histogram bytes should match original protobuf after base64 round-trip"
        );

        let parquet_file_bytes = parquet_buffer.into_inner();
        assert!(!parquet_file_bytes.is_empty(), "Parquet should have data");

        assert_eq!(
            original_sketch.count(),
            3,
            "Original sketch should have 3 samples"
        );
    }
}
