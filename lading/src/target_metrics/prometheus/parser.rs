//! Zero-copy Prometheus text format parser
//!
//! This module provides a parser for the Prometheus text exposition format.
//! <https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md>

use rustc_hash::FxHashMap;
use std::borrow::Cow;

/// Type alias for label pairs with borrowed data
type LabelPairs<'a> = Vec<(&'a str, Cow<'a, str>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Prometheus metric types
pub enum MetricType {
    /// A gauge is a metric that represents a single numerical value that can
    /// arbitrarily go up and down
    Gauge,
    /// A counter is a cumulative metric that represents a single monotonically
    /// increasing counter
    Counter,
    /// A histogram samples observations and counts them in configurable buckets
    Histogram,
    /// A summary samples observations
    Summary,
    /// An untyped metric for compatibility with systems that don't have typed
    /// metrics
    Untyped,
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// Errors that can occur while parsing Prometheus text format
pub enum ParseError {
    /// Unknown metric type in TYPE line
    UnknownMetricType,
    /// Invalid format in the line
    InvalidFormat(&'static str),
    /// Invalid value that cannot be parsed as a number
    InvalidValue,
    /// Missing value in metric line
    MissingValue,
    /// Missing name in metric line
    MissingName,
    /// Invalid label format
    InvalidLabel(&'static str),
}

impl MetricType {
    fn from_str(s: &str) -> Result<Self, ParseError> {
        match s {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            "summary" => Ok(Self::Summary),
            "untyped" => Ok(Self::Untyped),
            _ => Err(ParseError::UnknownMetricType),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A parsed Prometheus metric with borrowed data
pub struct ParsedMetric<'a> {
    /// The metric name
    pub name: &'a str,
    /// The metric type if known from a TYPE line
    pub metric_type: Option<MetricType>,
    /// The metric value
    pub value: f64,
    /// The metric labels as key-value pairs
    pub labels: Option<LabelPairs<'a>>,
    /// Optional timestamp in milliseconds since Unix epoch
    pub timestamp: Option<i64>,
}

#[derive(Debug, Default)]
/// Zero-copy parser for Prometheus text exposition format
pub struct Parser<'a> {
    typemap: FxHashMap<&'a str, MetricType>,
}

impl<'a> Parser<'a> {
    /// Create a new parser instance
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a complete Prometheus text format response
    pub fn parse_text(&mut self, text: &'a str) -> Vec<Result<ParsedMetric<'a>, ParseError>> {
        let mut results = Vec::new();

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(result) = self.parse_line(line) {
                results.push(result);
            }
        }

        results
    }

    /// Parse a single line of Prometheus text format
    pub fn parse_line(&mut self, line: &'a str) -> Option<Result<ParsedMetric<'a>, ParseError>> {
        let line = line.trim();

        if line.is_empty() {
            return None;
        }

        if line.starts_with('#') {
            if line.starts_with("# TYPE") {
                return match self.parse_type_line(line) {
                    Ok(()) => None,
                    Err(e) => Some(Err(e)),
                };
            }
            // Ignore other comments (including HELP)
            return None;
        }

        Some(self.parse_metric_line(line))
    }

    fn parse_type_line(&mut self, line: &'a str) -> Result<(), ParseError> {
        let mut parts = line.split_ascii_whitespace().skip(2);

        let name = parts.next().ok_or(ParseError::InvalidFormat(
            "Missing metric name in TYPE line",
        ))?;

        let metric_type_str = parts.next().ok_or(ParseError::InvalidFormat(
            "Missing metric type in TYPE line",
        ))?;

        let metric_type = MetricType::from_str(metric_type_str)?;
        // Store the metric type - for histogram and summary metrics, we store
        // the base name and handle suffixes (_sum, _count, _bucket) during lookup
        self.typemap.insert(name, metric_type);

        Ok(())
    }

    fn parse_metric_line(&self, line: &'a str) -> Result<ParsedMetric<'a>, ParseError> {
        let (name_and_labels, value_part) = Self::split_metric_line(line)?;
        let (name, labels) = Self::parse_name_and_labels(name_and_labels)?;
        let (value, timestamp) = Self::parse_value_and_timestamp(value_part)?;

        let metric_type = self.lookup_metric_type(name);

        Ok(ParsedMetric {
            name,
            metric_type,
            value,
            labels,
            timestamp,
        })
    }

    fn lookup_metric_type(&self, name: &str) -> Option<MetricType> {
        // First try direct lookup, won't match for histogram/summary types
        if let Some(&metric_type) = self.typemap.get(name) {
            return Some(metric_type);
        }

        // Now handle histogram/summary types
        if name.ends_with("_sum") || name.ends_with("_count") || name.ends_with("_bucket") {
            for suffix in &["_sum", "_count", "_bucket"] {
                if let Some(base_name) = name.strip_suffix(suffix) {
                    if let Some(&metric_type) = self.typemap.get(base_name) {
                        if matches!(metric_type, MetricType::Histogram | MetricType::Summary) {
                            return Some(metric_type);
                        }
                    }
                }
            }
        }

        None
    }

    fn split_metric_line(line: &str) -> Result<(&str, &str), ParseError> {
        // Find the split point - after labels (if any) and before value
        if let Some(label_start) = line.find('{') {
            if let Some(label_end) = line.find('}') {
                if label_start < label_end {
                    let after_labels = &line[label_end + 1..];
                    let value_start = after_labels
                        .find(|c: char| !c.is_whitespace())
                        .ok_or(ParseError::MissingValue)?;

                    return Ok((&line[..=label_end], &after_labels[value_start..]));
                }
            }
            return Err(ParseError::InvalidFormat("Unclosed labels bracket"));
        }

        // No labels, split on whitespace
        if let Some(space_idx) = line.find(char::is_whitespace) {
            let name = &line[..space_idx];
            let rest = &line[space_idx..];
            let value_start = rest
                .find(|c: char| !c.is_whitespace())
                .ok_or(ParseError::MissingValue)?;
            Ok((name, &rest[value_start..]))
        } else {
            Err(ParseError::InvalidFormat("Missing value in metric line"))
        }
    }

    fn parse_name_and_labels(
        name_and_labels: &'a str,
    ) -> Result<(&'a str, Option<LabelPairs<'a>>), ParseError> {
        if let Some(label_start) = name_and_labels.find('{') {
            let name = &name_and_labels[..label_start];
            if name.is_empty() || name.chars().all(char::is_whitespace) {
                return Err(ParseError::MissingName);
            }

            let labels_end = name_and_labels.len() - 1; // Skip the closing }
            let labels_str = &name_and_labels[label_start + 1..labels_end];
            let labels = Self::parse_labels(labels_str)?;
            Ok((name, Some(labels)))
        } else {
            if name_and_labels.is_empty() || name_and_labels.chars().all(char::is_whitespace) {
                return Err(ParseError::MissingName);
            }
            Ok((name_and_labels, None))
        }
    }

    fn parse_labels(labels_str: &'a str) -> Result<LabelPairs<'a>, ParseError> {
        let mut labels = Vec::new();
        let mut remaining = labels_str;

        while !remaining.is_empty() {
            remaining = remaining.trim();
            if remaining.is_empty() {
                break;
            }

            // Find the equals sign for the label name
            let eq_idx = remaining
                .find('=')
                .ok_or(ParseError::InvalidLabel("Label missing '='"))?;
            let label_name = remaining[..eq_idx].trim();
            if label_name.is_empty() {
                return Err(ParseError::InvalidLabel("Empty label key"));
            }

            // Move past the equals sign
            remaining = remaining[eq_idx + 1..].trim();

            // Parse the quoted value
            if !remaining.starts_with('"') {
                return Err(ParseError::InvalidLabel("Label value must be quoted"));
            }

            // Find the closing quote, handling escapes
            let chars = remaining[1..].char_indices(); // Skip opening quote
            let mut end_quote_pos = None;
            let mut escaped = false;

            for (idx, ch) in chars {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    end_quote_pos = Some(idx + 1); // +1 for the skipped opening quote
                    break;
                }
            }

            let end_quote_pos =
                end_quote_pos.ok_or(ParseError::InvalidLabel("Unclosed quoted value"))?;

            // Include closing quote
            let label_value_raw = &remaining[..end_quote_pos + 1];
            let label_value = Self::parse_label_value(label_value_raw)?;
            labels.push((label_name, label_value));

            // Move past the quoted value
            remaining = &remaining[end_quote_pos + 1..];

            // Skip comma if present
            if remaining.starts_with(',') {
                remaining = &remaining[1..];
            }
        }

        Ok(labels)
    }

    fn parse_label_value(value: &'a str) -> Result<Cow<'a, str>, ParseError> {
        let value = value.trim();

        // Label values must be quoted
        if !value.starts_with('"') || !value.ends_with('"') {
            return Err(ParseError::InvalidLabel("Label value must be quoted"));
        }

        // Check for minimum length (at least 2 characters for opening and closing quotes)
        if value.len() < 2 {
            return Err(ParseError::InvalidLabel(
                "Label value quotes not properly paired",
            ));
        }

        let inner = &value[1..value.len() - 1];

        // Fast path: if there are no escape sequences, return borrowed data
        if !inner.contains('\\') {
            return Ok(Cow::Borrowed(inner));
        }

        // Slow path: we need to unescape, so we must allocate
        let mut result = String::with_capacity(inner.len());
        let mut chars = inner.chars();

        while let Some(ch) = chars.next() {
            if ch == '\\' {
                match chars.next() {
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some('n') => result.push('\n'),
                    Some(_) => {
                        return Err(ParseError::InvalidLabel("Invalid escape sequence"));
                    }
                    None => {
                        return Err(ParseError::InvalidLabel("Backslash at end of label value"));
                    }
                }
            } else if ch == '"' {
                return Err(ParseError::InvalidLabel("Unescaped quote in label value"));
            } else {
                result.push(ch);
            }
        }

        Ok(Cow::Owned(result))
    }

    fn parse_value_and_timestamp(value_str: &str) -> Result<(f64, Option<i64>), ParseError> {
        let mut parts = value_str.split_whitespace();

        let value_part = parts.next().ok_or(ParseError::MissingValue)?;

        // Check for comment markers
        if value_part.contains('#') {
            return Err(ParseError::InvalidFormat("Value contains comment marker"));
        }

        // Handle special float values according to Prometheus spec
        let value = match value_part {
            "NaN" => f64::NAN,
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            _ => value_part
                .parse::<f64>()
                .map_err(|_| ParseError::InvalidValue)?,
        };

        // Parse optional timestamp
        let timestamp = if let Some(ts_str) = parts.next() {
            Some(
                ts_str
                    .parse::<i64>()
                    .map_err(|_| ParseError::InvalidFormat("Invalid timestamp"))?,
            )
        } else {
            None
        };

        Ok((value, timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::{Cow, MetricType, ParseError, Parser};
    use proptest::prelude::{prop_assert, prop_assert_eq, proptest};
    use proptest::{collection, num, sample};

    #[test]
    fn test_label_value_with_escaped_quotes() {
        let parser = Parser::new();
        let result = parser
            .parse_metric_line(r#"metric{key="value with \"quotes\""} 123"#)
            .unwrap();
        let labels = result.labels.as_ref().unwrap();
        match &labels[0].1 {
            Cow::Owned(s) => assert_eq!(s, "value with \"quotes\""),
            Cow::Borrowed(_) => panic!("Expected owned string due to escaping"),
        }
    }

    #[test]
    fn test_label_value_with_escaped_backslash() {
        let parser = Parser::new();
        let result = parser
            .parse_metric_line(r#"metric{key="path\\to\\file"} 123"#)
            .unwrap();
        let labels = result.labels.as_ref().unwrap();
        match &labels[0].1 {
            Cow::Owned(s) => assert_eq!(s, "path\\to\\file"),
            Cow::Borrowed(_) => panic!("Expected owned string due to escaping"),
        }
    }

    #[test]
    fn test_label_value_with_escaped_newline() {
        let parser = Parser::new();
        let result = parser
            .parse_metric_line(r#"metric{key="line1\nline2"} 123"#)
            .unwrap();
        let labels = result.labels.as_ref().unwrap();
        match &labels[0].1 {
            Cow::Owned(s) => assert_eq!(s, "line1\nline2"),
            Cow::Borrowed(_) => panic!("Expected owned string due to escaping"),
        }
    }

    #[test]
    fn test_unquoted_label_value_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line("metric{key=unquoted} 123");
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));
    }

    #[test]
    fn test_invalid_escape_sequence_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line(r#"metric{key="invalid\x"} 123"#);
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));
    }

    #[test]
    fn test_unclosed_quote_in_label_value_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line(r#"metric{key="} 123"#);
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));
    }

    #[test]
    fn test_parse_full_text() {
        let mut parser = Parser::new();

        let text = r#"
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"}    3 1395066363000

# TYPE memory_usage gauge
memory_usage 5264384
"#;

        let results = parser.parse_text(text);
        let successful_results: Vec<_> = results.into_iter().filter_map(Result::ok).collect();

        assert_eq!(successful_results.len(), 3);

        assert_eq!(successful_results[0].name, "http_requests_total");
        assert_eq!(successful_results[0].metric_type, Some(MetricType::Counter));
        assert_eq!(successful_results[0].value, 1027.0);

        assert_eq!(successful_results[1].name, "http_requests_total");
        assert_eq!(successful_results[1].metric_type, Some(MetricType::Counter));
        assert_eq!(successful_results[1].value, 3.0);

        assert_eq!(successful_results[2].name, "memory_usage");
        assert_eq!(successful_results[2].metric_type, Some(MetricType::Gauge));
        assert_eq!(successful_results[2].value, 5264384.0);
    }

    #[test]
    fn test_parse_label_with_spaces() {
        let parser = Parser::new();

        let result = parser.parse_metric_line(
            r#"vector_build_info{arch="aarch64",debug="false",host="d0cf527728fe",revision="745babd 2024-09-11 14:55:36.802851761",rust_version="1.78",version="0.41.1"} 1 1729113558073"#
        ).unwrap();

        assert_eq!(result.name, "vector_build_info");
        assert_eq!(result.value, 1.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 6);

        // Find the revision label
        let revision_label = labels.iter().find(|(k, _)| *k == "revision").unwrap();
        match &revision_label.1 {
            Cow::Borrowed(s) => assert_eq!(*s, "745babd 2024-09-11 14:55:36.802851761"),
            Cow::Owned(_) => panic!("Expected borrowed string"),
        }
    }

    #[test]
    fn test_gauge_label_value_with_comma() {
        let parser = Parser::new();

        let result = parser
            .parse_metric_line("go_info{version=\"go1.25rc1 X:jsonv2,greenteagc\"} 1")
            .unwrap();

        assert_eq!(result.name, "go_info");
        assert_eq!(result.value, 1.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].0, "version");
        match &labels[0].1 {
            Cow::Borrowed(s) => assert_eq!(*s, "go1.25rc1 X:jsonv2,greenteagc"),
            Cow::Owned(_) => panic!("Expected borrowed string"),
        }
    }

    #[test]
    fn test_gauge_multiple_labels_with_special_chars() {
        let parser = Parser::new();

        let result = parser.parse_metric_line(
            "test_metric{label1=\"value1\",label2=\"value,with,commas\",label3=\"value=with=equals\",label4=\"normal\"} 42"
        ).unwrap();

        assert_eq!(result.name, "test_metric");
        assert_eq!(result.value, 42.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 4);

        let expected = vec![
            ("label1", "value1"),
            ("label2", "value,with,commas"),
            ("label3", "value=with=equals"),
            ("label4", "normal"),
        ];

        for (i, (name, value)) in expected.iter().enumerate() {
            assert_eq!(labels[i].0, *name);
            match &labels[i].1 {
                Cow::Borrowed(s) => assert_eq!(s, value),
                Cow::Owned(_) => panic!("Expected borrowed string"),
            }
        }
    }

    #[test]
    fn test_gauge_edge_case_labels() {
        let parser = Parser::new();

        let result = parser.parse_metric_line(
            r#"edge_cases{empty="",spaces="value with spaces",underscore_123="test",path="/var/log/app.log",url="https://example.com/path?query=1"} 1"#
        ).unwrap();

        assert_eq!(result.name, "edge_cases");
        assert_eq!(result.value, 1.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 5);

        // Check empty label
        assert_eq!(labels[0].0, "empty");
        match &labels[0].1 {
            Cow::Borrowed(s) => assert_eq!(*s, ""),
            Cow::Owned(_) => panic!("Expected borrowed string"),
        }
    }

    #[test]
    fn test_gauge_unicode_labels() {
        let parser = Parser::new();

        let result = parser
            .parse_metric_line(r#"unicode_test{emoji="🚀",chinese="你好",mixed="hello-世界"} 1"#)
            .unwrap();

        assert_eq!(result.name, "unicode_test");
        assert_eq!(result.value, 1.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 3);

        assert_eq!(labels[0].0, "emoji");
        match &labels[0].1 {
            Cow::Borrowed(s) => assert_eq!(*s, "🚀"),
            Cow::Owned(_) => panic!("Expected borrowed string"),
        }

        assert_eq!(labels[1].0, "chinese");
        match &labels[1].1 {
            Cow::Borrowed(s) => assert_eq!(*s, "你好"),
            Cow::Owned(_) => panic!("Expected borrowed string"),
        }

        assert_eq!(labels[2].0, "mixed");
        match &labels[2].1 {
            Cow::Borrowed(s) => assert_eq!(*s, "hello-世界"),
            Cow::Owned(_) => panic!("Expected borrowed string"),
        }
    }

    #[test]
    fn test_gauge_numeric_label_names() {
        let parser = Parser::new();

        let result = parser.parse_metric_line(
            r#"numeric_names{label_123="value",_underscore="test",ALL_CAPS="VALUE",mixedCase_123="test"} 1"#
        ).unwrap();

        assert_eq!(result.name, "numeric_names");
        assert_eq!(result.value, 1.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 4);
    }

    #[test]
    fn test_gauge_mixed_quotes() {
        let parser = Parser::new();

        // Our parser requires all label values to be quoted
        // This should fail because "unquoted" value is not quoted
        let result = parser
            .parse_metric_line(r#"mixed_quotes{quoted="value",unquoted=novalue,another="test"} 1"#);

        assert!(result.is_err());
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));
    }

    #[test]
    fn test_label_value_with_multiple_escape_types() {
        let parser = Parser::new();
        let result = parser
            .parse_metric_line(r#"metric{label="value with \\ and \" and \n"} 1"#)
            .unwrap();
        let labels = result.labels.unwrap();
        match &labels[0].1 {
            Cow::Owned(s) => assert_eq!(s, "value with \\ and \" and \n"),
            Cow::Borrowed(_) => panic!("Expected owned string due to escaping"),
        }
    }

    #[test]
    fn test_empty_label_value() {
        let parser = Parser::new();
        let result = parser.parse_metric_line(r#"metric{label=""} 1"#).unwrap();
        let labels = result.labels.unwrap();
        match &labels[0].1 {
            Cow::Borrowed(s) => assert_eq!(*s, ""),
            Cow::Owned(_) => panic!("Expected borrowed string for empty value"),
        }
    }

    #[test]
    fn test_empty_line_produces_no_result() {
        let mut parser = Parser::new();
        let result = parser.parse_line("");
        assert!(result.is_none());
    }

    #[test]
    fn test_whitespace_only_line_produces_no_result() {
        let mut parser = Parser::new();
        let result = parser.parse_line("   ");
        assert!(result.is_none());
    }

    #[test]
    fn test_comment_line_produces_no_result() {
        let mut parser = Parser::new();
        let result = parser.parse_line("#");
        assert!(result.is_none());
    }

    #[test]
    fn test_help_line_produces_no_result() {
        let mut parser = Parser::new();
        let result = parser.parse_line("# HELP metric description");
        assert!(result.is_none());
    }

    #[test]
    fn test_metric_without_value_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line("metric");
        assert!(matches!(
            result,
            Err(ParseError::InvalidFormat("Missing value in metric line"))
        ));
    }

    #[test]
    fn test_metric_with_unclosed_bracket_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line("metric{");
        assert!(matches!(
            result,
            Err(ParseError::InvalidFormat("Unclosed labels bracket"))
        ));
    }

    #[test]
    fn test_metric_with_closing_bracket_only_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line("metric}");
        assert!(matches!(
            result,
            Err(ParseError::InvalidFormat("Missing value in metric line"))
        ));
    }

    #[test]
    fn test_empty_label_key_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line("metric{=value} 1");
        assert!(matches!(
            result,
            Err(ParseError::InvalidLabel("Empty label key"))
        ));
    }

    #[test]
    fn test_metric_without_name_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line(" {} 0");
        assert!(matches!(result, Err(ParseError::MissingName)));
    }

    #[test]
    fn test_labels_without_metric_name_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line("{label=\"value\"} 1");
        assert!(matches!(result, Err(ParseError::MissingName)));
    }

    #[test]
    fn test_invalid_escape_in_label_rejected() {
        let parser = Parser::new();
        let result = parser.parse_metric_line(r#"metric{a="\x"} 1"#);
        assert!(matches!(
            result,
            Err(ParseError::InvalidLabel("Invalid escape sequence"))
        ));
    }

    // Property-based tests
    proptest! {
        #[test]
        fn empty_names_always_rejected(
            prefix in "[ \t]*",
            suffix in "[ \t]*",
            labels in "\\{[^}]*\\}",
            value in "[0-9]+",
        ) {
            let parser = Parser::new();
            // Empty metric name with labels
            let line = format!("{prefix}{labels}{suffix} {value}");
            let result = parser.parse_metric_line(&line);
            assert!(matches!(result, Err(ParseError::MissingName)));
        }

        #[test]
        fn valid_metric_names_accepted(
            name in "[a-zA-Z_:][a-zA-Z0-9_:]*",
            value in num::f64::NORMAL | num::f64::POSITIVE | num::f64::NEGATIVE,
        ) {
            let parser = Parser::new();
            let line = format!("{} {}", name, value);
            let result = parser.parse_metric_line(&line);
            prop_assert!(result.is_ok());
            let parsed = result.unwrap();
            prop_assert_eq!(parsed.name, name.as_str());
            prop_assert_eq!(parsed.value, value);
        }


        #[test]
        fn label_escaping_roundtrip(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            label_name in "[a-zA-Z_][a-zA-Z0-9_]*",
            raw_value in ".*",
            metric_value in "[0-9]+",
        ) {
            let parser = Parser::new();

            // Escape the label value
            let escaped = raw_value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");

            let line = format!("{name}{{{label_name}=\"{escaped}\"}} {metric_value}");

            if let Ok(parsed) = parser.parse_metric_line(&line) {
                if let Some(labels) = parsed.labels {
                    if let Some((_, parsed_value)) = labels.iter().find(|(k, _)| *k == &label_name) {
                        match parsed_value {
                            Cow::Borrowed(_) => {
                                // If borrowed, it means no escaping was needed
                                prop_assert!(!raw_value.contains('\\') && !raw_value.contains('"') && !raw_value.contains('\n'));
                            }
                            Cow::Owned(s) => {
                                prop_assert_eq!(s, &raw_value);
                            }
                        }
                    }
                }
            }
        }

        #[test]
        fn special_floats_parsed_correctly(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
        ) {
            let parser = Parser::new();

            let test_cases = vec![
                ("NaN", f64::NAN),
                ("+Inf", f64::INFINITY),
                ("-Inf", f64::NEG_INFINITY),
            ];

            for (str_val, expected) in test_cases {
                let line = format!("{} {}", name, str_val);
                let result = parser.parse_metric_line(&line);
                prop_assert!(result.is_ok());
                let parsed = result.unwrap();
                if str_val == "NaN" {
                    prop_assert!(parsed.value.is_nan());
                } else {
                    prop_assert_eq!(parsed.value, expected);
                }
            }
        }

        #[test]
        fn timestamp_parsing(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            value in num::f64::NORMAL,
            timestamp in num::i64::ANY,
        ) {
            let parser = Parser::new();
            let line = format!("{} {} {}", name, value, timestamp);
            let result = parser.parse_metric_line(&line);
            prop_assert!(result.is_ok());
            let parsed = result.unwrap();
            prop_assert_eq!(parsed.timestamp, Some(timestamp));
        }

        #[test]
        fn invalid_values_rejected(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            invalid_value in "[a-zA-Z][a-zA-Z0-9]*", // alphanumeric strings that aren't valid numbers
        ) {
            let parser = Parser::new();
            let line = format!("{} {}", name, invalid_value);
            let result = parser.parse_metric_line(&line);
            prop_assert!(matches!(result, Err(ParseError::InvalidValue)));
        }

        #[test]
        fn metrics_with_labels(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            labels in collection::vec(
                ("[a-zA-Z_][a-zA-Z0-9_]*", "[^\"\\\\}]*"),  // Exclude } from label values
                1..5
            ),
            value in num::f64::NORMAL,
        ) {
            let parser = Parser::new();
            let label_str = labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect::<Vec<_>>()
                .join(",");
            let line = format!("{}{{{}}}{} {}", name, label_str, " ", value);
            let result = parser.parse_metric_line(&line);
            prop_assert!(result.is_ok());
            let parsed = result.unwrap();
            prop_assert_eq!(parsed.name, name.as_str());
            prop_assert_eq!(parsed.value, value);
            if let Some(parsed_labels) = parsed.labels {
                prop_assert_eq!(parsed_labels.len(), labels.len());
                for (i, (expected_key, expected_value)) in labels.iter().enumerate() {
                    prop_assert_eq!(parsed_labels[i].0, expected_key);
                    match &parsed_labels[i].1 {
                        Cow::Borrowed(s) => prop_assert_eq!(*s, expected_value),
                        Cow::Owned(_) => panic!("Expected borrowed string for simple label"),
                    }
                }
            }
        }

        #[test]
        fn type_line_parsing(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            metric_type in sample::select(vec!["counter", "gauge", "histogram", "summary", "untyped"]),
        ) {
            let mut parser = Parser::new();
            let type_line = format!("# TYPE {} {}", name, metric_type);
            let result = parser.parse_line(&type_line);
            prop_assert!(result.is_none()); // TYPE lines don't produce metrics

            // Verify the type was registered
            let expected = MetricType::from_str(&metric_type).unwrap();
            prop_assert_eq!(parser.typemap.get(name.as_str()), Some(&expected));

            // For histogram and summary, verify lookup_metric_type works for suffixes
            if metric_type == "histogram" || metric_type == "summary" {
                let sum_name = format!("{}_sum", name);
                let count_name = format!("{}_count", name);
                let bucket_name = format!("{}_bucket", name);
                prop_assert_eq!(parser.lookup_metric_type(&sum_name), Some(expected));
                prop_assert_eq!(parser.lookup_metric_type(&count_name), Some(expected));
                prop_assert_eq!(parser.lookup_metric_type(&bucket_name), Some(expected));
            }
        }

        #[test]
        fn invalid_label_formats(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            value in "[0-9]+",
        ) {
            let parser = Parser::new();

            // Test various invalid label formats
            let invalid_formats = vec![
                format!("{}{{=\"value\"}} {}", name, value), // empty label key
                format!("{}{{key}} {}", name, value), // label without equals
                format!("{}{{key=unquoted}} {}", name, value), // unquoted value
                format!("{}{{key=\"unclosed}} {}", name, value), // unclosed quote
                format!("{}{{key=\"value\"", name), // unclosed bracket
            ];

            for line in invalid_formats {
                let result = parser.parse_metric_line(&line);
                prop_assert!(result.is_err());
            }
        }
    }
}
