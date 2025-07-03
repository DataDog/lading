//! Prometheus text format parser
//!
//! This module provides a parser for the Prometheus text exposition format.
//! <https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md>

use rustc_hash::FxHashMap;
use std::str::FromStr;

/// Type alias for label pairs
type LabelPairs = Vec<(String, String)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Prometheus metric types
pub enum MetricType {
    /// A gauge is a metric that represents a single numerical value that can arbitrarily go up and down
    Gauge,
    /// A counter is a cumulative metric that represents a single monotonically increasing counter
    Counter,
    /// A histogram samples observations and counts them in configurable buckets
    Histogram,
    /// A summary samples observations
    Summary,
    /// An untyped metric for compatibility with systems that don't have typed metrics
    Untyped,
}

#[derive(Debug, Clone, PartialEq)]
/// Errors that can occur while parsing Prometheus text format
pub enum ParseError {
    /// Unknown metric type in TYPE line
    UnknownMetricType(String),
    /// Invalid format in the line
    InvalidFormat(String),
    /// Invalid value that cannot be parsed as a number
    InvalidValue(String),
    /// Missing value in metric line
    MissingValue,
    /// Missing name in metric line
    MissingName,
    /// Invalid label format
    InvalidLabel(String),
}

impl FromStr for MetricType {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            "summary" => Ok(Self::Summary),
            "untyped" => Ok(Self::Untyped),
            _ => Err(ParseError::UnknownMetricType(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A parsed Prometheus metric
pub struct ParsedMetric {
    /// The metric name
    pub name: String,
    /// The metric type if known from a TYPE line
    pub metric_type: Option<MetricType>,
    /// The metric value
    pub value: f64,
    /// The metric labels as key-value pairs
    pub labels: Option<LabelPairs>,
    /// Optional timestamp in milliseconds since Unix epoch
    pub timestamp: Option<i64>,
}

#[derive(Debug, Default)]
/// Parser for Prometheus text exposition format
pub struct PrometheusParser {
    #[allow(clippy::zero_sized_map_values)]
    typemap: FxHashMap<String, MetricType>,
}

impl PrometheusParser {
    /// Create a new parser instance
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a complete Prometheus text format response
    pub fn parse_text(&mut self, text: &str) -> Vec<Result<ParsedMetric, ParseError>> {
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
    pub fn parse_line(&mut self, line: &str) -> Option<Result<ParsedMetric, ParseError>> {
        let line = line.trim();

        if line.is_empty() {
            return None;
        }

        if line.starts_with("# HELP") {
            return None;
        }

        if line.starts_with("# TYPE") {
            return match self.parse_type_line(line) {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            };
        }

        // Parse metric line
        Some(self.parse_metric_line(line))
    }

    fn parse_type_line(&mut self, line: &str) -> Result<(), ParseError> {
        let mut parts = line.split_ascii_whitespace().skip(2);

        let name = parts.next().ok_or_else(|| {
            ParseError::InvalidFormat("Missing metric name in TYPE line".to_string())
        })?;

        let metric_type_str = parts.next().ok_or_else(|| {
            ParseError::InvalidFormat("Missing metric type in TYPE line".to_string())
        })?;

        let metric_type: MetricType = metric_type_str.parse()?;

        // Handle histogram and summary metrics with their suffixes
        if matches!(metric_type, MetricType::Histogram | MetricType::Summary) {
            self.typemap.insert(format!("{name}_sum"), metric_type);
            self.typemap.insert(format!("{name}_count"), metric_type);
            self.typemap.insert(format!("{name}_bucket"), metric_type);
        }

        self.typemap.insert(name.to_owned(), metric_type);
        Ok(())
    }

    fn parse_metric_line(&self, line: &str) -> Result<ParsedMetric, ParseError> {
        let (name_and_labels, value_part) = Self::split_metric_line(line)?;
        let (name, labels) = Self::parse_name_and_labels(name_and_labels)?;
        let (value, timestamp) = Self::parse_value_and_timestamp(value_part)?;

        let metric_type = self.typemap.get(&name).copied();

        Ok(ParsedMetric {
            name,
            metric_type,
            value,
            labels,
            timestamp,
        })
    }

    fn split_metric_line(line: &str) -> Result<(&str, &str), ParseError> {
        // Find the split point - after labels (if any) and before value
        if line.contains('{') && line.contains('}') {
            // Has labels
            if let Some(end_idx) = line.find('}') {
                let after_labels = &line[end_idx + 1..];
                let value_start = after_labels
                    .find(|c: char| !c.is_whitespace())
                    .ok_or(ParseError::MissingValue)?;

                Ok((&line[..=end_idx], after_labels[value_start..].trim()))
            } else {
                Err(ParseError::InvalidFormat(
                    "Unclosed labels bracket".to_string(),
                ))
            }
        } else {
            // No labels, split on whitespace
            let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
            if parts.len() != 2 {
                return Err(ParseError::InvalidFormat(
                    "Missing value in metric line".to_string(),
                ));
            }
            Ok((parts[0], parts[1].trim()))
        }
    }

    fn parse_name_and_labels(
        name_and_labels: &str,
    ) -> Result<(String, Option<LabelPairs>), ParseError> {
        if let Some((name, labels_str)) = name_and_labels.split_once('{') {
            if name.is_empty() || name.chars().all(char::is_whitespace) {
                return Err(ParseError::MissingName);
            }
            Self::validate_metric_name(name);
            let labels_str = labels_str.trim_end_matches('}');
            let labels = Self::parse_labels(labels_str)?;
            Ok((name.to_string(), Some(labels)))
        } else {
            if name_and_labels.is_empty() || name_and_labels.chars().all(char::is_whitespace) {
                return Err(ParseError::MissingName);
            }
            Self::validate_metric_name(name_and_labels);
            Ok((name_and_labels.to_string(), None))
        }
    }

    fn validate_metric_name(name: &str) {
        // Metric names can be any UTF-8, but SHOULD follow [a-zA-Z_:][a-zA-Z0-9_:]*
        // We'll validate but not reject non-conforming names

        // Check if it follows the recommended pattern
        let mut chars = name.chars();
        if let Some(first) = chars.next() {
            let follows_pattern = (first.is_ascii_alphabetic() || first == '_' || first == ':')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':');

            if !follows_pattern {
                // Still valid, but might need quoting in PromQL
                // In a real implementation, we might want to log a warning here
            }
        }
    }

    fn parse_labels(labels_str: &str) -> Result<LabelPairs, ParseError> {
        let mut labels = Vec::new();

        for label in labels_str.split(',') {
            let label = label.trim();
            if label.is_empty() {
                continue;
            }

            let (label_name, label_value) = label
                .split_once('=')
                .ok_or_else(|| ParseError::InvalidLabel(format!("Label missing '=': {label}")))?;

            if label_name.is_empty() {
                return Err(ParseError::InvalidLabel("Empty label key".to_string()));
            }

            Self::validate_label_name(label_name)?;

            // Parse quoted label value with proper escape handling
            let label_value = Self::parse_label_value(label_value)?;
            labels.push((label_name.to_string(), label_value));
        }

        Ok(labels)
    }

    fn validate_label_name(name: &str) -> Result<(), ParseError> {
        // Label names starting with __ are reserved for internal use
        if name.starts_with("__") {
            return Err(ParseError::InvalidLabel(
                "Label names starting with '__' are reserved for internal use".to_string(),
            ));
        }

        // Label names can be any UTF-8, but SHOULD follow [a-zA-Z_][a-zA-Z0-9_]*
        // We'll validate but not reject non-conforming names
        let mut chars = name.chars();
        if let Some(first) = chars.next() {
            let follows_pattern = (first.is_ascii_alphabetic() || first == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');

            if !follows_pattern {
                // Still valid, but might need special handling
                // In a real implementation, we might want to log a warning here
            }
        }

        Ok(())
    }

    fn parse_label_value(value: &str) -> Result<String, ParseError> {
        let value = value.trim();

        // Label values must be quoted
        if !value.starts_with('"') || !value.ends_with('"') {
            return Err(ParseError::InvalidLabel(
                "Label value must be quoted".to_string(),
            ));
        }

        // Check for minimum length (at least 2 characters for opening and closing quotes)
        if value.len() < 2 {
            return Err(ParseError::InvalidLabel(
                "Label value quotes not properly paired".to_string(),
            ));
        }

        let inner = &value[1..value.len() - 1];
        let mut result = String::with_capacity(inner.len());
        let mut chars = inner.chars();

        while let Some(ch) = chars.next() {
            if ch == '\\' {
                match chars.next() {
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some('n') => result.push('\n'),
                    Some(c) => {
                        return Err(ParseError::InvalidLabel(format!(
                            "Invalid escape sequence: \\{c}"
                        )));
                    }
                    None => {
                        return Err(ParseError::InvalidLabel(
                            "Backslash at end of label value".to_string(),
                        ));
                    }
                }
            } else if ch == '"' {
                return Err(ParseError::InvalidLabel(
                    "Unescaped quote in label value".to_string(),
                ));
            } else {
                result.push(ch);
            }
        }

        Ok(result)
    }

    fn parse_value_and_timestamp(value_str: &str) -> Result<(f64, Option<i64>), ParseError> {
        let mut parts = value_str.split_whitespace();

        let value_part = parts.next().ok_or(ParseError::MissingValue)?;

        // Check for comment markers
        if value_part.contains('#') {
            return Err(ParseError::InvalidFormat(
                "Value contains comment marker".to_string(),
            ));
        }

        // Handle special float values according to Prometheus spec
        let value = match value_part {
            "NaN" => f64::NAN,
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            _ => value_part
                .parse::<f64>()
                .map_err(|_| ParseError::InvalidValue(value_part.to_string()))?,
        };

        // Parse optional timestamp
        let timestamp =
            if let Some(ts_str) = parts.next() {
                Some(ts_str.parse::<i64>().map_err(|_| {
                    ParseError::InvalidFormat(format!("Invalid timestamp: {ts_str}"))
                })?)
            } else {
                None
            };

        Ok((value, timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_parse_type_line() {
        let mut parser = PrometheusParser::new();

        assert!(
            parser
                .parse_type_line("# TYPE http_requests_total counter")
                .is_ok()
        );
        assert_eq!(
            parser.typemap.get("http_requests_total"),
            Some(&MetricType::Counter)
        );

        assert!(parser.parse_type_line("# TYPE memory_usage gauge").is_ok());
        assert_eq!(parser.typemap.get("memory_usage"), Some(&MetricType::Gauge));

        assert!(
            parser
                .parse_type_line("# TYPE http_request_duration_seconds histogram")
                .is_ok()
        );
        assert_eq!(
            parser.typemap.get("http_request_duration_seconds"),
            Some(&MetricType::Histogram)
        );
        assert_eq!(
            parser.typemap.get("http_request_duration_seconds_sum"),
            Some(&MetricType::Histogram)
        );
        assert_eq!(
            parser.typemap.get("http_request_duration_seconds_count"),
            Some(&MetricType::Histogram)
        );
        assert_eq!(
            parser.typemap.get("http_request_duration_seconds_bucket"),
            Some(&MetricType::Histogram)
        );
    }

    #[test]
    fn test_parse_metric_line_no_labels() {
        let parser = PrometheusParser::new();

        let result = parser
            .parse_metric_line("http_requests_total 1027")
            .unwrap();
        assert_eq!(result.name, "http_requests_total");
        assert_eq!(result.value, 1027.0);
        assert!(result.labels.is_none());
    }

    #[test]
    fn test_parse_metric_line_with_labels() {
        let parser = PrometheusParser::new();

        let result = parser
            .parse_metric_line("http_requests_total{method=\"GET\",code=\"200\"} 1027")
            .unwrap();
        assert_eq!(result.name, "http_requests_total");
        assert_eq!(result.value, 1027.0);
        assert_eq!(
            result.labels,
            Some(vec![
                ("method".to_string(), "GET".to_string()),
                ("code".to_string(), "200".to_string()),
            ])
        );
    }

    #[test]
    fn test_parse_metric_line_with_timestamp() {
        let parser = PrometheusParser::new();

        let result = parser
            .parse_metric_line("http_requests_total 1027 1729113558073")
            .unwrap();
        assert_eq!(result.name, "http_requests_total");
        assert_eq!(result.value, 1027.0);
        assert_eq!(result.timestamp, Some(1729113558073));

        // Test without timestamp
        let result = parser
            .parse_metric_line("http_requests_total 1027")
            .unwrap();
        assert_eq!(result.timestamp, None);
    }

    #[test]
    fn test_parse_invalid_value() {
        let parser = PrometheusParser::new();

        let result = parser.parse_metric_line("http_requests_total foobar");
        assert!(matches!(result, Err(ParseError::InvalidValue(_))));
    }

    #[test]
    fn test_parse_empty_metric_name() {
        let parser = PrometheusParser::new();

        // Test empty name with labels
        let result = parser.parse_metric_line(" {}0 ");
        assert!(matches!(result, Err(ParseError::MissingName)));

        // Test completely empty name
        let result = parser.parse_metric_line(" 123");
        assert!(matches!(result, Err(ParseError::MissingName)));
    }

    #[test]
    fn test_parse_invalid_labels() {
        let parser = PrometheusParser::new();

        // Test empty label key
        let result = parser.parse_metric_line("metric{=\"value\"} 123");
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));

        // Test label without equals
        let result = parser.parse_metric_line("metric{key} 123");
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));

        // Empty label value is allowed
        let result = parser.parse_metric_line("metric{key=\"\"} 123");
        assert!(result.is_ok());
    }

    #[test]
    fn test_label_name_validation() {
        let parser = PrometheusParser::new();

        // Test reserved label names
        let result = parser.parse_metric_line("metric{__reserved=\"value\"} 123");
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));

        // Test valid label names
        let result = parser.parse_metric_line("metric{valid_label=\"value\"} 123");
        assert!(result.is_ok());

        // Test UTF-8 label names (allowed but not recommended)
        let result = parser.parse_metric_line("metric{français=\"value\"} 123");
        assert!(result.is_ok());
    }

    #[test]
    fn test_label_value_escaping() {
        let parser = PrometheusParser::new();

        // Test escaped quotes
        let result = parser
            .parse_metric_line(r#"metric{key="value with \"quotes\""} 123"#)
            .unwrap();
        assert_eq!(
            result.labels.as_ref().unwrap()[0].1,
            "value with \"quotes\""
        );

        // Test escaped backslash
        let result = parser
            .parse_metric_line(r#"metric{key="path\\to\\file"} 123"#)
            .unwrap();
        assert_eq!(result.labels.as_ref().unwrap()[0].1, "path\\to\\file");

        // Test escaped newline
        let result = parser
            .parse_metric_line(r#"metric{key="line1\nline2"} 123"#)
            .unwrap();
        assert_eq!(result.labels.as_ref().unwrap()[0].1, "line1\nline2");

        // Test unquoted label value (should fail)
        let result = parser.parse_metric_line("metric{key=unquoted} 123");
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));

        // Test invalid escape sequence
        let result = parser.parse_metric_line(r#"metric{key="invalid\x"} 123"#);
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));

        // Test single quote character (edge case from fuzzer)
        let result = parser.parse_metric_line(r#"metric{key="} 123"#);
        assert!(matches!(result, Err(ParseError::InvalidLabel(_))));
    }

    #[test]
    fn test_special_float_values() {
        let parser = PrometheusParser::new();

        // Test NaN
        let result = parser.parse_metric_line("metric NaN").unwrap();
        assert!(result.value.is_nan());

        // Test +Inf
        let result = parser.parse_metric_line("metric +Inf").unwrap();
        assert_eq!(result.value, f64::INFINITY);

        // Test -Inf
        let result = parser.parse_metric_line("metric -Inf").unwrap();
        assert_eq!(result.value, f64::NEG_INFINITY);

        // Test scientific notation
        let result = parser.parse_metric_line("metric 1.23e45").unwrap();
        assert_eq!(result.value, 1.23e45);

        // Test negative values
        let result = parser.parse_metric_line("metric -42.5").unwrap();
        assert_eq!(result.value, -42.5);
    }

    #[test]
    fn test_parse_full_text() {
        let mut parser = PrometheusParser::new();

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
        let parser = PrometheusParser::new();

        let result = parser.parse_metric_line(
            r#"vector_build_info{arch="aarch64",debug="false",host="d0cf527728fe",revision="745babd 2024-09-11 14:55:36.802851761",rust_version="1.78",version="0.41.1"} 1 1729113558073"#
        ).unwrap();

        assert_eq!(result.name, "vector_build_info");
        assert_eq!(result.value, 1.0);

        let labels = result.labels.unwrap();
        assert_eq!(labels.len(), 6);
        assert!(labels.contains(&(
            "revision".to_string(),
            "745babd 2024-09-11 14:55:36.802851761".to_string()
        )));
    }

    // Property-based tests
    proptest! {
        #[test]
        fn prop_no_panic_on_any_input(input: String) {
            let mut parser = PrometheusParser::new();
            // Should not panic on any input
            let _ = parser.parse_text(&input);
        }

        #[test]
        fn prop_empty_names_always_rejected(
            prefix in "[ \t]*",
            suffix in "[ \t]*",
            labels in "\\{[^}]*\\}",
            value in "[0-9]+",
        ) {
            let parser = PrometheusParser::new();
            // Empty metric name with labels
            let line = format!("{prefix}{labels}{suffix} {value}");
            let result = parser.parse_metric_line(&line);
            assert!(matches!(result, Err(ParseError::MissingName)));
        }

        #[test]
        fn prop_valid_metric_names_accepted(
            name in "[a-zA-Z_:][a-zA-Z0-9_:]*",
            value in prop::num::f64::NORMAL | prop::num::f64::POSITIVE | prop::num::f64::NEGATIVE,
        ) {
            let parser = PrometheusParser::new();
            let line = format!("{} {}", name, value);
            let result = parser.parse_metric_line(&line);
            prop_assert!(result.is_ok());
            let parsed = result.unwrap();
            prop_assert_eq!(parsed.name, name);
            prop_assert_eq!(parsed.value, value);
        }

        #[test]
        fn prop_reserved_label_names_rejected(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            label_suffix in "[a-zA-Z0-9_]*",
            value in "[0-9]+",
        ) {
            let parser = PrometheusParser::new();
            let line = format!("{name}{{__{label_suffix}=\"value\"}} {value}");
            let result = parser.parse_metric_line(&line);
            assert!(matches!(result, Err(ParseError::InvalidLabel(_))));
        }

        #[test]
        fn prop_label_escaping_roundtrip(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            label_name in "[a-zA-Z_][a-zA-Z0-9_]*",
            raw_value in ".*",
            metric_value in "[0-9]+",
        ) {
            let parser = PrometheusParser::new();

            // Escape the label value
            let escaped = raw_value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");

            let line = format!("{name}{{{label_name}=\"{escaped}\"}} {metric_value}");

            if let Ok(parsed) = parser.parse_metric_line(&line) {
                if let Some(labels) = parsed.labels {
                    if let Some((_, parsed_value)) = labels.iter().find(|(k, _)| k == &label_name) {
                        prop_assert_eq!(parsed_value, &raw_value);
                    }
                }
            }
        }

        #[test]
        fn prop_special_floats_parsed_correctly(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
        ) {
            let parser = PrometheusParser::new();

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
        fn prop_timestamp_parsing(
            name in "[a-zA-Z_][a-zA-Z0-9_]*",
            value in prop::num::f64::NORMAL,
            timestamp in prop::num::i64::ANY,
        ) {
            let parser = PrometheusParser::new();
            let line = format!("{} {} {}", name, value, timestamp);
            let result = parser.parse_metric_line(&line);
            prop_assert!(result.is_ok());
            let parsed = result.unwrap();
            prop_assert_eq!(parsed.timestamp, Some(timestamp));
        }
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    #[kani::proof]
    fn proof_no_empty_metric_names() {
        let parser = PrometheusParser::new();
        let empty_name: String = kani::any();

        kani::assume(empty_name.chars().all(char::is_whitespace) || empty_name.is_empty());

        // Test with labels
        let line_with_labels = format!("{}{{label=\"value\"}} 123", empty_name);
        let result = parser.parse_metric_line(&line_with_labels);
        kani::assert(
            matches!(result, Err(ParseError::MissingName)),
            "Empty metric names must be rejected with labels",
        );

        // Test without labels
        let line_no_labels = format!("{} 123", empty_name);
        let result = parser.parse_metric_line(&line_no_labels);
        kani::assert(
            matches!(result, Err(ParseError::MissingName)),
            "Empty metric names must be rejected without labels",
        );
    }

    #[kani::proof]
    fn proof_label_key_validation() {
        let parser = PrometheusParser::new();
        let label_name: String = kani::any();

        kani::assume(label_name.starts_with("__"));

        let line = format!("metric{{{}=\"value\"}} 123", label_name);
        let result = parser.parse_metric_line(&line);
        kani::assert(
            matches!(result, Err(ParseError::InvalidLabel(_))),
            "Label names starting with __ must be rejected",
        );
    }

    #[kani::proof]
    fn proof_escape_handling() {
        let input: String = kani::any();
        kani::assume(input.len() < 10); // Limit input size for tractability

        // Manually escape the input
        let escaped = input
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");

        // Try to parse the escaped value
        let quoted = format!("\"{}\"", escaped);
        if let Ok(parsed) = PrometheusParser::parse_label_value(&quoted) {
            kani::assert(parsed == input, "Escape/unescape must roundtrip");
        }
    }

    #[kani::proof]
    fn proof_value_parsing_complete() {
        let value_str: String = kani::any();
        kani::assume(value_str.len() < 20); // Limit for tractability

        // If it's a special value, it should parse correctly
        if value_str == "NaN" || value_str == "+Inf" || value_str == "-Inf" {
            let result = PrometheusParser::parse_value_and_timestamp(&value_str);
            kani::assert(result.is_ok(), "Special float values must parse");
        }
    }

    #[kani::proof]
    fn proof_no_panic_on_any_input() {
        let input: String = kani::any();
        kani::assume(input.len() < 100); // Limit for tractability

        let mut parser = PrometheusParser::new();

        // Should not panic on any UTF-8 input
        let _ = parser.parse_text(&input);

        // Should not panic on any single line
        let _ = parser.parse_line(&input);
    }
}
