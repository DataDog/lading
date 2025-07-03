#![no_main]

use libfuzzer_sys::fuzz_target;
use lading::target_metrics::prometheus::parser::{PrometheusParser, ParseError};

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as UTF-8
    if let Ok(text) = std::str::from_utf8(data) {
        fuzz_prometheus_parser(text);
    }
});

fn fuzz_prometheus_parser(text: &str) {
    let mut parser = PrometheusParser::new();
    
    // Test full text parsing
    let results = parser.parse_text(text);
    
    // Verify that parsing doesn't panic and results are consistent
    for result in results {
        match result {
            Ok(metric) => {
                // Empty metric names should have been rejected by parser
                assert!(!metric.name.is_empty() && !metric.name.chars().all(char::is_whitespace), 
                    "Parser should reject empty/whitespace-only metric names");
                
                // Special float values are valid
                // NaN, +Inf, -Inf are all valid prometheus values
                
                // If labels exist, verify they're valid
                if let Some(labels) = &metric.labels {
                    for (key, _value) in labels {
                        assert!(!key.is_empty(), "Label key should not be empty");
                        assert!(!key.starts_with("__"), "Reserved label names should be rejected");
                        // Label values can be empty, that's valid
                    }
                }
                
                // Timestamp should be valid if present
                if let Some(ts) = metric.timestamp {
                    // Just verify it parsed as i64
                    let _ = ts;
                }
            }
            Err(e) => {
                // Verify we get appropriate errors for known bad inputs
                match e {
                    ParseError::MissingName => {
                        // This is expected for empty metric names
                    }
                    ParseError::InvalidLabel(_) => {
                        // Expected for malformed labels
                    }
                    _ => {
                        // Other errors are fine too
                    }
                }
            }
        }
    }
    
    // Test individual line parsing
    for line in text.lines() {
        let mut line_parser = PrometheusParser::new();
        let _ = line_parser.parse_line(line);
    }
    
    // Test comprehensive edge cases
    fuzz_edge_cases(&mut parser, text);
}

fn fuzz_edge_cases(parser: &mut PrometheusParser, text: &str) {
    // Test with TYPE lines for all metric types
    let metric_types = ["counter", "gauge", "histogram", "summary", "untyped"];
    for metric_type in &metric_types {
        let type_line = format!("# TYPE test_metric {}", metric_type);
        let _ = parser.parse_line(&type_line);
    }
    
    // Test special float values
    let special_values = ["NaN", "+Inf", "-Inf", "1.23e45", "-1.23e-45", "0", "-0"];
    for value in &special_values {
        let line = format!("test_metric {}", value);
        let _ = parser.parse_line(&line);
    }
    
    // Test timestamp variations
    let timestamps = ["", " 1234567890", " -1234567890", " 0", " 9223372036854775807"];
    for ts in &timestamps {
        let line = format!("test_metric 42{}", ts);
        let _ = parser.parse_line(&line);
    }
    
    // Test label escaping edge cases
    let escape_tests = vec![
        r#"metric{label="value with \"quotes\""} 1"#,
        r#"metric{label="value with \\backslash\\"} 1"#,
        r#"metric{label="value with \nnewline"} 1"#,
        r#"metric{label="value with \\ and \" and \n"} 1"#,
        r#"metric{label=""} 1"#, // empty label value is valid
        r#"metric{label="very long value that goes on and on and on"} 1"#,
    ];
    
    for line in escape_tests {
        let _ = parser.parse_line(line);
    }
    
    // Test various malformed inputs
    let malformed_tests = vec![
        "",                           // empty line
        "   ",                       // whitespace only
        "#",                         // just comment marker
        "metric",                    // missing value
        "metric{",                   // unclosed label bracket
        "metric}",                   // unexpected closing bracket
        "metric{label=unquoted} 1",  // unquoted label value
        "metric{=value} 1",          // empty label key
        "metric{__reserved=\"x\"} 1", // reserved label prefix
        " {} 0",                     // empty metric name with labels
        "{label=\"value\"} 1",        // missing metric name
        "metric{a=\"\\x\"} 1",        // invalid escape sequence
    ];
    
    for line in malformed_tests {
        let _ = parser.parse_line(line);
    }
    
    // Test with input text as various parts of a metric
    if !text.is_empty() {
        // Use text as metric name (may be invalid)
        let _ = parser.parse_line(&format!("{} 123", text));
        
        // Use text as label key (may be invalid)
        let _ = parser.parse_line(&format!("metric{{{}=\"value\"}} 456", text));
        
        // Use text as label value (needs escaping)
        let escaped = text
            .replace('\\', r"\\")
            .replace('"', r#"\""#)
            .replace('\n', r"\n");
        let _ = parser.parse_line(&format!("metric{{label=\"{}\"}} 789", escaped));
        
        // Use text as value (may not parse as number)
        let _ = parser.parse_line(&format!("metric {}", text));
    }
}