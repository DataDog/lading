#![no_main]

use libfuzzer_sys::fuzz_target;
use lading::target_metrics::prometheus::parser::Parser;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as UTF-8
    if let Ok(text) = std::str::from_utf8(data) {
        // Test full text parsing - parser should not panic on any valid UTF-8 input
        let mut parser = Parser::new();
        let _ = parser.parse_text(text);
        
        // Test individual line parsing
        for line in text.lines() {
            let mut line_parser = Parser::new();
            let _ = line_parser.parse_line(line);
        }
    }
});