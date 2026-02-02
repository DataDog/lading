use super::{Handle, Pool};

/// Error type for `StringListPool` pattern expansion
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub(crate) enum Error {
    /// Invalid pattern found during expansion (mixed types like {{a-5}})
    #[error("Invalid pattern: {pattern}")]
    InvalidPattern {
        /// The invalid pattern string
        pattern: String,
    },
    /// Malformed pattern syntax
    #[error("Malformed pattern: {pattern}")]
    MalformedPattern {
        /// The malformed pattern string
        pattern: String,
    },
    /// Empty pattern list provided
    #[error("Pattern list cannot be empty")]
    EmptyPatternList,
    /// Max expansions must be greater than 0
    #[error("Max expansions must be greater than 0")]
    InvalidMaxExpansions,
    /// Pattern expansion resulted in empty list
    #[error("Pattern expansion resulted in empty list")]
    EmptyExpansion,
}

/// A list of strings
///
/// It can be useful to configure lading to create strings from a fixed list
/// to control the shape of data emitted. The pool will hand out a random string
/// from the list.
#[derive(Debug)]
pub(crate) struct StringListPool {
    metric_names: Vec<String>,
}

/// Represents a pattern range in a string
#[derive(Debug, Clone, PartialEq)]
enum PatternRange {
    Char(char, char),
    Number(u32, u32),
}

/// Represents a parsed pattern with its position in the string
#[derive(Debug, Clone)]
struct Pattern {
    start: usize,
    end: usize,
    range: PatternRange,
}

impl StringListPool {
    /// Create a new list of strings.
    pub(crate) fn new(metric_names: Vec<String>) -> Self {
        assert!(
            !metric_names.is_empty(),
            "don't create a string list with an empty list"
        );
        Self::new_with_patterns(&metric_names, 10_000).expect("valid patterns")
    }

    /// Create a new list of strings with pattern expansion.
    /// Patterns are defined as `{{X-Y}}` where X and Y can be:
    /// - Characters: `{{a-z}}`, `{{A-Z}}`
    /// - Numbers: `{{1-9}}`, `{{0-100}}`
    ///
    /// Patterns are expanded breadth-first across all input strings up to `max_expansions`.
    ///
    /// # Errors
    /// - `Error::EmptyPatternList` if patterns is empty
    /// - `Error::InvalidMaxExpansions` if `max_expansions` is 0
    /// - `Error::InvalidPattern` if a pattern has mixed types (e.g., `{{a-5}}`)
    /// - `Error::MalformedPattern` if a pattern has invalid syntax (e.g., `{{abc}}`, `{{z-a}}`)
    /// - `Error::EmptyExpansion` if pattern expansion resulted in empty list
    pub(crate) fn new_with_patterns(
        patterns: &[String],
        max_expansions: usize,
    ) -> Result<Self, Error> {
        if patterns.is_empty() {
            return Err(Error::EmptyPatternList);
        }
        if max_expansions == 0 {
            return Err(Error::InvalidMaxExpansions);
        }

        // Validate all patterns first
        for pattern in patterns {
            Self::validate_patterns(pattern)?;
        }

        let metric_names = Self::expand_patterns_breadth_first(patterns, max_expansions);
        if metric_names.is_empty() {
            return Err(Error::EmptyExpansion);
        }
        Ok(Self { metric_names })
    }

    /// Validate all patterns in a string for correctness
    /// Returns an error if any pattern is invalid (mixed types like {{a-5}})
    fn validate_patterns(input: &str) -> Result<(), Error> {
        let bytes = input.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            // Look for {{
            if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
                let start = i;
                i += 2;

                // Find the matching }}
                let mut pattern_end = None;
                while i + 1 < bytes.len() {
                    if bytes[i] == b'}' && bytes[i + 1] == b'}' {
                        pattern_end = Some(i);
                        break;
                    }
                    i += 1;
                }

                if let Some(end_pos) = pattern_end {
                    // Extract the pattern content between {{ and }}
                    let pattern_str = &input[start + 2..end_pos];
                    Self::validate_range(pattern_str)?;
                    i = end_pos + 2;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(())
    }

    /// Validate a range string like "a-z" or "1-10"
    /// Returns error if the range has mixed types (char and number) or is malformed
    fn validate_range(range_str: &str) -> Result<(), Error> {
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            return Err(Error::MalformedPattern {
                pattern: format!("{{{{{range_str}}}}}"),
            });
        }

        let start = parts[0].trim();
        let end = parts[1].trim();

        if start.is_empty() || end.is_empty() {
            return Err(Error::MalformedPattern {
                pattern: format!("{{{{{start}-{end}}}}}"),
            });
        }

        let start_is_num = start.parse::<u32>().is_ok();
        let end_is_num = end.parse::<u32>().is_ok();
        let start_is_char = start.len() == 1 && !start_is_num;
        let end_is_char = end.len() == 1 && !end_is_num;

        // Check for mixed types
        if (start_is_num && end_is_char) || (start_is_char && end_is_num) {
            return Err(Error::InvalidPattern {
                pattern: format!("{{{{{start}-{end}}}}}"),
            });
        }

        // Check if both are neither valid chars nor valid numbers
        if !start_is_num && !start_is_char {
            return Err(Error::MalformedPattern {
                pattern: format!("{{{{{start}-{end}}}}}"),
            });
        }
        if !end_is_num && !end_is_char {
            return Err(Error::MalformedPattern {
                pattern: format!("{{{{{start}-{end}}}}}"),
            });
        }

        // Validate that ranges are not backwards
        if start_is_num && end_is_num {
            let start_num = start
                .parse::<u32>()
                .expect("start_is_num guarantees this parses");
            let end_num = end
                .parse::<u32>()
                .expect("end_is_num guarantees this parses");
            if start_num > end_num {
                return Err(Error::MalformedPattern {
                    pattern: format!("{{{{{start}-{end}}}}}"),
                });
            }
        } else if start_is_char && end_is_char {
            let start_char = start
                .chars()
                .next()
                .expect("start_is_char guarantees non-empty");
            let end_char = end
                .chars()
                .next()
                .expect("end_is_char guarantees non-empty");
            if start_char > end_char {
                return Err(Error::MalformedPattern {
                    pattern: format!("{{{{{start}-{end}}}}}"),
                });
            }
        }

        Ok(())
    }

    /// Parse a string to find all pattern ranges
    fn parse_patterns(input: &str) -> Vec<Pattern> {
        let mut patterns = Vec::new();
        let bytes = input.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            // Look for {{
            if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
                let start = i;
                i += 2;

                // Find the matching }}
                let mut pattern_end = None;
                while i + 1 < bytes.len() {
                    if bytes[i] == b'}' && bytes[i + 1] == b'}' {
                        pattern_end = Some(i);
                        break;
                    }
                    i += 1;
                }

                if let Some(end_pos) = pattern_end {
                    // Extract the pattern content between {{ and }}
                    let pattern_str = &input[start + 2..end_pos];
                    if let Some(range) = Self::parse_range(pattern_str) {
                        patterns.push(Pattern {
                            start,
                            end: end_pos + 2,
                            range,
                        });
                    }
                    i = end_pos + 2;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        patterns
    }

    /// Parse a range string like "a-z" or "1-10"
    fn parse_range(range_str: &str) -> Option<PatternRange> {
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            return None;
        }

        let start = parts[0].trim();
        let end = parts[1].trim();

        // Try parsing as numbers first (more specific)
        if let (Ok(start_num), Ok(end_num)) = (start.parse::<u32>(), end.parse::<u32>())
            && start_num <= end_num
        {
            return Some(PatternRange::Number(start_num, end_num));
        }

        // Try parsing as characters
        if start.len() == 1 && end.len() == 1 {
            let start_char = start.chars().next()?;
            let end_char = end.chars().next()?;
            if start_char <= end_char {
                return Some(PatternRange::Char(start_char, end_char));
            }
        }

        None
    }

    /// Expand a single string with all its patterns
    fn expand_single_pattern(input: &str) -> Vec<String> {
        let patterns = Self::parse_patterns(input);
        if patterns.is_empty() {
            return vec![input.to_string()];
        }

        // Generate all combinations
        let mut results = vec![String::new()];
        let mut last_end = 0;

        for pattern in &patterns {
            // Add the literal part before this pattern
            let literal = &input[last_end..pattern.start];
            for result in &mut results {
                result.push_str(literal);
            }

            // Expand this pattern
            let expansions = Self::expand_range(&pattern.range);
            let mut new_results = Vec::new();
            for result in &results {
                for expansion in &expansions {
                    let mut new_result = result.clone();
                    new_result.push_str(expansion);
                    new_results.push(new_result);
                }
            }
            results = new_results;

            last_end = pattern.end;
        }

        // Add any remaining literal part
        let remaining = &input[last_end..];
        for result in &mut results {
            result.push_str(remaining);
        }

        results
    }

    /// Expand a single pattern range into all its values
    fn expand_range(range: &PatternRange) -> Vec<String> {
        match range {
            PatternRange::Char(start, end) => (*start..=*end).map(|c| c.to_string()).collect(),
            PatternRange::Number(start, end) => (*start..=*end).map(|n| n.to_string()).collect(),
        }
    }

    /// Expand patterns breadth-first across all input strings
    fn expand_patterns_breadth_first(patterns: &[String], max_expansions: usize) -> Vec<String> {
        // First, expand each pattern string to get all possible expansions
        let expansions: Vec<Vec<String>> = patterns
            .iter()
            .map(|p| Self::expand_single_pattern(p))
            .collect();

        // Now interleave breadth-first
        let mut result = Vec::new();
        let mut indices = vec![0; expansions.len()];
        let mut all_done = false;

        while !all_done && result.len() < max_expansions {
            all_done = true;
            for (i, expansion_list) in expansions.iter().enumerate() {
                if indices[i] < expansion_list.len() {
                    result.push(expansion_list[indices[i]].clone());
                    indices[i] += 1;
                    all_done = false;

                    if result.len() >= max_expansions {
                        break;
                    }
                }
            }
        }

        // If no patterns were found and we didn't generate anything, return originals
        if result.is_empty() {
            patterns.to_vec()
        } else {
            result
        }
    }
}

impl Pool for StringListPool {
    fn of_size_with_handle<'a, R>(&'a self, rng: &mut R, _bytes: usize) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized,
    {
        let idx: usize = rng.random_range(0..self.metric_names.len());
        Some((&self.metric_names[idx], Handle::Index(idx)))
    }

    fn using_handle(&self, handle: Handle) -> Option<&str> {
        let idx = handle
            .as_index()
            .expect("handle for string list pool should be a string pool handle");
        self.metric_names.get(idx).map(String::as_str)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::{Error, PatternRange, Pool, StringListPool};
    use rand::{SeedableRng, rngs::SmallRng};

    // Ensure that no returned string ever has a non-alphabet character.
    proptest! {
        #[test]
        fn string_is_in_list(seed: u64, mut names: Vec<String> ) {
            // Make sure the list is never empty.
            names.push("thing".to_string());

            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = StringListPool::new(names.clone());
            if let Some((s1, h)) = pool.of_size_with_handle(&mut rng, 0) {
                if let Some(s2) = pool.using_handle(h) {
                    assert_eq!(s1, s2);
                    assert!(names.iter().any(|s| s == s2));
                } else {
                    panic!("could not get string with handle");
                }
            } else {
                panic!("could not get handle from pool");
            }
        }
    }

    // Pattern parsing tests
    #[test]
    fn parse_range_char_lowercase() {
        let range = StringListPool::parse_range("a-c");
        assert_eq!(range, Some(PatternRange::Char('a', 'c')));
    }

    #[test]
    fn parse_range_char_uppercase() {
        let range = StringListPool::parse_range("A-Z");
        assert_eq!(range, Some(PatternRange::Char('A', 'Z')));
    }

    #[test]
    fn parse_range_number() {
        let range = StringListPool::parse_range("1-10");
        assert_eq!(range, Some(PatternRange::Number(1, 10)));
    }

    #[test]
    fn parse_range_number_zero() {
        let range = StringListPool::parse_range("0-5");
        assert_eq!(range, Some(PatternRange::Number(0, 5)));
    }

    #[test]
    fn parse_range_invalid_backwards() {
        let range = StringListPool::parse_range("z-a");
        assert_eq!(range, None);
    }

    #[test]
    fn parse_range_invalid_format() {
        let range = StringListPool::parse_range("abc");
        assert_eq!(range, None);
    }

    #[test]
    fn parse_patterns_single() {
        let patterns = StringListPool::parse_patterns("metric_{{a-c}}_value");
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].start, 7);
        assert_eq!(patterns[0].end, 14);
        assert_eq!(patterns[0].range, PatternRange::Char('a', 'c'));
    }

    #[test]
    fn parse_patterns_multiple() {
        let patterns = StringListPool::parse_patterns("{{a-c}}_{{1-3}}");
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0].range, PatternRange::Char('a', 'c'));
        assert_eq!(patterns[1].range, PatternRange::Number(1, 3));
    }

    #[test]
    fn parse_patterns_none() {
        let patterns = StringListPool::parse_patterns("no_patterns_here");
        assert_eq!(patterns.len(), 0);
    }

    // Pattern expansion tests
    #[test]
    fn expand_single_pattern_char() {
        let result = StringListPool::expand_single_pattern("metric_{{a-c}}");
        assert_eq!(result, vec!["metric_a", "metric_b", "metric_c"]);
    }

    #[test]
    fn expand_single_pattern_number() {
        let result = StringListPool::expand_single_pattern("value_{{1-3}}");
        assert_eq!(result, vec!["value_1", "value_2", "value_3"]);
    }

    #[test]
    fn expand_single_pattern_multiple() {
        let result = StringListPool::expand_single_pattern("{{a-b}}_{{1-2}}");
        assert_eq!(result, vec!["a_1", "a_2", "b_1", "b_2"]);
    }

    #[test]
    fn expand_single_pattern_no_pattern() {
        let result = StringListPool::expand_single_pattern("no_pattern");
        assert_eq!(result, vec!["no_pattern"]);
    }

    #[test]
    fn expand_single_pattern_complex() {
        let result = StringListPool::expand_single_pattern("prefix_{{x-y}}_middle_{{0-1}}_suffix");
        assert_eq!(
            result,
            vec![
                "prefix_x_middle_0_suffix",
                "prefix_x_middle_1_suffix",
                "prefix_y_middle_0_suffix",
                "prefix_y_middle_1_suffix"
            ]
        );
    }

    // Breadth-first expansion tests
    #[test]
    fn breadth_first_single_string() {
        let patterns = vec!["metric_{{a-c}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100);
        assert_eq!(result, vec!["metric_a", "metric_b", "metric_c"]);
    }

    #[test]
    fn breadth_first_multiple_strings() {
        let patterns = vec!["first_{{a-c}}".to_string(), "second_{{x-z}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100);
        // Should interleave: first_a, second_x, first_b, second_y, first_c, second_z
        assert_eq!(
            result,
            vec![
                "first_a", "second_x", "first_b", "second_y", "first_c", "second_z"
            ]
        );
    }

    #[test]
    fn breadth_first_with_max_limit() {
        let patterns = vec!["first_{{a-z}}".to_string(), "second_{{0-9}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 5);
        assert_eq!(result.len(), 5);
        // Should be: first_a, second_0, first_b, second_1, first_c
        assert_eq!(
            result,
            vec!["first_a", "second_0", "first_b", "second_1", "first_c"]
        );
    }

    #[test]
    fn breadth_first_uneven_expansions() {
        let patterns = vec!["short_{{a-b}}".to_string(), "long_{{0-5}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100);
        // Should interleave, but short runs out first
        assert_eq!(
            result,
            vec![
                "short_a", "long_0", "short_b", "long_1", "long_2", "long_3", "long_4", "long_5"
            ]
        );
    }

    #[test]
    fn breadth_first_no_patterns() {
        let patterns = vec!["no_pattern_1".to_string(), "no_pattern_2".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100);
        assert_eq!(result, vec!["no_pattern_1", "no_pattern_2"]);
    }

    #[test]
    fn breadth_first_max_smaller_than_patterns() {
        let patterns = vec!["first_{{a-z}}".to_string(), "second_{{0-9}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result, vec!["first_a", "second_0", "first_b"]);
    }

    // Integration tests with new_with_patterns
    #[test]
    fn new_with_patterns_simple() {
        let pool = StringListPool::new_with_patterns(&vec!["metric_{{a-c}}".to_string()], 100)
            .expect("should create pool");
        assert_eq!(pool.metric_names.len(), 3);
        assert!(pool.metric_names.contains(&"metric_a".to_string()));
        assert!(pool.metric_names.contains(&"metric_b".to_string()));
        assert!(pool.metric_names.contains(&"metric_c".to_string()));
    }

    #[test]
    fn new_with_patterns_multiple() {
        let pool = StringListPool::new_with_patterns(
            &vec!["first_{{a-b}}".to_string(), "second_{{1-2}}".to_string()],
            100,
        )
        .expect("should create pool");
        assert_eq!(pool.metric_names.len(), 4);
        assert_eq!(
            pool.metric_names,
            vec!["first_a", "second_1", "first_b", "second_2"]
        );
    }

    #[test]
    fn new_with_patterns_respects_max() {
        let pool = StringListPool::new_with_patterns(&vec!["metric_{{a-z}}".to_string()], 5)
            .expect("should create pool");
        assert_eq!(pool.metric_names.len(), 5);
    }

    #[test]
    fn new_with_patterns_number_range() {
        let pool = StringListPool::new_with_patterns(&vec!["counter_{{0-3}}".to_string()], 100)
            .expect("should create pool");
        assert_eq!(
            pool.metric_names,
            vec!["counter_0", "counter_1", "counter_2", "counter_3"]
        );
    }

    #[test]
    fn new_with_patterns_multiple_in_string() {
        let pool = StringListPool::new_with_patterns(&vec!["{{a-b}}_{{1-2}}".to_string()], 100)
            .expect("should create pool");
        assert_eq!(pool.metric_names, vec!["a_1", "a_2", "b_1", "b_2"]);
    }

    #[test]
    fn new_with_patterns_mixed() {
        let pool = StringListPool::new_with_patterns(
            &vec!["no_pattern".to_string(), "with_{{a-b}}".to_string()],
            100,
        )
        .expect("should create pool");
        assert_eq!(pool.metric_names, vec!["no_pattern", "with_a", "with_b"]);
    }

    #[test]
    fn new_with_patterns_empty_errors() {
        let result = StringListPool::new_with_patterns(&vec![], 100);
        assert!(matches!(result, Err(Error::EmptyPatternList)));
    }

    #[test]
    fn new_with_patterns_zero_max_errors() {
        let result = StringListPool::new_with_patterns(&vec!["test".to_string()], 0);
        assert!(matches!(result, Err(Error::InvalidMaxExpansions)));
    }

    // Invalid pattern tests
    #[test]
    fn invalid_pattern_char_to_number() {
        let result = StringListPool::new_with_patterns(&vec!["metric_{{a-5}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
        if let Err(Error::InvalidPattern { pattern }) = result {
            assert_eq!(pattern, "{{a-5}}");
        }
    }

    #[test]
    fn invalid_pattern_number_to_char() {
        let result = StringListPool::new_with_patterns(&vec!["metric_{{1-z}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
        if let Err(Error::InvalidPattern { pattern }) = result {
            assert_eq!(pattern, "{{1-z}}");
        }
    }

    #[test]
    fn invalid_pattern_mixed_in_multiple() {
        let result = StringListPool::new_with_patterns(
            &vec!["valid_{{a-c}}".to_string(), "invalid_{{x-9}}".to_string()],
            100,
        );
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn valid_patterns_pass_validation() {
        let result = StringListPool::new_with_patterns(
            &vec!["chars_{{a-z}}".to_string(), "nums_{{0-9}}".to_string()],
            100,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_pattern_uppercase_to_number() {
        let result = StringListPool::new_with_patterns(&vec!["{{A-5}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn invalid_pattern_number_to_uppercase() {
        let result = StringListPool::new_with_patterns(&vec!["{{0-Z}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn multiple_invalid_patterns() {
        let result = StringListPool::new_with_patterns(&vec!["{{a-1}}_{{2-b}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn valid_single_char_patterns() {
        // Single digit that is treated as a char should not error if both sides are chars
        let result = StringListPool::new_with_patterns(&vec!["{{a-c}}".to_string()], 100);
        assert!(result.is_ok());
    }

    #[test]
    fn malformed_pattern_no_dash() {
        // Patterns without proper dashes should error
        let result = StringListPool::new_with_patterns(&vec!["{{abc}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_empty_parts() {
        let result = StringListPool::new_with_patterns(&vec!["{{}}-{{}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_backwards_char() {
        let result = StringListPool::new_with_patterns(&vec!["{{z-a}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_backwards_number() {
        let result = StringListPool::new_with_patterns(&vec!["{{10-5}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_multi_char_start() {
        let result = StringListPool::new_with_patterns(&vec!["{{abc-z}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_multi_char_end() {
        let result = StringListPool::new_with_patterns(&vec!["{{a-xyz}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_too_many_dashes() {
        let result = StringListPool::new_with_patterns(&vec!["{{a-b-c}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_special_chars() {
        let result = StringListPool::new_with_patterns(&vec!["{{@-#}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    // Test that expanded patterns work with pool operations
    #[test]
    fn expanded_patterns_work_with_pool() {
        let pool = StringListPool::new_with_patterns(&vec!["metric_{{a-c}}".to_string()], 100)
            .expect("should create pool");
        let mut rng = SmallRng::seed_from_u64(42);

        // Get a string from the pool
        if let Some((s, h)) = pool.of_size_with_handle(&mut rng, 0) {
            assert!(s.starts_with("metric_"));
            assert!(s == "metric_a" || s == "metric_b" || s == "metric_c");

            // Verify handle works
            let s2 = pool.using_handle(h).expect("handle should be valid");
            assert_eq!(s, s2);
        } else {
            panic!("should get a string from pool");
        }
    }

    // Edge case tests
    #[test]
    fn pattern_with_single_value() {
        let result = StringListPool::expand_single_pattern("{{a-a}}");
        assert_eq!(result, vec!["a"]);
    }

    #[test]
    fn pattern_uppercase_range() {
        let result = StringListPool::expand_single_pattern("{{A-C}}");
        assert_eq!(result, vec!["A", "B", "C"]);
    }

    #[test]
    fn pattern_large_number_range() {
        let result = StringListPool::expand_single_pattern("{{10-12}}");
        assert_eq!(result, vec!["10", "11", "12"]);
    }

    #[test]
    fn pattern_adjacent_patterns() {
        let result = StringListPool::expand_single_pattern("{{a-b}}{{1-2}}");
        assert_eq!(result, vec!["a1", "a2", "b1", "b2"]);
    }

    #[test]
    fn pattern_with_special_chars() {
        let result = StringListPool::expand_single_pattern("metric-{{a-b}}.value");
        assert_eq!(result, vec!["metric-a.value", "metric-b.value"]);
    }
}
