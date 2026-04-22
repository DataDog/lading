use super::{Handle, Pool};

/// Error type for `StringListPool` pattern expansion
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Error {
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
    /// Numeric range cardinality overflows usize (too large for this platform)
    #[error(
        "Numeric range {{0-{end}}} has {len} values, which exceeds usize::MAX on this platform"
    )]
    RangeTooLarge {
        /// The end of the offending range
        end: u32,
        /// The computed 64-bit cardinality
        len: u64,
    },
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
    pub(crate) fn new(patterns: &[String], max_expansions: usize) -> Result<Self, Error> {
        if patterns.is_empty() {
            return Err(Error::EmptyPatternList);
        }
        if max_expansions == 0 {
            return Err(Error::InvalidMaxExpansions);
        }

        let metric_names = Self::expand_patterns_breadth_first(patterns, max_expansions)?;
        if metric_names.is_empty() {
            return Err(Error::EmptyExpansion);
        }
        Ok(Self { metric_names })
    }

    /// Expand patterns breadth-first across all input strings, generating
    /// expansions lazily to avoid materializing the full Cartesian product.
    fn expand_patterns_breadth_first(
        patterns: &[String],
        max_expansions: usize,
    ) -> Result<Vec<String>, Error> {
        let expanders: Vec<LazyPatternExpander> = patterns
            .iter()
            .map(|p| LazyPatternExpander::new(p))
            .collect::<Result<_, _>>()?;

        let result: Vec<String> = BreadthFirst::new(expanders)
            .take(max_expansions.max(patterns.len()))
            .collect();

        if result.is_empty() {
            Ok(patterns.to_vec())
        } else {
            Ok(result)
        }
    }

    /// Parse a string to find all pattern ranges
    fn parse_patterns(input: &str) -> Result<Vec<Pattern>, Error> {
        let mut patterns = Vec::new();
        for item in PatternIter::new(input) {
            let (pattern_str, start, end) = item?;
            Self::validate_range(pattern_str)?;

            if let Some(range) = Self::parse_range(pattern_str) {
                patterns.push(Pattern { start, end, range });
            }
        }

        Ok(patterns)
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
}

/// Return the number of values in a `PatternRange`, or `Err` if the
/// cardinality exceeds `usize::MAX` on this platform.
fn range_len(range: &PatternRange) -> Result<usize, Error> {
    match range {
        PatternRange::Char(start, end) => Ok((*end as u32 - *start as u32 + 1) as usize),
        PatternRange::Number(start, end) => {
            let len: u64 = u64::from(*end) - u64::from(*start) + 1;
            usize::try_from(len).map_err(|_| Error::RangeTooLarge { end: *end, len })
        }
    }
}

/// Return the string value at position `idx` within a `PatternRange`.
fn range_value_at(range: &PatternRange, idx: usize) -> String {
    match range {
        PatternRange::Char(start, _end) => char::from_u32(
            *start as u32 + u32::try_from(idx).expect("pattern shouldn't be that big"),
        )
        .expect("idx is within range bounds")
        .to_string(),
        PatternRange::Number(start, end) => {
            let numdigits = std::cmp::max(start.to_string().len(), end.to_string().len());
            let n = start + u32::try_from(idx).expect("pattern shouldn't be that big");
            format!("{n:0numdigits$}")
        }
    }
}

/// Lazily generates all expansions of a single pattern template string.
///
/// For a template like `"prefix_{{a-c}}_{{1-2}}"` this yields each combination
/// on demand (`prefix_a_1`, `prefix_a_2`, `prefix_b_1`, …) without
/// materialising the full Cartesian product up front.  This keeps memory usage
/// bounded when patterns with large ranges are interleaved by
/// `expand_patterns_breadth_first`.
struct LazyPatternExpander {
    template: String,
    patterns: Vec<Pattern>,
    /// Mixed-radix counter: one digit per placeholder, rightmost varies fastest.
    counters: Vec<usize>,
    /// Cardinality of each placeholder range, computed once at construction.
    range_lens: Vec<usize>,
    exhausted: bool,
}

impl LazyPatternExpander {
    fn new(template: &str) -> Result<Self, Error> {
        let patterns = StringListPool::parse_patterns(template)?;
        let range_lens = patterns
            .iter()
            .map(|p| range_len(&p.range))
            .collect::<Result<Vec<_>, _>>()?;
        let n = patterns.len();
        Ok(Self {
            template: template.to_string(),
            patterns,
            counters: vec![0; n],
            range_lens,
            exhausted: false,
        })
    }

    /// Build the current combination from `self.counters`.
    fn current(&self) -> String {
        let mut result = String::new();
        let mut last_end = 0;
        for (i, pattern) in self.patterns.iter().enumerate() {
            result.push_str(&self.template[last_end..pattern.start]);
            result.push_str(&range_value_at(&pattern.range, self.counters[i]));
            last_end = pattern.end;
        }
        result.push_str(&self.template[last_end..]);
        result
    }

    /// Advance to the next combination (rightmost counter increments first).
    fn advance(&mut self) {
        let n = self.counters.len();
        if n == 0 {
            self.exhausted = true;
            return;
        }
        let mut i = n - 1;
        loop {
            self.counters[i] += 1;
            if self.counters[i] < self.range_lens[i] {
                return;
            }
            self.counters[i] = 0;
            if i == 0 {
                self.exhausted = true;
                return;
            }
            i -= 1;
        }
    }
}

impl Iterator for LazyPatternExpander {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.exhausted {
            return None;
        }
        let s = self.current();
        if self.patterns.is_empty() {
            // Plain literal string — yield once then stop.
            self.exhausted = true;
        } else {
            self.advance();
        }
        Some(s)
    }
}

/// Iterates over a collection of iterators in round-robin (breadth-first) order.
///
/// Given iterators `A = [a1, a2, a3]` and `B = [b1, b2]`, yields:
/// `a1, b1, a2, b2, a3`.
///
/// Exhausted iterators are skipped; iteration ends when all are exhausted.
struct BreadthFirst<I> {
    iterators: Vec<Option<I>>,
    index: usize,
    active: usize,
}

impl<I: Iterator> BreadthFirst<I> {
    fn new(iterators: Vec<I>) -> Self {
        let active = iterators.len();
        Self {
            iterators: iterators.into_iter().map(Some).collect(),
            index: 0,
            active,
        }
    }
}

impl<I: Iterator> Iterator for BreadthFirst<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.active == 0 {
            return None;
        }
        let n = self.iterators.len();
        for _ in 0..n {
            let i = self.index;
            self.index = (self.index + 1) % n;
            if let Some(iter) = &mut self.iterators[i] {
                if let Some(item) = iter.next() {
                    return Some(item);
                }

                self.iterators[i] = None;
                self.active -= 1;
                if self.active == 0 {
                    return None;
                }
            }
        }
        None
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

/// An iterator through the patterns contained in the given string.
struct PatternIter<'a> {
    input: &'a str,
    p: usize,
}

impl<'a> PatternIter<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, p: 0 }
    }
}

impl<'a> Iterator for PatternIter<'a> {
    type Item = Result<(&'a str, usize, usize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.input.as_bytes();

        while self.p + 1 < bytes.len() {
            if bytes[self.p] == b'{' && bytes[self.p + 1] == b'{' {
                let start = self.p;
                self.p += 2;

                let mut pattern_end = None;
                while self.p + 1 < bytes.len() {
                    if bytes[self.p] == b'}' && bytes[self.p + 1] == b'}' {
                        pattern_end = Some(self.p);
                        break;
                    }
                    self.p += 1;
                }

                if let Some(end_pos) = pattern_end {
                    // Extract the pattern content between {{ and }}
                    let pattern_str = &self.input[start + 2..end_pos];
                    self.p = end_pos + 2;

                    return Some(Ok((pattern_str, start, self.p)));
                }

                // Unclosed {{ — no matching }} found; surface as error.
                self.p = bytes.len();
                return Some(Err(Error::MalformedPattern {
                    pattern: self.input[start..].to_string(),
                }));
            }

            self.p += 1;
        }

        None
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::{Error, LazyPatternExpander, PatternRange, Pool, StringListPool};
    use rand::{SeedableRng, rngs::SmallRng};

    // Ensure that no returned string ever has a non-alphabet character.
    proptest! {
        #[test]
        fn string_is_in_list(seed: u64, mut names: Vec<String> ) {
            // Make sure the list is never empty.
            names.push("thing".to_string());

            let mut rng = SmallRng::seed_from_u64(seed);

            // Arbitrary strings may contain malformed patterns (e.g. unclosed
            // `{{`); skip those cases — this test checks the handle round-trip
            // invariant, not error handling.
            let Ok(pool) = StringListPool::new(&names, 10_000) else {
                return Ok(())
            };

            if let Some((s1, h)) = pool.of_size_with_handle(&mut rng, 0) {
                if let Some(s2) = pool.using_handle(h) {
                    prop_assert_eq!(s1, s2);
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
    fn parse_range_multidigit_number() {
        let range = StringListPool::parse_range("102-10234");
        assert_eq!(range, Some(PatternRange::Number(102, 10234)));
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
        let patterns = StringListPool::parse_patterns("metric_{{a-c}}_value").unwrap();
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].start, 7);
        assert_eq!(patterns[0].end, 14);
        assert_eq!(patterns[0].range, PatternRange::Char('a', 'c'));
    }

    #[test]
    fn parse_patterns_multiple() {
        let patterns = StringListPool::parse_patterns("{{a-c}}_{{1-3}}").unwrap();
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0].range, PatternRange::Char('a', 'c'));
        assert_eq!(patterns[1].range, PatternRange::Number(1, 3));
    }

    #[test]
    fn parse_patterns_none() {
        let patterns = StringListPool::parse_patterns("no_patterns_here").unwrap();
        assert_eq!(patterns.len(), 0);
    }

    // Pattern expansion tests
    #[test]
    fn expand_single_pattern_char() {
        let result: Vec<String> = LazyPatternExpander::new("metric_{{a-c}}")
            .unwrap()
            .collect();
        assert_eq!(result, vec!["metric_a", "metric_b", "metric_c"]);
    }

    #[test]
    fn expand_single_pattern_number() {
        let result: Vec<String> = LazyPatternExpander::new("value_{{1-3}}").unwrap().collect();
        assert_eq!(result, vec!["value_1", "value_2", "value_3"]);
    }

    #[test]
    fn expand_single_pattern_multiple() {
        let result: Vec<String> = LazyPatternExpander::new("{{a-b}}_{{1-2}}")
            .unwrap()
            .collect();
        assert_eq!(result, vec!["a_1", "a_2", "b_1", "b_2"]);
    }

    #[test]
    fn expand_single_pattern_no_pattern() {
        let result: Vec<String> = LazyPatternExpander::new("no_pattern").unwrap().collect();
        assert_eq!(result, vec!["no_pattern"]);
    }

    #[test]
    fn expand_single_pattern_complex() {
        let result: Vec<String> = LazyPatternExpander::new("prefix_{{x-y}}_middle_{{0-1}}_suffix")
            .unwrap()
            .collect();
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
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100).unwrap();
        assert_eq!(result, vec!["metric_a", "metric_b", "metric_c"]);
    }

    #[test]
    fn breadth_first_multiple_strings() {
        let patterns = vec!["first_{{a-c}}".to_string(), "second_{{x-z}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100).unwrap();
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
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 5).unwrap();
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
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100).unwrap();
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
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 100).unwrap();
        assert_eq!(result, vec!["no_pattern_1", "no_pattern_2"]);
    }

    #[test]
    fn breadth_first_max_smaller_than_patterns() {
        let patterns = vec!["first_{{a-z}}".to_string(), "second_{{0-9}}".to_string()];
        let result = StringListPool::expand_patterns_breadth_first(&patterns, 3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result, vec!["first_a", "second_0", "first_b"]);
    }

    // Integration tests with new
    #[test]
    fn new_simple() {
        let pool =
            StringListPool::new(&["metric_{{a-c}}".to_string()], 100).expect("should create pool");
        assert_eq!(pool.metric_names.len(), 3);
        assert!(pool.metric_names.contains(&"metric_a".to_string()));
        assert!(pool.metric_names.contains(&"metric_b".to_string()));
        assert!(pool.metric_names.contains(&"metric_c".to_string()));
    }

    #[test]
    fn new_multiple() {
        let pool = StringListPool::new(
            &["first_{{a-b}}".to_string(), "second_{{1-2}}".to_string()],
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
    fn new_respects_max() {
        let pool =
            StringListPool::new(&["metric_{{a-z}}".to_string()], 5).expect("should create pool");
        assert_eq!(pool.metric_names.len(), 5);
    }

    #[test]
    fn new_number_range() {
        let pool =
            StringListPool::new(&["counter_{{0-3}}".to_string()], 100).expect("should create pool");
        assert_eq!(
            pool.metric_names,
            vec!["counter_0", "counter_1", "counter_2", "counter_3"]
        );
    }

    #[test]
    fn new_multiple_in_string() {
        let pool =
            StringListPool::new(&["{{a-b}}_{{1-2}}".to_string()], 100).expect("should create pool");
        assert_eq!(pool.metric_names, vec!["a_1", "a_2", "b_1", "b_2"]);
    }

    #[test]
    fn new_mixed() {
        let pool =
            StringListPool::new(&["no_pattern".to_string(), "with_{{a-b}}".to_string()], 100)
                .expect("should create pool");
        assert_eq!(pool.metric_names, vec!["no_pattern", "with_a", "with_b"]);
    }

    #[test]
    fn new_empty_errors() {
        let result = StringListPool::new(&[], 100);
        assert!(matches!(result, Err(Error::EmptyPatternList)));
    }

    #[test]
    fn new_zero_max_errors() {
        let result = StringListPool::new(&["test".to_string()], 0);
        assert!(matches!(result, Err(Error::InvalidMaxExpansions)));
    }

    // Invalid pattern tests
    #[test]
    fn invalid_pattern_char_to_number() {
        let result = StringListPool::new(&["metric_{{a-5}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
        if let Err(Error::InvalidPattern { pattern }) = result {
            assert_eq!(pattern, "{{a-5}}");
        }
    }

    #[test]
    fn invalid_pattern_number_to_char() {
        let result = StringListPool::new(&["metric_{{1-z}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
        if let Err(Error::InvalidPattern { pattern }) = result {
            assert_eq!(pattern, "{{1-z}}");
        }
    }

    #[test]
    fn invalid_pattern_mixed_in_multiple() {
        let result = StringListPool::new(
            &["valid_{{a-c}}".to_string(), "invalid_{{x-9}}".to_string()],
            100,
        );
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn valid_patterns_pass_validation() {
        let result = StringListPool::new(
            &["chars_{{a-z}}".to_string(), "nums_{{0-9}}".to_string()],
            100,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_pattern_uppercase_to_number() {
        let result = StringListPool::new(&["{{A-5}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn invalid_pattern_number_to_uppercase() {
        let result = StringListPool::new(&["{{0-Z}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn multiple_invalid_patterns() {
        let result = StringListPool::new(&["{{a-1}}_{{2-b}}".to_string()], 100);
        assert!(matches!(result, Err(Error::InvalidPattern { .. })));
    }

    #[test]
    fn valid_single_char_patterns() {
        // Single digit that is treated as a char should not error if both sides are chars
        let result = StringListPool::new(&["{{a-c}}".to_string()], 100);
        assert!(result.is_ok());
    }

    #[test]
    fn malformed_pattern_no_dash() {
        // Patterns without proper dashes should error
        let result = StringListPool::new(&["{{abc}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_empty_parts() {
        let result = StringListPool::new(&["{{}}-{{}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_backwards_char() {
        let result = StringListPool::new(&["{{z-a}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_backwards_number() {
        let result = StringListPool::new(&["{{10-5}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_multi_char_start() {
        let result = StringListPool::new(&["{{abc-z}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_multi_char_end() {
        let result = StringListPool::new(&["{{a-xyz}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_too_many_dashes() {
        let result = StringListPool::new(&["{{a-b-c}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_unclosed_braces() {
        let result = StringListPool::new(&["metric_{{a-z".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_unclosed_braces_mid_string() {
        let result = StringListPool::new(&["prefix_{{a-z_suffix".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    #[test]
    fn malformed_pattern_special_chars() {
        let result = StringListPool::new(&["{{@-#}}".to_string()], 100);
        assert!(matches!(result, Err(Error::MalformedPattern { .. })));
    }

    // Test that expanded patterns work with pool operations
    #[test]
    fn expanded_patterns_work_with_pool() {
        let pool =
            StringListPool::new(&["metric_{{a-c}}".to_string()], 100).expect("should create pool");
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
        let result: Vec<String> = LazyPatternExpander::new("{{a-a}}").unwrap().collect();
        assert_eq!(result, vec!["a"]);
    }

    #[test]
    fn pattern_uppercase_range() {
        let result: Vec<String> = LazyPatternExpander::new("{{A-C}}").unwrap().collect();
        assert_eq!(result, vec!["A", "B", "C"]);
    }

    #[test]
    fn pattern_large_number_range() {
        let result: Vec<String> = LazyPatternExpander::new("{{10-12}}").unwrap().collect();
        assert_eq!(result, vec!["10", "11", "12"]);
    }

    #[test]
    fn pattern_large_number_different_lengths_range() {
        let result: Vec<String> = LazyPatternExpander::new("{{8-12}}").unwrap().collect();
        assert_eq!(result, vec!["08", "09", "10", "11", "12"]);
    }

    #[test]
    fn pattern_large_number_different_lengths_huge_range() {
        let result: Vec<String> = LazyPatternExpander::new("{{8-120}}").unwrap().collect();
        assert_eq!(
            result,
            (8..=120).map(|num| format!("{num:03}")).collect::<Vec<_>>()
        );
    }

    #[test]
    fn pattern_adjacent_patterns() {
        let result: Vec<String> = LazyPatternExpander::new("{{a-b}}{{1-2}}")
            .unwrap()
            .collect();
        assert_eq!(result, vec!["a1", "a2", "b1", "b2"]);
    }

    #[test]
    fn pattern_with_special_chars() {
        let result: Vec<String> = LazyPatternExpander::new("metric-{{a-b}}.value")
            .unwrap()
            .collect();
        assert_eq!(result, vec!["metric-a.value", "metric-b.value"]);
    }

    #[test]
    #[cfg(target_pointer_width = "32")]
    fn numeric_range_too_large_for_32bit_fails_at_construction() {
        // 0..=u32::MAX has 2^32 values, which overflows usize on 32-bit.
        let err = LazyPatternExpander::new("{{0-4294967295}}").unwrap_err();
        assert!(matches!(err, Error::RangeTooLarge { .. }));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn numeric_range_full_u32_succeeds_on_64bit() {
        // On 64-bit, 2^32 fits in usize, so construction must not panic or err.
        // We don't collect (4 billion strings), just check new() succeeds.
        assert!(LazyPatternExpander::new("{{0-4294967295}}").is_ok());
    }
}
