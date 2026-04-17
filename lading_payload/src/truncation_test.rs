//! Truncation test payload.
//!
//! Generates log lines with embedded metadata headers to test log truncation
//! behavior in a downstream system. Each line has the form:
//!
//! ```text
//! [cat=<name> id=<id> size=<size>] <padding>\n
//! ```
//!
//! Where `size` is the **total** line length on disk (header + padding + `\n`).
//!
//! Categories are weighted; each category defines a size range. The generator
//! picks a category, then a specific size within that category's range.
//! Oversized lines exist to trigger the downstream system's truncation logic.
//!
//! # Constraint
//!
//! A single line must fit within one block. Callers must configure
//! `maximum_block_size` to be at least as large as the largest `size_range`
//! value configured for any category.

use std::io::Write;

use rand::{
    Rng,
    distr::{Distribution, weighted::WeightedIndex},
};
use serde::{Deserialize, Serialize as SerdeSerialize};

use crate::Error;

/// Format of the generated lines. Currently only `simple` is supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, SerdeSerialize, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Format {
    /// `[cat=<name> id=<id> size=<size>] <padding>\n`
    #[default]
    Simple,
}

/// A single category in the generator configuration.
#[derive(Debug, Clone, Deserialize, SerdeSerialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Category {
    /// Short name — appears in the line header and is used by the analysis
    /// tool to determine expected behavior.
    pub name: String,
    /// Relative weight when selecting which category a line belongs to.
    pub weight: u32,
    /// Inclusive range `[min, max]` of total line sizes (bytes) for lines
    /// in this category.
    pub size_range: [u64; 2],
}

/// Configuration for the `TruncationTest` payload.
#[derive(Debug, Clone, Deserialize, SerdeSerialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Config {
    /// Line format. See [`Format`].
    #[serde(default)]
    pub format: Format,
    /// Weighted categories.
    pub categories: Vec<Category>,
}

/// Minimum viable line size. The header needs at least this many bytes to
/// encode `[cat=X id=00000000 size=XX]` plus one char of padding plus the
/// newline. We enforce a floor of 64 bytes on any category's `size_range` to
/// guarantee the header fits comfortably.
const MIN_LINE_SIZE: u64 = 64;

/// Alphanumeric characters used for padding. Chosen to avoid characters that
/// might have special meaning in downstream log parsers (`{`, `}`, `"`, etc.).
const PADDING_CHARSET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// The `TruncationTest` payload generator.
pub struct TruncationTest {
    categories: Vec<Category>,
    weights: WeightedIndex<u32>,
    /// Monotonically increasing line ID, used to make each line unique within
    /// a single generator run.
    next_id: u64,
}

impl std::fmt::Debug for TruncationTest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TruncationTest")
            .field("categories", &self.categories.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl TruncationTest {
    /// Construct a new `TruncationTest` generator from a config.
    ///
    /// # Errors
    ///
    /// Returns an error if the config has no categories, any category has a
    /// zero weight, or any category's `size_range` is inverted or below the
    /// [`MIN_LINE_SIZE`] floor.
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.categories.is_empty() {
            return Err(Error::Validation(
                "truncation_test: at least one category is required".into(),
            ));
        }

        for cat in &config.categories {
            if cat.weight == 0 {
                return Err(Error::Validation(format!(
                    "truncation_test: category '{}' has zero weight",
                    cat.name
                )));
            }
            if cat.size_range[0] > cat.size_range[1] {
                return Err(Error::Validation(format!(
                    "truncation_test: category '{}' has inverted size_range [{}, {}]",
                    cat.name, cat.size_range[0], cat.size_range[1],
                )));
            }
            if cat.size_range[0] < MIN_LINE_SIZE {
                return Err(Error::Validation(format!(
                    "truncation_test: category '{}' min size {} is below floor {}",
                    cat.name, cat.size_range[0], MIN_LINE_SIZE,
                )));
            }
        }

        let weights: Vec<u32> = config.categories.iter().map(|c| c.weight).collect();
        let weighted = WeightedIndex::new(&weights)
            .map_err(|e| Error::Validation(format!("truncation_test: weights: {e}")))?;

        Ok(Self {
            categories: config.categories,
            weights: weighted,
            next_id: 0,
        })
    }

    /// Pick a category that can fit at least `MIN_LINE_SIZE` bytes within the
    /// remaining budget, falling back to any category if none fit.
    ///
    /// Returns `None` if no category can fit.
    fn pick_category<R: Rng>(&self, rng: &mut R, remaining: u64) -> Option<&Category> {
        if remaining < MIN_LINE_SIZE {
            return None;
        }

        // First try the weighted pick; if it fits, use it.
        let idx = self.weights.sample(rng);
        let cat = &self.categories[idx];
        if cat.size_range[0] <= remaining {
            return Some(cat);
        }

        // The weighted pick didn't fit. Fall back to any category whose
        // minimum size fits. Collect and pick uniformly among those.
        let candidates: Vec<&Category> = self
            .categories
            .iter()
            .filter(|c| c.size_range[0] <= remaining)
            .collect();
        if candidates.is_empty() {
            None
        } else {
            let i = rng.random_range(0..candidates.len());
            Some(candidates[i])
        }
    }

    /// Generate one line into `writer`, consuming exactly `size` bytes.
    fn write_line<W: Write, R: Rng>(
        writer: &mut W,
        rng: &mut R,
        cat_name: &str,
        id: u64,
        size: u64,
    ) -> Result<(), Error> {
        // Header: [cat=<name> id=<08d> size=<size>] <padding>\n
        // Build it as a string first so we can measure its length.
        let header = format!("[cat={cat_name} id={id:08} size={size}] ");
        let header_bytes = header.as_bytes();

        // Size accounting: header + padding + '\n' == size
        // So: padding_len = size - header.len() - 1
        let size_usize = usize::try_from(size).map_err(|_| Error::StringGenerate)?;
        if size_usize < header_bytes.len() + 1 {
            // Should never happen given MIN_LINE_SIZE floor, but guard anyway.
            return Err(Error::Validation(format!(
                "truncation_test: size {size} too small for header (header len {}, need +1 for newline)",
                header_bytes.len()
            )));
        }
        let padding_len = size_usize - header_bytes.len() - 1;

        writer.write_all(header_bytes)?;

        // Write padding in chunks to avoid allocating huge intermediate buffers
        // for multi-MB lines.
        let chunk = {
            let mut buf = [0u8; 4096];
            for b in &mut buf {
                *b = PADDING_CHARSET[rng.random_range(0..PADDING_CHARSET.len())];
            }
            buf
        };
        let mut written = 0;
        while written < padding_len {
            let remaining = padding_len - written;
            let n = remaining.min(chunk.len());
            writer.write_all(&chunk[..n])?;
            written += n;
        }

        writer.write_all(b"\n")?;
        Ok(())
    }
}

impl crate::Serialize for TruncationTest {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut remaining = u64::try_from(max_bytes).unwrap_or(u64::MAX);

        loop {
            // Scope the borrow so we can mutate self.next_id below.
            let (cat_name, size) = {
                let Some(cat) = self.pick_category(&mut rng, remaining) else {
                    break;
                };
                // Clamp size_range to what fits in the remaining budget.
                let lo = cat.size_range[0];
                let hi = cat.size_range[1].min(remaining);
                let size = if lo == hi {
                    lo
                } else {
                    rng.random_range(lo..=hi)
                };
                (cat.name.clone(), size)
            };

            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);

            Self::write_line(writer, &mut rng, &cat_name, id, size)?;

            remaining = remaining.saturating_sub(size);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Serialize;
    use rand::{SeedableRng, rngs::SmallRng};

    fn small_config() -> Config {
        Config {
            format: Format::Simple,
            categories: vec![
                Category {
                    name: "normal".into(),
                    weight: 80,
                    size_range: [100, 200],
                },
                Category {
                    name: "big".into(),
                    weight: 20,
                    size_range: [500, 600],
                },
            ],
        }
    }

    #[test]
    fn rejects_empty_categories() {
        let config = Config {
            format: Format::Simple,
            categories: vec![],
        };
        assert!(TruncationTest::new(config).is_err());
    }

    #[test]
    fn rejects_zero_weight() {
        let config = Config {
            format: Format::Simple,
            categories: vec![Category {
                name: "x".into(),
                weight: 0,
                size_range: [100, 200],
            }],
        };
        assert!(TruncationTest::new(config).is_err());
    }

    #[test]
    fn rejects_inverted_range() {
        let config = Config {
            format: Format::Simple,
            categories: vec![Category {
                name: "x".into(),
                weight: 1,
                size_range: [200, 100],
            }],
        };
        assert!(TruncationTest::new(config).is_err());
    }

    #[test]
    fn rejects_below_min_size() {
        let config = Config {
            format: Format::Simple,
            categories: vec![Category {
                name: "x".into(),
                weight: 1,
                size_range: [32, 200],
            }],
        };
        assert!(TruncationTest::new(config).is_err());
    }

    #[test]
    fn lines_match_target_size() {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut gener = TruncationTest::new(small_config()).unwrap();
        let mut buf = Vec::new();
        gener.to_bytes(&mut rng, 10_000, &mut buf).unwrap();
        assert!(buf.len() <= 10_000);

        // Each line must end with '\n' and the header's `size` field must
        // match the total line length.
        let text = String::from_utf8(buf).unwrap();
        for line in text.lines() {
            // "line" does not include the trailing '\n' — but the header's
            // size does, so compare line.len() + 1 to the header's size.
            let actual_size = line.len() + 1;
            // Parse header: [cat=X id=Y size=Z]
            let header_end = line.find(']').expect("header must be terminated by ]");
            let header = &line[..=header_end];
            let size_idx = header.find("size=").expect("header must have size=");
            let size_str = &header[size_idx + "size=".len()..header.len() - 1];
            let claimed_size: usize = size_str.parse().expect("size must parse");
            assert_eq!(
                claimed_size, actual_size,
                "claimed size {claimed_size} does not match actual {actual_size} for line {line:?}"
            );
        }
    }

    #[test]
    fn ids_are_unique_and_monotonic() {
        let mut rng = SmallRng::seed_from_u64(7);
        let mut gener = TruncationTest::new(small_config()).unwrap();
        let mut buf = Vec::new();
        gener.to_bytes(&mut rng, 10_000, &mut buf).unwrap();

        let text = String::from_utf8(buf).unwrap();
        let mut last_id: Option<u64> = None;
        for line in text.lines() {
            let header_end = line.find(']').unwrap();
            let header = &line[..=header_end];
            let id_idx = header.find("id=").unwrap();
            let rest = &header[id_idx + "id=".len()..];
            let space = rest.find(' ').unwrap();
            let id: u64 = rest[..space].parse().unwrap();
            if let Some(prev) = last_id {
                assert!(id > prev, "ids must be strictly increasing: {prev} then {id}");
            }
            last_id = Some(id);
        }
    }

    #[test]
    fn respects_max_bytes() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut gener = TruncationTest::new(small_config()).unwrap();
        for max in [64usize, 128, 500, 1000, 10_000] {
            let mut buf = Vec::new();
            gener.to_bytes(&mut rng, max, &mut buf).unwrap();
            assert!(buf.len() <= max, "produced {} > max {}", buf.len(), max);
        }
    }
}
