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

use crate::{
    Error,
    line_layout::{LineEntry, OPAQUE_TOKEN_WIDTH, SLOT_PLACEHOLDER, SlotFlavor},
};

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
/// encode `[cat=X id=00000000 size=XX u=<26-byte slot>]` plus one char of
/// padding plus the newline. We enforce a floor of 128 bytes on any
/// category's `size_range` to guarantee the header and slot fit
/// comfortably.
const MIN_LINE_SIZE: u64 = 128;

/// Flavor of slot filled at read time by the generator. Truncation testing
/// doesn't benefit from realistic timestamps; opaque tokens are
/// deterministic, unique, and don't interact with the agent's
/// timestamp-based heuristics.
const SLOT_FLAVOR: SlotFlavor = SlotFlavor::OpaqueToken;

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
    /// Per-line layout captured during the most recent `to_bytes` call.
    /// Returned via `Serialize::line_layout` so the generator can rewrite
    /// slots at FUSE read time.
    last_layout: Vec<LineEntry>,
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
            last_layout: Vec::new(),
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
    ///
    /// Returns the byte offset of the slot within the line. The slot is
    /// written as [`SLOT_PLACEHOLDER`] characters at cache-build time and
    /// overwritten with a unique token at FUSE read time by the
    /// generator. The layout this method produces is emitted via
    /// [`Serialize::line_layout`] so the generator knows where to write.
    fn write_line<W: Write, R: Rng>(
        writer: &mut W,
        rng: &mut R,
        cat_name: &str,
        id: u64,
        size: u64,
    ) -> Result<u32, Error> {
        // Header shape: `[cat=<name> id=<08d> size=<size> u=<26-byte slot>] <padding>\n`
        //
        // The slot sits inside the bracketed header so the parsers and the
        // downstream truncation check can skip over it in the usual way.
        // Header-before-slot and header-after-slot are fixed strings; the
        // slot itself is `SLOT_PLACEHOLDER` bytes (filled at read time).
        let prefix = format!("[cat={cat_name} id={id:08} size={size} ");
        let prefix_bytes = prefix.as_bytes();
        let slot_width_usize = OPAQUE_TOKEN_WIDTH as usize;
        // After the slot we close the bracket and emit a single padding-
        // separator space before the random padding begins.
        const SLOT_SUFFIX: &[u8] = b"] ";

        let fixed_header_len = prefix_bytes.len() + slot_width_usize + SLOT_SUFFIX.len();

        let size_usize = usize::try_from(size).map_err(|_| Error::StringGenerate)?;
        if size_usize < fixed_header_len + 1 {
            return Err(Error::Validation(format!(
                "truncation_test: size {size} too small for header+slot (header+slot len {fixed_header_len}, need +1 for newline)"
            )));
        }
        let padding_len = size_usize - fixed_header_len - 1;

        // slot_offset within the line is the number of bytes before the slot
        // starts (i.e. the length of `prefix_bytes`).
        let slot_offset = u32::try_from(prefix_bytes.len()).map_err(|_| Error::StringGenerate)?;

        writer.write_all(prefix_bytes)?;

        // Write the placeholder slot bytes. The value matters only insofar
        // as it's distinctive enough to notice if the rewrite step is
        // missing.
        let slot_placeholder = [SLOT_PLACEHOLDER; 64];
        let mut slot_written = 0;
        while slot_written < slot_width_usize {
            let remaining = slot_width_usize - slot_written;
            let n = remaining.min(slot_placeholder.len());
            writer.write_all(&slot_placeholder[..n])?;
            slot_written += n;
        }

        writer.write_all(SLOT_SUFFIX)?;

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
        Ok(slot_offset)
    }
}

impl crate::Serialize for TruncationTest {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        self.last_layout.clear();

        let mut remaining = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let mut line_start: u32 = 0;

        loop {
            let (cat_name, size) = {
                let Some(cat) = self.pick_category(&mut rng, remaining) else {
                    break;
                };
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

            let slot_offset = Self::write_line(writer, &mut rng, &cat_name, id, size)?;

            let line_length = u32::try_from(size).map_err(|_| Error::StringGenerate)?;
            self.last_layout.push(LineEntry {
                line_start,
                line_length,
                slot_offset,
                flavor: SLOT_FLAVOR,
            });
            line_start = line_start
                .checked_add(line_length)
                .ok_or(Error::StringGenerate)?;

            remaining = remaining.saturating_sub(size);
        }

        Ok(())
    }

    fn line_layout(&self) -> Option<&[LineEntry]> {
        Some(&self.last_layout)
    }

    fn microseconds_per_line(&self) -> Option<u64> {
        // truncation_test always uses OpaqueToken so the rate is
        // irrelevant for actual rendering, but the cache aggregator
        // expects a value to be present whenever a layout is.
        Some(crate::line_layout::default_microseconds_per_line())
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
                    size_range: [200, 300],
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
            // Header now contains `[cat=X id=Y size=Z u=<slot>]` with a
            // space-delimited `size=` field. Extract it by finding the
            // `size=` token and reading until the next space.
            let size_idx = line.find("size=").expect("header must have size=");
            let after = &line[size_idx + "size=".len()..];
            let space = after.find(' ').expect("size value must be space-terminated");
            let claimed_size: usize = after[..space].parse().expect("size must parse");
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
        for max in [200usize, 500, 1000, 10_000] {
            let mut buf = Vec::new();
            gener.to_bytes(&mut rng, max, &mut buf).unwrap();
            assert!(buf.len() <= max, "produced {} > max {}", buf.len(), max);
        }
    }

    /// End-to-end uniqueness: build a real cache, apply rewrite_slots the
    /// same way the generator does at read time, and verify that:
    /// (a) same (inode, offset) twice → same bytes (determinism)
    /// (b) different inode, same offset → different bytes (cross-file)
    /// (c) same inode, offset shifted by total_cycle_size → different
    ///     bytes (cache wrap).
    #[test]
    fn rewrite_path_is_unique_across_files_and_wraps() {
        use crate::block::Cache;
        use crate::line_layout;
        use rand::SeedableRng;
        use std::num::NonZeroU32;

        let config = Config {
            format: Format::Simple,
            // Single category so we know every line has the same size,
            // giving us predictable slot positions for the assertions
            // below.
            categories: vec![Category {
                name: "normal".into(),
                weight: 1,
                size_range: [256, 256],
            }],
        };

        let mut rng = SmallRng::seed_from_u64(123);
        let variant = crate::Config::TruncationTest(config);
        let total_bytes = NonZeroU32::new(4096).unwrap();
        let cache = Cache::fixed_with_max_overhead(
            &mut rng,
            total_bytes,
            u128::from(total_bytes.get()),
            &variant,
            total_bytes.get() as usize,
        )
        .unwrap();

        let layout = cache.cache_layout().expect("truncation_test produces layout");
        let total_cycle = u64::from(layout.total_cycle_size);
        assert!(total_cycle > 0);
        assert!(layout.lines_per_cache() > 1);

        // Read one line-sized window at offset 0 for two different inodes.
        let line_len = layout.entries[0].line_length as usize;
        let raw = cache.read_at(0, line_len);

        // (a) Determinism: rewriting twice with the same inputs yields
        //     identical bytes.
        let mut buf_a1 = raw.to_vec();
        let mut buf_a2 = raw.to_vec();
        line_layout::rewrite_slots(
            &mut buf_a1,
            &layout.entries,
            layout.lines_per_cache(),
            total_cycle,
            /*inode*/ 42,
            /*cache_read_offset*/ 0,
            /*microseconds_per_line*/ 1000,
        );
        line_layout::rewrite_slots(
            &mut buf_a2,
            &layout.entries,
            layout.lines_per_cache(),
            total_cycle,
            42,
            0,
            1000,
        );
        assert_eq!(buf_a1, buf_a2, "determinism: same inputs must yield same bytes");

        // (b) Cross-file: different inode, same offset → different slot
        //     → different bytes.
        let mut buf_b = raw.to_vec();
        line_layout::rewrite_slots(
            &mut buf_b,
            &layout.entries,
            layout.lines_per_cache(),
            total_cycle,
            /*inode*/ 43,
            0,
            1000,
        );
        assert_ne!(
            buf_a1, buf_b,
            "cross-file: different inode must produce different bytes"
        );

        // (c) Cache wrap: same inode, offset shifted by exactly
        //     total_cycle → line_idx jumps by lines_per_cache → different
        //     fill → different bytes.
        let wrapped = cache.read_at(total_cycle, line_len);
        assert_eq!(
            raw, wrapped,
            "read_at wraps cyclically so raw bytes at offset 0 and offset total_cycle match"
        );
        let mut buf_c = wrapped.to_vec();
        line_layout::rewrite_slots(
            &mut buf_c,
            &layout.entries,
            layout.lines_per_cache(),
            total_cycle,
            42,
            total_cycle,
            1000,
        );
        assert_ne!(
            buf_a1, buf_c,
            "cache wrap: same cache position, different wrap_count, must produce different bytes"
        );
    }

    #[test]
    fn emits_layout_with_slot_positions() {
        let mut rng = SmallRng::seed_from_u64(99);
        let mut gener = TruncationTest::new(small_config()).unwrap();
        let mut buf = Vec::new();
        gener.to_bytes(&mut rng, 10_000, &mut buf).unwrap();

        let layout = gener.line_layout().expect("truncation_test emits layout");
        assert!(
            !layout.is_empty(),
            "layout must have at least one entry for 10_000 bytes of content"
        );

        // Every entry's line range must match the actual byte positions, and
        // the slot range inside the line must contain exactly
        // OPAQUE_TOKEN_WIDTH placeholder bytes.
        let mut expected_start = 0u32;
        for e in layout {
            assert_eq!(
                e.line_start, expected_start,
                "line_start must chain from previous line's end"
            );
            assert_eq!(e.flavor, SlotFlavor::OpaqueToken);
            assert_eq!(e.slot_width(), OPAQUE_TOKEN_WIDTH);

            let slot_start = (e.line_start + e.slot_offset) as usize;
            let slot_end = slot_start + OPAQUE_TOKEN_WIDTH as usize;
            let slot_bytes = &buf[slot_start..slot_end];
            assert!(
                slot_bytes.iter().all(|&b| b == SLOT_PLACEHOLDER),
                "slot bytes at [{slot_start}, {slot_end}) are not all placeholder: {:?}",
                std::str::from_utf8(slot_bytes)
            );

            expected_start += e.line_length;
        }
        assert_eq!(
            expected_start as usize,
            buf.len(),
            "sum of line_lengths must equal total bytes written"
        );
    }
}
