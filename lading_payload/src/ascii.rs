//! ASCII payload.

use std::io::Write;

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    Error,
    common::strings,
    line_layout::{
        ISO8601_WIDTH, LineEntry, OPAQUE_TOKEN_WIDTH, SLOT_PLACEHOLDER, SlotFlavor, UniqueConfig,
        UniqueFlavor,
    },
};

const MAX_LENGTH: u16 = 6_144; // 6 KiB

/// Configuration for the [`Ascii`] payload.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Config {
    /// Opt-in per-line uniqueness via read-time slot rewriting. When set,
    /// each emitted line is prefixed with `[<slot>] ` where `<slot>` is a
    /// fixed-width region overwritten at FUSE read time as a pure function
    /// of `(inode, line_index_within_file)`. When unset (default), the
    /// variant emits plain random alphanumeric lines with no framing —
    /// identical to its pre-slot behavior.
    #[serde(default)]
    pub unique: Option<UniqueConfig>,
}

/// Map ascii's `UniqueFlavor` to the `SlotFlavor` ascii uses internally.
/// Ascii has no native timestamp shape so `Timestamp` falls back to
/// ISO-8601.
fn slot_flavor_for(unique: UniqueFlavor) -> SlotFlavor {
    match unique {
        UniqueFlavor::OpaqueToken => SlotFlavor::OpaqueToken,
        UniqueFlavor::Timestamp => SlotFlavor::Iso8601,
    }
}

#[derive(Debug, Clone)]
/// ASCII text payload
pub struct Ascii {
    pool: strings::RandomStringPool,
    slot_flavor: Option<SlotFlavor>,
    /// Microseconds the simulated clock advances per line. Only consulted
    /// when `slot_flavor` is set.
    microseconds_per_line: u64,
    /// Per-line layout captured during the most recent `to_bytes` call.
    /// Empty when `slot_flavor` is `None`.
    last_layout: Vec<LineEntry>,
}

impl Ascii {
    /// Construct a new instance of `Ascii` with default (no-slot) config.
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self::with_config(rng, Config::default())
    }

    /// Construct a new instance of `Ascii` with the given config. When
    /// `config.unique` is set, the variant emits per-line slot framing
    /// and reports a layout via [`crate::Serialize::line_layout`].
    pub fn with_config<R>(rng: &mut R, config: Config) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let slot_flavor = config.unique.map(|u| slot_flavor_for(u.flavor));
        let microseconds_per_line = config
            .unique
            .map(|u| u.microseconds_per_line)
            .unwrap_or_else(crate::line_layout::default_microseconds_per_line);
        Self {
            // SAFETY: Do not adjust this downward below MAX_LENGTH without also
            // adjusting the input to `self.pool.of_size` below.
            pool: strings::RandomStringPool::with_size(rng, usize::from(MAX_LENGTH * 4)),
            slot_flavor,
            microseconds_per_line,
            last_layout: Vec::new(),
        }
    }
}

/// Bytes consumed per line in addition to the random content, when slot
/// framing is active. Layout: `[` + slot + `] ` + content + `\n`. Only
/// the flavors ascii actually uses are listed here; flavors specific to
/// other variants would not appear in `slot_flavor_for(_)`.
#[inline]
fn slot_overhead(flavor: SlotFlavor) -> usize {
    let slot_width = match flavor {
        SlotFlavor::OpaqueToken => OPAQUE_TOKEN_WIDTH,
        SlotFlavor::Iso8601 => ISO8601_WIDTH,
        SlotFlavor::ApacheCommonTimestamp => unreachable!(
            "ascii does not produce ApacheCommonTimestamp slots — \
             slot_flavor_for() never maps to it"
        ),
    };
    // `[` (1) + slot (slot_width) + `] ` (2) + `\n` (1)
    slot_width as usize + 4
}

impl crate::Serialize for Ascii {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        self.last_layout.clear();
        match self.slot_flavor {
            None => to_bytes_plain(&self.pool, &mut rng, max_bytes, writer),
            Some(flavor) => to_bytes_slotted(
                &self.pool,
                &mut self.last_layout,
                &mut rng,
                max_bytes,
                writer,
                flavor,
            ),
        }
    }

    fn line_layout(&self) -> Option<&[LineEntry]> {
        if self.slot_flavor.is_some() {
            Some(&self.last_layout)
        } else {
            None
        }
    }

    fn microseconds_per_line(&self) -> Option<u64> {
        self.slot_flavor.map(|_| self.microseconds_per_line)
    }
}

/// No-slot path: emit `<random>\n` lines until the budget is exhausted.
/// Behavior is identical to the pre-slot implementation.
fn to_bytes_plain<W, R>(
    pool: &strings::RandomStringPool,
    rng: &mut R,
    max_bytes: usize,
    writer: &mut W,
) -> Result<(), Error>
where
    R: Rng,
    W: Write,
{
    let mut bytes_remaining = max_bytes;
    loop {
        let bytes = rng.random_range(1..MAX_LENGTH);
        // SAFETY: the maximum request is always less than the size of the
        // pool, per our constructor.
        let encoding: &str = pool
            .of_size(rng, usize::from(bytes))
            .ok_or(Error::StringGenerate)?;
        let line_length = encoding.len() + 1;
        match bytes_remaining.checked_sub(line_length) {
            Some(remainder) => {
                writeln!(writer, "{encoding}")?;
                bytes_remaining = remainder;
            }
            None => break,
        }
    }
    Ok(())
}

/// Slot path: emit `[<placeholder>] <random>\n` lines and record the
/// per-line layout so the FUSE read path can rewrite slots.
fn to_bytes_slotted<W, R>(
    pool: &strings::RandomStringPool,
    layout: &mut Vec<LineEntry>,
    rng: &mut R,
    max_bytes: usize,
    writer: &mut W,
    flavor: SlotFlavor,
) -> Result<(), Error>
where
    R: Rng,
    W: Write,
{
    let slot_width = match flavor {
        SlotFlavor::OpaqueToken => OPAQUE_TOKEN_WIDTH,
        SlotFlavor::Iso8601 => ISO8601_WIDTH,
        SlotFlavor::ApacheCommonTimestamp => unreachable!(
            "ascii does not produce ApacheCommonTimestamp slots"
        ),
    } as usize;
    let overhead = slot_overhead(flavor);
    // Prefix to the content: `[` + slot placeholder + `] `. The remainder
    // of the per-line cost is the trailing `\n`.
    let prefix_len = 1 + slot_width + 2;

    // Reusable placeholder buffer for the slot region.
    let placeholder = [SLOT_PLACEHOLDER; 64];

    let mut bytes_remaining = max_bytes;
    let mut line_start: u32 = 0;

    loop {
        // Need at least overhead + 1 content byte to emit a line.
        if bytes_remaining < overhead + 1 {
            break;
        }

        // Pick a content length such that the total line fits both the
        // remaining budget and the variant's per-line cap (MAX_LENGTH).
        // Content length is clamped to [1, max_content].
        let max_content = (bytes_remaining - overhead).min(MAX_LENGTH as usize);
        if max_content == 0 {
            break;
        }
        let content_len = if max_content == 1 {
            1
        } else {
            rng.random_range(1..=max_content)
        };

        // Inner scope so the borrow on `pool` ends before we mutate
        // `layout` below.
        let actual_content_len = {
            let encoding: &str = pool
                .of_size(rng, content_len)
                .ok_or(Error::StringGenerate)?;
            let len = encoding.len();

            // `[`
            writer.write_all(b"[")?;
            // slot placeholder, in chunks
            let mut written = 0;
            while written < slot_width {
                let n = (slot_width - written).min(placeholder.len());
                writer.write_all(&placeholder[..n])?;
                written += n;
            }
            // `] ` + content + `\n`
            writer.write_all(b"] ")?;
            writer.write_all(encoding.as_bytes())?;
            writer.write_all(b"\n")?;

            len
        };

        let line_length = prefix_len + actual_content_len + 1; // +1 for '\n'
        layout.push(LineEntry {
            line_start,
            line_length: line_length as u32,
            // Slot starts immediately after the leading `[`.
            slot_offset: 1,
            flavor,
        });

        line_start = line_start
            .checked_add(line_length as u32)
            .ok_or(Error::StringGenerate)?;
        bytes_remaining -= line_length;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::{Ascii, Config};
    use crate::{
        Serialize,
        line_layout::{
            ISO8601_WIDTH, OPAQUE_TOKEN_WIDTH, SLOT_PLACEHOLDER, SlotFlavor, UniqueConfig,
            UniqueFlavor,
        },
    };

    // --- Test 1 + 2: plain mode preserves prior behavior ---

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut ascii = Ascii::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            ascii.to_bytes(rng, max_bytes, &mut bytes)?;
            prop_assert!(bytes.len() <= max_bytes);
            // No layout in plain mode.
            prop_assert!(ascii.line_layout().is_none());
        }
    }

    #[test]
    fn plain_mode_emits_no_brackets() {
        let mut rng = SmallRng::seed_from_u64(7);
        let mut ascii = Ascii::new(&mut rng);
        let mut buf = Vec::new();
        ascii.to_bytes(&mut rng, 10_000, &mut buf).unwrap();
        // Plain ascii pool content uses only alphanumerics + a few specials,
        // never `[`/`]` from the variant's framing because plain mode adds
        // none. This is a coarse check — the pool itself shouldn't emit `[`
        // since it picks from an alphanumeric set.
        let s = String::from_utf8_lossy(&buf);
        for line in s.lines() {
            prop_assert_no_bracket_prefix(line);
        }
    }

    fn prop_assert_no_bracket_prefix(line: &str) {
        // Plain ascii lines start with the random pool content directly;
        // the framing `[` only appears in slotted mode.
        assert!(
            !line.starts_with('['),
            "plain mode line unexpectedly starts with '[': {line:?}"
        );
    }

    // --- Test 3 + 4: opaque slot mode line shape and layout ---

    #[test]
    fn opaque_slot_lines_have_correct_framing() {
        let mut rng = SmallRng::seed_from_u64(11);
        let mut ascii = Ascii::with_config(
            &mut rng,
            Config {
                unique: Some(UniqueConfig {
                    flavor: UniqueFlavor::OpaqueToken,
                    microseconds_per_line: 1000,
                }),
            },
        );
        let mut buf = Vec::new();
        ascii.to_bytes(&mut rng, 20_000, &mut buf).unwrap();

        let layout = ascii
            .line_layout()
            .expect("opaque slot mode emits a layout")
            .to_vec();
        assert!(!layout.is_empty(), "expected at least one line written");

        // Walk the buffer using layout entries, verifying:
        // - `[` at line_start
        // - `] ` after the slot region
        // - slot bytes are placeholders
        // - line ends with `\n`
        let slot_w = OPAQUE_TOKEN_WIDTH as usize;
        for entry in &layout {
            let start = entry.line_start as usize;
            let end = start + entry.line_length as usize;

            assert_eq!(
                buf[start], b'[',
                "line at offset {start} must start with `[`"
            );
            // slot placeholder bytes
            let slot_start = start + 1;
            let slot_end = slot_start + slot_w;
            for (i, &b) in buf[slot_start..slot_end].iter().enumerate() {
                assert_eq!(
                    b, SLOT_PLACEHOLDER,
                    "slot byte {i} at offset {} is not placeholder",
                    slot_start + i
                );
            }
            // closing `] ` after slot
            assert_eq!(&buf[slot_end..slot_end + 2], b"] ");
            // trailing newline
            assert_eq!(
                buf[end - 1],
                b'\n',
                "line ending at {end} must end with newline"
            );

            assert_eq!(entry.slot_offset, 1);
            assert_eq!(entry.flavor, SlotFlavor::OpaqueToken);
        }

        // line_starts must chain across the buffer.
        let mut expected = 0u32;
        for entry in &layout {
            assert_eq!(entry.line_start, expected, "line_starts must chain");
            expected += entry.line_length;
        }
        assert_eq!(
            expected as usize,
            buf.len(),
            "sum of line lengths must equal buffer length"
        );
    }

    // --- Test 5: timestamp flavor — placeholders pre-rewrite, correct flavor ---

    #[test]
    fn timestamp_slot_lines_have_correct_framing() {
        let mut rng = SmallRng::seed_from_u64(23);
        let mut ascii = Ascii::with_config(
            &mut rng,
            Config {
                unique: Some(UniqueConfig {
                    flavor: UniqueFlavor::Timestamp,
                    microseconds_per_line: 1000,
                }),
            },
        );
        let mut buf = Vec::new();
        ascii.to_bytes(&mut rng, 20_000, &mut buf).unwrap();

        let layout = ascii.line_layout().expect("timestamp slot mode emits layout");
        assert!(!layout.is_empty());

        let slot_w = ISO8601_WIDTH as usize;
        for entry in layout {
            assert_eq!(entry.flavor, SlotFlavor::Iso8601);
            assert_eq!(entry.slot_offset, 1);
            let start = entry.line_start as usize;
            assert_eq!(buf[start], b'[');
            let slot_start = start + 1;
            let slot_end = slot_start + slot_w;
            for &b in &buf[slot_start..slot_end] {
                assert_eq!(b, SLOT_PLACEHOLDER, "timestamp slot must be placeholder pre-rewrite");
            }
            assert_eq!(&buf[slot_end..slot_end + 2], b"] ");
        }
    }

    // --- Test 6: slot mode respects max_bytes (proptest) ---

    proptest! {
        #[test]
        fn slot_mode_respects_max_bytes(seed: u64, max_bytes in 0u16..u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut ascii = Ascii::with_config(
                &mut rng,
                Config { unique: Some(UniqueConfig { flavor: UniqueFlavor::OpaqueToken, microseconds_per_line: 1000 }) },
            );

            let mut buf = Vec::with_capacity(max_bytes);
            ascii.to_bytes(rng, max_bytes, &mut buf)?;
            prop_assert!(buf.len() <= max_bytes);
        }
    }

    // --- Test 7 + 8: end-to-end uniqueness via real Cache + rewrite_slots ---

    fn assert_rewrite_path_unique(flavor: UniqueFlavor) {
        use crate::block::Cache;
        use crate::line_layout;
        use std::num::NonZeroU32;

        let mut rng = SmallRng::seed_from_u64(31);
        let variant = crate::Config::Ascii(Config {
            unique: Some(UniqueConfig {
                flavor,
                microseconds_per_line: 1000,
            }),
        });
        let total_bytes = NonZeroU32::new(8 * 1024).unwrap();
        let cache = Cache::fixed_with_max_overhead(
            &mut rng,
            total_bytes,
            u128::from(total_bytes.get()),
            &variant,
            total_bytes.get() as usize,
        )
        .unwrap();

        let layout = cache
            .cache_layout()
            .expect("ascii with unique config produces a cache layout");
        assert!(!layout.entries.is_empty(), "layout must have entries");
        let total_cycle = u64::from(layout.total_cycle_size);
        assert!(total_cycle > 0);

        // Pick a window covering the first line.
        let line_len = layout.entries[0].line_length as usize;
        let raw = cache.read_at(0, line_len);

        // Determinism: same inputs yield the same bytes.
        let mut buf_a1 = raw.to_vec();
        let mut buf_a2 = raw.to_vec();
        line_layout::rewrite_slots(
            &mut buf_a1,
            &layout.entries,
            layout.lines_per_cache(),
            total_cycle,
            42,
            0,
            1000,
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
        assert_eq!(buf_a1, buf_a2, "determinism must hold");

        // Cross-file: different inode → different bytes.
        let mut buf_b = raw.to_vec();
        line_layout::rewrite_slots(
            &mut buf_b,
            &layout.entries,
            layout.lines_per_cache(),
            total_cycle,
            43,
            0,
            1000,
        );
        assert_ne!(
            buf_a1, buf_b,
            "different inode must produce different bytes"
        );

        // Cache wrap: read_at wraps cyclically; pre-rewrite bytes match,
        // post-rewrite bytes must differ because line_idx_within_file
        // differs by lines_per_cache.
        let wrapped = cache.read_at(total_cycle, line_len);
        assert_eq!(raw, wrapped, "raw bytes wrap cyclically");
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
            "wrap must produce different bytes via line_idx shift"
        );
    }

    #[test]
    fn rewrite_path_unique_opaque() {
        assert_rewrite_path_unique(UniqueFlavor::OpaqueToken);
    }

    #[test]
    fn rewrite_path_unique_timestamp() {
        assert_rewrite_path_unique(UniqueFlavor::Timestamp);
    }
}
