//! Per-line layout metadata for payload variants that opt into per-line
//! uniqueness via read-time slot rewriting.
//!
//! Variants that implement uniqueness reserve a fixed-width byte region
//! ("slot") in each line they emit. At cache-build time the slot contains
//! a placeholder. At read time, the FUSE read path rewrites the slot with
//! a value that is a pure function of `(inode, line_index_within_file)`,
//! giving each line in the file a unique payload while staying
//! deterministic across runs with the same seed + config.
//!
//! The variant reports slot positions via a [`LineLayout`] alongside its
//! bytes; the generator consumes this layout to do the rewrite.
//!
//! See `scratch/captures/README.md` ADR-3 for the full design.
//!
//! # Public config vs internal flavor
//!
//! Two enums coexist here:
//!
//! - [`UniqueFlavor`] is the user-facing config enum (deserialized from
//!   YAML). It expresses *intent* — "I want an opaque token" or "I want
//!   a timestamp". Variants reference this in their `Config` structs.
//! - [`SlotFlavor`] is the internal rendering enum. Each entry maps to a
//!   specific byte format and width. Variants map their `UniqueFlavor`
//!   to whichever `SlotFlavor` matches their *native protocol* — e.g.
//!   `apache_common` maps `UniqueFlavor::Timestamp` to
//!   `SlotFlavor::ApacheCommonTimestamp` so the slot fits inside the
//!   apache CLF brackets.
//!
//! # Slot widths
//!
//! - [`SlotFlavor::OpaqueToken`] — 26 bytes: `u=<24 hex chars>`. Universal,
//!   doesn't interact with agent parsers.
//! - [`SlotFlavor::Iso8601`] — 27 bytes:
//!   `YYYY-MM-DDTHH:MM:SS.uuuuuuZ` (microsecond precision).
//! - [`SlotFlavor::ApacheCommonTimestamp`] — 33 bytes:
//!   `DD/Mon/YYYY:HH:MM:SS.uuuuuu +ZZZZ` (apache CLF microsecond format).

use serde::{Deserialize, Serialize};
use std::hash::Hasher;

/// User-facing flavor selector for opt-in per-line uniqueness. Variants'
/// `Config` structs embed this inside [`UniqueConfig`] so users can
/// choose between an opaque token (no agent-side parser interaction) and
/// a timestamp-shaped value (which each variant renders in its own
/// native protocol — see [`SlotFlavor`]).
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum UniqueFlavor {
    /// Render the slot as an opaque hex token. Same on every variant.
    OpaqueToken,
    /// Render the slot as a timestamp in the variant's native format.
    /// Variants without a native timestamp shape default to ISO-8601.
    Timestamp,
}

/// User-facing config bundle for opt-in per-line uniqueness. Variants
/// carry `Option<UniqueConfig>`; when `Some`, slot rewriting is active.
///
/// Two knobs:
/// - [`Self::flavor`] picks what gets rendered into the slot.
/// - [`Self::microseconds_per_line`] controls how fast the simulated
///   clock advances per line in timestamp flavors. Ignored (but not
///   validated) for opaque flavors.
///
/// `microseconds_per_line` is currently a single integer. Future work
/// could extend this to a distribution (fixed stride + jitter, Poisson
/// inter-arrivals, etc.) for more realistic log timing patterns.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct UniqueConfig {
    /// Which flavor of slot value to render at FUSE read time.
    pub flavor: UniqueFlavor,
    /// Microseconds the simulated clock advances per line. Used by
    /// timestamp flavors only; ignored for opaque flavors. Default
    /// `1000` (1 ms/line). `0` is allowed (lines within a single file
    /// share a timestamp; cross-file uniqueness still holds via the
    /// inode-derived offset).
    #[serde(default = "default_microseconds_per_line")]
    pub microseconds_per_line: u64,
}

/// Default microseconds per line. `1000` = 1 ms/line.
#[must_use]
pub fn default_microseconds_per_line() -> u64 {
    1000
}

/// Internal rendering selector. One entry per concrete slot byte format.
/// New variants that need a native timestamp shape add a new entry here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotFlavor {
    /// Opaque hex token derived from a hash of `(inode, line_idx)`. Does
    /// not interact with the agent's timestamp or multi-line heuristics.
    OpaqueToken,
    /// ISO-8601 formatted timestamp `YYYY-MM-DDTHH:MM:SS.uuuuuuZ` derived
    /// from a simulated clock keyed on `(inode, line_idx)`. Used by
    /// variants without a native timestamp shape (e.g. `ascii`).
    Iso8601,
    /// Apache common log format with microsecond precision:
    /// `DD/Mon/YYYY:HH:MM:SS.uuuuuu +ZZZZ`. Used by `apache_common` so
    /// the slot fits the variant's native protocol.
    ApacheCommonTimestamp,
}

/// Slot width in bytes for the [`SlotFlavor::OpaqueToken`] flavor.
///
/// Layout: `u=` (2) + 24 hex chars (24) = 26 bytes. Variants should
/// reserve exactly this many bytes and fill with a placeholder (e.g.
/// `u=________________________`).
pub const OPAQUE_TOKEN_WIDTH: u32 = 26;

/// Slot width in bytes for the [`SlotFlavor::Iso8601`] flavor.
///
/// Layout: `YYYY-MM-DDTHH:MM:SS.uuuuuuZ` = 27 bytes (microsecond
/// precision). Microsecond width gives 1M distinct sub-second buckets,
/// which keeps cross-file collisions vanishingly rare. Millisecond width
/// (3 digits, 1000 buckets) was originally used but collided in practice
/// for adjacent inodes.
pub const ISO8601_WIDTH: u32 = 27;

/// Slot width in bytes for the [`SlotFlavor::ApacheCommonTimestamp`] flavor.
///
/// Layout: `DD/Mon/YYYY:HH:MM:SS.uuuuuu +ZZZZ` = 33 bytes. Apache CLF's
/// strict format is second-precision (26 bytes); we extend with
/// microseconds to provide enough uniqueness buckets at our throughputs.
/// The extended form is widely used in real apache deployments via
/// `mod_log_config`'s `%{%d/%b/%Y:%H:%M:%S.%f %z}t` strftime directive.
pub const APACHE_COMMON_TIMESTAMP_WIDTH: u32 = 33;

/// Placeholder byte for slot contents before rewrite. Chosen to be
/// visually distinct and unlikely to collide with real payload content.
pub const SLOT_PLACEHOLDER: u8 = b'_';

/// Metadata describing a single line's position and slot within a block's
/// byte buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineEntry {
    /// Byte offset of the line's start within the block.
    pub line_start: u32,
    /// Byte length of the line, including terminating newline and the
    /// reserved slot.
    pub line_length: u32,
    /// Byte offset of the slot within the line. `line_start + slot_offset`
    /// is the slot's absolute position within the block.
    pub slot_offset: u32,
    /// The slot flavor.
    pub flavor: SlotFlavor,
}

impl LineEntry {
    /// Slot width in bytes, derived from [`Self::flavor`].
    #[must_use]
    pub fn slot_width(&self) -> u32 {
        match self.flavor {
            SlotFlavor::OpaqueToken => OPAQUE_TOKEN_WIDTH,
            SlotFlavor::Iso8601 => ISO8601_WIDTH,
            SlotFlavor::ApacheCommonTimestamp => APACHE_COMMON_TIMESTAMP_WIDTH,
        }
    }

    /// Absolute byte offset of the slot's start within the block.
    #[must_use]
    pub fn slot_start_in_block(&self) -> u32 {
        self.line_start + self.slot_offset
    }

    /// Absolute byte offset one past the slot's end within the block.
    #[must_use]
    pub fn slot_end_in_block(&self) -> u32 {
        self.slot_start_in_block() + self.slot_width()
    }
}

/// Line entries emitted by a variant for one block.
#[derive(Debug, Clone, Default)]
pub struct LineLayout {
    /// Per-line entries in ascending order of `line_start`.
    pub entries: Vec<LineEntry>,
}

/// Fill a slot's bytes in-place from a pure function of `(inode,
/// line_idx, microseconds_per_line)`.
///
/// `dst` must be exactly the slot width for the flavor. Partial-slot
/// writes are supported by passing a sub-slice that represents the
/// portion visible to the caller; in that case `slot_byte_offset` must
/// give the offset of `dst`'s first byte within the logical slot.
///
/// `microseconds_per_line` controls the simulated-clock stride for
/// timestamp flavors. It's ignored for [`SlotFlavor::OpaqueToken`].
///
/// Writes are deterministic: for any
/// `(flavor, inode, line_idx, microseconds_per_line, slot_byte_offset, len)`
/// the same bytes are always produced.
pub fn fill_slot(
    flavor: SlotFlavor,
    inode: u64,
    line_idx: u64,
    microseconds_per_line: u64,
    slot_byte_offset: u32,
    dst: &mut [u8],
) {
    // Materialize the full-width slot value once, then copy the requested
    // sub-range. Materialization is cheap and avoids duplicating the
    // per-flavor logic across full and partial paths.
    match flavor {
        SlotFlavor::OpaqueToken => {
            let mut buf = [0u8; OPAQUE_TOKEN_WIDTH as usize];
            render_opaque(inode, line_idx, &mut buf);
            copy_slice(&buf, slot_byte_offset, dst);
        }
        SlotFlavor::Iso8601 => {
            let mut buf = [0u8; ISO8601_WIDTH as usize];
            render_iso8601(inode, line_idx, microseconds_per_line, &mut buf);
            copy_slice(&buf, slot_byte_offset, dst);
        }
        SlotFlavor::ApacheCommonTimestamp => {
            let mut buf = [0u8; APACHE_COMMON_TIMESTAMP_WIDTH as usize];
            render_apache_common_timestamp(inode, line_idx, microseconds_per_line, &mut buf);
            copy_slice(&buf, slot_byte_offset, dst);
        }
    }
}

fn copy_slice(full: &[u8], slot_byte_offset: u32, dst: &mut [u8]) {
    let start = slot_byte_offset as usize;
    let end = start + dst.len();
    debug_assert!(
        end <= full.len(),
        "slot byte range [{start}, {end}) exceeds slot width {}",
        full.len()
    );
    dst.copy_from_slice(&full[start..end]);
}

/// Render `u=<24 hex>` where the 24 hex chars are the first 96 bits of a
/// simple 64-bit mixing hash of `(inode, line_idx)`, extended to 96 bits
/// by a second mix step. Deterministic and cheap; collision-resistant
/// enough that distinct `(inode, line_idx)` pairs don't collide in
/// practice.
fn render_opaque(inode: u64, line_idx: u64, dst: &mut [u8; 26]) {
    debug_assert_eq!(dst.len(), 26);
    dst[0] = b'u';
    dst[1] = b'=';

    let h1 = mix64(inode.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ line_idx);
    let h2 = mix64(line_idx.wrapping_mul(0xBF58_476D_1CE4_E5B9) ^ inode);
    // Concatenate 96 bits: first 64 from h1, next 32 from h2's upper half.
    let mut bytes = [0u8; 12];
    bytes[0..8].copy_from_slice(&h1.to_le_bytes());
    bytes[8..12].copy_from_slice(&(h2 as u32).to_le_bytes());

    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (i, b) in bytes.iter().enumerate() {
        dst[2 + i * 2] = HEX[(b >> 4) as usize];
        dst[2 + i * 2 + 1] = HEX[(b & 0x0F) as usize];
    }
}

/// Compute simulated `(seconds_since_epoch, micros)` keyed on
/// `(inode, line_idx)`. Shared by all timestamp-shaped renderers so
/// they advance time consistently.
///
/// Inode contributes a 0..1_000_000 microsecond starting offset (so two
/// files at the same `line_idx` get distinct timestamps). `line_idx`
/// advances the clock by `microseconds_per_line` microseconds per line.
/// Epoch is fixed at `2024-01-01T00:00:00 UTC`.
///
/// `microseconds_per_line == 0` is allowed: every line in a file shares
/// the same timestamp, but cross-file uniqueness still holds via the
/// inode offset.
fn simulated_time(inode: u64, line_idx: u64, microseconds_per_line: u64) -> (u64, u32) {
    const EPOCH_SECONDS: u64 = 1_704_067_200;
    let inode_offset_micros = mix64(inode) % 1_000_000;
    let micros_total =
        inode_offset_micros.wrapping_add(line_idx.wrapping_mul(microseconds_per_line));
    let seconds_total = EPOCH_SECONDS + micros_total / 1_000_000;
    let micros = (micros_total % 1_000_000) as u32;
    (seconds_total, micros)
}

/// Render a 27-byte ISO-8601 timestamp `YYYY-MM-DDTHH:MM:SS.uuuuuuZ`.
///
/// Microsecond width (6 digits, 1M buckets) is necessary because
/// millisecond width (3 digits, 1000 buckets) collides in practice
/// between adjacent inodes — `mix64(42) % 1000` and `mix64(43) % 1000`
/// happened to both be 962, defeating cross-file uniqueness.
fn render_iso8601(inode: u64, line_idx: u64, microseconds_per_line: u64, dst: &mut [u8; 27]) {
    let (seconds_total, micros) = simulated_time(inode, line_idx, microseconds_per_line);
    let (year, month, day, hour, min, sec) = seconds_to_datetime(seconds_total);

    write_fixed_u32(&mut dst[0..4], year);
    dst[4] = b'-';
    write_fixed_u32(&mut dst[5..7], month);
    dst[7] = b'-';
    write_fixed_u32(&mut dst[8..10], day);
    dst[10] = b'T';
    write_fixed_u32(&mut dst[11..13], hour);
    dst[13] = b':';
    write_fixed_u32(&mut dst[14..16], min);
    dst[16] = b':';
    write_fixed_u32(&mut dst[17..19], sec);
    dst[19] = b'.';
    write_fixed_u32(&mut dst[20..26], micros);
    dst[26] = b'Z';
}

/// Render a 33-byte apache CLF microsecond timestamp
/// `DD/Mon/YYYY:HH:MM:SS.uuuuuu +ZZZZ`.
///
/// This is the apache `mod_log_config` extended format
/// `%{%d/%b/%Y:%H:%M:%S.%f %z}t` — same shape as the strict CLF
/// timestamp but with a fractional-second component appended before the
/// zone. Apache log consumers parse it correctly.
///
/// Zone is fixed at `+0000`; uniqueness comes from the microseconds
/// driven by `(inode, line_idx)`. Same simulated clock as
/// [`render_iso8601`].
fn render_apache_common_timestamp(
    inode: u64,
    line_idx: u64,
    microseconds_per_line: u64,
    dst: &mut [u8; 33],
) {
    let (seconds_total, micros) = simulated_time(inode, line_idx, microseconds_per_line);
    let (year, month, day, hour, min, sec) = seconds_to_datetime(seconds_total);

    // Three-letter month abbreviation indexed by `month` (1-based).
    const MONTHS: [&[u8; 3]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];
    let month_idx = (month.saturating_sub(1) as usize).min(11);

    // Layout: DD/Mon/YYYY:HH:MM:SS.uuuuuu +ZZZZ
    //         00 1 234 56789 01 23 45 67 89012 3 45678
    //         01    234       1     1
    //                         0     5   1   2 2  3
    //                         0     5   9   1 3  3
    write_fixed_u32(&mut dst[0..2], day);
    dst[2] = b'/';
    dst[3..6].copy_from_slice(MONTHS[month_idx]);
    dst[6] = b'/';
    write_fixed_u32(&mut dst[7..11], year);
    dst[11] = b':';
    write_fixed_u32(&mut dst[12..14], hour);
    dst[14] = b':';
    write_fixed_u32(&mut dst[15..17], min);
    dst[17] = b':';
    write_fixed_u32(&mut dst[18..20], sec);
    dst[20] = b'.';
    write_fixed_u32(&mut dst[21..27], micros);
    dst[27] = b' ';
    dst[28..33].copy_from_slice(b"+0000");
}

fn write_fixed_u32(dst: &mut [u8], mut v: u32) {
    for slot in dst.iter_mut().rev() {
        *slot = b'0' + (v % 10) as u8;
        v /= 10;
    }
}

/// 64-bit integer mixer (SplitMix64). Pure, deterministic.
#[inline]
fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Convert a seconds-since-epoch value to `(year, month, day, hour, min, sec)`.
/// Uses Hinnant's civil-from-days algorithm.
#[allow(clippy::similar_names)]
fn seconds_to_datetime(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let rem_seconds = (secs % 86_400) as u32;
    let hour = rem_seconds / 3600;
    let min = (rem_seconds % 3600) / 60;
    let sec = rem_seconds % 60;

    // Hinnant's algorithm: days since 1970-01-01 (Unix epoch) to
    // Gregorian date. Shift epoch to 0000-03-01 (which sits at the start
    // of a 400-year cycle and makes the math uniform).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as u32, m as u32, d as u32, hour, min, sec)
}

// A no-op impl of Hasher so that SlotFlavor can be used in hash contexts
// if ever needed. Not currently used; kept out of the public surface.
#[allow(dead_code)]
struct _Unused;
impl Hasher for _Unused {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _bytes: &[u8]) {}
}

/// Rewrite slots in a read buffer in place.
///
/// `data` is the bytes returned from a cyclic cache read of `data.len()`
/// bytes starting at the cache-cycle-extended offset `cache_read_offset`
/// (i.e. not yet wrapped via modulo — the absolute offset as seen by the
/// file's read path).
///
/// `entries` is the cache-wide layout from [`crate::block::Cache::cache_layout`];
/// each entry's `line_start` is relative to the cache cycle (i.e. within
/// `[0, total_cycle_size)`).
///
/// `lines_per_cache` is `entries.len() as u64`.
///
/// `total_cycle_size` is the cache's total byte size.
///
/// `microseconds_per_line` is the simulated-clock stride used by
/// timestamp-shaped flavors. Ignored for opaque flavors.
///
/// For each line whose slot intersects the read buffer, the slot bytes are
/// overwritten with the output of [`fill_slot`] for
/// `(inode, line_idx_within_file, microseconds_per_line)`, where
/// `line_idx_within_file` is `wrap_count × lines_per_cache + cache_line_idx`.
///
/// A no-op if the layout is empty or the cache size is zero.
pub fn rewrite_slots(
    data: &mut [u8],
    entries: &[LineEntry],
    lines_per_cache: u64,
    total_cycle_size: u64,
    inode: u64,
    cache_read_offset: u64,
    microseconds_per_line: u64,
) {
    if lines_per_cache == 0 || total_cycle_size == 0 || data.is_empty() {
        return;
    }
    let read_len = data.len() as u64;
    let read_end = cache_read_offset + read_len;

    // Find the starting line: within the starting wrap, the first line
    // whose end-position is strictly past the read-start-position-in-cycle.
    let mut wrap = cache_read_offset / total_cycle_size;
    let start_pos_in_cycle = (cache_read_offset % total_cycle_size) as u32;
    let mut line_idx =
        entries.partition_point(|e| e.line_start + e.line_length <= start_pos_in_cycle);

    loop {
        // If we've exhausted the current wrap's entries, advance to next wrap.
        if line_idx >= entries.len() {
            wrap += 1;
            line_idx = 0;
            // Short-circuit if the next wrap sits past the read end.
            if wrap * total_cycle_size >= read_end {
                break;
            }
            continue;
        }

        let entry = &entries[line_idx];
        let line_abs_start = wrap * total_cycle_size + u64::from(entry.line_start);
        if line_abs_start >= read_end {
            break;
        }

        let slot_width = u64::from(entry.slot_width());
        let slot_abs_start = line_abs_start + u64::from(entry.slot_offset);
        let slot_abs_end = slot_abs_start + slot_width;

        // Intersect slot's absolute range with the read buffer's range.
        let i_start = slot_abs_start.max(cache_read_offset);
        let i_end = slot_abs_end.min(read_end);
        if i_start < i_end {
            let buf_start = (i_start - cache_read_offset) as usize;
            let buf_end = (i_end - cache_read_offset) as usize;
            let slot_byte_offset = u32::try_from(i_start - slot_abs_start)
                .expect("slot_width fits in u32 so the byte offset does too");
            let file_line_idx = wrap * lines_per_cache + line_idx as u64;
            fill_slot(
                entry.flavor,
                inode,
                file_line_idx,
                microseconds_per_line,
                slot_byte_offset,
                &mut data[buf_start..buf_end],
            );
        }

        line_idx += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_fill_is_deterministic() {
        let mut a = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        let mut b = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 42, 17, 1000, 0, &mut a);
        fill_slot(SlotFlavor::OpaqueToken, 42, 17, 1000, 0, &mut b);
        assert_eq!(a, b);
        assert_eq!(&a[..2], b"u=");
    }

    #[test]
    fn opaque_fill_differs_for_different_inode() {
        let mut a = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        let mut b = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 42, 17, 1000, 0, &mut a);
        fill_slot(SlotFlavor::OpaqueToken, 43, 17, 1000, 0, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn opaque_fill_differs_for_different_line_idx() {
        let mut a = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        let mut b = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 42, 17, 1000, 0, &mut a);
        fill_slot(SlotFlavor::OpaqueToken, 42, 18, 1000, 0, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn opaque_partial_fill_matches_full_slice() {
        let mut full = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 100, 200, 1000, 0, &mut full);

        // Fill only bytes [5, 13) of the slot via the partial path.
        let mut partial = [0u8; 8];
        fill_slot(SlotFlavor::OpaqueToken, 100, 200, 1000, 5, &mut partial);
        assert_eq!(&partial[..], &full[5..13]);
    }

    #[test]
    fn iso8601_fill_is_iso8601_shape() {
        let mut ts = [0u8; ISO8601_WIDTH as usize];
        fill_slot(SlotFlavor::Iso8601, 0, 0, 1000, 0, &mut ts);
        // YYYY-MM-DDTHH:MM:SS.uuuuuuZ
        assert_eq!(ts[4], b'-');
        assert_eq!(ts[7], b'-');
        assert_eq!(ts[10], b'T');
        assert_eq!(ts[13], b':');
        assert_eq!(ts[16], b':');
        assert_eq!(ts[19], b'.');
        assert_eq!(ts[26], b'Z');
        // Year is 2024 since EPOCH_SECONDS sits at 2024-01-01.
        assert_eq!(&ts[0..4], b"2024");
    }

    #[test]
    fn iso8601_fill_differs_for_different_line_idx() {
        let mut a = [0u8; ISO8601_WIDTH as usize];
        let mut b = [0u8; ISO8601_WIDTH as usize];
        fill_slot(SlotFlavor::Iso8601, 42, 1000, 1000, 0, &mut a);
        fill_slot(SlotFlavor::Iso8601, 42, 2000, 1000, 0, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn iso8601_fill_differs_for_different_inode() {
        let mut a = [0u8; ISO8601_WIDTH as usize];
        let mut b = [0u8; ISO8601_WIDTH as usize];
        fill_slot(SlotFlavor::Iso8601, 1, 0, 1000, 0, &mut a);
        fill_slot(SlotFlavor::Iso8601, 2, 0, 1000, 0, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn apache_common_timestamp_fill_has_clf_shape() {
        let mut ts = [0u8; APACHE_COMMON_TIMESTAMP_WIDTH as usize];
        fill_slot(SlotFlavor::ApacheCommonTimestamp, 0, 0, 1000, 0, &mut ts);
        // DD/Mon/YYYY:HH:MM:SS.uuuuuu +ZZZZ
        assert_eq!(ts[2], b'/');
        assert_eq!(ts[6], b'/');
        assert_eq!(ts[11], b':');
        assert_eq!(ts[14], b':');
        assert_eq!(ts[17], b':');
        assert_eq!(ts[20], b'.');
        assert_eq!(ts[27], b' ');
        assert_eq!(&ts[28..33], b"+0000");
        // Year-2024 epoch. inode=0,line=0 → 2024-01-01 → "01/Jan/2024".
        assert_eq!(&ts[0..2], b"01");
        assert_eq!(&ts[3..6], b"Jan");
        assert_eq!(&ts[7..11], b"2024");
        // Bytes are all printable ASCII.
        assert!(ts.iter().all(|&b| b.is_ascii() && b >= b' '));
    }

    #[test]
    fn apache_common_timestamp_fill_differs_for_different_line_idx() {
        let mut a = [0u8; APACHE_COMMON_TIMESTAMP_WIDTH as usize];
        let mut b = [0u8; APACHE_COMMON_TIMESTAMP_WIDTH as usize];
        fill_slot(SlotFlavor::ApacheCommonTimestamp, 42, 1, 1000, 0, &mut a);
        fill_slot(SlotFlavor::ApacheCommonTimestamp, 42, 2, 1000, 0, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn apache_common_timestamp_fill_differs_for_different_inode() {
        let mut a = [0u8; APACHE_COMMON_TIMESTAMP_WIDTH as usize];
        let mut b = [0u8; APACHE_COMMON_TIMESTAMP_WIDTH as usize];
        fill_slot(SlotFlavor::ApacheCommonTimestamp, 1, 0, 1000, 0, &mut a);
        fill_slot(SlotFlavor::ApacheCommonTimestamp, 2, 0, 1000, 0, &mut b);
        assert_ne!(a, b);
    }

    /// Stride of `0` makes successive line_idx values yield the same
    /// timestamp within a file; cross-file uniqueness still holds via
    /// the inode-derived offset.
    #[test]
    fn iso8601_zero_stride_same_within_file_different_across_files() {
        let mut a = [0u8; ISO8601_WIDTH as usize];
        let mut b = [0u8; ISO8601_WIDTH as usize];
        let mut c = [0u8; ISO8601_WIDTH as usize];
        // Same inode, different line_idx, stride=0.
        fill_slot(SlotFlavor::Iso8601, 42, 1, 0, 0, &mut a);
        fill_slot(SlotFlavor::Iso8601, 42, 2, 0, 0, &mut b);
        assert_eq!(a, b, "stride 0 → same timestamp within a file");
        // Different inode, stride=0.
        fill_slot(SlotFlavor::Iso8601, 43, 1, 0, 0, &mut c);
        assert_ne!(a, c, "stride 0 still has cross-file uniqueness");
    }

    /// A larger stride produces visibly different timestamps for adjacent
    /// lines and matches the math: 1000 µs/line × 10000 lines = 10 s.
    #[test]
    fn iso8601_large_stride_advances_correctly() {
        // 100 ms/line means 10 lines per simulated second.
        let mut a = [0u8; ISO8601_WIDTH as usize];
        let mut b = [0u8; ISO8601_WIDTH as usize];
        fill_slot(SlotFlavor::Iso8601, 0, 0, 100_000, 0, &mut a);
        fill_slot(SlotFlavor::Iso8601, 0, 10, 100_000, 0, &mut b);
        // Both have the same year/month/day/hour/minute prefix; bytes
        // 17..19 (seconds) differ by exactly 1 between a and b: line 0
        // gets second = 0, line 10 gets second = 1.
        assert_eq!(&a[0..17], &b[0..17]);
        assert_ne!(&a[17..19], &b[17..19]);
    }

    fn mk_entry(line_start: u32, line_length: u32, slot_offset: u32) -> LineEntry {
        LineEntry {
            line_start,
            line_length,
            slot_offset,
            flavor: SlotFlavor::OpaqueToken,
        }
    }

    #[test]
    fn rewrite_fills_single_slot_in_range() {
        // Cache: 1 line of length 100, slot at offset 10, width 26.
        let entries = [mk_entry(0, 100, 10)];
        let total = 100u64;
        let mut data = vec![b'_'; 100];
        rewrite_slots(&mut data, &entries, 1, total, /*inode*/ 7, /*offset*/ 0, 1000);
        assert_eq!(&data[10..12], b"u=", "slot prefix must be u=");
        // Bytes outside the slot remain placeholders.
        assert!(data[0..10].iter().all(|&b| b == b'_'));
        assert!(data[36..].iter().all(|&b| b == b'_'));
    }

    #[test]
    fn rewrite_partial_slot_head() {
        // Read starts mid-slot. Only the tail portion of the slot should be
        // written.
        let entries = [mk_entry(0, 100, 10)];
        let total = 100u64;
        let mut data = vec![b'X'; 100];
        // Read offset 15 → partial slot starting at slot_byte_offset = 5.
        rewrite_slots(&mut data, &entries, 1, total, 0, 15, 1000);
        // The first OPAQUE_TOKEN_WIDTH-5 = 21 bytes of `data` should be
        // bytes 5..26 of the full slot (the "head" of `data` covers the
        // slot's tail).
        let mut reference = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 0, 0, 1000, 0, &mut reference);
        assert_eq!(
            &data[0..21],
            &reference[5..26],
            "partial-head of slot must match bytes [5, 26) of the full slot"
        );
    }

    #[test]
    fn rewrite_across_cache_wrap() {
        // Cache has 2 lines. Read spans a wrap boundary.
        let entries = [mk_entry(0, 50, 5), mk_entry(50, 50, 5)];
        let total = 100u64;
        let mut data = vec![b'_'; 150];
        // Read starts at absolute offset 90, covers offsets [90, 240).
        // That's wrap=0 line 1 partial, then wrap=1 line 0, line 1 partial.
        rewrite_slots(&mut data, &entries, 2, total, 42, 90, 1000);

        // Wrap 0 line 1 slot at absolute [55, 81) — but read starts at 90,
        // so this slot is NOT in range (it's before 90).
        // Wrap 1 line 0 slot at absolute [100+5, 100+31) = [105, 131) — in
        // range. Corresponds to data[105-90 .. 131-90] = data[15..41].
        let mut ref0 = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 42, 2, 1000, 0, &mut ref0);
        assert_eq!(&data[15..41], &ref0, "wrap 1 line 0 slot must be filled");

        // Wrap 1 line 1 slot at absolute [100+55, 100+81) = [155, 181) —
        // in range. data[155-90 .. 181-90] = data[65..91].
        let mut ref1 = [0u8; OPAQUE_TOKEN_WIDTH as usize];
        fill_slot(SlotFlavor::OpaqueToken, 42, 3, 1000, 0, &mut ref1);
        assert_eq!(&data[65..91], &ref1, "wrap 1 line 1 slot must be filled");
    }

    #[test]
    fn rewrite_empty_layout_is_noop() {
        let mut data = vec![0x42u8; 50];
        let before = data.clone();
        rewrite_slots(&mut data, &[], 0, 0, 0, 0, 1000);
        assert_eq!(data, before);
    }

    #[test]
    fn line_entry_slot_geometry() {
        let e = LineEntry {
            line_start: 100,
            line_length: 500,
            slot_offset: 20,
            flavor: SlotFlavor::OpaqueToken,
        };
        assert_eq!(e.slot_width(), OPAQUE_TOKEN_WIDTH);
        assert_eq!(e.slot_start_in_block(), 120);
        assert_eq!(e.slot_end_in_block(), 120 + OPAQUE_TOKEN_WIDTH);
    }
}
