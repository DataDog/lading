//! Apache common payload.

use crate::{
    Error, Generator,
    common::strings,
    line_layout::{
        APACHE_COMMON_TIMESTAMP_WIDTH, LineEntry, OPAQUE_TOKEN_WIDTH, SLOT_PLACEHOLDER, SlotFlavor,
        UniqueConfig, UniqueFlavor,
    },
};

use core::fmt;
use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use std::io::Write;

/// Configuration for the [`ApacheCommon`] payload.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Config {
    /// Opt-in per-line uniqueness via read-time slot rewriting. When set,
    /// the timestamp position in each line is replaced with a reserved
    /// slot rewritten at FUSE read time as a pure function of
    /// `(inode, line_index_within_file)`. When unset (default), the
    /// variant emits random apache-format timestamps as before.
    #[serde(default)]
    pub unique: Option<UniqueConfig>,
}

/// Map apache_common's `UniqueFlavor` to its native `SlotFlavor`. The
/// `Timestamp` choice maps to `ApacheCommonTimestamp` so the slot fits
/// the variant's natural protocol — apache CLF format with microseconds.
fn slot_flavor_for(unique: UniqueFlavor) -> SlotFlavor {
    match unique {
        UniqueFlavor::OpaqueToken => SlotFlavor::OpaqueToken,
        UniqueFlavor::Timestamp => SlotFlavor::ApacheCommonTimestamp,
    }
}

#[inline]
fn slot_width_for(flavor: SlotFlavor) -> usize {
    match flavor {
        SlotFlavor::OpaqueToken => OPAQUE_TOKEN_WIDTH as usize,
        SlotFlavor::ApacheCommonTimestamp => APACHE_COMMON_TIMESTAMP_WIDTH as usize,
        SlotFlavor::Iso8601 => unreachable!(
            "apache_common does not produce Iso8601 slots — slot_flavor_for() never maps to it"
        ),
    }
}

const PATH_NAMES: [&str; 25] = [
    "alfa", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliett",
    "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango",
    "uniform", "victor", "xray", "yankee", "zulu",
];

const STATUS_CODES: [u16; 64] = [
    100, 101, 102, 103, 200, 201, 202, 203, 204, 205, 206, 207, 208, 226, 300, 301, 302, 303, 304,
    305, 306, 307, 308, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414,
    415, 416, 417, 418, 421, 422, 423, 424, 425, 426, 428, 429, 431, 451, 500, 501, 502, 503, 504,
    505, 506, 507, 508, 509, 510, 511,
];

#[derive(Debug)]
struct StatusCode(u16);

impl Distribution<StatusCode> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> StatusCode
    where
        R: Rng + ?Sized,
    {
        StatusCode(
            *STATUS_CODES
                .choose(rng)
                .expect("failed to choose status codes"),
        )
    }
}

#[derive(Debug)]
enum Protocol {
    Http10,
    Http11,
    Http12,
    Http20,
    Http21,
    Http22,
}

impl Distribution<Protocol> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Protocol
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..6) {
            0 => Protocol::Http10,
            1 => Protocol::Http11,
            2 => Protocol::Http12,
            3 => Protocol::Http20,
            4 => Protocol::Http21,
            5 => Protocol::Http22,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Protocol::Http10 => "HTTP/1.0",
            Protocol::Http11 => "HTTP/1.1",
            Protocol::Http12 => "HTTP/1.2",
            Protocol::Http20 => "HTTP/2.0",
            Protocol::Http21 => "HTTP/2.1",
            Protocol::Http22 => "HTTP/2.2",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug)]
struct Path {
    components: [Option<&'static str>; 16],
}

impl Distribution<Path> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Path
    where
        R: Rng + ?Sized,
    {
        let total_components: usize = rng.random_range(1..=16);
        let mut components = [None; 16];

        for idx in components.iter_mut().take(total_components) {
            *idx = PATH_NAMES.choose(rng).copied();
        }

        Path { components }
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for component in self.components.iter().flatten() {
            write!(f, "/{component}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Month {
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
}

impl Distribution<Month> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Month
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..12) {
            0 => Month::January,
            1 => Month::February,
            2 => Month::March,
            3 => Month::April,
            4 => Month::May,
            5 => Month::June,
            6 => Month::July,
            7 => Month::August,
            8 => Month::September,
            9 => Month::October,
            10 => Month::November,
            11 => Month::December,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
struct Timestamp {
    day: u8,
    month: Month,
    year: i8,
    hour: u8,
    minute: u8,
    second: u8,
    timezone: u8,
}

impl Distribution<Timestamp> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Timestamp
    where
        R: Rng + ?Sized,
    {
        Timestamp {
            day: rng.random(),
            month: rng.random(),
            year: rng.random(),
            hour: rng.random(),
            minute: rng.random(),
            second: rng.random(),
            timezone: rng.random(),
        }
    }
}
impl fmt::Display for Timestamp {
    // [day/month/year:hour:minute:second zone]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let day = (self.day % 27) + 1; // note, not every month has only 28 days but all have at least
        let month = match self.month {
            Month::January => "Jan",
            Month::February => "Feb",
            Month::March => "Mar",
            Month::April => "Apr",
            Month::May => "May",
            Month::June => "Jun",
            Month::July => "Jul",
            Month::August => "Aug",
            Month::September => "Sep",
            Month::October => "Oct",
            Month::November => "Nov",
            Month::December => "Dec",
        };
        let year = 2022_i32 - i32::from(self.year % 40);
        let hour = self.hour % 24;
        let minute = self.minute % 60;
        let second = self.second % 60;
        let timezone = self.timezone % 24;

        write!(
            f,
            "{day:02}/{month}/{year}:{hour:02}:{minute:02}:{second:02} {timezone:04}"
        )
    }
}

#[derive(Debug)]
struct IpV4 {
    zero: u8,
    one: u8,
    two: u8,
    three: u8,
}

impl Distribution<IpV4> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> IpV4
    where
        R: Rng + ?Sized,
    {
        IpV4 {
            zero: rng.random(),
            one: rng.random(),
            two: rng.random(),
            three: rng.random(),
        }
    }
}

impl fmt::Display for IpV4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}.{}", self.zero, self.one, self.two, self.three,)
    }
}

#[derive(Debug)]
enum Method {
    Get,
    Put,
    Post,
    Delete,
    Patch,
}

impl Distribution<Method> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Method
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..5) {
            0 => Method::Get,
            1 => Method::Put,
            2 => Method::Post,
            3 => Method::Delete,
            4 => Method::Patch,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Method::Get => "GET",
            Method::Put => "PUT",
            Method::Post => "POST",
            Method::Delete => "DELETE",
            Method::Patch => "PATCH",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug)]
pub(crate) struct Member<'a> {
    host: IpV4,
    user: &'a str,
    timestamp: Timestamp,
    method: Method,
    path: Path,
    protocol: Protocol,
    status_code: StatusCode,
    bytes_out: u16,
}

impl fmt::Display for Member<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} - {} [{}] \"{} {} {}\" {} {}",
            self.host,
            self.user,
            self.timestamp,
            self.method,
            self.path,
            self.protocol,
            self.status_code.0,
            self.bytes_out
        )
    }
}

#[derive(Debug, Clone)]
/// Apache Common log format payload
pub struct ApacheCommon {
    str_pool: strings::RandomStringPool,
    slot_flavor: Option<SlotFlavor>,
    /// Microseconds the simulated clock advances per line. Only consulted
    /// when `slot_flavor` is set.
    microseconds_per_line: u64,
    /// Per-line layout captured during the most recent `to_bytes` call.
    /// Empty when `slot_flavor` is `None`.
    last_layout: Vec<LineEntry>,
}

impl ApacheCommon {
    /// Construct a new instance of `ApacheCommon` with default (no-slot)
    /// config.
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self::with_config(rng, Config::default())
    }

    /// Construct a new instance of `ApacheCommon` with the given config.
    /// When `config.unique` is set, the variant replaces the random
    /// timestamp position with a reserved slot and reports a per-line
    /// layout via [`crate::Serialize::line_layout`].
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
            str_pool: strings::RandomStringPool::with_size(rng, 1_000_000),
            slot_flavor,
            microseconds_per_line,
            last_layout: Vec::new(),
        }
    }
}

impl<'a> Generator<'a> for ApacheCommon {
    type Output = Member<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        Ok(Member {
            host: rng.random(),
            user: self
                .str_pool
                .of_size_range(&mut rng, 1_u16..16_u16)
                .ok_or(Error::StringGenerate)?,
            timestamp: rng.random(),
            method: rng.random(),
            path: rng.random(),
            protocol: rng.random(),
            status_code: rng.random(),
            bytes_out: rng.random(),
        })
    }
}

impl crate::Serialize for ApacheCommon {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        self.last_layout.clear();

        let mut bytes_remaining = max_bytes;
        // Reuse a single buffer across iterations to avoid repeated allocations.
        let mut buffer: Vec<u8> = Vec::with_capacity(256);
        let mut line_start: u32 = 0;

        loop {
            let member: Member = self.generate(&mut rng)?;
            buffer.clear();

            let slot_offset = match self.slot_flavor {
                None => {
                    // Default behavior: format the whole member including
                    // the random apache timestamp. No layout entry.
                    write!(&mut buffer, "{member}").expect("formatting to Vec<u8> cannot fail");
                    None
                }
                Some(flavor) => {
                    // Slotted behavior: emit
                    //   <host> - <user> [<placeholder>] "<method> <path> <protocol>" <status> <bytes>
                    // recording the slot position inside the brackets.
                    write!(&mut buffer, "{} - {} [", member.host, member.user)
                        .expect("formatting to Vec<u8> cannot fail");
                    let off = u32::try_from(buffer.len()).map_err(|_| Error::StringGenerate)?;

                    let slot_w = slot_width_for(flavor);
                    buffer.extend(std::iter::repeat_n(SLOT_PLACEHOLDER, slot_w));

                    write!(
                        &mut buffer,
                        "] \"{} {} {}\" {} {}",
                        member.method,
                        member.path,
                        member.protocol,
                        member.status_code.0,
                        member.bytes_out
                    )
                    .expect("formatting to Vec<u8> cannot fail");

                    Some((off, flavor))
                }
            };

            let line_length = buffer.len() + 1; // +1 for '\n'
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writer.write_all(&buffer)?;
                    writer.write_all(b"\n")?;
                    bytes_remaining = remainder;

                    if let Some((off, flavor)) = slot_offset {
                        let line_length_u32 = u32::try_from(line_length)
                            .map_err(|_| Error::StringGenerate)?;
                        self.last_layout.push(LineEntry {
                            line_start,
                            line_length: line_length_u32,
                            slot_offset: off,
                            flavor,
                        });
                        line_start = line_start
                            .checked_add(line_length_u32)
                            .ok_or(Error::StringGenerate)?;
                    }
                }
                None => break,
            }
        }
        Ok(())
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

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::{ApacheCommon, Config};
    use crate::{
        Serialize,
        line_layout::{
            APACHE_COMMON_TIMESTAMP_WIDTH, OPAQUE_TOKEN_WIDTH, SLOT_PLACEHOLDER, SlotFlavor,
            UniqueConfig, UniqueFlavor,
        },
    };

    // --- Test 1+2: plain mode preserves prior behavior ---

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut apache = ApacheCommon::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            apache.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            prop_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
            // No layout in plain mode.
            prop_assert!(apache.line_layout().is_none());
        }
    }

    // --- Test 3+4: opaque slot mode line shape and layout ---

    #[test]
    fn opaque_slot_lines_have_correct_framing() {
        let mut rng = SmallRng::seed_from_u64(11);
        let mut apache = ApacheCommon::with_config(
            &mut rng,
            Config {
                unique: Some(UniqueConfig {
                    flavor: UniqueFlavor::OpaqueToken,
                    microseconds_per_line: 1000,
                }),
            },
        );
        let mut buf = Vec::new();
        apache.to_bytes(&mut rng, 30_000, &mut buf).unwrap();

        let layout = apache
            .line_layout()
            .expect("opaque slot mode emits a layout")
            .to_vec();
        assert!(!layout.is_empty(), "expected at least one line written");

        let slot_w = OPAQUE_TOKEN_WIDTH as usize;
        for entry in &layout {
            // Slot is in the timestamp position: `[<placeholders>] `.
            let slot_start = (entry.line_start + entry.slot_offset) as usize;
            let slot_end = slot_start + slot_w;
            // Bytes immediately before slot_offset must be `[`.
            assert_eq!(buf[slot_start - 1], b'[');
            // Slot is all placeholder bytes.
            for (i, &b) in buf[slot_start..slot_end].iter().enumerate() {
                assert_eq!(
                    b, SLOT_PLACEHOLDER,
                    "slot byte {i} at {} not placeholder: {}",
                    slot_start + i,
                    b as char
                );
            }
            // Bytes immediately after slot must be `] `.
            assert_eq!(&buf[slot_end..slot_end + 2], b"] ");
            // Line ends with newline.
            let line_end = (entry.line_start + entry.line_length) as usize;
            assert_eq!(buf[line_end - 1], b'\n');
            assert_eq!(entry.flavor, SlotFlavor::OpaqueToken);
        }

        // line_starts chain across the buffer.
        let mut expected = 0u32;
        for entry in &layout {
            assert_eq!(entry.line_start, expected);
            expected += entry.line_length;
        }
        assert_eq!(expected as usize, buf.len());
    }

    // --- Test 5: timestamp flavor — apache CLF microsecond format ---

    #[test]
    fn timestamp_slot_lines_have_apache_clf_framing() {
        let mut rng = SmallRng::seed_from_u64(23);
        let mut apache = ApacheCommon::with_config(
            &mut rng,
            Config {
                unique: Some(UniqueConfig {
                    flavor: UniqueFlavor::Timestamp,
                    microseconds_per_line: 1000,
                }),
            },
        );
        let mut buf = Vec::new();
        apache.to_bytes(&mut rng, 30_000, &mut buf).unwrap();

        let layout = apache.line_layout().expect("timestamp slot mode emits layout");
        assert!(!layout.is_empty());

        let slot_w = APACHE_COMMON_TIMESTAMP_WIDTH as usize;
        for entry in layout {
            assert_eq!(entry.flavor, SlotFlavor::ApacheCommonTimestamp);
            let slot_start = (entry.line_start + entry.slot_offset) as usize;
            let slot_end = slot_start + slot_w;
            assert_eq!(buf[slot_start - 1], b'[');
            for &b in &buf[slot_start..slot_end] {
                assert_eq!(b, SLOT_PLACEHOLDER);
            }
            assert_eq!(&buf[slot_end..slot_end + 2], b"] ");
        }
    }

    // --- Test 6: slot mode respects max_bytes ---

    proptest! {
        #[test]
        fn slot_mode_respects_max_bytes(seed: u64, max_bytes in 0u16..u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut apache = ApacheCommon::with_config(
                &mut rng,
                Config { unique: Some(UniqueConfig { flavor: UniqueFlavor::OpaqueToken, microseconds_per_line: 1000 }) },
            );

            let mut buf = Vec::with_capacity(max_bytes);
            apache.to_bytes(rng, max_bytes, &mut buf).expect("to_bytes");
            prop_assert!(buf.len() <= max_bytes);
        }
    }

    // --- Test 7+8: end-to-end uniqueness via real Cache + rewrite_slots ---

    fn assert_rewrite_path_unique(flavor: UniqueFlavor) {
        use crate::block::Cache;
        use crate::line_layout;
        use std::num::NonZeroU32;

        let mut rng = SmallRng::seed_from_u64(31);
        let variant = crate::Config::ApacheCommon(Config {
            unique: Some(UniqueConfig {
                flavor,
                microseconds_per_line: 1000,
            }),
        });
        // 16 KiB cache; apache common lines are ~150 bytes so this gives
        // ~100 lines per cache cycle.
        let total_bytes = NonZeroU32::new(16 * 1024).unwrap();
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
            .expect("apache_common with unique config produces a layout");
        assert!(!layout.entries.is_empty());
        let total_cycle = u64::from(layout.total_cycle_size);
        assert!(total_cycle > 0);

        let line_len = layout.entries[0].line_length as usize;
        let raw = cache.read_at(0, line_len);

        // Determinism.
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
        assert_eq!(buf_a1, buf_a2);

        // Cross-file: different inode.
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
        assert_ne!(buf_a1, buf_b);

        // Cache wrap.
        let wrapped = cache.read_at(total_cycle, line_len);
        assert_eq!(raw, wrapped);
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
        assert_ne!(buf_a1, buf_c);
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
