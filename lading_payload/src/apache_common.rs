//! Apache common payload.

use crate::{common::strings, Error, Generator};

use core::fmt;
use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};
use std::io::Write;

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

impl Distribution<StatusCode> for Standard {
    fn sample<R>(&self, rng: &mut R) -> StatusCode
    where
        R: Rng + ?Sized,
    {
        StatusCode(
            *STATUS_CODES
                .choose(rng)
                .expect("Error: failed to choose status codes"),
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

impl Distribution<Protocol> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Protocol
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..6) {
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

impl Distribution<Path> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Path
    where
        R: Rng + ?Sized,
    {
        let total_components: usize = rng.gen_range(1..=16);
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

impl Distribution<Month> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Month
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..12) {
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

impl Distribution<Timestamp> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Timestamp
    where
        R: Rng + ?Sized,
    {
        Timestamp {
            day: rng.gen(),
            month: rng.gen(),
            year: rng.gen(),
            hour: rng.gen(),
            minute: rng.gen(),
            second: rng.gen(),
            timezone: rng.gen(),
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

impl Distribution<IpV4> for Standard {
    fn sample<R>(&self, rng: &mut R) -> IpV4
    where
        R: Rng + ?Sized,
    {
        IpV4 {
            zero: rng.gen(),
            one: rng.gen(),
            two: rng.gen(),
            three: rng.gen(),
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

impl Distribution<Method> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Method
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..5) {
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

impl<'a> fmt::Display for Member<'a> {
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
    str_pool: strings::Pool,
}

impl ApacheCommon {
    /// Construct a new instance of `ApacheCommon`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for ApacheCommon {
    type Output = Member<'a>;

    fn generate<R>(&'a self, mut rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        Member {
            host: rng.gen(),
            user: self
                .str_pool
                .of_size_range(&mut rng, 1_u16..16_u16)
                .expect("Error: failed to generate string"),
            timestamp: rng.gen(),
            method: rng.gen(),
            path: rng.gen(),
            protocol: rng.gen(),
            status_code: rng.gen(),
            bytes_out: rng.gen(),
        }
    }
}

impl crate::Serialize for ApacheCommon {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let member: Member = self.generate(&mut rng);
            let encoding = format!("{member}");
            let line_length = encoding.len() + 1; // add one for the newline
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
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::{ApacheCommon, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let apache = ApacheCommon::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            apache.to_bytes(rng, max_bytes, &mut bytes).expect("Error: failed to convert to bytes");
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("Error: failed to convert from utf-8 to str")
            );
        }
    }
}
