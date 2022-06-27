use crate::payload::{Error, Serialize};

use arbitrary::{self, Arbitrary, Unstructured};
use core::fmt;
use rand::Rng;
use std::{io::Write, mem};

use super::common::AsciiStr;

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

impl<'a> Arbitrary<'a> for StatusCode {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(StatusCode(*u.choose(&STATUS_CODES)?))
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (mem::size_of::<u16>(), Some(mem::size_of::<u16>()))
    }
}

#[derive(Arbitrary, Debug)]
enum Protocol {
    Http10,
    Http11,
    Http12,
    Http20,
    Http21,
    Http22,
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
        write!(f, "{}", s)
    }
}

#[derive(Debug)]
struct Path {
    components: Vec<&'static str>,
}

impl<'a> Arbitrary<'a> for Path {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let total_components: usize = (u.arbitrary::<usize>()? % 16) + 1;
        let mut components = Vec::with_capacity(total_components);

        for _ in 0..total_components {
            components.push(*u.choose(&PATH_NAMES)?);
        }

        Ok(Path { components })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        let max_component_bytes = 10;
        (max_component_bytes, Some(max_component_bytes * 16))
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for comp in &self.components {
            write!(f, "/{}", comp)?;
        }
        Ok(())
    }
}

#[derive(Arbitrary, Debug)]
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

#[derive(Arbitrary, Debug)]
struct Timestamp {
    day: u8,
    month: Month,
    year: i8,
    hour: u8,
    minute: u8,
    second: u8,
    timezone: u8,
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
            "{:02}/{}/{}:{:02}:{:02}:{:02} {:04}",
            day, month, year, hour, minute, second, timezone
        )
    }
}

#[derive(Arbitrary, Debug)]
struct User(AsciiStr);

impl fmt::Display for User {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

#[derive(Arbitrary, Debug)]
struct IpV4 {
    zero: u8,
    one: u8,
    two: u8,
    three: u8,
}

impl fmt::Display for IpV4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}.{}", self.zero, self.one, self.two, self.three,)
    }
}

#[derive(Arbitrary, Debug)]
enum Method {
    Get,
    Put,
    Post,
    Delete,
    Patch,
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
        write!(f, "{}", s)
    }
}

#[derive(Arbitrary, Debug)]
struct Member {
    host: IpV4,
    user: User,
    timestamp: Timestamp,
    method: Method,
    path: Path,
    protocol: Protocol,
    status_code: StatusCode,
    bytes_out: u16,
}

impl fmt::Display for Member {
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

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct ApacheCommon {}

impl Serialize for ApacheCommon {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        let mut bytes_remaining = max_bytes;
        while let Ok(member) = unstructured.arbitrary::<Member>() {
            let encoding = format!("{}", member);
            let line_length = encoding.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writeln!(writer, "{}", encoding)?;
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }
        Ok(())
    }
}
