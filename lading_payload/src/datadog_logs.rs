//! Datadog Logs payload.

use std::io::Write;

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use crate::{block::SplitStrategy, common::strings, Error, Generator};

const STATUSES: [&str; 3] = ["notice", "info", "warning"];
const HOSTNAMES: [&str; 4] = ["alpha", "beta", "gamma", "localhost"];
const SERVICES: [&str; 4] = ["vector", "lading", "cernan", "agent"];
const SOURCES: [&str; 7] = [
    "bergman",
    "keaton",
    "kurosawa",
    "lynch",
    "waters",
    "tarkovsky",
    "herzog",
];
const TAG_OPTIONS: [&str; 4] = ["", "env:prod", "env:dev", "env:prod,version:1.1"];

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Structured {
    proportional: u32,
    integral: u64,
    derivative: f64,
    vegetable: i16,
}

impl Distribution<Structured> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Structured
    where
        R: Rng + ?Sized,
    {
        Structured {
            proportional: rng.gen(),
            integral: rng.gen(),
            derivative: rng.gen(),
            vegetable: rng.gen(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum Message<'a> {
    Unstructured(&'a str),
    Structured(String),
}

fn message<'a, R>(rng: &mut R, str_pool: &'a strings::Pool) -> Message<'a>
where
    R: rand::Rng + ?Sized,
{
    match rng.gen_range(0..2) {
        0 => Message::Unstructured(
            str_pool
                .of_size_range(rng, 1_u8..16)
                .expect("failed to generate string"),
        ),
        1 => Message::Structured(
            serde_json::to_string(&rng.gen::<Structured>()).expect("failed to generate string"),
        ),
        _ => unreachable!(),
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
/// Derived from Datadog Agent sources, [here](https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49).
pub struct Member<'a> {
    /// The message is a short ascii string, without newlines for now
    pub(crate) message: Message<'a>,
    /// The message status
    pub(crate) status: &'a str,
    /// The timestamp is a simple integer value since epoch, presumably
    pub(crate) timestamp: u32,
    /// The hostname that sent the logs
    pub(crate) hostname: &'a str,
    /// The service that sent the logs
    pub(crate) service: &'a str,
    /// The ultimate source of the logs
    pub(crate) ddsource: &'a str,
    /// Comma-separate list of tags
    pub(crate) ddtags: &'a str,
}

#[derive(Debug)]
/// Datadog log format payload
pub struct DatadogLog {
    str_pool: strings::Pool,
}

impl DatadogLog {
    /// Create a new instance of `DatadogLog`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for DatadogLog {
    type Output = Member<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        Ok(Member {
            message: message(&mut rng, &self.str_pool),
            status: STATUSES.choose(rng).expect("failed to generate status"),
            timestamp: rng.gen(),
            hostname: HOSTNAMES.choose(rng).expect("failed to generate hostnames"),
            service: SERVICES.choose(rng).expect("failed to generate services"),
            ddsource: SOURCES.choose(rng).expect("failed to generate sources"),
            ddtags: TAG_OPTIONS
                .choose(rng)
                .expect("failed to generate tag options"),
        })
    }
}

impl crate::Serialize for DatadogLog {
    fn to_bytes<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<SplitStrategy, Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        let approx_member_encoded_size = 220; // bytes, determined experimentally

        if max_bytes < approx_member_encoded_size {
            // 'empty' payload  is []
            return Ok(SplitStrategy::None);
        }

        // We will arbitrarily generate Member instances and then serialize. If
        // this is below `max_bytes` we'll add more until we're over. Once we
        // are we'll start removing instances until we're back below the limit.

        let cap = (max_bytes / approx_member_encoded_size) + 100;
        let mut members: Vec<Member> = Vec::with_capacity(cap);
        for _ in 0..cap {
            members.push(self.generate(&mut rng)?);
        }

        // Search for an encoding that's just right.
        let mut high = members.len();
        while high != 0 {
            let encoding = serde_json::to_string(&members[0..high])?;
            if encoding.len() > max_bytes {
                high /= 2;
            } else {
                write!(writer, "{encoding}")?;
                break;
            }
        }
        Ok(SplitStrategy::None)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use super::Member;
    use crate::{DatadogLog, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let ddlogs = DatadogLog::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as json, is not truncated etc.
    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let ddlogs = DatadogLog::new(&mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            let payload = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str");
            for msg in payload.lines() {
                let _members: Vec<Member> = serde_json::from_str(msg).expect("failed to deserialize from str");
            }
        }
    }
}
