//! Datadog Logs payload.

use std::io::Write;

use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};
use serde::Deserialize;

use crate::{Error, Generator, common::config::ConfRange, common::strings};

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

/// Configure the `DatadogLog` payload.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Range of message body sizes in bytes
    pub message_size: ConfRange<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            message_size: ConfRange::Inclusive { min: 1, max: 15 },
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Structured {
    proportional: u32,
    integral: u64,
    derivative: f64,
    vegetable: i16,
}

impl Distribution<Structured> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Structured
    where
        R: Rng + ?Sized,
    {
        Structured {
            proportional: rng.random(),
            integral: rng.random(),
            derivative: rng.random(),
            vegetable: rng.random(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum Message<'a> {
    Unstructured(&'a str),
    Structured(String),
}

fn message<'a, R>(
    rng: &mut R,
    str_pool: &'a strings::RandomStringPool,
    config: Config,
) -> Message<'a>
where
    R: rand::Rng + ?Sized,
{
    let size = config.message_size.sample(rng) as usize;
    match rng.random_range(0..2) {
        0 => Message::Unstructured(
            str_pool
                .of_size(rng, size)
                .expect("failed to generate string"),
        ),
        1 => Message::Structured(
            serde_json::to_string(&rng.random::<Structured>()).expect("failed to generate string"),
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
    str_pool: strings::RandomStringPool,
    config: Config,
}

impl DatadogLog {
    /// Create a new instance of `DatadogLog`
    pub fn new<R>(config: &Config, rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::RandomStringPool::with_size(rng, 1_000_000),
            config: *config,
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
            message: message(&mut rng, &self.str_pool, self.config),
            status: STATUSES.choose(rng).expect("failed to generate status"),
            timestamp: rng.random(),
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
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        let approx_member_encoded_size = 220; // bytes, determined experimentally

        if max_bytes < approx_member_encoded_size {
            // 'empty' payload  is []
            return Ok(());
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
        // Reuse a single buffer across iterations to avoid repeated allocations.
        let mut buffer: Vec<u8> = Vec::with_capacity(max_bytes);
        let mut high = members.len();
        while high != 0 {
            buffer.clear();
            serde_json::to_writer(&mut buffer, &members[0..high])?;
            if buffer.len() > max_bytes {
                high /= 2;
            } else {
                writer.write_all(&buffer)?;
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::{Config, Member};
    use crate::{DatadogLog, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut ddlogs = DatadogLog::new(&Config::default(), &mut rng);

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
            let mut ddlogs = DatadogLog::new(&Config::default(), &mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            let payload = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str");
            for msg in payload.lines() {
                let _members: Vec<Member> = serde_json::from_str(msg).expect("failed to deserialize from str");
            }
        }
    }
}
