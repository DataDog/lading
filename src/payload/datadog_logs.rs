use std::io::Write;

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use crate::payload::{common::AsciiString, Error, Generator, Serialize};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Notice,
    Info,
    Warning,
}

impl Distribution<Status> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Status
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..3) {
            0 => Status::Notice,
            1 => Status::Info,
            2 => Status::Warning,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Hostname {
    Alpha,
    Beta,
    Gamma,
    Localhost,
}

impl Distribution<Hostname> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Hostname
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..4) {
            0 => Hostname::Alpha,
            1 => Hostname::Beta,
            2 => Hostname::Gamma,
            3 => Hostname::Localhost,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Service {
    Vector,
    Lading,
    Cernan,
}

impl Distribution<Service> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Service
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..3) {
            0 => Service::Vector,
            1 => Service::Lading,
            2 => Service::Cernan,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    Bergman,
    Keaton,
    Kurosawa,
    Lynch,
    Waters,
    Tarkovsky,
}

impl Distribution<Source> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Source
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..6) {
            0 => Source::Bergman,
            1 => Source::Keaton,
            2 => Source::Kurosawa,
            3 => Source::Lynch,
            4 => Source::Waters,
            5 => Source::Tarkovsky,
            _ => unreachable!(),
        }
    }
}

const TAG_OPTIONS: [&str; 4] = ["", "env:prod", "env:dev", "env:prod,version:1.1"];

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Structured {
    proportional: u32,
    integral: u64,
    derivative: f64,
    vegetable: i16,
    mineral: String,
}

impl Distribution<Structured> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Structured
    where
        R: Rng + ?Sized,
    {
        let mineral: String = AsciiString::default().generate(rng).expect("must not fail");

        Structured {
            proportional: rng.gen(),
            integral: rng.gen(),
            derivative: rng.gen(),
            vegetable: rng.gen(),
            mineral,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum Message {
    Unstructured(String),
    Structured(String),
}

impl Distribution<Message> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Message
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..2) {
            0 => {
                Message::Unstructured(AsciiString::default().generate(rng).expect("must not fail"))
            }
            1 => Message::Structured(serde_json::to_string(&rng.gen::<Structured>()).unwrap()),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
struct Member {
    /// The message is a short ascii string, without newlines for now
    pub(crate) message: Message,
    /// The message status
    pub(crate) status: Status,
    /// The timestamp is a simple integer value since epoch, presumably
    pub(crate) timestamp: u32,
    /// The hostname that sent the logs
    pub(crate) hostname: Hostname,
    /// The service that sent the logs
    pub(crate) service: Service,
    /// The ultimate source of the logs
    pub(crate) ddsource: Source,
    /// Comma-separate list of tags
    pub(crate) ddtags: String,
}

impl Distribution<Member> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        Member {
            message: rng.gen(),
            status: rng.gen(),
            timestamp: rng.gen(),
            hostname: rng.gen(),
            service: rng.gen(),
            ddsource: rng.gen(),
            ddtags: (*TAG_OPTIONS.choose(rng).unwrap()).to_string(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct DatadogLog {}

impl Serialize for DatadogLog {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        if max_bytes < 2 {
            // 'empty' payload  is []
            return Ok(());
        }

        // We will arbitrarily generate 1_000 Member instances and then
        // serialize. If this is below `max_bytes` we'll add more until we're
        // over. Once we are we'll start removing instances until we're back
        // below the limit.

        let mut members: Vec<Member> = Standard.sample_iter(&mut rng).take(1_000).collect();

        // Search for too many Member instances.
        loop {
            let encoding = serde_json::to_string(&members)?;
            if encoding.len() > max_bytes {
                break;
            }
            members.extend(Standard.sample_iter(&mut rng).take(100));
        }

        // Search for an encoding that's just right.
        let mut high = members.len();
        loop {
            let encoding = serde_json::to_string(&members[0..high])?;
            if encoding.len() > max_bytes {
                high /= 16;
            } else {
                write!(writer, "{encoding}")?;
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use super::Member;
    use crate::payload::{DatadogLog, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let ddlogs = DatadogLog::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as json, is not truncated etc.
    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let ddlogs = DatadogLog::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            let payload = std::str::from_utf8(&bytes).unwrap();
            for msg in payload.lines() {
                let _members: Vec<Member> = serde_json::from_str(msg).unwrap();
            }
        }
    }
}
