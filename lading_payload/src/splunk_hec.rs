//! Splunk HEC payload

use std::io::Write;

use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};
use serde::{Deserialize, Serialize};

use crate::Error;

const PARTITIONS: [&str; 4] = ["eu", "eu2", "ap1", "us1"];
const STAGES: [&str; 4] = ["production", "performance", "noprod", "staging"];
const CONTAINER_TYPES: [&str; 1] = ["ingress"];
const EVENT_TYPES: [&str; 1] = ["service"];
const SYSTEM_IDS: [&str; 4] = ["one", "two", "three", "four"];
const SERVICES: [&str; 7] = [
    "tablet",
    "phone",
    "phone2",
    "laptop",
    "desktop",
    "monitor",
    "bigger-monitor",
];
const MESSAGES: [&str; 5] = [
    "Es war ein Mann im Lande Uz, der hieß Hiob. Derselbe war schlecht und recht, gottesfürchtig und mied das Böse.",
    "Und zeugte sieben Söhne und drei Töchter;",
    "und seines Viehs waren siebentausend Schafe, dreitausend Kamele, fünfhundert Joch Rinder und fünfhundert Eselinnen, und er hatte viel Gesinde; und er war herrlicher denn alle, die gegen Morgen wohnten.",
    "Und seine Söhne gingen und machten ein Mahl, ein jeglicher in seinem Hause auf seinen Tag, und sandten hin und luden ihre drei Schwestern, mit ihnen zu essen und zu trinken",
    "Und wenn die Tage des Mahls um waren, sandte Hiob hin und heiligte sie und machte sich des Morgens früh auf und opferte Brandopfer nach ihrer aller Zahl; denn Hiob gedachte: Meine Söhne möchten gesündigt und Gott abgesagt haben in ihrem Herzen. Also tat Hiob allezeit.",
];

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Attrs {
    #[serde(rename = "systemid")]
    pub(crate) system_id: String,
    pub(crate) stage: String,
    #[serde(rename = "type")]
    pub(crate) event_type: String,
    #[serde(rename = "c2cService")]
    pub(crate) c2c_service: String,
    #[serde(rename = "c2cPartition")]
    pub(crate) c2c_partition: String,
    #[serde(rename = "c2cStage")]
    pub(crate) c2c_stage: String, // same as
    #[serde(rename = "c2cContainerType")]
    pub(crate) c2c_container_type: String,
    pub(crate) aws_account: String,
}

impl Distribution<Attrs> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Attrs
    where
        R: Rng + ?Sized,
    {
        Attrs {
            system_id: String::from(*SYSTEM_IDS.choose(rng).expect("failed to choose system ids")),
            stage: String::from(*STAGES.choose(rng).expect("failed to choose stages")),
            event_type: String::from(
                *EVENT_TYPES
                    .choose(rng)
                    .expect("failed to choose event types"),
            ),
            c2c_service: String::from(*SERVICES.choose(rng).expect("failed to choose services")),
            c2c_partition: String::from(
                *PARTITIONS.choose(rng).expect("failed to choose partitions"),
            ),
            c2c_stage: String::from(*STAGES.choose(rng).expect("failed to choose stages")),
            c2c_container_type: String::from(
                *CONTAINER_TYPES
                    .choose(rng)
                    .expect("failed to choose container types"),
            ),
            aws_account: String::from("verymodelofthemodernmajor"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Event {
    pub(crate) timestamp: f64,
    pub(crate) message: String,
    attrs: Attrs,
}

impl Distribution<Event> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Event
    where
        R: Rng + ?Sized,
    {
        Event {
            timestamp: 1_606_215_269.333_915,
            message: String::from(*MESSAGES.choose(rng).expect("failed to choose messages")),
            attrs: rng.random(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Member {
    pub(crate) event: Event,
    pub(crate) time: f64,
    pub(crate) host: String,
    pub(crate) index: String,
}

impl Distribution<Member> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        Member {
            event: rng.random(),
            time: rng.random(),
            host: String::from(*SYSTEM_IDS.choose(rng).expect("failed to choose system ids")),
            index: String::from(*PARTITIONS.choose(rng).expect("failed to choose partitions")),
        }
    }
}

/// Encoding to be used
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Encoding {
    /// Use text-encoded log messages
    Text,
    /// Use JSON-encoded log messages
    Json,
}

impl Default for Encoding {
    fn default() -> Self {
        Self::Json
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
/// Splunk's HEC
pub struct SplunkHec {
    encoding: Encoding,
}

impl SplunkHec {
    /// Create a new instance of [`SplunkHec`]
    #[must_use]
    pub fn new(encoding: Encoding) -> Self {
        Self { encoding }
    }
}

impl crate::Serialize for SplunkHec {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let member: Member = rng.random();
            let encoding = match self.encoding {
                Encoding::Text => {
                    let event = member.event;
                    format!(
                        "{} {} {}",
                        event.timestamp,
                        event.message,
                        serde_json::to_string(&event.attrs)?
                    )
                }
                Encoding::Json => serde_json::to_string(&member)?,
            };
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
    use rand::{SeedableRng, rngs::SmallRng};

    use super::Member;
    use crate::{Serialize, SplunkHec};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let mut hec = SplunkHec::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            hec.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            assert!(bytes.len() <= max_bytes);
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as splunk's hec, is not truncated etc.
    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes in 0..u16::MAX)  {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let mut hec = SplunkHec::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            hec.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            let payload = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str");
            for msg in payload.lines() {
                let _members: Member = serde_json::from_str(msg).expect("failed to deserialize from str");
            }
        }
    }
}
