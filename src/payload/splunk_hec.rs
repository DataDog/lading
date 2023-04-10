use std::io::Write;

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};
use serde::Deserialize;

use crate::payload::{Error, Serialize};

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

impl Distribution<Attrs> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Attrs
    where
        R: Rng + ?Sized,
    {
        Attrs {
            system_id: String::from(*SYSTEM_IDS.choose(rng).unwrap()),
            stage: String::from(*STAGES.choose(rng).unwrap()),
            event_type: String::from(*EVENT_TYPES.choose(rng).unwrap()),
            c2c_service: String::from(*SERVICES.choose(rng).unwrap()),
            c2c_partition: String::from(*PARTITIONS.choose(rng).unwrap()),
            c2c_stage: String::from(*STAGES.choose(rng).unwrap()),
            c2c_container_type: String::from(*CONTAINER_TYPES.choose(rng).unwrap()),
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

impl Distribution<Event> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Event
    where
        R: Rng + ?Sized,
    {
        Event {
            timestamp: 1_606_215_269.333_915,
            message: String::from(*MESSAGES.choose(rng).unwrap()),
            attrs: rng.gen(),
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

impl Distribution<Member> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        Member {
            event: rng.gen(),
            time: rng.gen(),
            host: String::from(*SYSTEM_IDS.choose(rng).unwrap()),
            index: String::from(*PARTITIONS.choose(rng).unwrap()),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Encoding {
    Text,
    Json,
}

impl Default for Encoding {
    fn default() -> Self {
        Self::Json
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct SplunkHec {
    encoding: Encoding,
}

impl SplunkHec {
    /// Create a new instance of [`SplunkHec`]
    #[must_use]
    pub(crate) fn new(encoding: Encoding) -> Self {
        Self { encoding }
    }
}

impl Serialize for SplunkHec {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let member: Member = rng.gen();
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
    use rand::{rngs::SmallRng, SeedableRng};

    use super::Member;
    use crate::payload::{Serialize, SplunkHec};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let hec = SplunkHec::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            hec.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            assert!(bytes.len() <= max_bytes);
        }
    }

    // We want to be sure that the serialized size of the payload is not zero.
    proptest! {
        #[test]
        fn payload_not_zero_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let hec = SplunkHec::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            hec.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() != 0,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as splunk's hec, is not truncated etc.
    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes in 0..u16::MAX)  {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let hec = SplunkHec::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            hec.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            let payload = std::str::from_utf8(&bytes).unwrap();
            for msg in payload.lines() {
                let _members: Member = serde_json::from_str(msg).unwrap();
            }
        }
    }
}
