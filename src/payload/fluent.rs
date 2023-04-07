//! Implements [this
//! protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1).
use std::{collections::HashMap, io::Write};

use rand::{distributions::Standard, prelude::Distribution, Rng};
use serde_tuple::Serialize_tuple;

use super::{common::AsciiString, Generator};
use crate::payload::{Error, Serialize};

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct Fluent {}

pub(crate) type Object = HashMap<String, u8>;

#[derive(serde::Serialize)]
#[serde(untagged)]
enum RecordValue {
    String(String),
    Object(Object),
}

impl Distribution<RecordValue> for Standard {
    fn sample<R>(&self, rng: &mut R) -> RecordValue
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..2) {
            0 => RecordValue::String(AsciiString::default().generate(rng)),
            1 => {
                let mut obj = HashMap::new();
                for _ in 0..rng.gen_range(0..128) {
                    let key = AsciiString::default().generate(rng);
                    let val = rng.gen();

                    obj.insert(key, val);
                }
                RecordValue::Object(obj)
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize_tuple)]
struct Entry {
    time: u32,
    record: HashMap<String, RecordValue>, // always contains 'message' and 'event' -> object key
}

impl Distribution<Entry> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Entry
    where
        R: Rng + ?Sized,
    {
        let mut rec = HashMap::new();
        rec.insert(String::from("message"), rng.gen());
        rec.insert(String::from("event"), rng.gen());
        for _ in 0..rng.gen_range(0..128) {
            let key = AsciiString::default().generate(rng);
            let val = rng.gen();

            rec.insert(key, val);
        }
        Entry {
            time: rng.gen(),
            record: rec,
        }
    }
}

#[derive(Serialize_tuple)]
struct FluentForward {
    tag: String,
    entries: Vec<Entry>,
}

impl Distribution<FluentForward> for Standard {
    fn sample<R>(&self, rng: &mut R) -> FluentForward
    where
        R: Rng + ?Sized,
    {
        let total_entries = rng.gen_range(0..32);
        FluentForward {
            tag: AsciiString::default().generate(rng),
            entries: rng.sample_iter(Standard).take(total_entries).collect(),
        }
    }
}

#[derive(serde::Serialize)]
struct FluentMessage {
    tag: String,
    time: u32,
    record: HashMap<String, RecordValue>, // always contains 'message' key
}

impl Distribution<FluentMessage> for Standard {
    fn sample<R>(&self, rng: &mut R) -> FluentMessage
    where
        R: Rng + ?Sized,
    {
        let mut rec = HashMap::new();
        rec.insert(String::from("message"), rng.gen());
        for _ in 0..rng.gen_range(0..128) {
            let key = AsciiString::default().generate(rng);
            let val = rng.gen();

            rec.insert(key, val);
        }
        FluentMessage {
            tag: AsciiString::default().generate(rng),
            time: rng.gen(),
            record: rec,
        }
    }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum Member {
    Message(FluentMessage),
    Forward(FluentForward),
}

impl Distribution<Member> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..2) {
            0 => Member::Message(rng.gen()),
            1 => Member::Forward(rng.gen()),
            _ => unimplemented!(),
        }
    }
}

impl Serialize for Fluent {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        if max_bytes < 16 {
            // 16 is just an arbitrarily big constant
            return Ok(());
        }

        // We will arbitrarily generate 1_000 Member instances and then
        // serialize. If this is below `max_bytes` we'll add more until we're
        // over. Once we are we'll start removing instances until we're back
        // below the limit.

        let mut members: Vec<Vec<u8>> = Standard
            .sample_iter(&mut rng)
            .take(1_000)
            .map(|m: Member| rmp_serde::to_vec(&m).unwrap())
            .collect();

        // Search for too many Member instances.
        loop {
            let encoding_len = members[0..].iter().fold(0, |acc, m| acc + m.len());
            if encoding_len > max_bytes {
                break;
            }

            members.extend(
                Standard
                    .sample_iter(&mut rng)
                    .take(100)
                    .map(|m: Member| rmp_serde::to_vec(&m).unwrap()),
            );
        }

        // Search for an encoding that's just right.
        let mut high = members.len();
        loop {
            let encoding_len = members[0..high].iter().fold(0, |acc, m| acc + m.len());

            if encoding_len > max_bytes {
                high /= 16;
            } else {
                for m in &members[0..high] {
                    writer.write_all(m)?;
                }
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

    use crate::payload::{Fluent, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let fluent = Fluent::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            fluent.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }
}
