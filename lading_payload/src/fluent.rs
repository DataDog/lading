//! Fluentd payload.
//!
//! Implements [this
//! protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1).
use std::io::Write;

use rand::Rng;
use rustc_hash::FxHashMap;
use serde_tuple::Serialize_tuple;

use crate::{common::strings, Error, Generator};

#[derive(Debug, Clone)]
/// Fluent payload
pub struct Fluent {
    str_pool: strings::Pool,
}

impl Fluent {
    /// Construct a new instance of `Fluent`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for Fluent {
    type Output = Member<'a>;

    fn generate<R>(&'a self, rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        match rng.gen_range(0..2) {
            0 => {
                let mut rec = FxHashMap::default();
                rec.insert("message", record_value(rng, &self.str_pool));
                for _ in 0..rng.gen_range(0..128) {
                    let key = self.str_pool.of_size_range(rng, 1_u8..16).unwrap();
                    let val = record_value(rng, &self.str_pool);
                    rec.insert(key, val);
                }
                Member::Message(FluentMessage {
                    tag: self.str_pool.of_size_range(rng, 1_u8..16).unwrap(),
                    time: rng.gen(),
                    record: rec,
                })
            }
            1 => {
                let mut entries = Vec::with_capacity(32);
                for _ in 0..32 {
                    let mut rec = FxHashMap::default();
                    rec.insert("message", record_value(rng, &self.str_pool));
                    rec.insert("event", record_value(rng, &self.str_pool));
                    for _ in 0..rng.gen_range(0..128) {
                        let key = self.str_pool.of_size_range(rng, 1_u8..16).unwrap();
                        let val = record_value(rng, &self.str_pool);
                        rec.insert(key, val);
                    }
                    entries.push(Entry {
                        time: rng.gen(),
                        record: rec,
                    });
                }

                Member::Forward(FluentForward {
                    tag: self.str_pool.of_size_range(rng, 1_u8..16).unwrap(),
                    entries,
                })
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
pub(crate) enum Member<'a> {
    Message(FluentMessage<'a>),
    Forward(FluentForward<'a>),
}

#[derive(serde::Serialize)]
pub(crate) struct FluentMessage<'a> {
    tag: &'a str,
    time: u32,
    record: FxHashMap<&'a str, RecordValue<'a>>, // always contains 'message' key
}

#[derive(Serialize_tuple)]
pub(crate) struct FluentForward<'a> {
    tag: &'a str,
    entries: Vec<Entry<'a>>,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum RecordValue<'a> {
    String(&'a str),
    Object(FxHashMap<&'a str, u8>),
}

fn record_value<'a, R>(rng: &mut R, str_pool: &'a strings::Pool) -> RecordValue<'a>
where
    R: rand::Rng + ?Sized,
{
    match rng.gen_range(0..2) {
        0 => RecordValue::String(str_pool.of_size_range(rng, 1_u8..16).unwrap()),
        1 => {
            let mut obj = FxHashMap::default();
            for _ in 0..rng.gen_range(0..128) {
                let key = str_pool.of_size_range(rng, 1_u8..16).unwrap();
                let val = rng.gen();

                obj.insert(key, val);
            }
            RecordValue::Object(obj)
        }
        _ => unreachable!(),
    }
}

#[derive(Serialize_tuple)]
struct Entry<'a> {
    time: u32,
    record: FxHashMap<&'a str, RecordValue<'a>>, // always contains 'message' and 'event' -> object key
}

impl crate::Serialize for Fluent {
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

        let mut members: Vec<Vec<u8>> = (0..10)
            .map(|_| self.generate(&mut rng))
            .map(|m: Member| rmp_serde::to_vec(&m).unwrap())
            .collect();

        // Search for too many Member instances.
        loop {
            let encoding_len = members[0..].iter().fold(0, |acc, m| acc + m.len());
            if encoding_len > max_bytes {
                break;
            }

            members.extend(
                (0..10)
                    .map(|_| self.generate(&mut rng))
                    .map(|m: Member| rmp_serde::to_vec(&m).unwrap()),
            );
        }

        // Search for an encoding that's just right.
        let mut high = members.len();
        loop {
            let encoding_len = members[0..high].iter().fold(0, |acc, m| acc + m.len());

            if encoding_len > max_bytes {
                high /= 2;
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

    use crate::{Fluent, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let fluent = Fluent::new(&mut rng);

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
