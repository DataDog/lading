//! Implements [this
//! protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1).
use std::{collections::HashMap, io::Write};

use arbitrary::{size_hint, Unstructured};
use rand::{distributions::Standard, prelude::Distribution, Rng};
use serde_tuple::Serialize_tuple;

use super::{
    common::{AsciiStr, AsciiString},
    Generator,
};
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
            0 => RecordValue::String(AsciiString::default().generate(rng).unwrap()),
            1 => {
                let mut obj = HashMap::new();
                for _ in 0..rng.gen_range(0..128) {
                    let key = AsciiString::default().generate(rng).unwrap();
                    let val = rng.gen();

                    obj.insert(key, val);
                }
                RecordValue::Object(obj)
            }
            _ => unreachable!(),
        }
    }
}

fn object(u: &mut Unstructured<'_>) -> arbitrary::Result<RecordValue> {
    let mut obj = HashMap::new();
    for _ in 0..u.arbitrary_len::<(AsciiStr, u8)>()? {
        let key = u.arbitrary::<AsciiStr>()?;
        let msg = u.arbitrary::<u8>()?;
        obj.insert(key.as_str().to_string(), msg);
    }
    Ok(RecordValue::Object(obj))
}

impl<'a> arbitrary::Arbitrary<'a> for RecordValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let v = if u.arbitrary::<bool>()? {
            RecordValue::String(AsciiStr::arbitrary(u)?.as_str().to_string())
        } else {
            object(u)?
        };
        Ok(v)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <HashMap<String, u8> as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
    }
}

fn record(u: &mut Unstructured<'_>) -> arbitrary::Result<HashMap<String, RecordValue>> {
    let mut record = HashMap::new();

    let msg = u.arbitrary::<AsciiStr>()?;
    record.insert(
        "message".to_string(),
        RecordValue::String(msg.as_str().to_string()),
    );
    record.insert("event".to_string(), object(u)?);

    for _ in 0..u.arbitrary_len::<(AsciiStr, AsciiStr)>()? {
        let key = u.arbitrary::<AsciiStr>()?;
        let msg = RecordValue::String(u.arbitrary::<AsciiStr>()?.as_str().to_string());
        record.insert(key.as_str().to_string(), msg);
    }
    Ok(record)
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
            let key = AsciiString::default().generate(rng).unwrap();
            let val = rng.gen();

            rec.insert(key, val);
        }
        Entry {
            time: rng.gen(),
            record: rec,
        }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for Entry {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Entry {
            time: u.arbitrary::<u32>()?,
            record: record(u)?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <HashMap<String, String> as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
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
            tag: AsciiString::default().generate(rng).unwrap(),
            entries: rng.sample_iter(Standard).take(total_entries).collect(),
        }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for FluentForward {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FluentForward {
            tag: u.arbitrary::<AsciiStr>()?.as_str().to_string(),
            entries: u.arbitrary::<Vec<Entry>>()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <Vec<Entry> as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
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
            let key = AsciiString::default().generate(rng).unwrap();
            let val = rng.gen();

            rec.insert(key, val);
        }
        FluentMessage {
            tag: AsciiString::default().generate(rng).unwrap(),
            time: rng.gen(),
            record: rec,
        }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for FluentMessage {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FluentMessage {
            tag: u.arbitrary::<AsciiStr>()?.as_str().to_string(),
            time: u.arbitrary::<u32>()?,
            record: record(u)?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <AsciiStr as arbitrary::Arbitrary>::size_hint(depth),
                <u32 as arbitrary::Arbitrary>::size_hint(depth),
                <HashMap<String, String> as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
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

impl<'a> arbitrary::Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice = u.arbitrary::<u8>()?;
        let res = match choice % 2 {
            0 => Member::Message(u.arbitrary::<FluentMessage>()?),
            1 => Member::Forward(u.arbitrary::<FluentForward>()?),
            _ => unreachable!(),
        };
        Ok(res)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <FluentMessage as arbitrary::Arbitrary>::size_hint(depth),
                <FluentForward as arbitrary::Arbitrary>::size_hint(depth),
            ])
        })
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
