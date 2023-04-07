use std::{fmt, io::Write};

use rand::{distributions::Standard, prelude::Distribution, Rng};

use crate::payload::{Error, Serialize};

use self::event::EventGenerator;

use super::{common::AsciiString, Generator};

mod common;
mod event;
//mod metric;
// mod service_check;

struct MemberGenerator {
    metric_names: Vec<String>,
    container_ids: Vec<String>,
    tag_keys: Vec<String>,
    tag_values: Vec<String>,
    hostnames: Vec<String>,
    titles: Vec<String>,
    texts_or_messages: Vec<String>,
    small_strings: Vec<String>,
}

#[inline]
fn random_strings<R>(cap: usize, rng: &mut R) -> Vec<String>
where
    R: Rng + ?Sized,
{
    random_strings_with_length(cap, 64, rng)
}

#[inline]
fn random_strings_with_length<R>(cap: usize, max_length: u16, rng: &mut R) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let mut buf = Vec::with_capacity(cap);
    for _ in 0..rng.gen_range(0..cap) {
        buf.push(AsciiString::with_maximum_length(max_length).generate(rng));
    }
    buf
}

impl Distribution<MemberGenerator> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> MemberGenerator
    where
        R: Rng + ?Sized,
    {
        MemberGenerator {
            metric_names: random_strings(64, &mut rng),
            container_ids: random_strings(128, &mut rng),
            tag_keys: random_strings(16, &mut rng),
            tag_values: random_strings(16, &mut rng),
            hostnames: random_strings(16, &mut rng),
            titles: random_strings(128, &mut rng),
            texts_or_messages: random_strings_with_length(128, 1024, &mut rng),
            small_strings: random_strings_with_length(1024, 8, &mut rng),
        }
    }
}

impl<'a> Generator<Member<'a>> for MemberGenerator {
    fn generate<R>(&'a self, rng: &mut R) -> Member<'a>
    where
        R: rand::Rng + ?Sized,
    {
        let idx = rng.gen_range(0..3);
        match idx {
            0 => {
                let gen = EventGenerator {
                    titles: &self.titles,
                    texts_or_messages: &self.texts_or_messages,
                    small_strings: &self.small_strings,
                };
                Member::Event(gen.generate(rng))
            }
            _ => unreachable!(),
        }
    }
}

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/
enum Member<'a> {
    //    Metric(metric::Metric),
    Event(event::Event<'a>),
    //    ServiceCheck(service_check::ServiceCheck),
}

impl<'a> fmt::Display for Member<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            //            Self::Metric(ref m) => write!(f, "{m}"),
            Self::Event(ref e) => write!(f, "{e}"),
            //            Self::ServiceCheck(ref sc) => write!(f, "{sc}"),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[allow(clippy::module_name_repetitions)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct DogStatsD {}

impl Serialize for DogStatsD {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let member_generator: MemberGenerator = rng.gen();

        let mut bytes_remaining = max_bytes;
        loop {
            let member: Member = member_generator.generate(&mut rng);
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

    use crate::payload::{DogStatsD, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let dogstatsd = DogStatsD::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }
}
