use std::{fmt, io::Write, ops::Range};

use rand::{seq::SliceRandom, Rng};

use crate::payload::{Error, Serialize};

use self::{
    common::tags, event::EventGenerator, metric::MetricGenerator,
    service_check::ServiceCheckGenerator,
};

use super::{common::AsciiString, Generator};

mod common;
mod event;
mod metric;
mod service_check;

fn choose_or_not<R, T>(mut rng: &mut R, pool: &[T]) -> Option<T>
where
    T: Clone,
    R: rand::Rng + ?Sized,
{
    if rng.gen() {
        pool.choose(&mut rng).cloned()
    } else {
        None
    }
}

struct MemberGenerator {
    event_generator: EventGenerator,
    service_check_generator: ServiceCheckGenerator,
    metric_generator: MetricGenerator,
}

#[inline]
fn random_strings_with_length<R>(min_max: Range<usize>, max_length: u16, rng: &mut R) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let mut buf = Vec::with_capacity(min_max.end);
    for _ in 0..rng.gen_range(min_max) {
        buf.push(AsciiString::with_maximum_length(max_length).generate(rng));
    }
    buf
}

impl MemberGenerator {
    fn new<R>(metric_range: Range<usize>, key_range: Range<usize>, mut rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let titles = random_strings_with_length(metric_range, 64, &mut rng);
        let texts_or_messages = random_strings_with_length(4..128, 1024, &mut rng);
        let small_strings = random_strings_with_length(16..1024, 8, &mut rng);

        let total_tag_sets = 512;
        let max_values_per_tag_set = 512;

        let mut tags = Vec::with_capacity(total_tag_sets);
        let tags_generator = tags::Generator::new(key_range, max_values_per_tag_set);
        for _ in 0..total_tag_sets {
            tags.push(tags_generator.generate(&mut rng));
        }

        let event_generator = EventGenerator {
            titles: titles.clone(),
            texts_or_messages: texts_or_messages.clone(),
            small_strings: small_strings.clone(),
            tags: tags.clone(),
        };

        let service_check_generator = ServiceCheckGenerator {
            names: titles.clone(),
            small_strings: small_strings.clone(),
            texts_or_messages,
            tags: tags.clone(),
        };
        let metric_generator = MetricGenerator {
            names: titles,
            container_ids: small_strings,
            tags,
        };

        MemberGenerator {
            event_generator,
            service_check_generator,
            metric_generator,
        }
    }
}

impl Generator<Member> for MemberGenerator {
    fn generate<R>(&self, rng: &mut R) -> Member
    where
        R: rand::Rng + ?Sized,
    {
        let idx = rng.gen_range(0..3);
        match idx {
            0 => Member::Event(self.event_generator.generate(rng)),
            1 => Member::ServiceCheck(self.service_check_generator.generate(rng)),
            2 => Member::Metric(self.metric_generator.generate(rng)),
            _ => unreachable!(),
        }
    }
}

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/
enum Member {
    Metric(metric::Metric),
    Event(event::Event),
    ServiceCheck(service_check::ServiceCheck),
}

impl fmt::Display for Member {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metric(ref m) => write!(f, "{m}"),
            Self::Event(ref e) => write!(f, "{e}"),
            Self::ServiceCheck(ref sc) => write!(f, "{sc}"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct DogStatsD {
    metric_names_range: Range<usize>,
    tag_keys_range: Range<usize>,
}

impl Default for DogStatsD {
    fn default() -> Self {
        Self {
            metric_names_range: 0..64,
            tag_keys_range: 0..32,
        }
    }
}

impl DogStatsD {
    pub(crate) fn new(metric_names_range: Range<usize>, tag_keys_range: Range<usize>) -> Self {
        Self {
            metric_names_range,
            tag_keys_range,
        }
    }
}

impl Serialize for DogStatsD {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let member_generator: MemberGenerator = MemberGenerator::new(
            self.metric_names_range.clone(),
            self.tag_keys_range.clone(),
            &mut rng,
        );

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
