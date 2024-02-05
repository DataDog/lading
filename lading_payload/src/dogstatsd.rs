//! `DogStatsD` payload.

use std::{cmp, fmt, io::Write, ops::Range, rc::Rc};

use rand::{
    distributions::{uniform::SampleUniform, WeightedError, WeightedIndex},
    prelude::Distribution,
    seq::SliceRandom,
    Rng,
};
use serde::{Deserialize, Serialize as SerdeSerialize};
use tracing::{debug, warn};

use crate::{common::strings, Serialize};

use self::{
    common::tags, event::EventGenerator, metric::MetricGenerator,
    service_check::ServiceCheckGenerator,
};

use super::Generator;

mod common;
pub mod event;
pub mod metric;
pub mod service_check;

/// Error for [`MemberGenerator`]
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum Error {
    /// See [`WeightedError`]
    #[error(transparent)]
    Weights(#[from] WeightedError),
}

const MAX_CONTEXTS: u32 = 100_000;
const MAX_NAME_LENGTH: u16 = 4_096;

fn contexts() -> ConfRange<u32> {
    ConfRange::Inclusive {
        min: 5_000,
        max: 10_000,
    }
}

fn service_check_names() -> ConfRange<u16> {
    ConfRange::Inclusive {
        min: 1,
        max: 10_000,
    }
}

fn value_config() -> ValueConf {
    ValueConf::default()
}

// https://docs.datadoghq.com/developers/guide/what-best-practices-are-recommended-for-naming-metrics-and-tags/#rules-and-best-practices-for-naming-metrics
fn name_length() -> ConfRange<u16> {
    ConfRange::Inclusive { min: 1, max: 200 }
}

fn tag_key_length() -> ConfRange<u8> {
    ConfRange::Inclusive { min: 1, max: 100 }
}

fn tag_value_length() -> ConfRange<u8> {
    ConfRange::Inclusive { min: 1, max: 100 }
}

fn tags_per_msg() -> ConfRange<u8> {
    ConfRange::Inclusive { min: 2, max: 50 }
}

fn multivalue_count() -> ConfRange<u16> {
    ConfRange::Inclusive { min: 2, max: 32 }
}

fn multivalue_pack_probability() -> f32 {
    0.08
}

fn sampling_range() -> ConfRange<f32> {
    ConfRange::Inclusive { min: 0.1, max: 1.0 }
}

fn sampling_probability() -> f32 {
    0.5
}

fn length_prefix_framed() -> bool {
    false
}

/// Weights for `DogStatsD` kinds: metrics, events, service checks
///
/// Defines the relative probability of each kind of `DogStatsD` datagram.
#[derive(Debug, Deserialize, SerdeSerialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]

pub struct KindWeights {
    metric: u8,
    event: u8,
    service_check: u8,
}

impl KindWeights {
    /// Create a new instance of `KindWeights` according to the args
    #[must_use]
    pub fn new(metric: u8, event: u8, service_check: u8) -> Self {
        Self {
            metric,
            event,
            service_check,
        }
    }
}

impl Default for KindWeights {
    fn default() -> Self {
        KindWeights {
            metric: 80,        // 80%
            event: 10,         // 10%
            service_check: 10, // 10%
        }
    }
}

/// Weights for `DogStatsD` metrics: gauges, counters, etc
#[derive(Debug, Deserialize, SerdeSerialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]

pub struct MetricWeights {
    count: u8,
    gauge: u8,
    timer: u8,
    distribution: u8,
    set: u8,
    histogram: u8,
}

impl MetricWeights {
    /// Create a new instance of `MetricWeights` according to the args
    #[must_use]
    pub fn new(count: u8, gauge: u8, timer: u8, distribution: u8, set: u8, histogram: u8) -> Self {
        Self {
            count,
            gauge,
            timer,
            distribution,
            set,
            histogram,
        }
    }
}

impl Default for MetricWeights {
    fn default() -> Self {
        MetricWeights {
            count: 34,       // 34%
            gauge: 34,       // 34%
            timer: 5,        // 5%
            distribution: 1, // 1%
            set: 1,          // 1%
            histogram: 25,   // 25%
        }
    }
}

/// Configuration for the values of a metric.
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]

pub struct ValueConf {
    /// Odds out of 256 that the value will be a float and not an integer.
    float_probability: f32,
    range: ConfRange<i64>,
}

impl ValueConf {
    fn valid(&self) -> bool {
        self.range.valid()
    }
}

impl Default for ValueConf {
    fn default() -> Self {
        Self {
            float_probability: 0.5, // 50%
            range: ConfRange::Inclusive {
                min: i64::MIN,
                max: i64::MAX,
            },
        }
    }
}
/// Range expression for configuration
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]

pub enum ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy,
{
    /// A constant T
    Constant(T),
    /// In which a T is chosen between `min` and `max`, inclusive of `max`.
    Inclusive {
        /// The minimum of the range.
        min: T,
        /// The maximum of the range.
        max: T,
    },
}

impl<T> ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy,
{
    /// Returns true if the range provided by the user is valid, false
    /// otherwise.
    fn valid(&self) -> bool {
        match self {
            Self::Constant(_) => true,
            Self::Inclusive { min, max } => min < max,
        }
    }

    fn start(&self) -> T {
        match self {
            ConfRange::Constant(c) => *c,
            ConfRange::Inclusive { min, .. } => *min,
        }
    }

    fn end(&self) -> T {
        match self {
            ConfRange::Constant(c) => *c,
            ConfRange::Inclusive { max, .. } => *max,
        }
    }
}

impl<T> ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy + SampleUniform,
{
    fn sample<R>(&self, rng: &mut R) -> T
    where
        R: rand::Rng + ?Sized,
    {
        match self {
            ConfRange::Constant(c) => *c,
            ConfRange::Inclusive { min, max } => rng.gen_range(*min..*max),
        }
    }
}

/// Configure the `DogStatsD` payload.
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The unique metric contexts to generate. A context is a set of unique
    /// metric name + tags
    #[serde(default = "contexts")]
    pub contexts: ConfRange<u32>,

    /// The range of unique service check names.
    #[serde(default = "service_check_names")]
    pub service_check_names: ConfRange<u16>,

    /// Length for a dogstatsd message name
    #[serde(default = "name_length")]
    pub name_length: ConfRange<u16>,

    /// Length for the 'key' part of a dogstatsd tag
    #[serde(default = "tag_key_length")]
    pub tag_key_length: ConfRange<u8>,

    /// Length for the 'value' part of a dogstatsd tag
    #[serde(default = "tag_value_length")]
    pub tag_value_length: ConfRange<u8>,

    /// Number of tags per individual dogstatsd msg a tag is a key-value pair
    /// separated by a :
    #[serde(default = "tags_per_msg")]
    pub tags_per_msg: ConfRange<u8>,

    /// Probability between 0 and 1 that a given dogstatsd msg
    /// contains multiple values
    #[serde(default = "multivalue_pack_probability")]
    pub multivalue_pack_probability: f32,

    /// The count of values that will be generated if multi-value is chosen to
    /// be generated
    #[serde(default = "multivalue_count")]
    pub multivalue_count: ConfRange<u16>,

    /// Range of possible values for the sampling rate sent in dogstatsd messages
    #[serde(default = "sampling_range")]
    pub sampling_range: ConfRange<f32>,

    /// Probability between 0 and 1 that a given dogstatsd msg will specify a sampling rate.
    /// The sampling rate is chosen from `sampling_range`
    #[serde(default = "sampling_probability")]
    pub sampling_probability: f32,

    /// Defines the relative probability of each kind of DogStatsD kinds of
    /// payload.
    #[serde(default)]
    pub kind_weights: KindWeights,

    /// Defines the relative probability of each kind of DogStatsD metric.
    #[serde(default)]
    pub metric_weights: MetricWeights,

    /// The configuration of values that appear in all metrics.
    #[serde(default = "value_config")]
    pub value: ValueConf,

    /// Whether completed blocks should use length-prefix framing.
    ///
    /// If enabled, each block emitted from this generator will have
    /// a 4-byte header that is a little-endian u32 representing the
    /// total length of the data block.
    #[serde(default = "length_prefix_framed")]
    pub length_prefix_framed: bool,
}

impl Config {
    /// Determine whether the passed configuration obeys validation criteria
    pub(crate) fn valid(&self) -> bool {
        // TODO these constraints need to become part of the type system or parsing runtime errors.
        self.contexts.valid()
            && self.contexts.start() > 0
            && self.contexts.end() <= MAX_CONTEXTS
            && self.service_check_names.valid()
            && self.service_check_names.start() > 0
            && self.service_check_names.end() > 0
            && self.tag_key_length.start() > 0
            && self.tag_key_length.end() > 0
            && self.tag_value_length.start() > 0
            && self.tag_value_length.end() > 0
            && self.name_length.valid()
            && self.name_length.end() <= MAX_NAME_LENGTH
            && self.tag_key_length.valid()
            && self.tag_value_length.valid()
            && self.tags_per_msg.valid()
            && self.multivalue_count.valid()
            && self.value.valid()
    }
}

fn choose_or_not_ref<'a, R, T>(mut rng: &mut R, pool: &'a [T]) -> Option<&'a T>
where
    R: rand::Rng + ?Sized,
{
    if rng.gen() {
        pool.choose(&mut rng)
    } else {
        None
    }
}

fn choose_or_not_fn<R, T, F>(rng: &mut R, func: F) -> Option<T>
where
    T: Clone,
    R: rand::Rng + ?Sized,
    F: FnOnce(&mut R) -> Option<T>,
{
    if rng.gen() {
        func(rng)
    } else {
        None
    }
}

#[inline]
/// Generate a total number of strings randomly chosen from the range `min_max` with a maximum length
/// per string of `max_length`.
fn random_strings_with_length<R>(
    pool: &strings::Pool,
    min_max: Range<usize>,
    max_length: u16,
    rng: &mut R,
) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let total = rng.gen_range(min_max);
    let length_range = ConfRange::Inclusive {
        min: 1,
        max: max_length,
    };

    random_strings_with_length_range(pool, total, length_range, rng)
}

#[inline]
/// Generate a `total` number of strings with a maximum length per string of
/// `max_length`.
fn random_strings_with_length_range<R>(
    pool: &strings::Pool,
    total: usize,
    length_range: ConfRange<u16>,
    mut rng: &mut R,
) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let mut buf = Vec::with_capacity(total);
    for _ in 0..total {
        let sz = length_range.sample(&mut rng) as usize;
        buf.push(String::from(
            pool.of_size(&mut rng, sz)
                .expect("failed to generate string"),
        ));
    }
    buf
}

#[derive(Debug, Clone)]
struct MemberGenerator {
    kind_weights: WeightedIndex<u16>,
    event_generator: EventGenerator,
    service_check_generator: ServiceCheckGenerator,
    metric_generator: MetricGenerator,
}

impl MemberGenerator {
    #[allow(clippy::too_many_arguments)]
    fn new<R>(
        contexts: ConfRange<u32>,
        service_check_names: ConfRange<u16>,
        name_length: ConfRange<u16>,
        tag_key_length: ConfRange<u8>,
        tag_value_length: ConfRange<u8>,
        tags_per_msg: ConfRange<u8>,
        multivalue_count: ConfRange<u16>,
        multivalue_pack_probability: f32,
        sampling: ConfRange<f32>,
        sampling_probability: f32,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        value_conf: ValueConf,
        mut rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        // TODO only create the Generators if they're needed per the kind weights

        let pool = Rc::new(strings::Pool::with_size(&mut rng, 8_000_000));

        let num_contexts = contexts.sample(rng);

        let mut tags_generator = tags::Generator::new(
            rng.gen(),
            tags_per_msg,
            tag_key_length,
            tag_value_length,
            Rc::clone(&pool),
            num_contexts as usize,
        );

        // BUG: Resulting size is contexts * name_length, so 2**32 * 2**16.
        let service_event_titles = random_strings_with_length_range(
            pool.as_ref(),
            service_check_names.sample(rng) as usize,
            name_length,
            &mut rng,
        );

        let texts_or_messages = random_strings_with_length(pool.as_ref(), 4..128, 1024, &mut rng);
        let small_strings = random_strings_with_length(pool.as_ref(), 16..1024, 8, &mut rng);

        // NOTE the ordering here of `metric_choices` is very important! If you
        // change it here you MUST also change it in `Generator<Metric> for
        // MetricGenerator`.
        let metric_choices = [
            u16::from(metric_weights.count),
            u16::from(metric_weights.gauge),
            u16::from(metric_weights.timer),
            u16::from(metric_weights.distribution),
            u16::from(metric_weights.set),
            u16::from(metric_weights.histogram),
        ];

        let event_generator = EventGenerator {
            str_pool: Rc::clone(&pool),
            title_length: name_length,
            texts_or_messages_length_range: 1..1024,
            small_strings_length_range: 1..8,
            tags_generator: tags_generator.clone(),
        };

        let service_check_generator = ServiceCheckGenerator {
            names: service_event_titles.clone(),
            small_strings: small_strings.clone(),
            texts_or_messages,
            tags_generator: tags_generator.clone(),
        };

        let metric_generator = MetricGenerator::new(
            num_contexts as usize,
            name_length,
            multivalue_count,
            multivalue_pack_probability,
            sampling,
            sampling_probability,
            &WeightedIndex::new(metric_choices)?,
            small_strings,
            &mut tags_generator,
            pool.as_ref(),
            value_conf,
            &mut rng,
        );

        // NOTE the ordering here of `member_choices` is very important! If you
        // change it here you MUST also change it in `Generator<Member> for
        // MemberGenerator`.
        let member_choices = [
            u16::from(kind_weights.metric),
            u16::from(kind_weights.event),
            u16::from(kind_weights.service_check),
        ];
        Ok(MemberGenerator {
            kind_weights: WeightedIndex::new(member_choices)?,
            event_generator,
            service_check_generator,
            metric_generator,
        })
    }
}

impl MemberGenerator {
    fn generate<'a, R>(&'a self, rng: &mut R) -> Member<'a>
    where
        R: rand::Rng + ?Sized,
    {
        match self.kind_weights.sample(rng) {
            0 => Member::Metric(self.metric_generator.generate(rng)),
            1 => {
                let event = self.event_generator.generate(rng);
                Member::Event(event)
            }
            2 => Member::ServiceCheck(self.service_check_generator.generate(rng)),
            _ => unreachable!(),
        }
    }
}

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/
#[derive(Debug)]
/// Supra-type for all dogstatsd variants
pub enum Member<'a> {
    /// Metrics
    Metric(metric::Metric<'a>),
    /// Events, think syslog.
    Event(event::Event<'a>),
    /// Services, checked.
    ServiceCheck(service_check::ServiceCheck<'a>),
}

impl<'a> fmt::Display for Member<'a> {
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
/// A generator for `DogStatsD` payloads
pub struct DogStatsD {
    member_generator: MemberGenerator,
    length_prefix_framed: bool,
}

impl DogStatsD {
    /// Create a new default instance of `DogStatsD` with reasonable settings.
    ///
    /// # Errors
    /// Function will error if dogstatd could not be created
    pub fn default<R>(rng: &mut R) -> Result<Self, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let dogstatd = Self::new(
            contexts(),
            service_check_names(),
            name_length(),
            tag_key_length(),
            tag_value_length(),
            tags_per_msg(),
            multivalue_count(),
            multivalue_pack_probability(),
            sampling_range(),
            sampling_probability(),
            KindWeights::default(),
            MetricWeights::default(),
            value_config(),
            length_prefix_framed(),
            rng,
        )?;
        Ok(dogstatd)
    }

    /// Generate a single `Member`.
    /// Prefer using the Serialize implementation for `DogStatsD` which
    /// generates a stream of `Member`s according to some constraints.
    pub fn generate<R>(&self, rng: &mut R) -> Member
    where
        R: rand::Rng + ?Sized,
    {
        self.member_generator.generate(rng)
    }

    /// Create a new instance of `DogStatsD`.
    ///
    /// # Errors
    ///
    /// See documentation for [`Error`]
    #[allow(clippy::too_many_arguments)]
    pub fn new<R>(
        contexts: ConfRange<u32>,
        service_check_names: ConfRange<u16>,
        name_length: ConfRange<u16>,
        tag_key_length: ConfRange<u8>,
        tag_value_length: ConfRange<u8>,
        tags_per_msg: ConfRange<u8>,
        multivalue_count: ConfRange<u16>,
        multivalue_pack_probability: f32,
        sampling: ConfRange<f32>,
        sampling_probability: f32,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        value_conf: ValueConf,
        length_prefix_framed: bool,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let member_generator = MemberGenerator::new(
            contexts,
            service_check_names,
            name_length,
            tag_key_length,
            tag_value_length,
            tags_per_msg,
            multivalue_count,
            multivalue_pack_probability,
            sampling,
            sampling_probability,
            kind_weights,
            metric_weights,
            value_conf,
            rng,
        )?;

        Ok(Self {
            member_generator,
            length_prefix_framed,
        })
    }
}

impl Serialize for DogStatsD {
    fn to_bytes<W, R>(&self, rng: R, max_bytes: usize, writer: &mut W) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if self.length_prefix_framed {
            self.to_bytes_length_prefix_framed(rng, max_bytes, writer)
        } else {
            self.to_bytes(rng, max_bytes, writer)
        }
    }
}
impl DogStatsD {
    fn to_bytes_length_prefix_framed<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        let mut members = Vec::new();
        // generate as many messages as we can fit
        loop {
            let member: Member = self.member_generator.generate(&mut rng);
            let encoding = format!("{member}");
            let line_length = encoding.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    members.push(encoding);
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }
        if bytes_remaining == max_bytes {
            warn!("Could not fit any messages into the block with requested size {max_bytes}. Omitting this block.");
            return Ok(());
        }

        let max_bytes: u32 = max_bytes.try_into().unwrap_or_else(|_| {
            panic!(
                "Could not convert max_bytes to a u32, are you using a block size greater than {}?",
                u32::MAX
            );
        });
        let bytes_remaining: u32 = bytes_remaining.try_into().unwrap_or_else(|_| {
            panic!(
                "Could not convert bytes_remaining to a u32, are you using a block size greater than {}?",
                u32::MAX
            );
        });
        let length = max_bytes - bytes_remaining;

        // write prefix
        writer.write_all(&length.to_le_bytes())?;
        debug!(
            "Filling block. Requested: {max_bytes} bytes. Actual: {} bytes.",
            length
        );

        // write contents
        for member in members {
            writeln!(writer, "{member}")?;
        }

        Ok(())
    }
    fn to_bytes<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let member: Member = self.member_generator.generate(&mut rng);
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
        if bytes_remaining == max_bytes {
            warn!("Could not fit any messages into the block with requested size {max_bytes}. Omitting this block.");
            return Ok(());
        }

        let length = max_bytes - bytes_remaining;
        debug!(
            "Filling block. Requested: {max_bytes} bytes. Actual: {} bytes.",
            length
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::{
        dogstatsd::{
            contexts, multivalue_count, multivalue_pack_probability, name_length,
            sampling_probability, sampling_range, service_check_names, tag_key_length,
            tag_value_length, tags_per_msg, value_config, KindWeights, MetricWeights,
        },
        DogStatsD,
    };

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let multivalue_pack_probability = multivalue_pack_probability();
            let value_conf = value_config();

            let kind_weights = KindWeights::default();
            let metric_weights = MetricWeights::default();
            let dogstatsd = DogStatsD::new(contexts(), service_check_names(),
                                           name_length(), tag_key_length(),
                                           tag_value_length(), tags_per_msg(),
                                           multivalue_count(), multivalue_pack_probability, sampling_range(), sampling_probability(), kind_weights,
                                           metric_weights, value_conf, false, &mut rng)?;

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes)?;
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes` if framing is enabled
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes_with_length_prefix_frames(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let multivalue_pack_probability = multivalue_pack_probability();
            let value_conf = value_config();

            let kind_weights = KindWeights::default();
            let metric_weights = MetricWeights::default();
            let dogstatsd = DogStatsD::new(contexts(), service_check_names(),
                                           name_length(), tag_key_length(),
                                           tag_value_length(), tags_per_msg(),
                                           multivalue_count(), multivalue_pack_probability, sampling_range(), sampling_probability(), kind_weights,
                                           metric_weights, value_conf, true, &mut rng).expect("failed to create DogStatsD");

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }
}
