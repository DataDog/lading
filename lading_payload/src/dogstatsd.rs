//! `DogStatsD` payload.

use std::{fmt, io::Write, rc::Rc};

use rand::{Rng, distr::weighted::WeightedIndex, prelude::Distribution};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{
    Serialize,
    common::{
        config::ConfRange,
        strings,
        strings::{random_strings_with_length, random_strings_with_length_range},
    },
};

use self::{
    common::tags, event::EventGenerator, metric::MetricGenerator,
    service_check::ServiceCheckGenerator,
};

use super::Generator;

mod common;
pub mod event;
pub mod metric;
pub mod service_check;

const MAX_CONTEXTS: u32 = 10_000_000;
const MAX_NAME_LENGTH: u16 = 4_096;
const LENGTH_PREFIX_SIZE: usize = std::mem::size_of::<u32>();

/// Weights for `DogStatsD` kinds: metrics, events, service checks
///
/// Defines the relative probability of each kind of `DogStatsD` datagram.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
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
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
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
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ValueConf {
    /// Odds out of 256 that the value will be a float and not an integer.
    float_probability: f32,
    range: ConfRange<i64>,
}

impl ValueConf {
    /// Create a new instance of `ValueConf` according to the args
    #[must_use]
    pub fn new(float_probability: f32, range: ConfRange<i64>) -> Self {
        Self {
            float_probability,
            range,
        }
    }

    fn valid(&self) -> (bool, &'static str) {
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

/// Configure the `DogStatsD` payload.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// The unique metric contexts to generate. A context is a set of unique
    /// metric name + tags
    pub contexts: ConfRange<u32>,
    /// The range of unique service check names.
    pub service_check_names: ConfRange<u16>,
    /// Length for a dogstatsd message name
    pub name_length: ConfRange<u16>,
    /// Length for a dogstatsd tag
    pub tag_length: ConfRange<u16>,
    /// Number of tags per individual dogstatsd msg a tag is a key-value pair
    /// separated by a :
    pub tags_per_msg: ConfRange<u8>,
    /// Probability between 0 and 1 that a given dogstatsd msg
    /// contains multiple values
    pub multivalue_pack_probability: f32,
    /// The count of values that will be generated if multi-value is chosen to
    /// be generated
    pub multivalue_count: ConfRange<u16>,
    /// Range of possible values for the sampling rate sent in dogstatsd messages
    pub sampling_range: ConfRange<f32>,
    /// Probability between 0 and 1 that a given dogstatsd msg will specify a sampling rate.
    /// The sampling rate is chosen from `sampling_range`
    pub sampling_probability: f32,
    /// Defines the relative probability of each kind of `DogStatsD` kinds of
    /// payload.
    pub kind_weights: KindWeights,
    /// Defines the relative probability of each kind of `DogStatsD` metric.
    pub metric_weights: MetricWeights,
    /// The configuration of values that appear in all metrics.
    pub value: ValueConf,
    /// Whether completed blocks should use length-prefix framing.
    ///
    /// If enabled, each block emitted from this generator will have
    /// a 4-byte header that is a little-endian u32 representing the
    /// total length of the data block.
    pub length_prefix_framed: bool,
    /// This is a ratio between 0.10 and 1.0 which determines how many
    /// individual tags are unique vs re-used tags.
    /// If this is 1, then every single tag will be unique.
    /// If this is 0.10, then most of the tags (90%) will be re-used
    /// from existing tags.
    pub unique_tag_ratio: f32,
    /// A list of possible metric names to generate
    pub metric_names: Vec<String>,
    /// A list of possible tag names to generate
    pub tag_names: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            contexts: ConfRange::Inclusive {
                min: 5_000,
                max: 10_000,
            },
            service_check_names: ConfRange::Inclusive {
                min: 1,
                max: 10_000,
            },
            // https://docs.datadoghq.com/developers/guide/what-best-practices-are-recommended-for-naming-metrics-and-tags/#rules-and-best-practices-for-naming-metrics
            name_length: ConfRange::Inclusive { min: 1, max: 200 },
            tag_length: ConfRange::Inclusive { min: 3, max: 100 },
            tags_per_msg: ConfRange::Inclusive { min: 2, max: 50 },
            multivalue_count: ConfRange::Inclusive { min: 2, max: 32 },
            multivalue_pack_probability: 0.08,
            sampling_range: ConfRange::Inclusive { min: 0.1, max: 1.0 },
            sampling_probability: 0.5,
            kind_weights: KindWeights::default(),
            metric_weights: MetricWeights::default(),
            value: ValueConf::default(),
            // This should be enabled for UDS-streams, but not for UDS-datagram nor UDP
            length_prefix_framed: false,
            unique_tag_ratio: 0.11,
            metric_names: Vec::default(),
            tag_names: Vec::default(),
        }
    }
}

impl Config {
    /// Determine whether the passed configuration obeys validation criteria
    /// # Errors
    /// Function will error if the configuration is invalid
    pub fn valid(&self) -> Result<(), String> {
        let (contexts_valid, reason) = self.contexts.valid();
        if !contexts_valid {
            return Result::Err(format!("Contexts value is invalid: {reason}"));
        }
        if self.contexts.start() == 0 {
            return Result::Err("Contexts start value cannot be 0".to_string());
        }
        if let ConfRange::Constant(0) = self.contexts {
            return Result::Err("Contexts cannot be 0".to_string());
        }
        if self.contexts.end() > MAX_CONTEXTS {
            return Result::Err(format!(
                "Contexts end value is greater than the maximum allowed value of {MAX_CONTEXTS}"
            ));
        }
        let (service_check_names_valid, reason) = self.service_check_names.valid();
        if !service_check_names_valid {
            return Result::Err(format!("Service check names value is invalid: {reason}"));
        }
        if self.service_check_names.start() == 0 {
            return Result::Err("Service check names start value cannot be 0".to_string());
        }

        let (tag_length_valid, reason) = self.tag_length.valid();
        if !tag_length_valid {
            return Result::Err(format!("Tag length value is invalid: {reason}"));
        }
        if self.tag_length.start() == 0 {
            return Result::Err("Tag length start value cannot be 0".to_string());
        }
        let (name_length_valid, reason) = self.name_length.valid();
        if !name_length_valid {
            return Result::Err(format!("Name length value is invalid: {reason}"));
        }
        if self.name_length.start() == 0 {
            return Result::Err("Name length start value cannot be 0".to_string());
        }
        if self.name_length.end() > MAX_NAME_LENGTH {
            return Result::Err("Name length end value cannot be 0".to_string());
        }
        let (tags_per_msg_valid, reason) = self.tags_per_msg.valid();
        if !tags_per_msg_valid {
            return Result::Err(format!("Tags per msg value is invalid: {reason}"));
        }
        let (multivalue_count_valid, reason) = self.multivalue_count.valid();
        if !multivalue_count_valid {
            return Result::Err(format!("Multivalue count value is invalid: {reason}"));
        }
        let (value_valid, reason) = self.value.valid();
        if !value_valid {
            return Result::Err(format!("Value configuration is invalid: {reason}"));
        }

        let (sampling_range_valid, reason) = self.sampling_range.valid();
        if !sampling_range_valid {
            return Result::Err(format!("Sampling range is invalid: {reason}"));
        }
        match self.sampling_range {
            ConfRange::Constant(v) => {
                if !v.is_finite() || !(0.0..=1.0).contains(&v) {
                    return Result::Err(format!(
                        "Sampling range constant {v} must be finite and in range [0.0, 1.0]"
                    ));
                }
            }
            ConfRange::Inclusive { min, max } => {
                if !min.is_finite() || !max.is_finite() {
                    return Result::Err("Sampling range values must be finite".to_string());
                }
                if min < 0.0 || max > 1.0 {
                    return Result::Err("Sampling range must be within [0.0, 1.0]".to_string());
                }
            }
        }

        if !self.sampling_probability.is_finite()
            || self.sampling_probability < 0.0
            || self.sampling_probability > 1.0
        {
            return Result::Err(format!(
                "Sampling probability {} must be finite and in range [0.0, 1.0]",
                self.sampling_probability
            ));
        }

        if self.kind_weights.metric == 0
            && self.kind_weights.event == 0
            && self.kind_weights.service_check == 0
        {
            return Err("KindWeights cannot all be 0".to_string());
        }

        if self.metric_weights.gauge == 0
            && self.metric_weights.count == 0
            && self.metric_weights.histogram == 0
            && self.metric_weights.set == 0
            && self.metric_weights.distribution == 0
        {
            return Err("MetricWeights cannot all be 0".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MemberGenerator {
    kind_weights: WeightedIndex<u16>,
    event_generator: EventGenerator,
    service_check_generator: ServiceCheckGenerator,
    metric_generator: MetricGenerator,
}

/// The collection of pools from which to choose our strings
/// Cheap to clone.
#[derive(Clone, Debug)]
pub(crate) struct StringPools {
    tag_pool: Rc<strings::PoolKind>,
    value_pool: Rc<strings::PoolKind>,
    name_pool: Rc<strings::PoolKind>,
    str_pool: Rc<strings::RandomStringPool>,
}

impl MemberGenerator {
    #[allow(clippy::too_many_arguments)]
    fn new<R>(
        contexts: ConfRange<u32>,
        service_check_names: ConfRange<u16>,
        name_length: ConfRange<u16>,
        tag_length: ConfRange<u16>,
        tags_per_msg: ConfRange<u8>,
        multivalue_count: ConfRange<u16>,
        multivalue_pack_probability: f32,
        sampling: ConfRange<f32>,
        sampling_probability: f32,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        value_conf: ValueConf,
        unique_tag_ratio: f32,
        metric_names: Vec<String>,
        tag_names: Vec<String>,
        mut rng: &mut R,
    ) -> Result<Self, crate::Error>
    where
        R: Rng + ?Sized,
    {
        // TODO only create the Generators if they're needed per the kind weights

        let pool = strings::RandomStringPool::with_size(&mut rng, 8_000_000);

        // BUG: Resulting size is contexts * name_length, so 2**32 * 2**16.
        let service_event_titles = random_strings_with_length_range(
            &pool,
            service_check_names.sample(rng) as usize,
            name_length,
            &mut rng,
        );

        let texts_or_messages = random_strings_with_length(&pool, 4..128, 1024, &mut rng);
        let small_strings = random_strings_with_length(&pool, 16..1024, 8, &mut rng);

        let str_pool = Rc::new(pool.clone());
        let pool = Rc::new(strings::PoolKind::RandomStringPool(pool));

        let tag_pool = if tag_names.is_empty() {
            Rc::clone(&pool)
        } else {
            Rc::new(strings::PoolKind::StringListPool(
                strings::StringListPool::new(tag_names),
            ))
        };

        let name_pool = if metric_names.is_empty() {
            Rc::clone(&pool)
        } else {
            Rc::new(strings::PoolKind::StringListPool(
                strings::StringListPool::new(metric_names),
            ))
        };

        let pools = StringPools {
            tag_pool,
            value_pool: Rc::clone(&pool),
            name_pool,
            str_pool,
        };

        let num_contexts = contexts.sample(rng);

        let mut tags_generator = match tags::Generator::new(
            rng.random(),
            tags_per_msg,
            tag_length,
            num_contexts as usize,
            Rc::clone(&pools.value_pool),
            Rc::clone(&pools.tag_pool),
            unique_tag_ratio,
        ) {
            Ok(tg) => tg,
            Err(e) => {
                warn!("Encountered error while constructing tag generator: {e}");
                return Err(crate::Error::StringGenerate);
            }
        };

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
            pools: pools.clone(),
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
            pools: pools.clone(),
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
            &pools,
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
            metric_generator: metric_generator?,
        })
    }
}

impl MemberGenerator {
    fn generate<'a, R>(&'a self, rng: &mut R) -> Result<Member<'a>, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        match self.kind_weights.sample(rng) {
            0 => Ok(Member::Metric(self.metric_generator.generate(rng)?)),
            1 => {
                let event = self.event_generator.generate(rng)?;
                Ok(Member::Event(event))
            }
            2 => Ok(Member::ServiceCheck(
                self.service_check_generator.generate(rng)?,
            )),
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

impl fmt::Display for Member<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metric(m) => write!(f, "{m}"),
            Self::Event(e) => write!(f, "{e}"),
            Self::ServiceCheck(sc) => write!(f, "{sc}"),
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
    pub fn default<R>(rng: &mut R) -> Result<Self, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let config = Config::default();
        let dogstatd = Self::new(config, rng)?;
        Ok(dogstatd)
    }

    /// Generate a single `Member`.
    /// Prefer using the Serialize implementation for `DogStatsD` which
    /// generates a stream of `Member`s according to some constraints.
    ///
    /// # Errors
    ///
    /// Function will error if a member could not be generated
    pub fn generate<R>(&self, rng: &mut R) -> Result<Member<'_>, crate::Error>
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
    pub fn new<R>(mut config: Config, rng: &mut R) -> Result<Self, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let member_generator = MemberGenerator::new(
            config.contexts,
            config.service_check_names,
            config.name_length,
            config.tag_length,
            config.tags_per_msg,
            config.multivalue_count,
            config.multivalue_pack_probability,
            config.sampling_range,
            config.sampling_probability,
            config.kind_weights,
            config.metric_weights,
            config.value,
            config.unique_tag_ratio,
            std::mem::take(&mut config.metric_names),
            std::mem::take(&mut config.tag_names),
            rng,
        )?;

        Ok(Self {
            member_generator,
            length_prefix_framed: config.length_prefix_framed,
        })
    }
}

impl Serialize for DogStatsD {
    fn to_bytes<W, R>(
        &mut self,
        rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if self.length_prefix_framed {
            self.to_bytes_length_prefix_framed(rng, max_bytes, writer)
        } else {
            self.to_bytes_unframed(rng, max_bytes, writer)
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
        // If max_bytes is less than the length prefix size, we can't write anything
        if max_bytes < LENGTH_PREFIX_SIZE {
            return Ok(());
        }

        let content_budget = max_bytes.saturating_sub(LENGTH_PREFIX_SIZE);
        // Accumulate all content into a single buffer instead of Vec<String>.
        // This eliminates per-member String allocations.
        let mut content_buffer: Vec<u8> = Vec::with_capacity(content_budget);
        // Temporary buffer for formatting each member before measuring/appending
        let mut member_buffer: Vec<u8> = Vec::with_capacity(256);

        loop {
            let member: Member = self.member_generator.generate(&mut rng)?;
            member_buffer.clear();
            // Format into the temporary buffer - write! on Vec<u8> is infallible
            write!(&mut member_buffer, "{member}").expect("formatting to Vec<u8> cannot fail");
            let line_length = member_buffer.len() + 1; // add one for the newline
            if content_buffer.len() + line_length > content_budget {
                break;
            }
            content_buffer.extend_from_slice(&member_buffer);
            content_buffer.push(b'\n');
        }

        if content_buffer.is_empty() {
            return Ok(());
        }

        let length: u32 = content_buffer.len().try_into().unwrap_or_else(|_| {
            panic!(
                "Could not convert content length to a u32, are you using a block size greater than {}?",
                u32::MAX
            );
        });

        // write prefix
        writer.write_all(&length.to_le_bytes())?;
        // LENGTH_PREFIX_SIZE is always 4 (size_of::<u32>()), safe to cast
        #[allow(clippy::cast_possible_truncation)]
        let total_bytes = length + LENGTH_PREFIX_SIZE as u32;
        debug!("Filling block. Requested: {max_bytes} bytes. Actual: {total_bytes} bytes.",);

        // write contents
        writer.write_all(&content_buffer)?;

        Ok(())
    }

    fn to_bytes_unframed<W, R>(
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
        // Reuse a single buffer across iterations to avoid repeated allocations.
        // Each member is formatted here, measured, then written to the output.
        let mut buffer: Vec<u8> = Vec::with_capacity(256);
        loop {
            let member: Member = self.member_generator.generate(&mut rng)?;
            buffer.clear();
            // Format into the reusable buffer - write! on Vec<u8> is infallible
            write!(&mut buffer, "{member}").expect("formatting to Vec<u8> cannot fail");
            let line_length = buffer.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writer.write_all(&buffer)?;
                    writer.write_all(b"\n")?;
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }
        if bytes_remaining == max_bytes {
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
    use super::{ConfRange, Config};
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use crate::{DogStatsD, Serialize};

    #[test]
    fn zero_contexts_should_be_rejected() {
        let config = Config {
            contexts: ConfRange::Constant(0),
            ..Default::default()
        };

        // Validation should reject 0 contexts
        let validation_result = config.valid();
        assert!(validation_result.is_err());
        // The error message could be either one, both indicate 0 contexts is invalid
        let err = validation_result.unwrap_err();
        assert!(
            err == "Contexts start value cannot be 0" || err == "Contexts cannot be 0",
            "Expected error about 0 contexts, got: {}",
            err
        );
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let dogstatsd_config = Config::default();
            let mut dogstatsd = match DogStatsD::new(dogstatsd_config, &mut rng) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Failed to create DogStatsD with error: {e:?}");
                    return Err(TestCaseError::fail(format!("Failed to create DogStatsD: {e:?}")));
                }
            };

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes)?;
            prop_assert!(
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

            let dogstatsd_config = Config { length_prefix_framed: true, ..Default::default() };
            let mut dogstatsd = DogStatsD::new(dogstatsd_config, &mut rng).expect("failed to create DogStatsD");

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            prop_assert!(
                bytes.len() <= max_bytes,
                "{l} <= {max_bytes}, {pyld:?}",
                l = bytes.len(),
                pyld = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }
}
