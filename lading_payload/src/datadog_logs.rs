//! Datadog Logs payload.

use std::io::Write;

use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};
use serde::Deserialize;

use crate::{Error, Generator, common::config::ConfRange, common::strings};

const STATUSES: [&str; 3] = ["notice", "info", "warning"];
const HOSTNAMES: [&str; 4] = ["alpha", "beta", "gamma", "localhost"];
const SERVICES: [&str; 4] = ["vector", "lading", "cernan", "agent"];
const SOURCES: [&str; 7] = [
    "bergman",
    "keaton",
    "kurosawa",
    "lynch",
    "waters",
    "tarkovsky",
    "herzog",
];
const TAG_OPTIONS: [&str; 4] = ["", "env:prod", "env:dev", "env:prod,version:1.1"];

/// A group of tags sharing a key with a pool of possible values.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct TagGroup {
    /// The tag key (e.g., "env", "team", "region")
    pub key: String,
    /// Pool of possible values for this key
    pub values: Vec<String>,
    /// Probability (0.0-1.0) that this tag group appears in a given log
    pub frequency: f32,
}

/// Configuration for the `DatadogLog` payload generator.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Tag groups defining key/value pools and their inclusion frequency.
    /// When empty, uses a small built-in set for backward compatibility.
    pub tag_groups: Vec<TagGroup>,
    /// Range of tags per message. Only used when `tag_groups` is non-empty.
    /// When `tag_groups` is provided, this clamps the total tag count per log.
    pub tags_per_msg: ConfRange<u8>,
    /// Number of unique tag strings to pre-generate at startup.
    /// Default: 10,000.
    pub tag_pool_size: u32,
    /// Length range for the log message field (bytes).
    /// Default: 1..=16 (current behavior).
    pub message_length: ConfRange<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tag_groups: Vec::new(),
            tags_per_msg: ConfRange::Inclusive { min: 1, max: 4 },
            tag_pool_size: 10_000,
            message_length: ConfRange::Inclusive { min: 1, max: 16 },
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Structured {
    proportional: u32,
    integral: u64,
    derivative: f64,
    vegetable: i16,
}

impl Distribution<Structured> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Structured
    where
        R: Rng + ?Sized,
    {
        Structured {
            proportional: rng.random(),
            integral: rng.random(),
            derivative: rng.random(),
            vegetable: rng.random(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum Message<'a> {
    Unstructured(&'a str),
    Structured(String),
}

fn message<'a, R>(
    rng: &mut R,
    str_pool: &'a strings::RandomStringPool,
    message_length: ConfRange<u16>,
) -> Message<'a>
where
    R: rand::Rng + ?Sized,
{
    let min = message_length.start();
    let max = message_length.end();
    match rng.random_range(0..2) {
        0 => Message::Unstructured(
            str_pool
                .of_size_range(rng, min..max)
                .expect("failed to generate string"),
        ),
        1 => Message::Structured(
            serde_json::to_string(&rng.random::<Structured>()).expect("failed to generate string"),
        ),
        _ => unreachable!(),
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
/// Derived from Datadog Agent sources, [here](https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49).
pub struct Member<'a> {
    /// The message is a short ascii string, without newlines for now
    pub(crate) message: Message<'a>,
    /// The message status
    pub(crate) status: &'a str,
    /// The timestamp is a simple integer value since epoch, presumably
    pub(crate) timestamp: u32,
    /// The hostname that sent the logs
    pub(crate) hostname: &'a str,
    /// The service that sent the logs
    pub(crate) service: &'a str,
    /// The ultimate source of the logs
    pub(crate) ddsource: &'a str,
    /// Comma-separate list of tags
    pub(crate) ddtags: &'a str,
}

#[derive(Debug)]
/// Datadog log format payload
pub struct DatadogLog {
    str_pool: strings::RandomStringPool,
    /// Pre-generated comma-separated tag strings (used when `tag_groups` is non-empty)
    tag_strings: Vec<String>,
    /// True when `tag_groups` is empty; uses legacy `TAG_OPTIONS`
    use_legacy_tags: bool,
    /// Stored config for `message_length` range
    message_length: ConfRange<u16>,
}

impl DatadogLog {
    /// Create a new instance of `DatadogLog`
    pub fn new<R>(config: &Config, rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let use_legacy_tags = config.tag_groups.is_empty();
        let tag_strings = if use_legacy_tags {
            Vec::new()
        } else {
            Self::build_tag_pool(config, rng)
        };

        Self {
            str_pool: strings::RandomStringPool::with_size(rng, 1_000_000),
            tag_strings,
            use_legacy_tags,
            message_length: config.message_length,
        }
    }

    /// Pre-generate a pool of comma-separated tag strings from the configured tag groups.
    fn build_tag_pool<R>(config: &Config, rng: &mut R) -> Vec<String>
    where
        R: rand::Rng + ?Sized,
    {
        let min_tags = usize::from(config.tags_per_msg.start());
        let max_tags = usize::from(config.tags_per_msg.end());

        (0..config.tag_pool_size)
            .map(|_| {
                // Determine which groups are included based on frequency
                let mut tags: Vec<String> = Vec::new();
                for group in &config.tag_groups {
                    if rng.random::<f32>() < group.frequency {
                        let value = group
                            .values
                            .choose(rng)
                            .expect("tag group values must not be empty");
                        tags.push(format!("{}:{}", group.key, value));
                    }
                }

                // Clamp to configured range
                tags.truncate(max_tags);
                // If we have fewer than min, add more from random groups
                while tags.len() < min_tags && !config.tag_groups.is_empty() {
                    let group = config
                        .tag_groups
                        .choose(rng)
                        .expect("tag_groups is non-empty");
                    let value = group
                        .values
                        .choose(rng)
                        .expect("tag group values must not be empty");
                    tags.push(format!("{}:{}", group.key, value));
                    if tags.len() >= max_tags {
                        break;
                    }
                }

                tags.join(",")
            })
            .collect()
    }
}

impl<'a> Generator<'a> for DatadogLog {
    type Output = Member<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let ddtags = if self.use_legacy_tags {
            *TAG_OPTIONS
                .choose(rng)
                .expect("failed to generate tag options")
        } else {
            &self.tag_strings[rng.random_range(0..self.tag_strings.len())]
        };

        Ok(Member {
            message: message(&mut rng, &self.str_pool, self.message_length),
            status: STATUSES.choose(rng).expect("failed to generate status"),
            timestamp: rng.random(),
            hostname: HOSTNAMES.choose(rng).expect("failed to generate hostnames"),
            service: SERVICES.choose(rng).expect("failed to generate services"),
            ddsource: SOURCES.choose(rng).expect("failed to generate sources"),
            ddtags,
        })
    }
}

impl crate::Serialize for DatadogLog {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        let approx_member_encoded_size = if self.use_legacy_tags {
            220 // bytes, determined experimentally
        } else {
            // Estimate from average pre-generated tag string length + fixed overhead
            let avg_tag_len = if self.tag_strings.is_empty() {
                0
            } else {
                let sample_count = self.tag_strings.len().min(100);
                let total: usize = self.tag_strings[..sample_count]
                    .iter()
                    .map(String::len)
                    .sum();
                total / sample_count
            };
            // ~180 bytes fixed overhead (JSON keys, other fields) + tag string length
            180 + avg_tag_len
        };

        if max_bytes < approx_member_encoded_size {
            // 'empty' payload  is []
            return Ok(());
        }

        // We will arbitrarily generate Member instances and then serialize. If
        // this is below `max_bytes` we'll add more until we're over. Once we
        // are we'll start removing instances until we're back below the limit.

        let cap = (max_bytes / approx_member_encoded_size) + 100;
        let mut members: Vec<Member> = Vec::with_capacity(cap);
        for _ in 0..cap {
            members.push(self.generate(&mut rng)?);
        }

        // Search for an encoding that's just right.
        // Reuse a single buffer across iterations to avoid repeated allocations.
        let mut buffer: Vec<u8> = Vec::with_capacity(max_bytes);
        let mut high = members.len();
        while high != 0 {
            buffer.clear();
            serde_json::to_writer(&mut buffer, &members[0..high])?;
            if buffer.len() > max_bytes {
                high /= 2;
            } else {
                writer.write_all(&buffer)?;
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::{Config, Member, TagGroup};
    use crate::{DatadogLog, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut ddlogs = DatadogLog::new(&Config::default(), &mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as json, is not truncated etc.
    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut ddlogs = DatadogLog::new(&Config::default(), &mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            let payload = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str");
            for msg in payload.lines() {
                let _members: Vec<Member> = serde_json::from_str(msg).expect("failed to deserialize from str");
            }
        }
    }

    proptest! {
        #[test]
        fn custom_tag_groups_payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let config = Config {
                tag_groups: vec![
                    TagGroup {
                        key: "env".to_string(),
                        values: vec!["staging".into(), "production".into(), "development".into()],
                        frequency: 1.0,
                    },
                    TagGroup {
                        key: "team".to_string(),
                        values: vec!["infra".into(), "platform".into(), "ml-ops".into()],
                        frequency: 0.8,
                    },
                    TagGroup {
                        key: "region".to_string(),
                        values: vec!["us-east-1".into(), "eu-west-1".into()],
                        frequency: 0.7,
                    },
                ],
                tag_pool_size: 100,
                ..Config::default()
            };
            let mut ddlogs = DatadogLog::new(&config, &mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }

    proptest! {
        #[test]
        fn custom_tag_groups_every_payload_deserializes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let config = Config {
                tag_groups: vec![
                    TagGroup {
                        key: "env".to_string(),
                        values: vec!["staging".into(), "production".into()],
                        frequency: 1.0,
                    },
                    TagGroup {
                        key: "team".to_string(),
                        values: vec!["infra".into(), "platform".into()],
                        frequency: 0.5,
                    },
                ],
                tag_pool_size: 100,
                ..Config::default()
            };
            let mut ddlogs = DatadogLog::new(&config, &mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            ddlogs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            let payload = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str");
            for msg in payload.lines() {
                let _members: Vec<Member> = serde_json::from_str(msg).expect("failed to deserialize from str");
            }
        }
    }

    #[test]
    fn tag_strings_contain_expected_key_value_format() {
        let mut rng = SmallRng::seed_from_u64(42);
        let config = Config {
            tag_groups: vec![
                TagGroup {
                    key: "env".to_string(),
                    values: vec!["prod".into(), "dev".into()],
                    frequency: 1.0,
                },
                TagGroup {
                    key: "region".to_string(),
                    values: vec!["us-east-1".into()],
                    frequency: 1.0,
                },
            ],
            tag_pool_size: 50,
            ..Config::default()
        };
        let ddlogs = DatadogLog::new(&config, &mut rng);

        assert!(!ddlogs.use_legacy_tags);
        assert_eq!(ddlogs.tag_strings.len(), 50);

        for tag_str in &ddlogs.tag_strings {
            // Each tag string should contain key:value pairs
            for part in tag_str.split(',') {
                assert!(part.contains(':'), "expected key:value format, got: {part}");
            }
        }
    }
}
