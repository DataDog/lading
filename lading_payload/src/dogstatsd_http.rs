//! `DogStatsD`-over-HTTP payload (protobuf).
//!
//! Serializes `DogStatsD` Count and Gauge metrics into the binary protobuf format
//! defined by the Datadog Agent's `dogstatsdhttp/payload.proto`. Metric names
//! and tag strings are stored in byte dictionaries with varint-length prefixes;
//! per-metric arrays use 1-based index references with delta encoding.

use std::io::Write;

use rustc_hash::FxHashMap;

use bytes::BytesMut;
use rand::Rng;

use crate::common::strings::Pool;
use crate::dogstatsd::common::NumValue;
use crate::dogstatsd::metric::Metric;
use crate::{Generator, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Prost message structs (mirrors dogstatsdhttp/payload.proto)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, prost::Message)]
struct Payload {
    #[prost(message, optional, tag = "2")]
    metadata: Option<Metadata>,
    #[prost(message, optional, tag = "3")]
    metric_data: Option<MetricData>,
}

#[derive(Clone, prost::Message)]
struct Metadata {
    #[prost(string, repeated, tag = "1")]
    tags: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    resources: Vec<String>,
}

#[derive(Clone, prost::Message)]
struct MetricData {
    /// Dictionary of metric name strings: varint(len) || utf8 bytes per entry.
    #[prost(bytes = "vec", tag = "1")]
    dict_name_str: Vec<u8>,
    /// Dictionary of tag strings: varint(len) || utf8 bytes per entry.
    #[prost(bytes = "vec", tag = "2")]
    dict_tag_str: Vec<u8>,
    /// Flat delta-encoded tagset entries: `[num_tags, delta_idx1, delta_idx2, …]`
    #[prost(sint64, repeated, tag = "3")]
    dict_tagsets: Vec<i64>,

    /// Bitwise-OR of metricType | valueType | metricFlags per metric.
    #[prost(uint64, repeated, tag = "10")]
    types: Vec<u64>,
    /// Delta-encoded 1-based name indices.
    #[prost(sint64, repeated, tag = "11")]
    name_refs: Vec<i64>,
    /// Delta-encoded tagset references (0 = no tags).
    #[prost(sint64, repeated, tag = "12")]
    tagset_refs: Vec<i64>,
    /// Reporting interval per metric (unused; kept for proto compatibility).
    #[prost(uint64, repeated, tag = "14")]
    intervals: Vec<u64>,
    /// Number of values per metric point (always 1 here).
    #[prost(uint64, repeated, tag = "15")]
    num_points: Vec<u64>,
    /// Delta-encoded timestamps (0 = inherit/delta from previous).
    #[prost(sint64, repeated, tag = "16")]
    timestamps: Vec<i64>,
    /// Integer values for metrics whose valueType is Sint64 (0x10).
    #[prost(sint64, repeated, tag = "17")]
    vals_sint64: Vec<i64>,
    /// Float64 values for metrics whose valueType is Float64 (0x30).
    #[prost(double, repeated, tag = "19")]
    vals_float64: Vec<f64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Dictionary helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Builds `dictNameStr` / `dictTagStr`: varint(len) || utf8 bytes per entry.
///
/// Indices are **1-based**; 0 is reserved for "empty / not present".
struct StringDict {
    map: FxHashMap<String, usize>,
    bytes: Vec<u8>,
}

impl StringDict {
    fn new() -> Self {
        Self {
            map: FxHashMap::default(),
            bytes: Vec::new(),
        }
    }

    /// Return the 1-based index for `s`, inserting it if not already present.
    fn get_or_insert(&mut self, s: &str) -> usize {
        if let Some(&idx) = self.map.get(s) {
            return idx;
        }
        let idx = self.map.len() + 1;
        self.map.insert(s.to_owned(), idx);
        let mut buf = BytesMut::new();
        prost::encoding::encode_varint(s.len() as u64, &mut buf);
        self.bytes.extend_from_slice(&buf);
        self.bytes.extend_from_slice(s.as_bytes());
        idx
    }
}

/// Builds `dictTagsets` (flat repeated sint64).
///
/// Each tagset entry occupies: `[num_tags, delta_idx1, delta_idx2, …]`.
/// A tagset reference of 0 means "no tags" and is never stored in this dict.
struct TagsetDict {
    map: FxHashMap<Vec<usize>, usize>,
    flat: Vec<i64>,
}

impl TagsetDict {
    fn new() -> Self {
        Self {
            map: FxHashMap::default(),
            flat: Vec::new(),
        }
    }

    /// Return the 1-based tagset reference for `tag_indices` (sorted for
    /// canonical identity), inserting a new entry if not already present.
    #[expect(clippy::cast_possible_wrap)]
    fn get_or_insert(&mut self, mut tag_indices: Vec<usize>) -> usize {
        tag_indices.sort_unstable();
        if let Some(&idx) = self.map.get(&tag_indices) {
            return idx;
        }
        let idx = self.map.len() + 1;
        self.map.insert(tag_indices.clone(), idx);
        self.flat.push(tag_indices.len() as i64);
        let mut prev = 0usize;
        for &ti in &tag_indices {
            self.flat.push((ti as i64) - (prev as i64));
            prev = ti;
        }
        idx
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DogStatsDHttp
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
/// Generator for DogStatsD-over-HTTP protobuf payloads.
///
/// Only **Count** and **Gauge** metric types are encoded; all other types
/// (Timer, Histogram, Distribution, Set) are skipped silently.
pub struct DogStatsDHttp {
    metric_generator: crate::dogstatsd::metric::MetricGenerator,
}

impl DogStatsDHttp {
    /// Create a new `DogStatsDHttp` instance from a [`crate::dogstatsd::Config`].
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid or internal string pool
    /// creation fails.
    pub fn new<R: Rng + ?Sized>(
        config: &crate::dogstatsd::Config,
        rng: &mut R,
    ) -> Result<Self, crate::Error> {
        use crate::common::strings::{RandomStringPool, StringListPool, PoolKind, random_strings_with_length};
        use rand::distr::weighted::WeightedIndex;
        use std::rc::Rc;

        let pool = RandomStringPool::with_size(rng, 8_000_000);
        let small_strings = random_strings_with_length(&pool, 16..1024, 8, rng);

        let str_pool = Rc::new(pool.clone());
        let pool_kind = Rc::new(PoolKind::RandomStringPool(pool));

        let tag_name_pool = if config.tag_names.is_empty() {
            Rc::clone(&pool_kind)
        } else {
            Rc::new(PoolKind::StringListPool(StringListPool::new(
                config.tag_names.clone(),
            )))
        };

        let tag_value_pool = if config.tag_values.is_empty() {
            Rc::clone(&pool_kind)
        } else {
            Rc::new(PoolKind::StringListPool(StringListPool::new(
                config.tag_values.clone(),
            )))
        };

        let name_pool = if config.metric_names.is_empty() {
            Rc::clone(&pool_kind)
        } else {
            Rc::new(PoolKind::StringListPool(StringListPool::new(
                config.metric_names.clone(),
            )))
        };

        let pools = crate::dogstatsd::StringPools {
            tag_name: tag_name_pool,
            tag_value: tag_value_pool,
            name: name_pool,
            randomstring: str_pool,
        };

        let num_contexts = config.contexts.sample(rng);

        let mut tags_generator = match crate::dogstatsd::common::tags::Generator::new(
            rng.random(),
            config.tags_per_msg,
            config.tag_length,
            num_contexts as usize,
            Rc::clone(&pools.tag_value),
            Rc::clone(&pools.tag_name),
            config.unique_tag_ratio,
        ) {
            Ok(tg) => tg,
            Err(e) => {
                tracing::warn!("Encountered error while constructing tag generator: {e}");
                return Err(crate::Error::StringGenerate);
            }
        };

        let metric_choices = [
            u16::from(config.metric_weights.count),
            u16::from(config.metric_weights.gauge),
            u16::from(config.metric_weights.timer),
            u16::from(config.metric_weights.distribution),
            u16::from(config.metric_weights.set),
            u16::from(config.metric_weights.histogram),
        ];

        let metric_generator = crate::dogstatsd::metric::MetricGenerator::new(
            num_contexts as usize,
            config.name_length,
            config.multivalue_count,
            config.multivalue_pack_probability,
            config.sampling_range,
            config.sampling_probability,
            &WeightedIndex::new(metric_choices)?,
            small_strings,
            &mut tags_generator,
            &pools,
            config.value,
            rng,
        )?;

        Ok(DogStatsDHttp { metric_generator })
    }
}

impl Serialize for DogStatsDHttp {
    #[expect(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_wrap)]
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), crate::Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        use prost::Message;

        let mut md = MetricData::default();
        let mut name_dict = StringDict::new();
        let mut tag_dict = StringDict::new();
        let mut tagset_dict = TagsetDict::new();
        let mut prev_name_ref: i64 = 0;
        let mut prev_tagset_ref: i64 = 0;

        loop {
            let member = self.metric_generator.generate(&mut rng)?;

            // Extract metric type and first value; skip unsupported types.
            let (proto_metric_type, num_value) = match &member {
                Metric::Count(count) => (1u64, count.values[0]),
                Metric::Gauge(gauge) => (3u64, gauge.values[0]),
                _ => continue,
            };

            // Name is already a resolved &str on the metric struct.
            let name_str = match &member {
                Metric::Count(count) => count.name,
                Metric::Gauge(gauge) => gauge.name,
                _ => unreachable!(),
            };

            // Checkpoint dict state so we can roll back if this metric doesn't fit.
            let name_bytes_checkpoint = name_dict.bytes.len();
            let name_map_checkpoint = name_dict.map.len();
            let tag_bytes_checkpoint = tag_dict.bytes.len();
            let tag_map_checkpoint = tag_dict.map.len();
            let tagset_flat_checkpoint = tagset_dict.flat.len();
            let tagset_map_checkpoint = tagset_dict.map.len();

            let name_idx = name_dict.get_or_insert(name_str) as i64;

            // Resolve tags from handle → string and look up in tag dict.
            let tag_indices: Vec<usize> = {
                let (tags, pools) = match &member {
                    Metric::Count(count) => (count.tags, count.pools),
                    Metric::Gauge(gauge) => (gauge.tags, gauge.pools),
                    _ => unreachable!(),
                };
                tags.iter()
                    .map(|tag| {
                        let k = pools
                            .tag_name
                            .using_handle(tag.key)
                            .expect("invalid tag key handle");
                        let v = pools
                            .tag_value
                            .using_handle(tag.value)
                            .expect("invalid tag value handle");
                        tag_dict.get_or_insert(&format!("{k}:{v}"))
                    })
                    .collect()
            };

            let tagset_idx = if tag_indices.is_empty() {
                0i64 // 0 = no tags
            } else {
                tagset_dict.get_or_insert(tag_indices) as i64
            };

            // Determine value encoding.
            let (value_type, value_sint64, value_f64): (u64, Option<i64>, Option<f64>) =
                match num_value {
                    NumValue::Int(i) => (0x10, Some(i), None),
                    NumValue::Float(f) => (0x30, None, Some(f)),
                };

            // Append to MetricData arrays.
            md.types.push(proto_metric_type | value_type);
            md.name_refs.push(name_idx - prev_name_ref);
            prev_name_ref = name_idx;
            md.tagset_refs.push(tagset_idx - prev_tagset_ref);
            prev_tagset_ref = tagset_idx;
            md.num_points.push(1);
            md.timestamps.push(0); // 0 = delta from previous (first = epoch)
            if let Some(v) = value_sint64 {
                md.vals_sint64.push(v);
            }
            if let Some(v) = value_f64 {
                md.vals_float64.push(v);
            }

            // Budget check: snapshot dict bytes into MetricData to get the real
            // encoded size. This clone is intentional — profiling can determine
            // if a cheaper heuristic is worthwhile.
            let snapshot = MetricData {
                dict_name_str: name_dict.bytes.clone(),
                dict_tag_str: tag_dict.bytes.clone(),
                dict_tagsets: tagset_dict.flat.clone(),
                ..md.clone()
            };
            let required = Payload {
                metadata: None,
                metric_data: Some(snapshot),
            }
            .encoded_len();

            if required > max_bytes {
                // Undo metric data array appends.
                md.types.pop();
                md.name_refs.pop();
                md.tagset_refs.pop();
                md.num_points.pop();
                md.timestamps.pop();
                if value_sint64.is_some() {
                    md.vals_sint64.pop();
                }
                if value_f64.is_some() {
                    md.vals_float64.pop();
                }
                // Undo dict mutations: truncate bytes/flat and remove new map entries.
                // New entries have indices > checkpoint lengths (indices are 1-based).
                name_dict.bytes.truncate(name_bytes_checkpoint);
                name_dict.map.retain(|_, &mut v| v <= name_map_checkpoint);
                tag_dict.bytes.truncate(tag_bytes_checkpoint);
                tag_dict.map.retain(|_, &mut v| v <= tag_map_checkpoint);
                tagset_dict.flat.truncate(tagset_flat_checkpoint);
                tagset_dict.map.retain(|_, &mut v| v <= tagset_map_checkpoint);
                break;
            }
        }

        // Nothing fit within the budget.
        if md.types.is_empty() {
            return Ok(());
        }

        // Finalize: move dict bytes into MetricData.
        md.dict_name_str = name_dict.bytes;
        md.dict_tag_str = tag_dict.bytes;
        md.dict_tagsets = tagset_dict.flat;

        let payload = Payload {
            metadata: None,
            metric_data: Some(md),
        };
        let mut buf = Vec::with_capacity(payload.encoded_len());
        payload.encode(&mut buf)?;
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dogstatsd::Config;
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    fn make_generator(seed: u64) -> DogStatsDHttp {
        let mut rng = SmallRng::seed_from_u64(seed);
        DogStatsDHttp::new(&Config::default(), &mut rng).expect("failed to create DogStatsDHttp")
    }

    /// Decode the serialized bytes and verify basic structural invariants.
    #[test]
    fn roundtrip_decodes_and_has_metrics() {
        use prost::Message;

        let mut dd = make_generator(42);
        let mut rng = SmallRng::seed_from_u64(42);
        let max_bytes = 65_536;

        let mut buf = Vec::with_capacity(max_bytes);
        dd.to_bytes(&mut rng, max_bytes, &mut buf)
            .expect("serialization failed");

        assert!(!buf.is_empty(), "expected non-empty output");

        let payload = Payload::decode(bytes::Bytes::from(buf)).expect("decode failed");
        let md = payload.metric_data.expect("metric_data should be present");

        assert!(!md.types.is_empty(), "types array should not be empty");
        assert_eq!(
            md.name_refs.len(),
            md.types.len(),
            "name_refs and types must have the same length"
        );
        assert_eq!(
            md.tagset_refs.len(),
            md.types.len(),
            "tagset_refs and types must have the same length"
        );
        assert_eq!(
            md.num_points.len(),
            md.types.len(),
            "num_points and types must have the same length"
        );
        assert_eq!(
            md.timestamps.len(),
            md.types.len(),
            "timestamps and types must have the same length"
        );
        // Every value goes into exactly one of the two value arrays.
        assert_eq!(
            md.vals_sint64.len() + md.vals_float64.len(),
            md.types.len(),
            "total value count must equal number of metrics"
        );
    }

    /// All `types` entries should be Count (1) or Gauge (3) combined with a
    /// valid value type (0x10 or 0x30).
    #[test]
    fn metric_types_are_count_or_gauge() {
        use prost::Message;

        let mut dd = make_generator(7);
        let mut rng = SmallRng::seed_from_u64(7);

        let mut buf = Vec::new();
        dd.to_bytes(&mut rng, 32_768, &mut buf)
            .expect("serialization failed");

        if buf.is_empty() {
            return; // nothing generated — budget too small, skip
        }

        let payload = Payload::decode(bytes::Bytes::from(buf)).expect("decode failed");
        let md = payload.metric_data.unwrap();

        for &t in &md.types {
            let metric_type = t & 0x0F;
            let value_type = t & 0xF0;
            assert!(
                metric_type == 1 || metric_type == 3,
                "unexpected metric type {metric_type} in type word {t:#04x}"
            );
            assert!(
                value_type == 0x10 || value_type == 0x30,
                "unexpected value type {value_type:#04x} in type word {t:#04x}"
            );
        }
    }

    /// An empty output is expected when `max_bytes` is too small to hold even a
    /// single metric.
    #[test]
    fn tiny_budget_produces_empty_output() {
        let mut dd = make_generator(1);
        let mut rng = SmallRng::seed_from_u64(1);
        let mut buf = Vec::new();
        // 1 byte is never enough for a full proto message.
        dd.to_bytes(&mut rng, 1, &mut buf)
            .expect("serialization failed");
        assert!(buf.is_empty());
    }

    proptest! {
        /// The serialized output must never exceed `max_bytes`.
        #[test]
        fn payload_never_exceeds_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut dd = DogStatsDHttp::new(&Config::default(), &mut rng)
                .expect("failed to create generator");

            let mut buf = Vec::with_capacity(max_bytes);
            dd.to_bytes(rng, max_bytes, &mut buf)
                .expect("serialization failed");

            prop_assert!(
                buf.len() <= max_bytes,
                "output {} bytes exceeds max_bytes {}",
                buf.len(),
                max_bytes
            );
        }
    }

    proptest! {
        /// Whatever is written must always be valid protobuf.
        #[test]
        fn output_is_always_valid_proto(seed: u64, max_bytes in 0usize..65_536) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut dd = DogStatsDHttp::new(&Config::default(), &mut rng)
                .expect("failed to create generator");

            let mut buf = Vec::new();
            dd.to_bytes(rng, max_bytes, &mut buf)
                .expect("serialization failed");

            if buf.is_empty() {
                return Ok(());
            }

            use prost::Message as _;
            let result = Payload::decode(bytes::Bytes::from(buf));
            prop_assert!(result.is_ok(), "invalid proto: {:?}", result.err());
        }
    }
}
