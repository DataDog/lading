//! OpenTelemetry OTLP metric payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

// Alright, to summarize this payload generator's understanding of the
// data-model described above:
//
// The central concern is a `Metric`. A `Metric` is:
//
// * name: String -- unique metric identifier
// * description: String -- optional human friendly description
// * unit: String -- unit of measurement, described by http://unitsofmeasure.org/ucum.html
// * data: enum { Gauge, Sum, Histogram, ExponentialHistogram, Summary }
//
// Each `Metric` is associated with a `Resource` -- called `ResourceMetrics` in
// the proto -- which defines the origination point of the `Metric`. Interior to
// the `Resource` is the `Scope` -- called `ScopeMetrics` in the proto -- which
// defines the library/smaller-than-resource scope that generated the
// `Metric`. We will not set `Scope` until we find out that it's important to do
// so for modeling purposes.
//
// The data types are as follows:
//
// * Gauge -- a collection of `NumberDataPoint`, values sampled at specific times
// * Sum -- `Gauge` with the addition of monotonic flag, aggregation metadata
//
// We omit Histogram / ExponentialHistogram / Summary in this current version
// but will introduce them in the near-term. The `NumberDataPoint` is a
//
// * attributes: Vec<KeyValue> -- tags
// * start_time_unix_nano: u64 -- represents the first possible moment a measurement could be recorded, optional
// * time_unix_nano: u64 -- a timestamp when the value was sampled
// * value: enum { u64, f64 } -- the value
// * flags: uu32 -- I'm not sure what to make of this yet

pub(crate) mod templates;
pub(crate) mod unit;

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::rc::Rc;
use std::{cell::RefCell, io::Write};

use crate::SizedGenerator;
use crate::opentelemetry::common::templates::PoolError;
use crate::{Error, common::config::ConfRange, common::strings};
use bytes::BytesMut;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, Metric, ResourceMetrics, exemplar, exponential_histogram_data_point, metric::Data,
    number_data_point,
};
use prost::Message;
use serde::Deserialize;
use templates::{Pool, ResourceTemplateGenerator};
use tracing::debug;
use unit::UnitGenerator;

/// Smallest useful protobuf, determined by experimentation and enforced in
/// `smallest_protobuf` test.
pub const SMALLEST_PROTOBUF: usize = 31;

/// Advance collection timestamps by 100 milliseconds per synthetic tick.
const TIME_INCREMENT_NANOS: u64 = 100_000_000;
const BASE_TIME_UNIX_NANOS: u64 = 1_700_000_000_000_000_000;
const DELTA_TEMPORALITY: i32 = 1;
const CUMULATIVE_TEMPORALITY: i32 = 2;
const SUMMARY_RESERVOIR_CAP: usize = 512;

#[derive(Clone, Debug)]
struct SeriesClock {
    stream_start_time_unix_nano: u64,
    last_time_unix_nano: u64,
    emissions: u64,
}

impl SeriesClock {
    fn new(current_time_unix_nano: u64, previous_time_unix_nano: u64) -> Self {
        Self {
            stream_start_time_unix_nano: current_time_unix_nano,
            last_time_unix_nano: previous_time_unix_nano,
            emissions: 0,
        }
    }

    fn start_time_for_temporality(&self, temporality: i32) -> u64 {
        if temporality == DELTA_TEMPORALITY {
            self.last_time_unix_nano
        } else {
            self.stream_start_time_unix_nano
        }
    }

    fn note_emission(&mut self, current_time_unix_nano: u64) {
        self.last_time_unix_nano = current_time_unix_nano;
        self.emissions += 1;
    }
}

#[derive(Clone, Debug)]
struct HistogramPointState {
    clock: SeriesClock,
    cumulative_bucket_counts: Vec<u64>,
    cumulative_count: u64,
    cumulative_sum: f64,
    cumulative_min: Option<f64>,
    cumulative_max: Option<f64>,
}

impl HistogramPointState {
    fn new(current_time_unix_nano: u64, previous_time_unix_nano: u64, bucket_count: usize) -> Self {
        Self {
            clock: SeriesClock::new(current_time_unix_nano, previous_time_unix_nano),
            cumulative_bucket_counts: vec![0; bucket_count],
            cumulative_count: 0,
            cumulative_sum: 0.0,
            cumulative_min: None,
            cumulative_max: None,
        }
    }
}

#[derive(Clone, Debug)]
struct SummaryPointState {
    clock: SeriesClock,
    cumulative_count: u64,
    cumulative_sum: f64,
    retained_observations: Vec<f64>,
}

impl SummaryPointState {
    fn new(current_time_unix_nano: u64, previous_time_unix_nano: u64) -> Self {
        Self {
            clock: SeriesClock::new(current_time_unix_nano, previous_time_unix_nano),
            cumulative_count: 0,
            cumulative_sum: 0.0,
            retained_observations: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct ExponentialHistogramPointState {
    clock: SeriesClock,
    cumulative_positive: BTreeMap<i32, u64>,
    cumulative_negative: BTreeMap<i32, u64>,
    cumulative_zero_count: u64,
    cumulative_count: u64,
    cumulative_sum: f64,
    cumulative_min: Option<f64>,
    cumulative_max: Option<f64>,
}

impl ExponentialHistogramPointState {
    fn new(current_time_unix_nano: u64, previous_time_unix_nano: u64) -> Self {
        Self {
            clock: SeriesClock::new(current_time_unix_nano, previous_time_unix_nano),
            cumulative_positive: BTreeMap::new(),
            cumulative_negative: BTreeMap::new(),
            cumulative_zero_count: 0,
            cumulative_count: 0,
            cumulative_sum: 0.0,
            cumulative_min: None,
            cumulative_max: None,
        }
    }
}

#[derive(Clone, Debug)]
struct PopulationStats {
    observations: Vec<f64>,
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

fn tick_time_unix_nano(tick: u64) -> u64 {
    BASE_TIME_UNIX_NANOS + tick.saturating_mul(TIME_INCREMENT_NANOS)
}

fn hash_any_value(value: &any_value::Value, hasher: &mut DefaultHasher) {
    match value {
        any_value::Value::StringValue(v) => {
            "string".hash(hasher);
            v.hash(hasher);
        }
        any_value::Value::BoolValue(v) => {
            "bool".hash(hasher);
            v.hash(hasher);
        }
        any_value::Value::IntValue(v) => {
            "int".hash(hasher);
            v.hash(hasher);
        }
        any_value::Value::DoubleValue(v) => {
            "double".hash(hasher);
            v.to_bits().hash(hasher);
        }
        // ArrayValue / KvlistValue / BytesValue are never produced by this
        // generator; hashing the discriminant keeps identity stable if that
        // ever changes.
        other => std::mem::discriminant(other).hash(hasher),
    }
}

fn hash_key_values(values: &[KeyValue], hasher: &mut DefaultHasher) {
    for value in values {
        value.key.hash(hasher);
        if let Some(any_value) = &value.value
            && let Some(inner) = &any_value.value
        {
            hash_any_value(inner, hasher);
        }
    }
}

fn metric_identity_hash(
    resource_attributes: Option<&[KeyValue]>,
    scope_attributes: Option<&[KeyValue]>,
    metric: &Metric,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    if let Some(resource_attributes) = resource_attributes {
        hash_key_values(resource_attributes, &mut hasher);
    }
    if let Some(scope_attributes) = scope_attributes {
        hash_key_values(scope_attributes, &mut hasher);
    }
    metric.name.hash(&mut hasher);
    metric.description.hash(&mut hasher);
    metric.unit.hash(&mut hasher);
    hash_key_values(&metric.metadata, &mut hasher);

    if let Some(data) = &metric.data {
        match data {
            Data::Gauge(_) => "gauge".hash(&mut hasher),
            Data::Sum(sum) => {
                "sum".hash(&mut hasher);
                sum.aggregation_temporality.hash(&mut hasher);
                sum.is_monotonic.hash(&mut hasher);
            }
            Data::Histogram(histogram) => {
                "histogram".hash(&mut hasher);
                histogram.aggregation_temporality.hash(&mut hasher);
            }
            Data::ExponentialHistogram(histogram) => {
                "exponential_histogram".hash(&mut hasher);
                histogram.aggregation_temporality.hash(&mut hasher);
            }
            Data::Summary(_) => "summary".hash(&mut hasher),
        }
    }

    hasher.finish()
}

fn point_series_id(metric_identity: u64, point_index: usize, attributes: &[KeyValue]) -> u64 {
    let mut hasher = DefaultHasher::new();
    metric_identity.hash(&mut hasher);
    point_index.hash(&mut hasher);
    hash_key_values(attributes, &mut hasher);
    hasher.finish()
}

fn update_min(current: Option<f64>, candidate: Option<f64>) -> Option<f64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (None, Some(candidate)) => Some(candidate),
        (Some(current), None) => Some(current),
        (None, None) => None,
    }
}

fn update_max(current: Option<f64>, candidate: Option<f64>) -> Option<f64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (None, Some(candidate)) => Some(candidate),
        (Some(current), None) => Some(current),
        (None, None) => None,
    }
}

fn population_stats(observations: Vec<f64>) -> PopulationStats {
    let mut sum = 0.0;
    let mut min = None;
    let mut max = None;

    for observation in &observations {
        sum += observation;
        min = update_min(min, Some(*observation));
        max = update_max(max, Some(*observation));
    }

    PopulationStats {
        count: observations.len() as u64,
        sum,
        min,
        max,
        observations,
    }
}

fn random_exemplars<R>(
    current_time_unix_nano: u64,
    min: Option<f64>,
    max: Option<f64>,
    rng: &mut R,
) -> Vec<Exemplar>
where
    R: rand::Rng + ?Sized,
{
    let min = min.unwrap_or(0.0);
    let max = max.unwrap_or(min).max(min);
    let exemplar_count = rng.random_range(2_usize..=4);
    let mut exemplars = Vec::with_capacity(exemplar_count);

    for _ in 0..exemplar_count {
        let trace_id: [u8; 16] = rng.random();
        let span_id: [u8; 8] = rng.random();
        exemplars.push(Exemplar {
            filtered_attributes: Vec::new(),
            time_unix_nano: current_time_unix_nano,
            span_id: span_id.to_vec(),
            trace_id: trace_id.to_vec(),
            value: Some(exemplar::Value::AsDouble(rng.random_range(min..=max))),
        });
    }

    exemplars
}

fn non_negative_population<R>(
    profile: templates::PointProfile,
    emissions: u64,
    rng: &mut R,
) -> PopulationStats
where
    R: rand::Rng + ?Sized,
{
    let count = rng.random_range(8_usize..=48);
    let center = profile.sample + profile.jitter * emissions as f64;
    let spread = profile.spread.max(1.0);
    let mut observations = Vec::with_capacity(count);

    for index in 0..count {
        let deterministic_offset = ((index as f64 + 1.0) / count as f64) * spread;
        let lower = (center - spread).max(0.0);
        let upper = center + deterministic_offset + profile.jitter;
        observations.push(rng.random_range(lower..=upper.max(lower)));
    }

    population_stats(observations)
}

fn signed_population<R>(
    profile: templates::PointProfile,
    zero_threshold: f64,
    emissions: u64,
    rng: &mut R,
) -> PopulationStats
where
    R: rand::Rng + ?Sized,
{
    let count = rng.random_range(12_usize..=64);
    let spread = (profile.spread / 4.0).max(1.0);
    let drift = profile.jitter * emissions as f64 / 8.0;
    let magnitude_base = (profile.sample / 16.0).max(1.0) + drift;
    let mut observations = Vec::with_capacity(count);

    for index in 0..count {
        let value = match rng.random_range(0_u8..=9) {
            0 => rng.random_range(-zero_threshold..=zero_threshold),
            1..=4 => magnitude_base + rng.random_range(0.0..=spread) + index as f64 * 0.25,
            _ => -(magnitude_base + rng.random_range(0.0..=spread) + index as f64 * 0.25),
        };
        observations.push(value);
    }

    population_stats(observations)
}

fn histogram_bucket_counts(observations: &[f64], explicit_bounds: &[f64]) -> Vec<u64> {
    let mut bucket_counts = vec![0; explicit_bounds.len() + 1];
    for observation in observations {
        let bucket_index = explicit_bounds.partition_point(|bound| *bound < *observation);
        if let Some(bucket_count) = bucket_counts.get_mut(bucket_index) {
            *bucket_count += 1;
        }
    }
    bucket_counts
}

fn merge_bucket_counts(target: &mut [u64], update: &[u64]) {
    for (target, update) in target.iter_mut().zip(update.iter().copied()) {
        *target += update;
    }
}

// OTLP defines bucket indices as sint32; the truncating cast is intentional.
#[allow(clippy::cast_possible_truncation)]
fn exponential_histogram_bucket_index(value: f64, scale: i32) -> i32 {
    if value <= 0.0 {
        return 0;
    }

    let scale_factor = 2_f64.powi(scale);
    (value.log2() * scale_factor).floor() as i32
}

/// Bucket `observations` into positive/negative/zero maps under the given
/// scale and `zero_threshold`. Returns `(zero_count, positive, negative)`.
fn exponential_histogram_buckets(
    observations: &[f64],
    scale: i32,
    zero_threshold: f64,
) -> (u64, BTreeMap<i32, u64>, BTreeMap<i32, u64>) {
    let mut zero_count = 0_u64;
    let mut positive = BTreeMap::new();
    let mut negative = BTreeMap::new();

    for observation in observations {
        if observation.abs() <= zero_threshold {
            zero_count += 1;
            continue;
        }

        let bucket_index = exponential_histogram_bucket_index(observation.abs(), scale);
        if *observation > 0.0 {
            *positive.entry(bucket_index).or_insert(0) += 1;
        } else {
            *negative.entry(bucket_index).or_insert(0) += 1;
        }
    }

    (zero_count, positive, negative)
}

// first_index/last_index come from an ordered BTreeMap and bucket_index is
// always >= first_index, so the differences are non-negative.
#[allow(clippy::cast_sign_loss)]
fn dense_exponential_buckets(
    counts: &BTreeMap<i32, u64>,
) -> exponential_histogram_data_point::Buckets {
    let (Some(first_index), Some(last_index)) = (counts.keys().next(), counts.keys().next_back())
    else {
        return exponential_histogram_data_point::Buckets {
            offset: 0,
            bucket_counts: Vec::new(),
        };
    };

    let mut bucket_counts = vec![0; (*last_index - *first_index + 1) as usize];
    for (bucket_index, count) in counts {
        let dense_index = (*bucket_index - *first_index) as usize;
        if let Some(bucket_count) = bucket_counts.get_mut(dense_index) {
            *bucket_count = *count;
        }
    }

    exponential_histogram_data_point::Buckets {
        offset: *first_index,
        bucket_counts,
    }
}

fn extend_summary_reservoir(reservoir: &mut Vec<f64>, observations: &[f64]) {
    reservoir.extend_from_slice(observations);
    if reservoir.len() > SUMMARY_RESERVOIR_CAP {
        let overflow = reservoir.len() - SUMMARY_RESERVOIR_CAP;
        reservoir.drain(0..overflow);
    }
}

// quantile is clamped to [0, 1] and the result lies in [0, len-1] as a
// non-negative finite float, so the usize cast is safe.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn quantile_value(sorted_observations: &[f64], quantile: f64) -> f64 {
    if sorted_observations.is_empty() {
        return 0.0;
    }

    let quantile = quantile.clamp(0.0, 1.0);
    let index = ((sorted_observations.len() - 1) as f64 * quantile).round() as usize;
    sorted_observations[index]
}

fn metric_data_points_total(metric: &Metric) -> usize {
    match &metric.data {
        Some(Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(Data::Sum(sum)) => sum.data_points.len(),
        Some(Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

fn trim_one_data_point(resource_metrics: &mut ResourceMetrics) -> bool {
    for scope_idx in (0..resource_metrics.scope_metrics.len()).rev() {
        let mut trimmed = false;
        let mut remove_metric_idx = None;
        let remove_scope;

        {
            let scope_metrics = &mut resource_metrics.scope_metrics[scope_idx];
            for metric_idx in (0..scope_metrics.metrics.len()).rev() {
                let metric = &mut scope_metrics.metrics[metric_idx];
                let did_trim = match &mut metric.data {
                    Some(Data::Gauge(gauge)) => gauge.data_points.pop().is_some(),
                    Some(Data::Sum(sum)) => sum.data_points.pop().is_some(),
                    Some(Data::Histogram(histogram)) => histogram.data_points.pop().is_some(),
                    Some(Data::ExponentialHistogram(histogram)) => {
                        histogram.data_points.pop().is_some()
                    }
                    Some(Data::Summary(summary)) => summary.data_points.pop().is_some(),
                    None => false,
                };
                if did_trim {
                    trimmed = true;
                    if metric_data_points_total(metric) == 0 {
                        remove_metric_idx = Some(metric_idx);
                    }
                    break;
                }
            }
            if let Some(metric_idx) = remove_metric_idx {
                scope_metrics.metrics.remove(metric_idx);
            }
            remove_scope = scope_metrics.metrics.is_empty();
        }

        if trimmed {
            if remove_scope {
                resource_metrics.scope_metrics.remove(scope_idx);
            }
            return true;
        }
    }

    false
}

fn total_data_points(resource_metrics: &ResourceMetrics) -> u64 {
    let mut total = 0_u64;
    for scope_metrics in &resource_metrics.scope_metrics {
        for metric in &scope_metrics.metrics {
            total += metric_data_points_total(metric) as u64;
        }
    }
    total
}

/// Configure the OpenTelemetry metric payload.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
/// OpenTelemetry metric contexts
pub struct Contexts {
    /// The range of contexts -- see documentation for `context_id` function --
    /// that will be generated.
    pub total_contexts: ConfRange<u32>,

    /// The range of attributes for resources.
    pub attributes_per_resource: ConfRange<u8>,
    /// The range of scopes that will be generated per resource.
    pub scopes_per_resource: ConfRange<u8>,
    /// The range of attributes for each scope.
    pub attributes_per_scope: ConfRange<u8>,
    /// The range of metrics that will be generated per scope.
    pub metrics_per_scope: ConfRange<u8>,
    /// The range of attributes for each metric.
    pub attributes_per_metric: ConfRange<u8>,
}

impl Default for Contexts {
    fn default() -> Self {
        Self {
            total_contexts: ConfRange::Constant(100),
            attributes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            scopes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_scope: ConfRange::Constant(0),
            metrics_per_scope: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_metric: ConfRange::Inclusive { min: 0, max: 10 },
        }
    }
}

/// Defines the relative probability of each kind of OpenTelemetry metric.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct MetricWeights {
    /// The relative probability of generating a gauge metric.
    pub gauge: u8,
    /// The relative probability of generating a sum delta metric.
    pub sum_delta: u8,
    /// The relative probability of generating a sum cumulative metric.
    pub sum_cumulative: u8,
    /// The relative probability of generating a histogram metric.
    pub histogram: u8,
    /// The relative probability of generating an exponential histogram metric.
    pub exponential_histogram: u8,
    /// The relative probability of generating a summary metric.
    pub summary: u8,
}

impl Default for MetricWeights {
    fn default() -> Self {
        Self {
            gauge: 40,                // 40%
            sum_delta: 25,            // 20%
            sum_cumulative: 25,       // 20%
            histogram: 15,            //15%
            exponential_histogram: 3, // 3%
            summary: 2,               //2%
        }
    }
}

/// Configure the OpenTelemetry metric payload.
#[derive(Debug, Default, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Defines the relative probability of each kind of OpenTelemetry metric.
    pub metric_weights: MetricWeights,
    /// Define the contexts available when generating metrics
    pub contexts: Contexts,
}

impl Config {
    /// Determine whether the passed configuration obeys validation criteria.
    ///
    /// # Errors
    /// Function will error if the configuration is invalid
    #[expect(clippy::too_many_lines)]
    pub fn valid(&self) -> Result<(), String> {
        // Validate metric weights - both types must have non-zero probability to ensure
        // we can generate a diverse set of metrics
        if self.metric_weights.gauge == 0
            || self.metric_weights.sum_delta == 0
            || self.metric_weights.sum_cumulative == 0
            || self.metric_weights.summary == 0
            || self.metric_weights.histogram == 0
            || self.metric_weights.exponential_histogram == 0
        {
            return Err("Metric weights cannot be 0".to_string());
        }

        // Validate total_contexts - we need at least one context to generate metrics
        match self.contexts.total_contexts {
            ConfRange::Constant(0) => return Err("total_contexts cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // Non-zero constant is valid
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("total_contexts minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err("total_contexts minimum cannot be greater than maximum".to_string());
                }
            }
        }

        // Validate scopes_per_resource - each resource must have at least one scope
        // to contain metrics
        match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => return Err("scopes_per_resource cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // Non-zero constant is valid
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("scopes_per_resource minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err(
                        "scopes_per_resource minimum cannot be greater than maximum".to_string()
                    );
                }
            }
        }

        // Validate metrics_per_scope - each scope must contain at least one metric
        // to be meaningful
        match self.contexts.metrics_per_scope {
            ConfRange::Constant(0) => return Err("metrics_per_scope cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // Non-zero constant is valid
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("metrics_per_scope minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err(
                        "metrics_per_scope minimum cannot be greater than maximum".to_string()
                    );
                }
            }
        }

        // Validate attributes_per_resource range
        if let ConfRange::Inclusive { min, max } = self.contexts.attributes_per_resource
            && min > max
        {
            return Err(
                "attributes_per_resource minimum cannot be greater than maximum".to_string(),
            );
        }

        // Validate attributes_per_scope range
        if let ConfRange::Inclusive { min, max } = self.contexts.attributes_per_scope
            && min > max
        {
            return Err("attributes_per_scope minimum cannot be greater than maximum".to_string());
        }

        // Validate attributes_per_metric range
        if let ConfRange::Inclusive { min, max } = self.contexts.attributes_per_metric
            && min > max
        {
            return Err("attributes_per_metric minimum cannot be greater than maximum".to_string());
        }

        let min_contexts = match self.contexts.total_contexts {
            ConfRange::Constant(n) => n,
            ConfRange::Inclusive { min, .. } => min,
        };
        let max_contexts = match self.contexts.total_contexts {
            ConfRange::Constant(n) => n,
            ConfRange::Inclusive { max, .. } => max,
        };

        // Calculate minimum and maximum possible contexts based on composition
        let min_configured = match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => 0u32,
            ConfRange::Constant(n) => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { min, .. } => u32::from(min),
                };
                u32::from(n) * metrics
            }
            ConfRange::Inclusive { min, .. } => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { min, .. } => u32::from(min),
                };
                u32::from(min) * metrics
            }
        };

        let max_configured = match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => 0u32,
            ConfRange::Constant(n) => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { max, .. } => u32::from(max),
                };
                u32::from(n) * metrics
            }
            ConfRange::Inclusive { max, .. } => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { max, .. } => u32::from(max),
                };
                u32::from(max) * metrics
            }
        };

        // Validate that the requested contexts are achievable
        if min_contexts > max_configured {
            return Err(format!(
                "Minimum requested contexts {min_contexts} cannot be achieved with current configuration (max possible: {max_configured})"
            ));
        }

        if max_contexts < min_configured {
            return Err(format!(
                "Maximum requested contexts {max_contexts} is less than minimum possible contexts {min_configured}"
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
/// OTLP metric payload
pub struct OpentelemetryMetrics {
    pool: Pool,
    scratch: RefCell<BytesMut>,
    /// Current tick count for monotonic timing (starts at 0)
    tick: u64,
    /// Most recent collection timestamp emitted by this generator.
    last_collection_time_unix_nano: u64,
    /// Accumulating sum increment, floating point
    incr_f: f64,
    /// Accumulating sum increment, integer
    incr_i: i64,
    /// Per-series time tracking for sums.
    sum_clocks: BTreeMap<u64, SeriesClock>,
    /// Per-series state for explicit histograms.
    histogram_series: BTreeMap<u64, HistogramPointState>,
    /// Per-series state for exponential histograms.
    exponential_histogram_series: BTreeMap<u64, ExponentialHistogramPointState>,
    /// Per-series state for summaries.
    summary_series: BTreeMap<u64, SummaryPointState>,
    /// Number of data points in the most recent `ResourceMetrics` (set by
    /// `generate`).
    data_points_per_resource: u64,
    /// Number of data points in the most recent block (set by `to_bytes`).
    data_points_per_block: u64,
}

impl OpentelemetryMetrics {
    /// Construct a new instance of `OpentelemetryMetrics`
    ///
    /// # Errors
    /// Function will error if the configuration is invalid
    pub fn new<R>(config: Config, max_overhead_bytes: usize, rng: &mut R) -> Result<Self, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let context_cap = config.contexts.total_contexts.sample(rng);
        // Moby Dick is 1.2Mb. 128Kb should be more than enough for metric
        // names, descriptions, etc.
        let str_pool = Rc::new(strings::RandomStringPool::with_size(rng, 128_000));
        let rt_gen = ResourceTemplateGenerator::new(&config, &str_pool, rng)?;

        Ok(Self {
            pool: Pool::new(context_cap, max_overhead_bytes, rt_gen),
            scratch: RefCell::new(BytesMut::with_capacity(4096)),
            tick: 0,
            last_collection_time_unix_nano: BASE_TIME_UNIX_NANOS,
            incr_f: 0.0,
            incr_i: 0,
            sum_clocks: BTreeMap::new(),
            histogram_series: BTreeMap::new(),
            exponential_histogram_series: BTreeMap::new(),
            summary_series: BTreeMap::new(),
            data_points_per_resource: 0,
            data_points_per_block: 0,
        })
    }
}

impl<'a> SizedGenerator<'a> for OpentelemetryMetrics {
    type Output = ResourceMetrics;
    type Error = Error;

    /// Generate OTLP metrics with the following enhancements:
    ///
    /// * Monotonic sums are truly monotonic, incrementing by a random amount
    ///   each tick
    /// * Timestamps advance monotonically based on internal tick counter
    ///   starting from a fixed non-zero base timestamp
    /// * Each call advances the tick counter by a random amount (1-60)
    #[allow(clippy::too_many_lines)]
    fn generate<R>(&'a mut self, rng: &mut R, budget: &mut usize) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        self.tick += rng.random_range(1..=60);
        self.incr_f += rng.random_range(1.0..=100.0);
        self.incr_i += rng.random_range(1_i64..=100_i64);
        let current_time_unix_nano = tick_time_unix_nano(self.tick);
        let previous_time_unix_nano = self.last_collection_time_unix_nano;

        let budget_before_fetch = *budget;
        let mut tpl: ResourceMetrics = match self.pool.fetch(rng, budget) {
            Ok(t) => t.to_owned(),
            Err(PoolError::EmptyChoice) => {
                debug!("Pool was unable to satify request for {budget} size");
                Err(PoolError::EmptyChoice)?
            }
            Err(e) => Err(e)?,
        };

        let resource_attributes = tpl
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice());

        // Update data points in each metric using per-series state so identity
        // attributes, timestamps, and cumulative aggregates stay coherent
        // across repeated emissions of the same template.
        for scope_metrics in &mut tpl.scope_metrics {
            let scope_attributes = scope_metrics
                .scope
                .as_ref()
                .map(|scope| scope.attributes.as_slice());

            for metric in &mut scope_metrics.metrics {
                let metric_identity =
                    metric_identity_hash(resource_attributes, scope_attributes, metric);

                if let Some(data) = &mut metric.data {
                    match data {
                        Data::Gauge(gauge) => {
                            for point in &mut gauge.data_points {
                                point.time_unix_nano = current_time_unix_nano;
                                if let Some(value) = &mut point.value {
                                    match value {
                                        number_data_point::Value::AsDouble(v) => {
                                            *v = rng.random();
                                        }
                                        number_data_point::Value::AsInt(v) => {
                                            *v = rng.random();
                                        }
                                    }
                                }
                            }
                        }
                        Data::Sum(sum) => {
                            let is_accumulating = sum.is_monotonic;
                            let aggregation_temporality = sum.aggregation_temporality;

                            for (point_index, point) in sum.data_points.iter_mut().enumerate() {
                                let series_id = point_series_id(
                                    metric_identity,
                                    point_index,
                                    &point.attributes,
                                );
                                let clock = self.sum_clocks.entry(series_id).or_insert_with(|| {
                                    SeriesClock::new(
                                        current_time_unix_nano,
                                        previous_time_unix_nano,
                                    )
                                });

                                point.start_time_unix_nano =
                                    clock.start_time_for_temporality(aggregation_temporality);
                                point.time_unix_nano = current_time_unix_nano;

                                if is_accumulating {
                                    if let Some(value) = &mut point.value {
                                        match value {
                                            number_data_point::Value::AsDouble(v) => {
                                                *v += self.incr_f;
                                            }
                                            #[allow(clippy::cast_possible_wrap)]
                                            number_data_point::Value::AsInt(v) => {
                                                *v += self.incr_i;
                                            }
                                        }
                                    }
                                } else if let Some(value) = &mut point.value {
                                    match value {
                                        number_data_point::Value::AsDouble(v) => {
                                            *v = rng.random();
                                        }
                                        number_data_point::Value::AsInt(v) => {
                                            *v = rng.random();
                                        }
                                    }
                                }

                                clock.note_emission(current_time_unix_nano);
                            }
                        }
                        Data::Histogram(histogram) => {
                            let aggregation_temporality = histogram.aggregation_temporality;

                            for (point_index, point) in histogram.data_points.iter_mut().enumerate()
                            {
                                let series_id = point_series_id(
                                    metric_identity,
                                    point_index,
                                    &point.attributes,
                                );
                                let bucket_count = point.explicit_bounds.len() + 1;
                                let profile = templates::point_profile(series_id);

                                let state =
                                    self.histogram_series.entry(series_id).or_insert_with(|| {
                                        HistogramPointState::new(
                                            current_time_unix_nano,
                                            previous_time_unix_nano,
                                            bucket_count,
                                        )
                                    });

                                let interval_population =
                                    non_negative_population(profile, state.clock.emissions, rng);
                                let interval_bucket_counts = histogram_bucket_counts(
                                    &interval_population.observations,
                                    &point.explicit_bounds,
                                );

                                point.start_time_unix_nano = state
                                    .clock
                                    .start_time_for_temporality(aggregation_temporality);
                                point.time_unix_nano = current_time_unix_nano;

                                if aggregation_temporality == CUMULATIVE_TEMPORALITY {
                                    merge_bucket_counts(
                                        &mut state.cumulative_bucket_counts,
                                        &interval_bucket_counts,
                                    );
                                    state.cumulative_count += interval_population.count;
                                    state.cumulative_sum += interval_population.sum;
                                    state.cumulative_min =
                                        update_min(state.cumulative_min, interval_population.min);
                                    state.cumulative_max =
                                        update_max(state.cumulative_max, interval_population.max);

                                    point
                                        .bucket_counts
                                        .clone_from(&state.cumulative_bucket_counts);
                                    point.count = state.cumulative_count;
                                    point.sum = Some(state.cumulative_sum);
                                    point.min = state.cumulative_min;
                                    point.max = state.cumulative_max;
                                } else {
                                    point.bucket_counts = interval_bucket_counts;
                                    point.count = interval_population.count;
                                    point.sum = Some(interval_population.sum);
                                    point.min = interval_population.min;
                                    point.max = interval_population.max;
                                }
                                point.exemplars = random_exemplars(
                                    current_time_unix_nano,
                                    point.min,
                                    point.max,
                                    rng,
                                );

                                state.clock.note_emission(current_time_unix_nano);
                            }
                        }
                        Data::ExponentialHistogram(histogram) => {
                            let aggregation_temporality = histogram.aggregation_temporality;

                            for (point_index, point) in histogram.data_points.iter_mut().enumerate()
                            {
                                let series_id = point_series_id(
                                    metric_identity,
                                    point_index,
                                    &point.attributes,
                                );
                                let profile = templates::point_profile(series_id);
                                let state = self
                                    .exponential_histogram_series
                                    .entry(series_id)
                                    .or_insert_with(|| {
                                        ExponentialHistogramPointState::new(
                                            current_time_unix_nano,
                                            previous_time_unix_nano,
                                        )
                                    });
                                let interval_population = signed_population(
                                    profile,
                                    point.zero_threshold,
                                    state.clock.emissions,
                                    rng,
                                );
                                let (interval_zero_count, interval_positive, interval_negative) =
                                    exponential_histogram_buckets(
                                        &interval_population.observations,
                                        point.scale,
                                        point.zero_threshold,
                                    );

                                point.start_time_unix_nano = state
                                    .clock
                                    .start_time_for_temporality(aggregation_temporality);
                                point.time_unix_nano = current_time_unix_nano;

                                if aggregation_temporality == CUMULATIVE_TEMPORALITY {
                                    for (bucket_index, count) in &interval_positive {
                                        *state
                                            .cumulative_positive
                                            .entry(*bucket_index)
                                            .or_insert(0) += *count;
                                    }
                                    for (bucket_index, count) in &interval_negative {
                                        *state
                                            .cumulative_negative
                                            .entry(*bucket_index)
                                            .or_insert(0) += *count;
                                    }
                                    state.cumulative_zero_count += interval_zero_count;
                                    state.cumulative_count += interval_population.count;
                                    state.cumulative_sum += interval_population.sum;
                                    state.cumulative_min =
                                        update_min(state.cumulative_min, interval_population.min);
                                    state.cumulative_max =
                                        update_max(state.cumulative_max, interval_population.max);

                                    point.positive =
                                        Some(dense_exponential_buckets(&state.cumulative_positive));
                                    point.negative =
                                        Some(dense_exponential_buckets(&state.cumulative_negative));
                                    point.zero_count = state.cumulative_zero_count;
                                    point.count = state.cumulative_count;
                                    point.sum = Some(state.cumulative_sum);
                                    point.min = state.cumulative_min;
                                    point.max = state.cumulative_max;
                                } else {
                                    point.positive =
                                        Some(dense_exponential_buckets(&interval_positive));
                                    point.negative =
                                        Some(dense_exponential_buckets(&interval_negative));
                                    point.zero_count = interval_zero_count;
                                    point.count = interval_population.count;
                                    point.sum = Some(interval_population.sum);
                                    point.min = interval_population.min;
                                    point.max = interval_population.max;
                                }
                                point.exemplars = random_exemplars(
                                    current_time_unix_nano,
                                    point.min,
                                    point.max,
                                    rng,
                                );

                                state.clock.note_emission(current_time_unix_nano);
                            }
                        }
                        Data::Summary(summary) => {
                            for (point_index, point) in summary.data_points.iter_mut().enumerate() {
                                let series_id = point_series_id(
                                    metric_identity,
                                    point_index,
                                    &point.attributes,
                                );
                                let state =
                                    self.summary_series.entry(series_id).or_insert_with(|| {
                                        SummaryPointState::new(
                                            current_time_unix_nano,
                                            previous_time_unix_nano,
                                        )
                                    });
                                let profile = templates::point_profile(series_id);
                                let interval_population =
                                    non_negative_population(profile, state.clock.emissions, rng);

                                point.start_time_unix_nano =
                                    state.clock.stream_start_time_unix_nano;
                                point.time_unix_nano = current_time_unix_nano;
                                state.cumulative_count += interval_population.count;
                                state.cumulative_sum += interval_population.sum;
                                extend_summary_reservoir(
                                    &mut state.retained_observations,
                                    &interval_population.observations,
                                );

                                let mut sorted_observations = state.retained_observations.clone();
                                sorted_observations.sort_by(f64::total_cmp);
                                for quantile in &mut point.quantile_values {
                                    quantile.value =
                                        quantile_value(&sorted_observations, quantile.quantile);
                                }

                                point.count = state.cumulative_count;
                                point.sum = state.cumulative_sum;
                                state.clock.note_emission(current_time_unix_nano);
                            }
                        }
                    }
                }
            }
        }

        while tpl.encoded_len() > budget_before_fetch {
            if !trim_one_data_point(&mut tpl) {
                debug!(
                    budget_before_fetch,
                    actual_size = tpl.encoded_len(),
                    "metric payload could not be trimmed to fit budget"
                );
                Err(PoolError::EmptyChoice)?;
            }
        }

        self.data_points_per_resource = total_data_points(&tpl);
        self.last_collection_time_unix_nano = current_time_unix_nano;

        // The pool deducted the template's stored encoded size from budget, but
        // mutations (timestamp, values, bucket counts) may change varint sizes.
        // Correct the budget so it reflects the actual bytes the caller will
        // consume.
        *budget = budget_before_fetch.saturating_sub(tpl.encoded_len());

        Ok(tpl)
    }
}

impl crate::Serialize for OpentelemetryMetrics {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: rand::Rng + Sized,
        W: Write,
    {
        // Our approach here is simple: pack v1::ResourceMetrics from the
        // OpentelemetryMetrics generator into a ExportMetricsServiceRequest
        // until we hit max_bytes worth of serialized bytes.
        let mut bytes_remaining = max_bytes;
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: Vec::with_capacity(8),
        };

        let mut total_data_points = 0;
        let loop_id: u32 = rng.random();

        // Build up the request by adding ResourceMetrics one by one until we
        // would exceed max_bytes.
        //
        // Example: max_bytes = 1000
        //
        // - Request with 0 ResourceMetrics: encoded size = 100 bytes
        // - Add ResourceMetrics #1: encoded size = 400 bytes (still under 1000,
        //   continue)
        // - Add ResourceMetrics #2: encoded size = 700 bytes (still under 1000,
        //   continue)
        // - Add ResourceMetrics #3: encoded size = 1050 bytes (over 1000!)
        // - Code detects overflow, pops #3, breaks the loop
        // - Request now has #1 and #2, encoded size = 700 bytes
        //
        // When we call generate we pass bytes_remaining as the budget. After
        // adding ResourceMetrics #2, we set bytes_remaining = 1000 - 700 =
        // 300. So when generating #3, we tell it "you have 300 bytes to work
        // with". The generate method might return something that encodes to 250
        // bytes on its own.
        //
        // But, protobuf encoding isn't strictly additive. When we add that 250
        // byte ResourceMetrics to a request that's already 700 bytes, the
        // combined encoding might be > 1000 bytes (not 950) due to additional
        // framing, length prefixes, field tags or whatever.  We handle this by
        // checking after adding and removing the item if it exceeds the budget.
        while bytes_remaining >= SMALLEST_PROTOBUF {
            if let Ok(rm) = self.generate(&mut rng, &mut bytes_remaining) {
                total_data_points += self.data_points_per_resource;
                request.resource_metrics.push(rm);

                let required_bytes = request.encoded_len();
                debug!(
                    ?loop_id,
                    ?max_bytes,
                    ?bytes_remaining,
                    ?required_bytes,
                    ?SMALLEST_PROTOBUF,
                    "to_bytes inner loop"
                );
                if required_bytes > max_bytes {
                    total_data_points -= self.data_points_per_resource;
                    drop(request.resource_metrics.pop());
                    break;
                }
                bytes_remaining = max_bytes.saturating_sub(required_bytes);
            } else {
                // Belt with suspenders time: verify no templates could possibly
                // fit. If we pass this assertion, break as no template will
                // ever fit the requested max_bytes.
                assert!(
                    !self.pool.template_fits(bytes_remaining),
                    "Pool claims template fits {bytes_remaining} bytes but generate() failed, indicative of a logic error",
                );
                break;
            }
        }

        let needed = request.encoded_len();
        {
            let mut buf = self.scratch.borrow_mut();
            buf.clear(); // keep the allocation, drop the contents
            let capacity = buf.capacity();
            let diff = capacity.saturating_sub(needed);
            if buf.capacity() < needed {
                buf.reserve(diff); // at most one malloc here
            }
            request.encode(&mut *buf)?;
            writer.write_all(&buf)?;
        }

        self.data_points_per_block = total_data_points;

        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.data_points_per_block)
    }
}

#[cfg(test)]
mod test {
    use super::{Config, Contexts, OpentelemetryMetrics, ResourceMetrics, SMALLEST_PROTOBUF};
    use crate::{Serialize, SizedGenerator, common::config::ConfRange};
    use opentelemetry_proto::tonic::common::v1::any_value;
    use opentelemetry_proto::tonic::metrics::v1::{
        Metric, NumberDataPoint, ScopeMetrics, metric::Data, number_data_point,
    };
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::Gauge,
    };
    use proptest::prelude::*;
    use prost::Message;
    use rand::{SeedableRng, rngs::SmallRng};
    use rustc_hash::FxHashMap;
    use std::{
        collections::HashSet,
        hash::{DefaultHasher, Hash, Hasher},
    };

    // Budget always decreases equivalent to the size of the returned value.
    proptest! {
        #[test]
        fn generate_decrease_budget(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut budget = 10_000_000;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;

            for _ in 0..steps {
                let prev = budget;
                match otel_metrics.generate(&mut rng, &mut budget) {
                    Ok(rm) => prop_assert_eq!(prev-rm.encoded_len(), budget),
                    Err(crate::Error::Pool(_)) => break,
                    Err(e) => return Err(e.into())
                }
            }
        }
    }

    // Generation of metrics must be deterministic. In order to assert this
    // property we generate two instances of OpentelemetryMetrics from disinct
    // rngs and drive them forward for a random amount of steps, asserting equal
    // outcomes.
    proptest! {
        #[test]
        fn is_deterministic(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut b1 = budget;
            let mut rng1 = SmallRng::seed_from_u64(seed);
            let mut otel_metrics1 = OpentelemetryMetrics::new(config, budget, &mut rng1)?;
            let mut b2 = budget;
            let mut rng2 = SmallRng::seed_from_u64(seed);
            let mut otel_metrics2 = OpentelemetryMetrics::new(config, budget, &mut rng2)?;

            for _ in 0..steps {
                if let Ok(gen_1) = otel_metrics1.generate(&mut rng1, &mut b1) {
                    let gen_2 = otel_metrics2.generate(&mut rng2, &mut b2).expect("gen_2 was not Ok");
                    prop_assert_eq!(gen_1, gen_2);
                    prop_assert_eq!(b1, b2);
                } else {
                    break;
                }
            }
        }
    }

    // Generation of metrics must be context bounded. If `generate` is called
    // more than total_context times only total_context contexts should be
    // produced.
    proptest! {
        #[test]
        fn contexts_bound_metric_generation(
            seed: u64,
            total_contexts_min in 1..4_u32,
            total_contexts_max in 5..100_u32,
            attributes_per_resource in 0..25_u8,
            scopes_per_resource in 1..10_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 1..10_u8,
            attributes_per_metric in 0..100_u8,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Inclusive { min: total_contexts_min, max: total_contexts_max },
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut ids = HashSet::new();
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut b = budget;
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;

            let total_generations = total_contexts_max + (total_contexts_max / 2);
            for _ in 0..total_generations {
                if let Ok(res) = otel_metrics.generate(&mut rng, &mut b) {
                    let id = context_id(&res);
                    ids.insert(id);
                }
            }

            let actual_contexts = ids.len();
            let bounded_above = actual_contexts <= total_contexts_max as usize;
            prop_assert!(bounded_above,
                         "expected {actual_contexts} ≤ {total_contexts_max}");
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `budget`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };
            let mut rng = SmallRng::seed_from_u64(seed);
            let max_bytes = 10_000;
            let mut metrics = OpentelemetryMetrics::new(config, max_bytes, &mut rng)
                .expect("failed to create metrics generator");

            let mut bytes = Vec::new();
            metrics
                .to_bytes(&mut rng, budget, &mut bytes)
                .expect("failed to convert to bytes");
            assert!(
                bytes.len() <= budget,
                "max len: {budget}, actual: {}",
                bytes.len()
            );
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL metrics.
    #[test]
    fn payload_deserializes() {
        let config = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(5),
                attributes_per_resource: ConfRange::Constant(3),
                scopes_per_resource: ConfRange::Constant(2),
                attributes_per_scope: ConfRange::Constant(1),
                metrics_per_scope: ConfRange::Constant(3),
                attributes_per_metric: ConfRange::Constant(2),
            },
            ..Default::default()
        };

        let max_bytes = 256;
        let mut rng = SmallRng::seed_from_u64(42);
        let mut metrics = OpentelemetryMetrics::new(config, max_bytes, &mut rng)
            .expect("failed to create metrics generator");

        let mut bytes: Vec<u8> = Vec::new();
        metrics
            .to_bytes(&mut rng, max_bytes, &mut bytes)
            .expect("failed to convert to bytes");

        ExportMetricsServiceRequest::decode(bytes.as_slice())
            .expect("failed to decode the message from the buffer");
    }

    // Confirm that configuration bounds are naively obeyed. For instance, this
    // test will pass if every resource generated has identical attributes, etc
    // etc. Confirmation of context bounds being obeyed -- implying examination
    // of attributes et al -- is appropriate for another property test.
    proptest! {
        #[test]
        fn counts_within_bounds(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            budget in SMALLEST_PROTOBUF..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut b = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;

            for _ in 0..steps {
                if let Ok(metric) = otel_metrics.generate(&mut rng, &mut b) {
                    if let Some(resource) = &metric.resource {
                        prop_assert!(
                            resource.attributes.len() <= attributes_per_resource as usize,
                            "Resource attributes count {len} exceeds configured maximum {attributes_per_resource}",
                            len = resource.attributes.len(),
                        );
                    }

                    prop_assert!(
                        metric.scope_metrics.len() <= scopes_per_resource as usize,
                        "Scopes per resource count {len} exceeds configured maximum {scopes_per_resource}",
                        len = metric.scope_metrics.len(),
                    );

                    for scope in &metric.scope_metrics {
                        if let Some(scope) = &scope.scope {
                            prop_assert!(
                                scope.attributes.len() <= attributes_per_scope as usize,
                                "Scope attributes count {len} exceeds configured maximum {attributes_per_scope}",
                                len = scope.attributes.len(),
                            );
                        }

                        prop_assert!(
                            scope.metrics.len() <= metrics_per_scope as usize,
                            "Metrics per scope count {len} exceeds configured maximum {metrics_per_scope}",
                            len = scope.metrics.len(),
                        );

                        for metric in &scope.metrics {
                            prop_assert!(
                                metric.metadata.len() <= attributes_per_metric as usize,
                                "Metric attributes count {len} exceeds configured maximum {attributes_per_metric}",
                                len = metric.metadata.len(),
                            );
                        }
                    }
                }
            }
        }
    }

    /// Extracts and hashes the context from a `ResourceMetrics`.
    ///
    /// A context is defined by the unique combination of:
    /// - Resource attributes
    /// - Scope attributes
    /// - Metric name, attributes, data kind, and unit
    fn context_id(metric: &ResourceMetrics) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash resource attributes
        if let Some(resource) = &metric.resource {
            for attr in &resource.attributes {
                attr.key.hash(&mut hasher);
                if let Some(value) = &attr.value
                    && let Some(any_value::Value::StringValue(s)) = &value.value
                {
                    s.hash(&mut hasher);
                }
            }
        }

        // Hash each scope's context
        for scope_metrics in &metric.scope_metrics {
            // Hash scope attributes
            if let Some(scope) = &scope_metrics.scope {
                for attr in &scope.attributes {
                    attr.key.hash(&mut hasher);
                    if let Some(value) = &attr.value
                        && let Some(any_value::Value::StringValue(s)) = &value.value
                    {
                        s.hash(&mut hasher);
                    }
                }
            }

            // Hash each metric's context
            for metric in &scope_metrics.metrics {
                // Hash metric name
                metric.name.hash(&mut hasher);

                // Hash metric attributes
                for attr in &metric.metadata {
                    attr.key.hash(&mut hasher);
                    if let Some(value) = &attr.value
                        && let Some(any_value::Value::StringValue(s)) = &value.value
                    {
                        s.hash(&mut hasher);
                    }
                }

                // Hash metric data kind and unit
                metric.unit.hash(&mut hasher);
                if let Some(data) = &metric.data {
                    match data {
                        Data::Gauge(_) => "gauge".hash(&mut hasher),
                        Data::Sum(sum) => {
                            "sum".hash(&mut hasher);
                            sum.aggregation_temporality.hash(&mut hasher);
                            sum.is_monotonic.hash(&mut hasher);
                        }
                        Data::Histogram(h) => {
                            "histogram".hash(&mut hasher);
                            h.aggregation_temporality.hash(&mut hasher);
                        }
                        Data::ExponentialHistogram(eh) => {
                            "exponential_histogram".hash(&mut hasher);
                            eh.aggregation_temporality.hash(&mut hasher);
                        }
                        Data::Summary(_) => "summary".hash(&mut hasher),
                    }
                }
            }
        }

        hasher.finish()
    }

    // Ensures that the context_id function serves as an equality test, ignoring
    // value differences etc.
    proptest! {
        #[test]
        fn context_is_equality(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 1..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 1..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..4098_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut b1 = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;
            let metric1 = otel_metrics.generate(&mut rng, &mut b1);

            // Generate two identical metrics
            let mut b2 = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;
            let metric2 = otel_metrics.generate(&mut rng, &mut b2);

            // Ensure that the metrics are equal and that their contexts, when
            // applicable, are also equal.
            if let Ok(m1) = metric1 {
                let m2 = metric2.unwrap();
                prop_assert_eq!(context_id(&m1), context_id(&m2));
                prop_assert_eq!(m1, m2);
            }
        }
    }

    // Property: timestamps are monotonic
    proptest! {
        #[test]
        fn timestamps_increase_monotonically(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            budget in SMALLEST_PROTOBUF..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut budget = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;

            let mut timestamps_by_metric: FxHashMap<u64, Vec<u64>> = FxHashMap::default();

            for _ in 0..steps {
                if let Ok(resource_metric) = otel_metrics.generate(&mut rng, &mut budget) {
                    for scope_metric in &resource_metric.scope_metrics {
                        for metric in &scope_metric.metrics {
                            let id = context_id(&resource_metric);

                            if let Some(data) = &metric.data {
                                match data {
                                    Data::Gauge(gauge) => {
                                        for point in &gauge.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_default()
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                    Data::Sum(sum) => {
                                        for point in &sum.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_default()
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                    Data::Histogram(histogram) => {
                                        for point in &histogram.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_default()
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                    Data::ExponentialHistogram(eh) => {
                                        for point in &eh.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_default()
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                    Data::Summary(summary) => {
                                        for point in &summary.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_default()
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            }

            if !timestamps_by_metric.is_empty() {
                // For each metric, verify its timestamps increase monotonically
                for (metric_id, timestamps) in &timestamps_by_metric {
                    if timestamps.len() > 1 {
                        for i in 1..timestamps.len() {
                            let current = timestamps[i];
                            let previous = timestamps[i-1]; // safety: iterator begins at 1
                            prop_assert!(
                                current >= previous,
                                "Timestamp for metric {metric_id} did not increase monotonically: current={current}, previous={previous}",
                            );
                        }
                    }
                }
            }
        }
    }

    // Property: tick tally in OpentelemetryMetrics increase with calls to
    // `generate`.
    proptest! {
        #[test]
        fn increasing_ticks(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..4098_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                metric_weights: super::MetricWeights {
                    gauge: 0,   // Only generate sum metrics
                    sum_delta: 50,
                    sum_cumulative: 50,
                    histogram: 0,
                    exponential_histogram: 0,
                    summary: 0,
                },
            };

            let mut budget = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;
            let prev = otel_metrics.tick;
            let _ = otel_metrics.generate(&mut rng, &mut budget);
            let cur = otel_metrics.tick;

            prop_assert!(cur > prev, "Ticks did not advance properly: current: {cur}, previous: {prev}");
        }
    }

    // Property: accumulated sums are monotonic
    proptest! {
        #[test]
        fn accumulating_sums_only_increase(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..512_usize, // see note below about repetition
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                metric_weights: super::MetricWeights {
                    gauge: 0,   // Only generate sum metrics
                    sum_delta: 50,
                    sum_cumulative: 50,
                    histogram: 0,
                    exponential_histogram: 0,
                    summary: 0,
                },
            };

            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, budget, &mut rng)?;

            let mut values: FxHashMap<u64, f64> = FxHashMap::default();

            // Generate the initial batch of values. It's entirely possible we
            // will not run into the same context_ids again. Please run this
            // test with a very high number of cases. We have arbitrarily
            // constrained the budget relative to other tests in this module for
            // try and force repetition of contexts.
            let mut budget = budget;
            {
                if let Ok(resource_metrics) = otel_metrics.generate(&mut rng, &mut budget) {
                    let id = context_id(&resource_metrics);
                    for scope_metric in &resource_metrics.scope_metrics {
                        for metric in &scope_metric.metrics {
                            if let Some(Data::Sum(sum)) = &metric.data
                                && sum.is_monotonic {
                                    for point in &sum.data_points {
                                        if let Some(number_data_point::Value::AsDouble(v)) = point.value {
                                            values.insert(id, v);
                                        }
                                    }
                                }
                        }
                    }
                }
            }
            {
                if let Ok(resource_metrics) = otel_metrics.generate(&mut rng, &mut budget) {
                    let id = context_id(&resource_metrics);
                    for scope_metric in &resource_metrics.scope_metrics {
                        for metric in &scope_metric.metrics {
                            if let Some(Data::Sum(sum)) = &metric.data
                                && sum.is_monotonic {
                                    for point in &sum.data_points {
                                        if let Some(number_data_point::Value::AsDouble(v)) = point.value {
                                            if let Some(&previous_value) = values.get(&id) {
                                                prop_assert!(
                                                    v >= previous_value,
                                                    "Monotonic sum decreased for {id}: previous={previous_value}, current={v}",
                                                );
                                            }
                                            values.insert(id, v);
                                        }
                                    }
                                }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn smallest_protobuf() {
        let data_point = NumberDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1, // Minimal non-zero timestamp
            value: Some(number_data_point::Value::AsInt(1)), // Smallest value
            exemplars: Vec::new(),
            flags: 0,
        };

        let gauge = Gauge {
            data_points: vec![data_point],
        };

        let metric = Metric {
            name: "x".into(), // Minimal valid name
            description: String::new(),
            unit: String::new(),
            data: Some(Data::Gauge(gauge)),
            metadata: Vec::new(), // No metadata
        };

        let scope_metrics = ScopeMetrics {
            scope: None, // No scope info
            metrics: vec![metric],
            schema_url: String::new(),
        };

        let resource_metrics = ResourceMetrics {
            resource: None, // No resource info
            scope_metrics: vec![scope_metrics],
            schema_url: String::new(),
        };

        // The most minimal request we care to support, just one single data point
        let minimal_request = ExportMetricsServiceRequest {
            resource_metrics: vec![resource_metrics],
        };

        let encoded_size = minimal_request.encoded_len();

        assert!(
            encoded_size == SMALLEST_PROTOBUF,
            "Minimal useful request size ({encoded_size}) should be == SMALLEST_PROTOBUF ({SMALLEST_PROTOBUF})"
        );
    }

    #[test]
    #[expect(clippy::too_many_lines)]
    fn config_validation() {
        // Valid configuration
        let valid_config = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(100),
                attributes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
                scopes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
                attributes_per_scope: ConfRange::Constant(0),
                metrics_per_scope: ConfRange::Inclusive { min: 1, max: 20 },
                attributes_per_metric: ConfRange::Inclusive { min: 0, max: 10 },
            },
            ..Default::default()
        };
        assert!(valid_config.valid().is_ok());

        // Invalid: Zero total_contexts
        let zero_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_contexts.valid().is_err());

        // Invalid: Zero minimum total_contexts in range
        let zero_min_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 0, max: 100 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_contexts.valid().is_err());

        // Invalid: Min greater than max in total_contexts
        let min_gt_max_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 100, max: 50 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max_contexts.valid().is_err());

        // Invalid: Zero scopes_per_resource
        let zero_scopes = Config {
            contexts: Contexts {
                scopes_per_resource: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_scopes.valid().is_err());

        // Invalid: Zero minimum scopes_per_resource in range
        let zero_min_scopes = Config {
            contexts: Contexts {
                scopes_per_resource: ConfRange::Inclusive { min: 0, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_scopes.valid().is_err());

        // Invalid: Min greater than max in scopes_per_resource
        let min_gt_max_scopes = Config {
            contexts: Contexts {
                scopes_per_resource: ConfRange::Inclusive { min: 20, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max_scopes.valid().is_err());

        // Invalid: Zero metrics_per_scope
        let zero_metrics = Config {
            contexts: Contexts {
                metrics_per_scope: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_metrics.valid().is_err());

        // Invalid: Zero minimum metrics_per_scope in range
        let zero_min_metrics = Config {
            contexts: Contexts {
                metrics_per_scope: ConfRange::Inclusive { min: 0, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_metrics.valid().is_err());

        // Invalid: Min greater than max in metrics_per_scope
        let min_gt_max_metrics = Config {
            contexts: Contexts {
                metrics_per_scope: ConfRange::Inclusive { min: 20, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max_metrics.valid().is_err());

        // Invalid: Impossible to achieve minimum contexts (context requirement exceeds capacity)
        let impossible_min = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive {
                    min: 1000,
                    max: 2000,
                },
                scopes_per_resource: ConfRange::Constant(1),
                metrics_per_scope: ConfRange::Constant(1),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(impossible_min.valid().is_err());

        // Invalid: Impossible to achieve maximum contexts (max is too small)
        let impossible_max = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 1, max: 1 },
                scopes_per_resource: ConfRange::Constant(2),
                metrics_per_scope: ConfRange::Constant(2),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(impossible_max.valid().is_err());

        // Invalid: Zero metric weights
        let zero_gauge_weight = Config {
            metric_weights: super::MetricWeights {
                gauge: 0,
                sum_delta: 25,
                sum_cumulative: 25,
                histogram: 15,
                exponential_histogram: 3,
                summary: 2,
            },
            ..valid_config
        };
        assert!(zero_gauge_weight.valid().is_err());

        let zero_sum_weight = Config {
            metric_weights: super::MetricWeights {
                gauge: 50,
                sum_delta: 0,
                sum_cumulative: 0,
                histogram: 15,
                exponential_histogram: 3,
                summary: 2,
            },
            ..valid_config
        };
        assert!(zero_sum_weight.valid().is_err());

        let zero_sum_cumulative_weight = Config {
            metric_weights: super::MetricWeights {
                gauge: 50,
                sum_delta: 25,
                sum_cumulative: 0,
                histogram: 15,
                exponential_histogram: 3,
                summary: 2,
            },
            ..valid_config
        };
        assert!(zero_sum_cumulative_weight.valid().is_err());

        let zero_weights = Config {
            metric_weights: super::MetricWeights {
                gauge: 0,
                sum_delta: 0,
                sum_cumulative: 0,
                histogram: 0,
                exponential_histogram: 0,
                summary: 0,
            },
            ..valid_config
        };
        assert!(zero_weights.valid().is_err());
    }
}
