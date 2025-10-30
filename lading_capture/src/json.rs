//! JSON form of a Lading capture Line, meant to be read line by line from a
//! file.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
/// The kinds of metrics that are recorded in [`Line`].
pub enum MetricKind {
    /// A monotonically increasing value.
    Counter,
    /// A point-at-time value.
    Gauge,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
/// The value for [`Line`].
#[serde(untagged)]
pub enum LineValue {
    /// A signless integer, 64 bits wide
    Int(u64),
    /// A floating point, 64 bits wide
    Float(f64),
}

impl LineValue {
    /// Get an f64 representation of this value. Extremely large integers will be truncated.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn as_f64(&self) -> f64 {
        match self {
            LineValue::Int(int) => *int as f64,
            LineValue::Float(float) => *float,
        }
    }
}

impl std::fmt::Display for LineValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineValue::Int(int) => write!(f, "{int}"),
            LineValue::Float(float) => write!(f, "{float}"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// The structure of a capture file line.
pub struct Line {
    /// The time in milliseconds that this line was written.
    pub time: u128,
    /// The "fetch index". Previous versions of lading scraped prometheus
    /// metrics from their targets and kept an increment index of polls. Now
    /// this records the number of times the internal metrics cache has been
    /// flushed.
    pub fetch_index: u64,
    /// The name of the metric recorded by this line.
    pub metric_name: String,
    /// The kind of metric recorded by this line.
    pub metric_kind: MetricKind,
    /// The value of the metric on this line.
    pub value: LineValue,
    #[serde(flatten)]
    /// The labels associated with this metric.
    pub labels: FxHashMap<String, String>,
}

impl Line {
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    /// Returns the number of seconds since unix epoch
    pub fn seconds_since_epoch(&self) -> u64 {
        let seconds: u128 = self.time / 1_000;
        seconds as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn serialize_deserialize_isomorphism(
            run_id in any::<[u8; 16]>().prop_map(Uuid::from_bytes),
            time in any::<u128>(),
            fetch_index in any::<u64>(),
            metric_name in "[a-z][a-z0-9_]*",
            metric_kind in prop_oneof![
                Just(MetricKind::Counter),
                Just(MetricKind::Gauge),
            ],
            value in prop_oneof![
                any::<u64>().prop_map(LineValue::Int),
                any::<f64>().prop_filter("must be finite", |f| f.is_finite()).prop_map(LineValue::Float),
            ],
            labels in prop::collection::hash_map("[a-z][a-z0-9_]*", "[a-z][a-z0-9_]*", 0..10),
        ) {
            let line = Line {
                run_id,
                time,
                fetch_index,
                metric_name,
                metric_kind,
                value,
                labels: labels.into_iter().collect(),
            };

            // Serialize to JSON
            let serialized = serde_json::to_string(&line)
                .expect("serialization should succeed");

            // Deserialize back
            let deserialized: Line = serde_json::from_str(&serialized)
                .expect("deserialization should succeed");

            // Check that key fields match
            prop_assert_eq!(line.run_id, deserialized.run_id);
            prop_assert_eq!(line.time, deserialized.time);
            prop_assert_eq!(line.fetch_index, deserialized.fetch_index);
            prop_assert_eq!(line.metric_name, deserialized.metric_name);

            // For values, handle float comparison specially
            match (line.value, deserialized.value) {
                (LineValue::Int(a), LineValue::Int(b)) => prop_assert_eq!(a, b),
                (LineValue::Float(a), LineValue::Float(b)) => {
                    let diff = (a - b).abs();
                    prop_assert!(diff < 1e-10 || diff / a.abs().max(b.abs()) < 1e-10,
                        "floats not approximately equal: {a} vs {b}");
                }
                (a, b) => prop_assert!(false, "value types don't match: {a:?} vs {b:?}"),
            }
        }
    }
}
