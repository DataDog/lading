//! JSON form of a Lading capture Line, meant to be read line by line from a
//! file.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    /// An id that is mostly unique to this run, allowing us to distinguish
    /// duplications of the same observational setup.
    pub run_id: Uuid,
    /// The time in milliseconds that this line was written.
    pub time: u128,
    /// The time in milliseconds when this metric was recorded.
    pub recorded_at: u128,
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
