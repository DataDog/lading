//! JSON form of a Lading capture Line, meant to be read line by line from a
//! file.

use serde::Serialize;
use std::{borrow::Cow, collections::HashMap};
use uuid::Uuid;

use crate::proto::lading::v1;

#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
/// The kinds of metrics that are recorded in [`Line`].
pub enum MetricKind {
    /// A monotonically increasing value.
    Counter,
    /// A point-at-time value.
    Gauge,
}

impl From<v1::MetricKind> for MetricKind {
    fn from(value: v1::MetricKind) -> Self {
        match value {
            v1::MetricKind::Unspecified => unreachable!(),
            v1::MetricKind::Counter => MetricKind::Counter,
            v1::MetricKind::Gauge => MetricKind::Gauge,
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
/// The value for [`Line`].
#[serde(untagged)]
pub enum LineValue {
    /// A signless integer, 64 bits wide
    Int(u64),
    /// A floating point, 64 bits wide
    Float(f64),
}

#[derive(Debug, Serialize)]
/// The structure of a capture file line.
pub struct Line<'a> {
    #[serde(borrow)]
    /// An id that is mostly unique to this run, allowing us to distinguish
    /// duplications of the same observational setup.
    pub run_id: Cow<'a, Uuid>,
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
    pub labels: HashMap<String, String>,
}
