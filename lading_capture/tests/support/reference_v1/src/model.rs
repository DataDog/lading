//! Keep the reference data view independent of the writer's record types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A fixed data view shared only by the reference process and its test caller.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(crate) struct Record {
    /// Parsed run identity, independent of equivalent UUID spellings.
    pub run_id: [u8; 16],
    /// Stored epoch-millisecond timestamp.
    pub time: i64,
    /// Logical capture tick.
    pub fetch_index: u64,
    /// Metric name, independent of writer constants.
    pub metric_name: String,
    /// Known counter, gauge, or histogram kind.
    pub metric_kind: String,
    /// Integer scalar slot.
    pub value_int: Option<u64>,
    /// Exact bits preserve non-finite values without JSON number conversion.
    pub value_float_bits: Option<u64>,
    /// Name-based labels, independent of their entry order.
    pub labels: BTreeMap<String, String>,
    /// Histogram payload bytes, without a writer-side codec dependency.
    pub value_histogram: Option<Vec<u8>>,
}
