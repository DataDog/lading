/// A capture value container, called a 'line' for historical reasons.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Line {
    /// The name of the metric recorded by this line.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// The kind of metric recorded by this line.
    #[prost(enumeration = "MetricKind", tag = "3")]
    pub kind: i32,
    /// The value of the metric recorded by this line.
    #[prost(double, tag = "4")]
    pub value: f64,
    /// Labels associated with this line.
    #[prost(map = "string, string", tag = "5")]
    pub labels: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
/// A collection of Lines
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Payload {
    /// An ID that is unique to a single lading run.
    #[prost(string, tag = "1")]
    pub run_id: ::prost::alloc::string::String,
    /// The time in milliseconds past the epoch that this Payload was collected.
    #[prost(uint64, tag = "2")]
    pub time: u64,
    /// Global labels to be applied to every Line.
    #[prost(map = "string, string", tag = "3")]
    pub global_labels: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// The collection of Line instances
    #[prost(message, repeated, tag = "4")]
    pub lines: ::prost::alloc::vec::Vec<Line>,
}
/// The kinds of metrics that Lading produces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MetricKind {
    /// Unset
    Unspecified = 0,
    /// A monotonically increasing value.
    Counter = 1,
    /// A point-in-time value.
    Gauge = 2,
}
impl MetricKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            MetricKind::Unspecified => "METRIC_KIND_UNSPECIFIED",
            MetricKind::Counter => "METRIC_KIND_COUNTER",
            MetricKind::Gauge => "METRIC_KIND_GAUGE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "METRIC_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "METRIC_KIND_COUNTER" => Some(Self::Counter),
            "METRIC_KIND_GAUGE" => Some(Self::Gauge),
            _ => None,
        }
    }
}
