//! The protobuf fields emitted by the agent-to-intake payload generator.
//!
//! Field numbers follow the Datadog Agent definitions at
//! <https://github.com/DataDog/datadog-agent/tree/8cc5eb3e024ee54283efad4614175a065642bd9c/pkg/proto/datadog/trace>.
//! Unmodeled fields retain their protobuf defaults. `BTreeMap` encoding keeps output deterministic.

use std::collections::BTreeMap;

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct AgentPayload {
    #[prost(string, tag = "1")]
    pub(super) host_name: String,
    #[prost(string, tag = "2")]
    pub(super) env: String,
    #[prost(message, repeated, tag = "5")]
    pub(super) tracer_payloads: Vec<TracerPayload>,
    #[prost(string, tag = "7")]
    pub(super) agent_version: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct TracerPayload {
    #[prost(string, tag = "2")]
    pub(super) language_name: String,
    #[prost(string, tag = "4")]
    pub(super) tracer_version: String,
    #[prost(message, repeated, tag = "6")]
    pub(super) chunks: Vec<TraceChunk>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct TraceChunk {
    #[prost(int32, tag = "1")]
    pub(super) priority: i32,
    #[prost(message, repeated, tag = "3")]
    pub(super) spans: Vec<Span>,
}

#[derive(Clone, PartialEq, prost::Message)]
#[expect(
    clippy::struct_field_names,
    reason = "names match the protobuf wire schema"
)]
pub(super) struct Span {
    #[prost(string, tag = "1")]
    pub(super) service: String,
    #[prost(string, tag = "2")]
    pub(super) name: String,
    #[prost(string, tag = "3")]
    pub(super) resource: String,
    #[prost(uint64, tag = "4")]
    pub(super) trace_id: u64,
    #[prost(uint64, tag = "5")]
    pub(super) span_id: u64,
    #[prost(uint64, tag = "6")]
    pub(super) parent_id: u64,
    #[prost(int64, tag = "7")]
    pub(super) start: i64,
    #[prost(int64, tag = "8")]
    pub(super) duration: i64,
    #[prost(int32, tag = "9")]
    pub(super) error: i32,
    #[prost(btree_map = "string, string", tag = "10")]
    pub(super) meta: BTreeMap<String, String>,
    #[prost(btree_map = "string, double", tag = "11")]
    pub(super) metrics: BTreeMap<String, f64>,
    #[prost(string, tag = "12")]
    pub(super) r#type: String,
    #[prost(btree_map = "string, bytes", tag = "13")]
    pub(super) meta_struct: BTreeMap<String, Vec<u8>>,
}
