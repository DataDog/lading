//! Realistic, topology-driven OpenTelemetry OTLP trace payload generator.
//!
//! Unlike the random-string-based generator in [`super::trace`], this generator lets users define a
//! distributed system topology — services, their operations, and the call graph between them — and
//! produces traces that walk that topology. All string data comes from the user configuration or
//! built-in dictionaries, producing output that resembles real distributed system traffic.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/)

use crate::{Error, Generator, common::strings};
use opentelemetry_proto::tonic::{
    collector::trace,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    trace::v1::Status,
};
use opentelemetry_proto::tonic::{resource::v1::Resource, trace::v1};
use prost::Message;
use rand::{
    Rng,
    distr::{Distribution, weighted::WeightedIndex},
    seq::IndexedRandom,
};
use std::{collections::BTreeMap, io::Write, rc::Rc};

use super::trace::{get_safe_start_end_timestamps, str_kv};

const STRING_POOL_SIZE: usize = 65_536;
const MAX_TRACE_DEPTH: usize = 16;

// ---------------------------------------------------------------------------
// Built-in dictionaries
// ---------------------------------------------------------------------------

mod dictionaries {
    pub(super) const HTTP_METHODS: &[&str] =
        &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];

    pub(super) const HTTP_STATUS_CODES: &[&str] = &[
        "200", "201", "204", "301", "302", "400", "401", "403", "404", "405", "409", "422", "429",
        "500", "502", "503", "504",
    ];

    pub(super) const GRPC_STATUS_CODES: &[&str] = &[
        "OK",
        "CANCELLED",
        "UNKNOWN",
        "INVALID_ARGUMENT",
        "DEADLINE_EXCEEDED",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "PERMISSION_DENIED",
        "RESOURCE_EXHAUSTED",
        "FAILED_PRECONDITION",
        "ABORTED",
        "OUT_OF_RANGE",
        "UNIMPLEMENTED",
        "INTERNAL",
        "UNAVAILABLE",
        "DATA_LOSS",
        "UNAUTHENTICATED",
    ];

    pub(super) const CLOUD_REGIONS: &[&str] = &[
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
        "ap-northeast-1",
    ];

    pub(super) const ENVIRONMENTS: &[&str] = &["production", "staging", "development", "canary"];
}

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Configuration for the realistic, topology-driven OTLP trace generator.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The services that make up the simulated distributed system.
    pub services: Vec<ServiceConfig>,

    /// The edges of the call graph: which service/operation calls which.
    pub edges: Vec<EdgeConfig>,

    /// Probability \[0.0, 1.0\] that any individual span has error status.
    #[serde(default = "default_error_rate")]
    pub error_rate: f32,

    /// When a service has multiple outgoing edges, at most this many child
    /// calls are made per invocation.
    #[serde(default = "default_max_fanout")]
    pub max_fanout: u8,
}

fn default_error_rate() -> f32 {
    0.01
}
fn default_max_fanout() -> u8 {
    3
}

/// A single service in the topology.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Service name (becomes `service.name` resource attribute).
    pub name: String,

    /// Extra resource-level attributes attached to all spans from this service.
    #[serde(default)]
    pub resource_attributes: Vec<AttributeConfig>,

    /// Instrumentation scope name.
    #[serde(default)]
    pub scope_name: Option<String>,

    /// Instrumentation scope version.
    #[serde(default = "default_scope_version")]
    pub scope_version: String,
}

fn default_scope_version() -> String {
    "1.0.0".to_string()
}

/// A directed edge in the call graph.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeConfig {
    /// The service initiating the call (must match a `ServiceConfig::name`).
    pub caller: String,
    /// The operation name on the caller side (e.g. `"GET /users/{id}"`).
    pub caller_operation: String,
    /// The service being called (must match a `ServiceConfig::name`).
    pub callee: String,
    /// The operation name on the callee side (e.g. `"SELECT users"`).
    pub callee_operation: String,

    /// Span kind for the caller-side span (OTLP `SpanKind` value 0–5).
    #[serde(default = "default_client_kind")]
    pub caller_span_kind: i32,

    /// Span kind for the callee-side span.
    #[serde(default = "default_server_kind")]
    pub callee_span_kind: i32,

    /// Attributes added to spans generated by this edge.
    #[serde(default)]
    pub attributes: Vec<AttributeConfig>,

    /// Relative weight for selecting this edge when multiple outgoing edges
    /// exist from the same caller service. Higher values are more likely.
    #[serde(default = "default_weight")]
    pub weight: u32,

    /// Maximum number of times this edge can repeat when selected. The
    /// generator picks a random count in `[1, max_repeat]`. Each repetition
    /// produces an independent client+server span pair under the same parent,
    /// simulating batch or loop patterns (e.g. fetching N items).
    /// Defaults to 1 (no repetition).
    #[serde(default = "default_max_repeat")]
    pub max_repeat: u32,
}

fn default_client_kind() -> i32 {
    3 // SPAN_KIND_CLIENT
}
fn default_server_kind() -> i32 {
    2 // SPAN_KIND_SERVER
}
fn default_weight() -> u32 {
    1
}
fn default_max_repeat() -> u32 {
    1
}

/// An attribute key/value definition.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttributeConfig {
    /// The attribute key (e.g. `"http.method"`).
    pub key: String,
    /// How the value is produced.
    pub value: AttributeValueConfig,
}

/// How an attribute value is generated at runtime.
///
/// Bare scalars are accepted as shorthand for static values:
/// - `value: "some string"` is equivalent to `value: { static: "some string" }`
/// - `value: 42` is equivalent to `value: { static_int: 42 }`
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributeValueConfig {
    /// A fixed string value. Can also be written as a bare string: `value: "hello"`.
    Static(String),
    /// A fixed integer value. Can also be written as a bare integer: `value: 42`.
    StaticInt(i64),
    /// A random alphanumeric string of the given length.
    RandomString {
        /// Number of characters to generate.
        length: usize,
    },
    /// A random integer in \[min, max\] inclusive.
    RandomInt {
        /// Minimum value (inclusive).
        min: i64,
        /// Maximum value (inclusive).
        max: i64,
    },
    /// Choose uniformly at random from a built-in dictionary.
    Dictionary(DictionaryName),
    /// Choose uniformly at random from the given list.
    OneOf(Vec<String>),
}

/// Helper enum for untagged deserialization of `AttributeValueConfig`.
///
/// Serde tries each variant in order: bare integer, bare string, then the tagged map form.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum AttributeValueConfigHelper {
    /// Bare integer: `value: 42` → `StaticInt(42)`
    BareInt(i64),
    /// Bare string: `value: "hello"` → `Static("hello")`
    BareString(String),
    /// Tagged map form: `value: { random_string: { length: 10 } }`
    Tagged(TaggedAttributeValueConfig),
}

/// Tagged form used as a fallback when deserializing `AttributeValueConfig`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum TaggedAttributeValueConfig {
    Static(String),
    StaticInt(i64),
    RandomString { length: usize },
    RandomInt { min: i64, max: i64 },
    Dictionary(DictionaryName),
    OneOf(Vec<String>),
}

impl<'de> serde::Deserialize<'de> for AttributeValueConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = AttributeValueConfigHelper::deserialize(deserializer)?;
        Ok(match helper {
            AttributeValueConfigHelper::BareInt(n) => Self::StaticInt(n),
            AttributeValueConfigHelper::BareString(s) => Self::Static(s),
            AttributeValueConfigHelper::Tagged(tagged) => match tagged {
                TaggedAttributeValueConfig::Static(s) => Self::Static(s),
                TaggedAttributeValueConfig::StaticInt(n) => Self::StaticInt(n),
                TaggedAttributeValueConfig::RandomString { length } => {
                    Self::RandomString { length }
                }
                TaggedAttributeValueConfig::RandomInt { min, max } => Self::RandomInt { min, max },
                TaggedAttributeValueConfig::Dictionary(d) => Self::Dictionary(d),
                TaggedAttributeValueConfig::OneOf(v) => Self::OneOf(v),
            },
        })
    }
}

/// Names of built-in value dictionaries.
#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DictionaryName {
    /// HTTP request methods (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS).
    HttpMethods,
    /// Common HTTP response status codes.
    HttpStatusCodes,
    /// gRPC status codes (OK, CANCELLED, UNKNOWN, etc.).
    GrpcStatusCodes,
    /// Cloud provider region identifiers.
    CloudRegions,
    /// Deployment environment names (production, staging, development, canary).
    Environments,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

impl Config {
    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns a description of the first validation error found.
    pub fn valid(&self) -> Result<(), String> {
        if self.services.is_empty() {
            return Err("at least one service is required".into());
        }
        if self.edges.is_empty() {
            return Err("at least one edge is required".into());
        }
        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(format!(
                "error_rate must be finite and in [0.0, 1.0], got {}",
                self.error_rate
            ));
        }
        if self.max_fanout == 0 {
            return Err("max_fanout must be >= 1".into());
        }

        // Check for duplicate service names.
        let mut seen = BTreeMap::new();
        for (i, svc) in self.services.iter().enumerate() {
            if let Some(prev) = seen.insert(&svc.name, i) {
                return Err(format!(
                    "duplicate service name {:?} at indices {} and {}",
                    svc.name, prev, i
                ));
            }
        }

        // Check edges reference valid services.
        for (i, edge) in self.edges.iter().enumerate() {
            if !seen.contains_key(&edge.caller) {
                return Err(format!(
                    "edge[{}].caller {:?} does not match any service",
                    i, edge.caller
                ));
            }
            if !seen.contains_key(&edge.callee) {
                return Err(format!(
                    "edge[{}].callee {:?} does not match any service",
                    i, edge.callee
                ));
            }
            if edge.weight == 0 {
                return Err(format!("edge[{i}].weight must be >= 1"));
            }
            if edge.max_repeat == 0 {
                return Err(format!("edge[{i}].max_repeat must be >= 1"));
            }
        }

        // Validate attributes.
        let validate_attrs = |attrs: &[AttributeConfig], ctx: &str| -> Result<(), String> {
            for attr in attrs {
                match &attr.value {
                    AttributeValueConfig::RandomString { length } if *length == 0 => {
                        return Err(format!(
                            "{ctx}: attribute {:?} random_string length must be > 0",
                            attr.key
                        ));
                    }
                    AttributeValueConfig::RandomInt { min, max } if min > max => {
                        return Err(format!(
                            "{ctx}: attribute {:?} random_int min ({min}) > max ({max})",
                            attr.key
                        ));
                    }
                    AttributeValueConfig::OneOf(values) if values.is_empty() => {
                        return Err(format!(
                            "{ctx}: attribute {:?} one_of list must not be empty",
                            attr.key
                        ));
                    }
                    _ => {}
                }
            }
            Ok(())
        };

        for (i, svc) in self.services.iter().enumerate() {
            validate_attrs(
                &svc.resource_attributes,
                &format!("services[{}]({:?})", i, svc.name),
            )?;
        }
        for (i, edge) in self.edges.iter().enumerate() {
            validate_attrs(&edge.attributes, &format!("edges[{i}]"))?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Compiled topology (internal)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CompiledService {
    resource: Resource,
    scope: InstrumentationScope,
}

#[derive(Debug, Clone)]
struct CompiledEdge {
    caller_idx: usize,
    caller_operation: String,
    caller_span_kind: i32,
    callee_idx: usize,
    callee_operation: String,
    callee_span_kind: i32,
    attributes: Vec<AttributeConfig>,
    weight: u32,
    max_repeat: u32,
}

#[derive(Debug, Clone)]
struct CompiledTopology {
    services: Vec<CompiledService>,
    edges: Vec<CompiledEdge>,
    /// For each service index, the indices of edges where that service is the caller.
    adjacency: Vec<Vec<usize>>,
    /// Edge indices that can start a trace (caller is never a callee in any edge).
    root_edges: Vec<usize>,
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// Realistic, topology-driven OpenTelemetry trace generator.
///
/// Produces OTLP traces by walking a user-defined service call graph, yielding spans with
/// human-readable service names, operation names, and structured attributes.
#[derive(Debug, Clone)]
pub struct OpentelemetryTracesGraph {
    topology: CompiledTopology,
    error_rate: f32,
    max_fanout: u8,
    str_pool: Rc<strings::RandomStringPool>,
}

impl OpentelemetryTracesGraph {
    /// Create a new generator from the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn with_config<R>(config: &Config, rng: &mut R) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        config
            .valid()
            .map_err(|e| Error::Validation(e.to_string()))?;

        let str_pool = Rc::new(strings::RandomStringPool::with_size(rng, STRING_POOL_SIZE));

        // Build service name -> index mapping.
        let svc_index: BTreeMap<&str, usize> = config
            .services
            .iter()
            .enumerate()
            .map(|(i, s)| (s.name.as_str(), i))
            .collect();

        // Compile services: pre-build OTLP Resource and InstrumentationScope.
        let compiled_services: Vec<CompiledService> = config
            .services
            .iter()
            .map(|svc| {
                let mut resource_attrs = vec![str_kv("service.name", &svc.name)];
                for attr in &svc.resource_attributes {
                    resource_attrs.push(evaluate_attribute(attr, rng, &str_pool));
                }
                let resource = Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                };
                let scope = InstrumentationScope {
                    name: svc.scope_name.clone().unwrap_or_default(),
                    version: svc.scope_version.clone(),
                    ..Default::default()
                };
                CompiledService { resource, scope }
            })
            .collect();

        // Compile edges.
        let compiled_edges: Vec<CompiledEdge> = config
            .edges
            .iter()
            .map(|e| CompiledEdge {
                caller_idx: svc_index[e.caller.as_str()],
                caller_operation: e.caller_operation.clone(),
                caller_span_kind: e.caller_span_kind,
                callee_idx: svc_index[e.callee.as_str()],
                callee_operation: e.callee_operation.clone(),
                callee_span_kind: e.callee_span_kind,
                attributes: e.attributes.clone(),
                weight: e.weight,
                max_repeat: e.max_repeat,
            })
            .collect();

        // Build adjacency list: for each service, which edges have it as the caller.
        let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); compiled_services.len()];
        for (edge_idx, edge) in compiled_edges.iter().enumerate() {
            adjacency[edge.caller_idx].push(edge_idx);
        }

        // Identify root edges: edges whose caller is never a callee.
        let is_callee: Vec<bool> = {
            let mut v = vec![false; compiled_services.len()];
            for edge in &compiled_edges {
                v[edge.callee_idx] = true;
            }
            v
        };
        let root_edges: Vec<usize> = compiled_edges
            .iter()
            .enumerate()
            .filter(|(_, e)| !is_callee[e.caller_idx])
            .map(|(i, _)| i)
            .collect();

        // If all services appear as callees (fully cyclic graph), fall back to using all edges.
        let root_edges = if root_edges.is_empty() {
            (0..compiled_edges.len()).collect()
        } else {
            root_edges
        };

        let topology = CompiledTopology {
            services: compiled_services,
            edges: compiled_edges,
            adjacency,
            root_edges,
        };

        Ok(Self {
            topology,
            error_rate: config.error_rate,
            max_fanout: config.max_fanout,
            str_pool,
        })
    }
}

// ---------------------------------------------------------------------------
// Span helpers
// ---------------------------------------------------------------------------

/// A span associated with its owning service index, used for grouping into `ResourceSpans`.
struct ServiceSpan {
    service_idx: usize,
    span: v1::Span,
}

/// Parameters for constructing a span.
struct SpanParams<'a> {
    trace_id: [u8; 16],
    operation: &'a str,
    span_kind: i32,
    parent: Option<&'a v1::Span>,
    edge_attributes: &'a [AttributeConfig],
    error_rate: f32,
    str_pool: &'a strings::RandomStringPool,
    /// When set, the span's start time will be at least this value.
    /// Used to sequence repeated sibling spans so they don't overlap.
    earliest_start: Option<u64>,
}

fn make_span<R: Rng + ?Sized>(rng: &mut R, params: &SpanParams<'_>) -> v1::Span {
    let span_id: [u8; 8] = rng.random();

    let (start_ns, end_ns) = match params.parent {
        Some(p) => {
            let ps = params.earliest_start.unwrap_or(p.start_time_unix_nano);
            let pe = p.end_time_unix_nano;
            let s = rng.random_range(ps..=pe);
            let e = rng.random_range(s..=pe);
            (s, e)
        }
        None => get_safe_start_end_timestamps(rng),
    };

    let mut attributes: Vec<KeyValue> = params
        .edge_attributes
        .iter()
        .map(|a| evaluate_attribute(a, rng, params.str_pool))
        .collect();

    // Deduplicate within the span's own attributes.
    let mut seen_keys = std::collections::BTreeSet::new();
    attributes.retain(|kv| seen_keys.insert(kv.key.clone()));

    let is_error = rng.random::<f32>() < params.error_rate;

    v1::Span {
        trace_id: params.trace_id.to_vec(),
        span_id: span_id.to_vec(),
        parent_span_id: params.parent.map_or(vec![], |p| p.span_id.clone()),
        start_time_unix_nano: start_ns,
        end_time_unix_nano: end_ns,
        name: params.operation.to_string(),
        kind: params.span_kind,
        attributes,
        status: is_error.then_some(Status {
            code: 2,
            ..Default::default()
        }),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Attribute evaluation
// ---------------------------------------------------------------------------

fn evaluate_attribute<R: Rng + ?Sized>(
    attr: &AttributeConfig,
    rng: &mut R,
    str_pool: &strings::RandomStringPool,
) -> KeyValue {
    let value = match &attr.value {
        AttributeValueConfig::Static(s) => Value::StringValue(s.clone()),
        AttributeValueConfig::StaticInt(n) => Value::IntValue(*n),
        AttributeValueConfig::RandomString { length } => {
            let s = str_pool.of_size(rng, *length).unwrap_or("").to_string();
            Value::StringValue(s)
        }
        AttributeValueConfig::RandomInt { min, max } => {
            Value::IntValue(rng.random_range(*min..=*max))
        }
        AttributeValueConfig::Dictionary(name) => {
            let dict = match name {
                DictionaryName::HttpMethods => dictionaries::HTTP_METHODS,
                DictionaryName::HttpStatusCodes => dictionaries::HTTP_STATUS_CODES,
                DictionaryName::GrpcStatusCodes => dictionaries::GRPC_STATUS_CODES,
                DictionaryName::CloudRegions => dictionaries::CLOUD_REGIONS,
                DictionaryName::Environments => dictionaries::ENVIRONMENTS,
            };
            let s = dict.choose(rng).copied().unwrap_or("");
            Value::StringValue(s.to_string())
        }
        AttributeValueConfig::OneOf(values) => {
            let s = values.choose(rng).map_or("", String::as_str);
            Value::StringValue(s.to_string())
        }
    };

    KeyValue {
        key: attr.key.clone(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

// ---------------------------------------------------------------------------
// Generator implementation
// ---------------------------------------------------------------------------

impl<'a> crate::Generator<'a> for OpentelemetryTracesGraph {
    type Output = trace::v1::ExportTraceServiceRequest;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: Rng + ?Sized,
    {
        let topo = &self.topology;

        // Pick a root edge (weighted).
        let root_edge_idx = if topo.root_edges.len() == 1 {
            topo.root_edges[0]
        } else {
            let weights: Vec<u32> = topo
                .root_edges
                .iter()
                .map(|&i| topo.edges[i].weight)
                .collect();
            let dist = WeightedIndex::new(&weights).map_err(|e| {
                Error::Validation(format!(
                    "failed to build weighted index for root edges: {e}"
                ))
            })?;
            topo.root_edges[dist.sample(rng)]
        };

        let root_edge = &topo.edges[root_edge_idx];
        let trace_id: [u8; 16] = rng.random();

        let service_spans = walk_topology(
            rng,
            topo,
            root_edge,
            trace_id,
            self.error_rate,
            self.max_fanout,
            &self.str_pool,
        );

        // Group spans by service and build the ExportTraceServiceRequest.
        // BTreeMap ensures deterministic iteration order for reproducible output.
        let mut by_service: BTreeMap<usize, Vec<v1::Span>> = BTreeMap::new();
        for ss in service_spans {
            by_service.entry(ss.service_idx).or_default().push(ss.span);
        }

        let resource_spans: Vec<v1::ResourceSpans> = by_service
            .into_iter()
            .map(|(svc_idx, spans)| {
                let svc = &topo.services[svc_idx];
                v1::ResourceSpans {
                    resource: Some(svc.resource.clone()),
                    scope_spans: vec![v1::ScopeSpans {
                        scope: Some(svc.scope.clone()),
                        spans,
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }
            })
            .collect();

        Ok(trace::v1::ExportTraceServiceRequest { resource_spans })
    }
}

/// Walk the compiled topology starting from `root_edge`, producing spans for each service visited.
#[allow(clippy::similar_names)]
fn walk_topology<R: Rng + ?Sized>(
    rng: &mut R,
    topo: &CompiledTopology,
    root_edge: &CompiledEdge,
    trace_id: [u8; 16],
    error_rate: f32,
    max_fanout: u8,
    str_pool: &strings::RandomStringPool,
) -> Vec<ServiceSpan> {
    let root_params = SpanParams {
        trace_id,
        operation: &root_edge.caller_operation,
        span_kind: root_edge.caller_span_kind,
        parent: None,
        edge_attributes: &root_edge.attributes,
        error_rate,
        str_pool,
        earliest_start: None,
    };
    let caller_span = make_span(rng, &root_params);

    let callee_params = SpanParams {
        trace_id,
        operation: &root_edge.callee_operation,
        span_kind: root_edge.callee_span_kind,
        parent: Some(&caller_span),
        edge_attributes: &root_edge.attributes,
        error_rate,
        str_pool,
        earliest_start: None,
    };
    let callee_span = make_span(rng, &callee_params);

    let mut service_spans: Vec<ServiceSpan> = Vec::with_capacity(32);
    service_spans.push(ServiceSpan {
        service_idx: root_edge.caller_idx,
        span: caller_span,
    });
    service_spans.push(ServiceSpan {
        service_idx: root_edge.callee_idx,
        span: callee_span.clone(),
    });

    // Work stack: (service_idx, parent_span, depth).
    let mut work: Vec<(usize, v1::Span, usize)> = vec![(root_edge.callee_idx, callee_span, 1)];

    while let Some((svc_idx, parent_span, depth)) = work.pop() {
        if depth >= MAX_TRACE_DEPTH {
            continue;
        }

        let outgoing = &topo.adjacency[svc_idx];
        if outgoing.is_empty() {
            continue;
        }

        let fanout = std::cmp::min(max_fanout as usize, outgoing.len());
        let num_calls = rng.random_range(1..=fanout);

        let selected = if outgoing.len() <= num_calls {
            outgoing.clone()
        } else {
            weighted_sample_without_replacement(rng, outgoing, &topo.edges, num_calls)
        };

        for &edge_idx in &selected {
            let edge = &topo.edges[edge_idx];
            let repeats = if edge.max_repeat <= 1 {
                1
            } else {
                rng.random_range(1..=edge.max_repeat)
            };

            // For repeated edges, each repetition starts after the previous
            // one ends, simulating sequential calls (e.g. a batch loop).
            // `cursor` tracks the earliest allowed start time.
            let mut cursor = parent_span.start_time_unix_nano;

            for _ in 0..repeats {
                let client_params = SpanParams {
                    trace_id,
                    operation: &edge.caller_operation,
                    span_kind: edge.caller_span_kind,
                    parent: Some(&parent_span),
                    edge_attributes: &edge.attributes,
                    error_rate,
                    str_pool,
                    earliest_start: Some(cursor),
                };
                let client_span = make_span(rng, &client_params);
                cursor = client_span.end_time_unix_nano;

                let server_params = SpanParams {
                    trace_id,
                    operation: &edge.callee_operation,
                    span_kind: edge.callee_span_kind,
                    parent: Some(&client_span),
                    edge_attributes: &edge.attributes,
                    error_rate,
                    str_pool,
                    earliest_start: None,
                };
                let server_span = make_span(rng, &server_params);

                service_spans.push(ServiceSpan {
                    service_idx: edge.caller_idx,
                    span: client_span,
                });
                service_spans.push(ServiceSpan {
                    service_idx: edge.callee_idx,
                    span: server_span.clone(),
                });

                work.push((edge.callee_idx, server_span, depth + 1));
            }
        }
    }

    service_spans
}

/// Weighted sampling without replacement from outgoing edge indices.
fn weighted_sample_without_replacement<R: Rng + ?Sized>(
    rng: &mut R,
    candidates: &[usize],
    edges: &[CompiledEdge],
    count: usize,
) -> Vec<usize> {
    // For small counts this simple approach is efficient enough.
    let mut remaining: Vec<usize> = candidates.to_vec();
    let mut selected = Vec::with_capacity(count);

    for _ in 0..count {
        if remaining.is_empty() {
            break;
        }
        let weights: Vec<u32> = remaining.iter().map(|&i| edges[i].weight).collect();
        if let Ok(dist) = WeightedIndex::new(&weights) {
            let pick = dist.sample(rng);
            selected.push(remaining.swap_remove(pick));
        } else {
            // Fallback: uniform selection if weights are somehow all zero.
            let pick = rng.random_range(0..remaining.len());
            selected.push(remaining.swap_remove(pick));
        }
    }

    selected
}

// ---------------------------------------------------------------------------
// Trimming
// ---------------------------------------------------------------------------

/// Remove spans from the request until `encoded_len() <= max_bytes`.
fn trim_to_fit(
    mut request: trace::v1::ExportTraceServiceRequest,
    max_bytes: usize,
) -> trace::v1::ExportTraceServiceRequest {
    while request.encoded_len() > max_bytes {
        // Find the ResourceSpans with the most spans and pop one.
        let mut removed = false;
        if let Some(rs) = request
            .resource_spans
            .iter_mut()
            .filter(|rs| rs.scope_spans.iter().any(|ss| !ss.spans.is_empty()))
            .max_by_key(|rs| {
                rs.scope_spans
                    .iter()
                    .map(|ss| ss.spans.len())
                    .sum::<usize>()
            })
        {
            for ss in rs.scope_spans.iter_mut().rev() {
                if !ss.spans.is_empty() {
                    ss.spans.pop();
                    removed = true;
                    break;
                }
            }
        }
        if !removed {
            break;
        }

        // Remove empty ScopeSpans and ResourceSpans.
        for rs in &mut request.resource_spans {
            rs.scope_spans.retain(|ss| !ss.spans.is_empty());
        }
        request
            .resource_spans
            .retain(|rs| !rs.scope_spans.is_empty());

        if request.resource_spans.is_empty() {
            break;
        }
    }

    request
}

// ---------------------------------------------------------------------------
// Serialize implementation
// ---------------------------------------------------------------------------

impl crate::Serialize for OpentelemetryTracesGraph {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let request = self.generate(&mut rng)?;
        let trimmed = trim_to_fit(request, max_bytes);
        let buf = trimmed.encode_to_vec();
        if buf.len() <= max_bytes {
            writer.write_all(&buf)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::Serialize;
    use opentelemetry_proto::tonic::collector::trace as collector_trace;
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    /// A minimal but representative topology for testing.
    fn test_config() -> Config {
        Config {
            services: vec![
                ServiceConfig {
                    name: "api-gateway".into(),
                    resource_attributes: vec![AttributeConfig {
                        key: "deployment.environment".into(),
                        value: AttributeValueConfig::Static("production".into()),
                    }],
                    scope_name: Some("com.example.gateway".into()),
                    scope_version: "1.0.0".into(),
                },
                ServiceConfig {
                    name: "user-service".into(),
                    resource_attributes: vec![],
                    scope_name: Some("com.example.users".into()),
                    scope_version: "1.0.0".into(),
                },
                ServiceConfig {
                    name: "postgres".into(),
                    resource_attributes: vec![AttributeConfig {
                        key: "db.system".into(),
                        value: AttributeValueConfig::Static("postgresql".into()),
                    }],
                    scope_name: None,
                    scope_version: "1.0.0".into(),
                },
            ],
            edges: vec![
                EdgeConfig {
                    caller: "api-gateway".into(),
                    caller_operation: "GET /users/{id}".into(),
                    callee: "user-service".into(),
                    callee_operation: "grpc.UserService/GetUser".into(),
                    caller_span_kind: 3,
                    callee_span_kind: 2,
                    attributes: vec![
                        AttributeConfig {
                            key: "http.method".into(),
                            value: AttributeValueConfig::Static("GET".into()),
                        },
                        AttributeConfig {
                            key: "http.status_code".into(),
                            value: AttributeValueConfig::Dictionary(
                                DictionaryName::HttpStatusCodes,
                            ),
                        },
                    ],
                    weight: 1,
                    max_repeat: 1,
                },
                EdgeConfig {
                    caller: "user-service".into(),
                    caller_operation: "db.query".into(),
                    callee: "postgres".into(),
                    callee_operation: "SELECT users".into(),
                    caller_span_kind: 3,
                    callee_span_kind: 2,
                    attributes: vec![],
                    weight: 1,
                    max_repeat: 1,
                },
            ],
            error_rate: 0.02,
            max_fanout: 3,
        }
    }

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes in 0u16..=u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = OpentelemetryTracesGraph::with_config(&test_config(), &mut rng)
                .expect("valid config");

            let mut bytes = Vec::with_capacity(max_bytes);
            generator.to_bytes(&mut rng, max_bytes, &mut bytes)
                .expect("failed to serialize");
            prop_assert!(bytes.len() <= max_bytes, "max: {max_bytes}, actual: {}", bytes.len());
        }
    }

    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes in 64u16..=u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = OpentelemetryTracesGraph::with_config(&test_config(), &mut rng)
                .expect("valid config");

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            generator.to_bytes(&mut rng, max_bytes, &mut bytes)
                .expect("failed to serialize");

            if !bytes.is_empty() {
                collector_trace::v1::ExportTraceServiceRequest::decode(bytes.as_slice())
                    .expect("failed to decode protobuf");
            }
        }
    }

    #[test]
    fn deterministic_output() {
        let config = test_config();
        let max_bytes = 65536;

        let mut rng1 = SmallRng::seed_from_u64(42);
        let mut generator1 = OpentelemetryTracesGraph::with_config(&config, &mut rng1).unwrap();
        let mut bytes1 = Vec::new();
        generator1
            .to_bytes(&mut rng1, max_bytes, &mut bytes1)
            .unwrap();

        let mut rng2 = SmallRng::seed_from_u64(42);
        let mut generator2 = OpentelemetryTracesGraph::with_config(&config, &mut rng2).unwrap();
        let mut bytes2 = Vec::new();
        generator2
            .to_bytes(&mut rng2, max_bytes, &mut bytes2)
            .unwrap();

        assert_eq!(bytes1, bytes2, "same seed must produce identical output");
    }

    #[test]
    fn config_validation_empty_services() {
        let config = Config {
            services: vec![],
            edges: vec![EdgeConfig {
                caller: "a".into(),
                caller_operation: "op".into(),
                callee: "b".into(),
                callee_operation: "op".into(),
                caller_span_kind: 3,
                callee_span_kind: 2,
                attributes: vec![],
                weight: 1,
                max_repeat: 1,
            }],
            error_rate: 0.01,
            max_fanout: 3,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_empty_edges() {
        let config = Config {
            services: vec![ServiceConfig {
                name: "svc".into(),
                resource_attributes: vec![],
                scope_name: None,
                scope_version: "1.0.0".into(),
            }],
            edges: vec![],
            error_rate: 0.01,
            max_fanout: 3,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_unknown_service_in_edge() {
        let config = Config {
            services: vec![ServiceConfig {
                name: "svc".into(),
                resource_attributes: vec![],
                scope_name: None,
                scope_version: "1.0.0".into(),
            }],
            edges: vec![EdgeConfig {
                caller: "svc".into(),
                caller_operation: "op".into(),
                callee: "nonexistent".into(),
                callee_operation: "op".into(),
                caller_span_kind: 3,
                callee_span_kind: 2,
                attributes: vec![],
                weight: 1,
                max_repeat: 1,
            }],
            error_rate: 0.01,
            max_fanout: 3,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn spans_have_correct_service_names() {
        let config = test_config();
        let mut rng = SmallRng::seed_from_u64(99);
        let mut generator = OpentelemetryTracesGraph::with_config(&config, &mut rng).unwrap();

        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, 65536, &mut bytes).unwrap();

        let request =
            collector_trace::v1::ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap();

        let expected_names: std::collections::HashSet<&str> =
            ["api-gateway", "user-service", "postgres"].into();
        for rs in &request.resource_spans {
            if let Some(resource) = &rs.resource {
                for kv in &resource.attributes {
                    if kv.key == "service.name" {
                        if let Some(AnyValue {
                            value: Some(Value::StringValue(ref name)),
                        }) = kv.value
                        {
                            assert!(
                                expected_names.contains(name.as_str()),
                                "unexpected service name: {name}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn trace_hierarchy_is_valid() {
        let config = test_config();
        let mut rng = SmallRng::seed_from_u64(123);
        let generator = OpentelemetryTracesGraph::with_config(&config, &mut rng).unwrap();

        let request = generator.generate(&mut rng).unwrap();

        // Collect all spans and verify parent-child relationships.
        let mut all_span_ids: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut all_spans: Vec<&v1::Span> = Vec::new();
        let mut trace_ids: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

        for rs in &request.resource_spans {
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    all_span_ids.insert(span.span_id.clone());
                    all_spans.push(span);
                    trace_ids.insert(span.trace_id.clone());
                }
            }
        }

        // All spans share the same trace_id.
        assert_eq!(
            trace_ids.len(),
            1,
            "all spans should share the same trace_id"
        );

        // Every non-root span's parent_span_id should reference a valid span.
        let mut root_count = 0;
        for span in &all_spans {
            if span.parent_span_id.is_empty() {
                root_count += 1;
            } else {
                assert!(
                    all_span_ids.contains(&span.parent_span_id),
                    "span {:?} has parent_span_id {:?} that doesn't match any span",
                    span.span_id,
                    span.parent_span_id
                );
            }
        }

        assert_eq!(root_count, 1, "there should be exactly one root span");
    }

    #[test]
    fn traces_traverse_full_topology() {
        // With a linear chain (api-gateway → user-service → postgres), every
        // generated trace must contain spans from all three services.
        let config = test_config();
        let mut rng = SmallRng::seed_from_u64(77);
        let generator = OpentelemetryTracesGraph::with_config(&config, &mut rng).unwrap();

        let request = generator.generate(&mut rng).unwrap();

        let service_names: std::collections::BTreeSet<String> = request
            .resource_spans
            .iter()
            .filter_map(|rs| rs.resource.as_ref())
            .flat_map(|r| &r.attributes)
            .filter(|kv| kv.key == "service.name")
            .filter_map(|kv| match &kv.value {
                Some(AnyValue {
                    value: Some(Value::StringValue(name)),
                }) => Some(name.clone()),
                _ => None,
            })
            .collect();

        assert!(
            service_names.contains("api-gateway"),
            "trace must include api-gateway, got: {service_names:?}"
        );
        assert!(
            service_names.contains("user-service"),
            "trace must include user-service, got: {service_names:?}"
        );
        assert!(
            service_names.contains("postgres"),
            "trace must include postgres, got: {service_names:?}"
        );
    }

    #[test]
    fn max_repeat_produces_multiple_span_pairs() {
        // Use max_repeat: 5 on the user-service → postgres edge. With
        // max_repeat=5 the generator creates 1–5 client+server pairs per
        // walk, so across many seeds at least one trace must have more
        // postgres spans than a non-repeating config would produce.
        let config = Config {
            services: test_config().services,
            edges: vec![
                EdgeConfig {
                    caller: "api-gateway".into(),
                    caller_operation: "GET /users/{id}".into(),
                    callee: "user-service".into(),
                    callee_operation: "grpc.UserService/GetUser".into(),
                    caller_span_kind: 3,
                    callee_span_kind: 2,
                    attributes: vec![],
                    weight: 1,
                    max_repeat: 1,
                },
                EdgeConfig {
                    caller: "user-service".into(),
                    caller_operation: "db.query".into(),
                    callee: "postgres".into(),
                    callee_operation: "SELECT users".into(),
                    caller_span_kind: 3,
                    callee_span_kind: 2,
                    attributes: vec![],
                    weight: 1,
                    max_repeat: 5,
                },
            ],
            error_rate: 0.0,
            max_fanout: 3,
        };

        let mut rng = SmallRng::seed_from_u64(42);
        let generator = OpentelemetryTracesGraph::with_config(&config, &mut rng).unwrap();

        // Generate several traces; at least one should have >1 postgres span
        // pair (i.e. more than 2 spans attributed to postgres).
        let mut saw_repeat = false;
        for _ in 0..20 {
            let request = generator.generate(&mut rng).unwrap();
            let postgres_span_count: usize = request
                .resource_spans
                .iter()
                .filter(|rs| {
                    rs.resource
                        .as_ref()
                        .and_then(|r| {
                            r.attributes.iter().find(|kv| {
                                kv.key == "service.name"
                                    && matches!(
                                        &kv.value,
                                        Some(AnyValue {
                                            value: Some(Value::StringValue(n))
                                        }) if n == "postgres"
                                    )
                            })
                        })
                        .is_some()
                })
                .flat_map(|rs| &rs.scope_spans)
                .map(|ss| ss.spans.len())
                .sum();
            if postgres_span_count > 1 {
                saw_repeat = true;
                break;
            }
        }
        assert!(
            saw_repeat,
            "expected max_repeat=5 to produce multiple postgres span pairs in at least one of 20 traces"
        );
    }

    #[test]
    fn repeated_spans_are_sequential() {
        // With max_repeat=10 on the db edge, the repeated client spans
        // (db.query) under user-service should not overlap: each one's
        // start_time must be >= the previous one's end_time.
        let config = Config {
            services: test_config().services,
            edges: vec![
                EdgeConfig {
                    caller: "api-gateway".into(),
                    caller_operation: "GET /users/{id}".into(),
                    callee: "user-service".into(),
                    callee_operation: "grpc.UserService/GetUser".into(),
                    caller_span_kind: 3,
                    callee_span_kind: 2,
                    attributes: vec![],
                    weight: 1,
                    max_repeat: 1,
                },
                EdgeConfig {
                    caller: "user-service".into(),
                    caller_operation: "db.query".into(),
                    callee: "postgres".into(),
                    callee_operation: "SELECT users".into(),
                    caller_span_kind: 3,
                    callee_span_kind: 2,
                    attributes: vec![],
                    weight: 1,
                    max_repeat: 10,
                },
            ],
            error_rate: 0.0,
            max_fanout: 3,
        };

        let mut rng = SmallRng::seed_from_u64(42);
        let generator = OpentelemetryTracesGraph::with_config(&config, &mut rng).unwrap();

        for _ in 0..50 {
            let request = generator.generate(&mut rng).unwrap();

            // Collect all spans, grouping by parent_span_id.
            let mut by_parent: BTreeMap<Vec<u8>, Vec<&v1::Span>> = BTreeMap::new();
            for rs in &request.resource_spans {
                for ss in &rs.scope_spans {
                    for span in &ss.spans {
                        if !span.parent_span_id.is_empty() {
                            by_parent
                                .entry(span.parent_span_id.clone())
                                .or_default()
                                .push(span);
                        }
                    }
                }
            }

            // For each group of siblings, sort by start time and verify
            // no overlap: each span starts at or after the previous ends.
            for (_parent_id, mut siblings) in by_parent {
                if siblings.len() <= 1 {
                    continue;
                }
                siblings.sort_by_key(|s| s.start_time_unix_nano);
                for pair in siblings.windows(2) {
                    assert!(
                        pair[1].start_time_unix_nano >= pair[0].end_time_unix_nano,
                        "sibling spans overlap: first ends at {}, next starts at {}",
                        pair[0].end_time_unix_nano,
                        pair[1].start_time_unix_nano,
                    );
                }
            }
        }
    }

    #[test]
    fn bare_scalar_deserialization() {
        // Bare string -> Static
        let json = r#"{"key": "http.method", "value": "GET"}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(attr.key, "http.method");
        assert_eq!(attr.value, AttributeValueConfig::Static("GET".into()));

        // Bare integer -> StaticInt
        let json = r#"{"key": "http.status_code", "value": 200}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(attr.key, "http.status_code");
        assert_eq!(attr.value, AttributeValueConfig::StaticInt(200));

        // Tagged static string still works
        let json = r#"{"key": "env", "value": {"static": "production"}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::Static("production".into())
        );

        // Tagged dictionary still works
        let json = r#"{"key": "region", "value": {"dictionary": "cloud_regions"}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::Dictionary(DictionaryName::CloudRegions)
        );

        // Tagged random_string still works
        let json = r#"{"key": "req_id", "value": {"random_string": {"length": 16}}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::RandomString { length: 16 }
        );

        // Tagged one_of still works
        let json = r#"{"key": "color", "value": {"one_of": ["red", "green", "blue"]}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::OneOf(vec!["red".into(), "green".into(), "blue".into()])
        );
    }
}
