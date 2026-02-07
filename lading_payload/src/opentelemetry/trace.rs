//! Topology-driven OpenTelemetry OTLP trace payload generator.
//!
//! Users define a distributed system topology — services, their operations, and the call graph
//! between them — and the generator produces traces that walk that topology. All string data comes
//! from the user configuration or built-in dictionaries, producing output that resembles real
//! distributed system traffic.
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
use rand::{Rng, seq::IndexedRandom};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    rc::Rc,
};

const STRING_POOL_SIZE: usize = 65_536;
const MAX_TRACE_DEPTH: usize = 16;
const MAX_TRACE_DURATION_NS: u64 = 30 * 1_000_000_000; // 30 seconds

/// Minimum valid start timestamp (Year 2000 in nanoseconds since `UNIX_EPOCH`).
///
/// While this isn't a limitation in OTLP, we do this for practical interoperation with the Datadog
/// Agent and related components, which _do_ have a minimum timestamp requirement.
const YEAR_2000_NANOS: u64 = 946_684_800_000_000_000;

fn get_safe_start_end_timestamps<R>(rng: &mut R) -> (u64, u64)
where
    R: Rng + ?Sized,
{
    // We generate a random start timestamp, and duration, to satisfy some basic constraints:
    //
    // 1. Must be >= Year 2000 expressed in nanoseconds (946684800000000000)
    // 2. Must be <= i64::MAX to avoid overflow in Go
    // 3. Duration must not overflow i64::MAX when added to the start timestamp
    //
    // We do this to make these traces/spans interoperable with the Datadog Agent.
    let start_timestamp_ns = rng.random_range(YEAR_2000_NANOS..=i64::MAX as u64);
    let max_duration_ns = i64::MAX as u64 - start_timestamp_ns;
    let duration_upper_bound = std::cmp::min(max_duration_ns, MAX_TRACE_DURATION_NS);
    let duration_ns = rng.random_range(0..=duration_upper_bound);

    (start_timestamp_ns, start_timestamp_ns + duration_ns)
}

fn str_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

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
///
/// The call graph is defined by embedding `suboperations` within each
/// operation, rather than as a separate edge list. Suboperations execute
/// sequentially in their defined order.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The services that make up the simulated distributed system.
    pub services: Vec<ServiceConfig>,

    /// Probability \[0.0, 1.0\] that any individual span has error status.
    #[serde(default = "default_error_rate")]
    pub error_rate: f32,
}

fn default_error_rate() -> f32 {
    0.01
}

/// The protocol type of a service, determining the operation config format.
///
/// When set on a [`ServiceConfig`], all operations use a type-specific format
/// and automatically derive span names and protocol-specific attributes.
#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum ServiceType {
    /// HTTP/REST service — operations define endpoints with `method` + `route`.
    Http,
    /// gRPC service — operations define RPC methods with `method`.
    Grpc,
    /// Database or data store — operations define queries with `query` + optional `table`.
    Database,
}

/// Service-level gRPC configuration.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct GrpcServiceConfig {
    /// The gRPC service name (e.g. `"ProductService"`).
    pub service: String,
}

/// Service-level database configuration.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct DatabaseServiceConfig {
    /// Database system (e.g. `"postgresql"`, `"mysql"`, `"redis"`).
    pub system: String,
    /// Database name (optional, becomes `db.name` attribute).
    #[serde(default)]
    pub name: Option<String>,
}

/// A single service in the topology.
///
/// Services can optionally declare a [`ServiceType`] that determines the
/// format of their operations and auto-derives span names and attributes.
///
/// ```yaml
/// # HTTP service — operations have method + route
/// - name: api-gateway
///   service_type: http
///   operations:
///     - id: get-product
///       method: GET
///       route: "/api/v1/products/{id}"
///
/// # Plain service — operations have manual span_name
/// - name: custom-svc
///   operations:
///     - id: do-thing
///       span_name: "my.operation"
/// ```
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ServiceConfig {
    /// Service name (becomes `service.name` resource attribute).
    pub name: String,

    /// Protocol type of this service. When set, operations use a
    /// type-specific format and auto-derive span names and attributes.
    #[serde(default)]
    pub service_type: Option<ServiceType>,

    /// Service-level gRPC configuration (required when `service_type` is `grpc`).
    #[serde(default)]
    pub grpc: Option<GrpcServiceConfig>,

    /// Service-level database configuration (required when `service_type` is `database`).
    #[serde(default)]
    pub database: Option<DatabaseServiceConfig>,

    /// The operations this service can perform.
    pub operations: Vec<OperationConfig>,

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

/// An operation that a service can perform.
///
/// The required fields depend on the parent service's [`ServiceType`]:
/// - **No type (plain)**: `span_name` is required.
/// - **HTTP**: `method` and `route` are required.
/// - **gRPC**: `method` is required.
/// - **Database**: `query` is required, `table` is optional.
///
/// Operations may define `suboperations` — downstream calls that are executed
/// sequentially when this operation is invoked:
///
/// ```yaml
/// operations:
///   - id: get-product
///     method: GET
///     route: "/api/v1/products/{id}"
///     suboperations:
///       - to: product-cache/get-product
///       - to: product-db/select-product-by-id
///         rate: 0.01
/// ```
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OperationConfig {
    /// Short identifier: lowercase ASCII letters, digits, and hyphens only.
    /// Used to reference this operation in suboperation definitions
    /// (e.g. `"my-service/get-users"`).
    pub id: String,

    /// Manual span name (plain services only, no `service_type`).
    #[serde(default)]
    pub span_name: Option<String>,

    /// HTTP method (e.g. `"GET"`) or gRPC method name (e.g. `"GetProduct"`).
    #[serde(default)]
    pub method: Option<String>,

    /// HTTP route pattern, e.g. `"/api/v1/products/{id}"` (HTTP services only).
    #[serde(default)]
    pub route: Option<String>,

    /// Database table or collection name (database services only, optional).
    #[serde(default)]
    pub table: Option<String>,

    /// Database query string, e.g. `"SELECT * FROM products WHERE id = $1"`
    /// (database services only).
    #[serde(default)]
    pub query: Option<String>,

    /// Downstream operations called sequentially when this operation is invoked.
    /// Each suboperation fires independently based on its `rate`.
    #[serde(default)]
    pub suboperations: Vec<SuboperationConfig>,

    /// Extra attributes attached to this operation's span.
    #[serde(default)]
    pub attributes: Vec<AttributeConfig>,

    /// Mark this operation as a trace entry point (root). When not set, roots
    /// are inferred as operations that are never referenced as a suboperation
    /// target.
    #[serde(default)]
    pub root: Option<bool>,
}

/// A downstream operation called sequentially as part of a parent operation.
///
/// ```yaml
/// suboperations:
///   - to: product-cache/get-product
///   - to: product-db/select-product-by-id
///     rate: 0.01
/// ```
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct SuboperationConfig {
    /// The callee endpoint: `"service-name/operation-id"`.
    pub to: String,

    /// Probability \[0.0, 1.0\] that this suboperation executes on each
    /// invocation of the parent operation. Defaults to 1.0 (always fires).
    #[serde(default = "default_subop_rate")]
    pub rate: f32,

    /// Maximum number of times this suboperation repeats when it fires.
    /// The actual count is chosen uniformly in \[1, `max_repeat`\].
    /// Defaults to 1 (no repetition).
    #[serde(default = "default_max_repeat")]
    pub max_repeat: u32,

    /// Attributes added to spans generated by this suboperation.
    #[serde(default)]
    pub attributes: Vec<AttributeConfig>,
}

fn default_subop_rate() -> f32 {
    1.0
}

fn default_max_repeat() -> u32 {
    1
}

// ---------------------------------------------------------------------------
// Operation type helpers
// ---------------------------------------------------------------------------

/// Resolved span name and auto-generated attributes from an operation config.
struct ResolvedOperation {
    span_name: String,
    attributes: Vec<AttributeConfig>,
}

/// Resolve an operation into its span name and auto-generated attributes,
/// taking the parent service's type into account.
///
/// # Panics
///
/// Panics if the service/operation combination is invalid (should be caught
/// by validation).
fn resolve_service_operation(svc: &ServiceConfig, op: &OperationConfig) -> ResolvedOperation {
    match svc.service_type {
        None => resolve_plain_operation(op),
        Some(ServiceType::Http) => resolve_http_operation(op),
        Some(ServiceType::Grpc) => resolve_grpc_operation(svc, op),
        Some(ServiceType::Database) => resolve_database_operation(svc, op),
    }
}

fn resolve_plain_operation(op: &OperationConfig) -> ResolvedOperation {
    ResolvedOperation {
        span_name: op.span_name.clone().unwrap_or_default(),
        attributes: vec![],
    }
}

fn resolve_http_operation(op: &OperationConfig) -> ResolvedOperation {
    let method = op.method.as_deref().unwrap_or("");
    let route = op.route.as_deref().unwrap_or("");
    ResolvedOperation {
        span_name: format!("{method} {route}"),
        attributes: vec![
            AttributeConfig::new("http.request.method", method),
            AttributeConfig::new("http.route", route),
            AttributeConfig::new("http.response.status_code", DictionaryName::HttpStatusCodes),
        ],
    }
}

fn resolve_grpc_operation(svc: &ServiceConfig, op: &OperationConfig) -> ResolvedOperation {
    let grpc_svc = svc.grpc.as_ref().map_or("", |g| g.service.as_str());
    let method = op.method.as_deref().unwrap_or("");
    let fully_qualified_method = format!("{grpc_svc}/{method}");
    ResolvedOperation {
        span_name: fully_qualified_method.clone(),
        attributes: vec![
            AttributeConfig::new("rpc.system.name", "grpc"),
            AttributeConfig::new("rpc.method", fully_qualified_method),
            AttributeConfig::new("rpc.response.status_code", DictionaryName::GrpcStatusCodes),
        ],
    }
}

fn resolve_database_operation(svc: &ServiceConfig, op: &OperationConfig) -> ResolvedOperation {
    let db_cfg = svc.database.as_ref();
    let system = db_cfg.map_or("", |d| d.system.as_str());
    let query = op.query.as_deref().unwrap_or("");
    // Extract the operation verb from the first token of the query string.
    let db_op = query.split_whitespace().next().unwrap_or("");
    let span_name = match (system, &op.table) {
        (_, Some(t)) => format!("{db_op} {t}"),
        ("redis", _) => query.to_string(),
        _ => db_op.to_string(),
    };
    let mut attributes = vec![
        AttributeConfig::new("db.system.name", system),
        AttributeConfig::new("db.operation.name", db_op),
    ];
    if let Some(name) = db_cfg.and_then(|d| d.name.as_ref()) {
        attributes.push(AttributeConfig::new("db.namespace", name));
    }
    if let Some(ref table) = op.table {
        attributes.push(AttributeConfig::new("db.collection.name", table));
    }
    if !query.is_empty() {
        attributes.push(AttributeConfig::new("db.query.text", query));
    }
    ResolvedOperation {
        span_name,
        attributes,
    }
}

fn default_scope_version() -> String {
    "1.0.0".to_string()
}

const CALLER_SPAN_KIND: i32 = 3; // SPAN_KIND_CLIENT
const CALLEE_SPAN_KIND: i32 = 2; // SPAN_KIND_SERVER

/// An attribute key/value definition.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct AttributeConfig {
    /// The attribute key (e.g. `"http.method"`).
    pub key: String,
    /// How the value is produced.
    pub value: AttributeValueConfig,
}

impl AttributeConfig {
    /// Create a new attribute configuration.
    pub fn new(key: impl Into<String>, value: impl Into<AttributeValueConfig>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// How an attribute value is generated at runtime.
///
/// Bare scalars are accepted as shorthand for static values:
/// - `value: "some string"` is equivalent to `value: { static: "some string" }`
/// - `value: 42` is equivalent to `value: { static_int: 42 }`
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<&str> for AttributeValueConfig {
    fn from(value: &str) -> Self {
        Self::Static(value.to_string())
    }
}

impl From<&String> for AttributeValueConfig {
    fn from(value: &String) -> Self {
        Self::Static(value.clone())
    }
}

impl From<String> for AttributeValueConfig {
    fn from(value: String) -> Self {
        Self::Static(value)
    }
}

impl From<DictionaryName> for AttributeValueConfig {
    fn from(value: DictionaryName) -> Self {
        Self::Dictionary(value)
    }
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
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

/// Parse a `"service-name/operation-id"` reference into its two parts.
///
/// # Errors
///
/// Returns a description if the format is invalid.
fn parse_endpoint_ref<'a>(reference: &'a str, ctx: &str) -> Result<(&'a str, &'a str), String> {
    let (svc, op) = reference.split_once('/').ok_or_else(|| {
        format!("{ctx}: {reference:?} must be in \"service-name/operation-id\" format")
    })?;
    if svc.is_empty() {
        return Err(format!(
            "{ctx}: {reference:?}: service name must not be empty"
        ));
    }
    if op.is_empty() {
        return Err(format!(
            "{ctx}: {reference:?}: operation id must not be empty"
        ));
    }
    Ok((svc, op))
}

/// Validate that an operation ID contains only lowercase ASCII letters, digits, and hyphens,
/// with no leading/trailing hyphens and no consecutive hyphens.
fn validate_operation_id(id: &str, svc_name: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err(format!(
            "service {svc_name:?}: operation id must not be empty"
        ));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(format!(
            "service {svc_name:?}: operation id {id:?} must contain only lowercase letters, digits, and hyphens"
        ));
    }
    if id.starts_with('-') || id.ends_with('-') {
        return Err(format!(
            "service {svc_name:?}: operation id {id:?} must not start or end with a hyphen"
        ));
    }
    if id.contains("--") {
        return Err(format!(
            "service {svc_name:?}: operation id {id:?} must not contain consecutive hyphens"
        ));
    }
    Ok(())
}

/// Validate a single endpoint reference against the set of known operations.
fn validate_endpoint_ref(
    valid_ops: &BTreeMap<&str, BTreeSet<&str>>,
    svc: &str,
    op: &str,
    ctx: &str,
) -> Result<(), String> {
    match valid_ops.get(svc) {
        None => Err(format!("{ctx}: service {svc:?} does not exist")),
        Some(ops) if !ops.contains(op) => Err(format!(
            "{ctx}: operation {op:?} does not exist on service {svc:?}"
        )),
        _ => Ok(()),
    }
}

/// Reject operation fields that are not valid for the given context.
fn reject_fields(
    op: &OperationConfig,
    svc_name: &str,
    disallowed: &[(&str, bool)],
) -> Result<(), String> {
    for &(name, present) in disallowed {
        if present {
            return Err(format!(
                "service {svc_name:?}: operation {:?} must not set {name:?} for this service type",
                op.id
            ));
        }
    }
    Ok(())
}

/// Validate service-type consistency and per-operation fields.
fn validate_service_type(svc: &ServiceConfig) -> Result<(), String> {
    match svc.service_type {
        None => validate_plain_service(svc),
        Some(ServiceType::Http) => validate_http_service(svc),
        Some(ServiceType::Grpc) => validate_grpc_service(svc),
        Some(ServiceType::Database) => validate_database_service(svc),
    }
}

fn validate_plain_service(svc: &ServiceConfig) -> Result<(), String> {
    let name = &svc.name;
    if svc.grpc.is_some() {
        return Err(format!(
            "service {name:?}: 'grpc' block requires service_type: grpc"
        ));
    }
    if svc.database.is_some() {
        return Err(format!(
            "service {name:?}: 'database' block requires service_type: database"
        ));
    }
    for op in &svc.operations {
        if op.span_name.is_none() {
            return Err(format!(
                "service {name:?}: operation {:?} requires span_name (no service_type set)",
                op.id
            ));
        }
        reject_fields(
            op,
            name,
            &[
                ("method", op.method.is_some()),
                ("route", op.route.is_some()),
                ("table", op.table.is_some()),
                ("query", op.query.is_some()),
            ],
        )?;
    }
    Ok(())
}

fn validate_http_service(svc: &ServiceConfig) -> Result<(), String> {
    let name = &svc.name;
    if svc.grpc.is_some() || svc.database.is_some() {
        return Err(format!(
            "service {name:?}: HTTP service must not have 'grpc' or 'database' blocks"
        ));
    }
    for op in &svc.operations {
        reject_fields(
            op,
            name,
            &[
                ("span_name", op.span_name.is_some()),
                ("table", op.table.is_some()),
                ("query", op.query.is_some()),
            ],
        )?;
        if op.method.as_deref().unwrap_or("").is_empty() {
            return Err(format!(
                "service {name:?}: operation {:?} requires non-empty 'method'",
                op.id
            ));
        }
        if op.route.as_deref().unwrap_or("").is_empty() {
            return Err(format!(
                "service {name:?}: operation {:?} requires non-empty 'route'",
                op.id
            ));
        }
    }
    Ok(())
}

fn validate_grpc_service(svc: &ServiceConfig) -> Result<(), String> {
    let name = &svc.name;
    if svc.database.is_some() {
        return Err(format!(
            "service {name:?}: gRPC service must not have 'database' block"
        ));
    }
    match &svc.grpc {
        None => {
            return Err(format!(
                "service {name:?}: gRPC service requires a 'grpc' block with 'service'"
            ));
        }
        Some(g) if g.service.is_empty() => {
            return Err(format!("service {name:?}: grpc.service must not be empty"));
        }
        _ => {}
    }
    for op in &svc.operations {
        reject_fields(
            op,
            name,
            &[
                ("span_name", op.span_name.is_some()),
                ("route", op.route.is_some()),
                ("table", op.table.is_some()),
                ("query", op.query.is_some()),
            ],
        )?;
        if op.method.as_deref().unwrap_or("").is_empty() {
            return Err(format!(
                "service {name:?}: operation {:?} requires non-empty 'method'",
                op.id
            ));
        }
    }
    Ok(())
}

fn validate_database_service(svc: &ServiceConfig) -> Result<(), String> {
    let name = &svc.name;
    if svc.grpc.is_some() {
        return Err(format!(
            "service {name:?}: database service must not have 'grpc' block"
        ));
    }
    match &svc.database {
        None => {
            return Err(format!(
                "service {name:?}: database service requires a 'database' block with 'system'"
            ));
        }
        Some(d) if d.system.is_empty() => {
            return Err(format!(
                "service {name:?}: database.system must not be empty"
            ));
        }
        _ => {}
    }
    for op in &svc.operations {
        reject_fields(
            op,
            name,
            &[
                ("span_name", op.span_name.is_some()),
                ("method", op.method.is_some()),
                ("route", op.route.is_some()),
            ],
        )?;
        if op.query.as_deref().unwrap_or("").is_empty() {
            return Err(format!(
                "service {name:?}: operation {:?} requires non-empty 'query'",
                op.id
            ));
        }
    }
    Ok(())
}

/// Validated operation info: maps service name to the set of known operation IDs.
/// Returns a structure the edge validator can check references against.
fn validate_services(services: &[ServiceConfig]) -> Result<BTreeMap<&str, BTreeSet<&str>>, String> {
    let mut svc_seen = BTreeMap::new();
    let mut valid_ops: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (i, svc) in services.iter().enumerate() {
        if let Some(prev) = svc_seen.insert(&svc.name, i) {
            return Err(format!(
                "duplicate service name {:?} at indices {prev} and {i}",
                svc.name
            ));
        }
        if svc.operations.is_empty() {
            return Err(format!(
                "service {:?} must have at least one operation",
                svc.name
            ));
        }
        validate_service_type(svc)?;
        let mut op_seen = BTreeSet::new();
        for op in &svc.operations {
            validate_operation_id(&op.id, &svc.name)?;
            if !op_seen.insert(op.id.as_str()) {
                return Err(format!(
                    "service {:?}: duplicate operation id {:?}",
                    svc.name, op.id
                ));
            }
        }
        valid_ops.insert(&svc.name, op_seen);
    }
    Ok(valid_ops)
}

/// Validate attributes within a context label.
fn validate_attrs(attrs: &[AttributeConfig], ctx: &str) -> Result<(), String> {
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
}

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
        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(format!(
                "error_rate must be finite and in [0.0, 1.0], got {}",
                self.error_rate
            ));
        }

        let valid_ops = validate_services(&self.services)?;

        // Validate suboperations and collect the set of called targets.
        let mut called_ops: BTreeSet<(&str, &str)> = BTreeSet::new();
        let mut has_subops = false;

        for svc in &self.services {
            for op in &svc.operations {
                for (i, subop) in op.suboperations.iter().enumerate() {
                    has_subops = true;
                    let ctx = format!(
                        "service {:?}, operation {:?}, suboperation[{i}]",
                        svc.name, op.id
                    );
                    if !subop.rate.is_finite() || !(0.0..=1.0).contains(&subop.rate) {
                        return Err(format!(
                            "{ctx}: rate must be finite and in [0.0, 1.0], got {}",
                            subop.rate
                        ));
                    }
                    if subop.max_repeat == 0 {
                        return Err(format!("{ctx}: max_repeat must be >= 1"));
                    }
                    let (to_svc, to_op) = parse_endpoint_ref(&subop.to, &ctx)?;
                    validate_endpoint_ref(&valid_ops, to_svc, to_op, &ctx)?;
                    validate_attrs(&subop.attributes, &ctx)?;
                    called_ops.insert((to_svc, to_op));
                }
                let op_ctx = format!("service {:?}, operation {:?}", svc.name, op.id);
                validate_attrs(&op.attributes, &op_ctx)?;
            }
        }

        if !has_subops {
            return Err(
                "at least one operation must have suboperations to define a call graph".into(),
            );
        }

        // Identify root operations: those never referenced as a suboperation target,
        // or explicitly marked with `root: true`.
        let mut has_root = false;
        for svc in &self.services {
            for op in &svc.operations {
                let is_called = called_ops.contains(&(svc.name.as_str(), op.id.as_str()));
                let is_explicit_root = op.root == Some(true);
                if is_explicit_root || !is_called {
                    has_root = true;
                }
            }
        }
        if !has_root {
            return Err(
                "no root operations found; mark at least one operation with root: true".into(),
            );
        }

        for (i, svc) in self.services.iter().enumerate() {
            validate_attrs(
                &svc.resource_attributes,
                &format!("services[{i}]({:?})", svc.name),
            )?;
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
    service_type: Option<ServiceType>,
}

/// A compiled operation with its resolved span name, attributes, and
/// sequential list of downstream suboperations.
#[derive(Debug, Clone)]
struct CompiledOperation {
    service_idx: usize,
    span_name: String,
    attributes: Vec<AttributeConfig>,
    suboperations: Vec<CompiledSuboperation>,
}

/// A compiled suboperation linking to a target operation by index.
#[derive(Debug, Clone)]
struct CompiledSuboperation {
    to_operation_idx: usize,
    rate: f32,
    max_repeat: u32,
    caller_attributes: Vec<AttributeConfig>,
    callee_attributes: Vec<AttributeConfig>,
}

#[derive(Debug, Clone)]
struct CompiledTopology {
    services: Vec<CompiledService>,
    operations: Vec<CompiledOperation>,
    /// Indices into `operations` that can start a trace.
    root_operation_indices: Vec<usize>,
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// Realistic, topology-driven OpenTelemetry trace generator.
///
/// Produces OTLP traces by walking a user-defined service call graph, yielding spans with
/// human-readable service names, operation names, and structured attributes.
#[derive(Debug, Clone)]
pub struct OpentelemetryTraces {
    topology: CompiledTopology,
    error_rate: f32,
    str_pool: Rc<strings::RandomStringPool>,
}

impl OpentelemetryTraces {
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

        // Resolve each operation into its span name and auto-generated attributes.
        let resolved_ops: BTreeMap<(&str, &str), ResolvedOperation> = config
            .services
            .iter()
            .flat_map(|svc| {
                svc.operations.iter().map(move |op| {
                    (
                        (svc.name.as_str(), op.id.as_str()),
                        resolve_service_operation(svc, op),
                    )
                })
            })
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
                CompiledService {
                    resource,
                    scope,
                    service_type: svc.service_type,
                }
            })
            .collect();

        let topology = compile_topology(config, &svc_index, &resolved_ops, compiled_services)?;

        Ok(Self {
            topology,
            error_rate: config.error_rate,
            str_pool,
        })
    }
}

/// Compile operations and suboperations into the internal topology.
#[allow(clippy::similar_names)]
fn compile_topology(
    config: &Config,
    svc_index: &BTreeMap<&str, usize>,
    resolved_ops: &BTreeMap<(&str, &str), ResolvedOperation>,
    compiled_services: Vec<CompiledService>,
) -> Result<CompiledTopology, Error> {
    // Build (service_name, op_id) -> operation_index map.
    let mut op_index: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    let mut operations: Vec<CompiledOperation> = Vec::new();

    for svc in &config.services {
        for op in &svc.operations {
            let key = (svc.name.as_str(), op.id.as_str());
            let resolved = &resolved_ops[&key];
            op_index.insert(key, operations.len());
            let mut attrs = resolved.attributes.clone();
            attrs.extend(op.attributes.iter().cloned());
            operations.push(CompiledOperation {
                service_idx: svc_index[svc.name.as_str()],
                span_name: resolved.span_name.clone(),
                attributes: attrs,
                suboperations: Vec::new(),
            });
        }
    }

    // Second pass: fill in suboperations now that all operation indices exist.
    let mut op_idx = 0;
    for svc in &config.services {
        for op in &svc.operations {
            for subop_cfg in &op.suboperations {
                let (to_svc, to_op) = subop_cfg.to.split_once('/').ok_or_else(|| {
                    Error::Validation(format!("bad suboperation ref: {:?}", subop_cfg.to))
                })?;
                let to_op_idx = op_index[&(to_svc, to_op)];
                let caller_attrs =
                    combine_attributes(&subop_cfg.attributes, &operations[op_idx].attributes);
                let callee_attrs =
                    combine_attributes(&subop_cfg.attributes, &operations[to_op_idx].attributes);
                operations[op_idx].suboperations.push(CompiledSuboperation {
                    to_operation_idx: to_op_idx,
                    rate: subop_cfg.rate,
                    max_repeat: subop_cfg.max_repeat,
                    caller_attributes: caller_attrs,
                    callee_attributes: callee_attrs,
                });
            }
            op_idx += 1;
        }
    }

    // Identify root operations: those never referenced as a suboperation target,
    // or explicitly marked with root: true.
    let mut called_op_indices: BTreeSet<usize> = BTreeSet::new();
    for compiled_op in &operations {
        for subop in &compiled_op.suboperations {
            called_op_indices.insert(subop.to_operation_idx);
        }
    }

    let mut root_operation_indices: Vec<usize> = Vec::new();
    let mut global_op_idx = 0;
    for svc in &config.services {
        for op in &svc.operations {
            let is_called = called_op_indices.contains(&global_op_idx);
            let is_explicit_root = op.root == Some(true);
            if is_explicit_root || !is_called {
                root_operation_indices.push(global_op_idx);
            }
            global_op_idx += 1;
        }
    }

    Ok(CompiledTopology {
        services: compiled_services,
        operations,
        root_operation_indices,
    })
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
    let mut seen_keys = BTreeSet::new();
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

/// Combine edge-level attributes (priority) with operation-type attributes.
///
/// Edge attributes come first. Operation-type attributes whose keys are
/// already present in edge attributes are dropped, so edge-level overrides win.
fn combine_attributes(
    edge_attrs: &[AttributeConfig],
    op_attrs: &[AttributeConfig],
) -> Vec<AttributeConfig> {
    if op_attrs.is_empty() {
        return edge_attrs.to_vec();
    }
    let mut combined = edge_attrs.to_vec();
    let edge_keys: BTreeSet<&str> = edge_attrs.iter().map(|a| a.key.as_str()).collect();
    for attr in op_attrs {
        if !edge_keys.contains(attr.key.as_str()) {
            combined.push(attr.clone());
        }
    }
    combined
}

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

impl<'a> crate::Generator<'a> for OpentelemetryTraces {
    type Output = trace::v1::ExportTraceServiceRequest;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: Rng + ?Sized,
    {
        let topo = &self.topology;

        // Pick a root operation (uniform random).
        let root_op_idx = *topo
            .root_operation_indices
            .choose(rng)
            .ok_or_else(|| Error::Validation("no root operations".into()))?;

        let trace_id: [u8; 16] = rng.random();

        let service_spans = walk_topology(
            rng,
            topo,
            root_op_idx,
            trace_id,
            self.error_rate,
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

/// Walk the compiled topology starting from a root operation, producing spans
/// for each service visited. Suboperations are executed sequentially.
fn walk_topology<R: Rng + ?Sized>(
    rng: &mut R,
    topo: &CompiledTopology,
    root_op_idx: usize,
    trace_id: [u8; 16],
    error_rate: f32,
    str_pool: &strings::RandomStringPool,
) -> Vec<ServiceSpan> {
    let root_op = &topo.operations[root_op_idx];

    // Create root span (no parent).
    let root_span = make_span(
        rng,
        &SpanParams {
            trace_id,
            operation: &root_op.span_name,
            span_kind: CALLEE_SPAN_KIND,
            parent: None,
            edge_attributes: &root_op.attributes,
            error_rate,
            str_pool,
            earliest_start: None,
        },
    );

    let mut service_spans: Vec<ServiceSpan> = Vec::with_capacity(32);
    service_spans.push(ServiceSpan {
        service_idx: root_op.service_idx,
        span: root_span.clone(),
    });

    // Work stack: (operation_idx, parent_span, depth, current_trace_id).
    let mut work: Vec<(usize, v1::Span, usize, [u8; 16])> =
        vec![(root_op_idx, root_span, 0, trace_id)];

    while let Some((op_idx, parent_span, depth, current_trace_id)) = work.pop() {
        if depth >= MAX_TRACE_DEPTH {
            continue;
        }

        let operation = &topo.operations[op_idx];

        // Execute suboperations sequentially. A cursor tracks the earliest
        // allowed start time so sibling spans don't overlap.
        let mut cursor = parent_span.start_time_unix_nano;

        for subop in &operation.suboperations {
            // Probabilistic execution based on rate.
            if rng.random::<f32>() >= subop.rate {
                continue;
            }

            // Determine how many times to repeat this suboperation.
            let repeat_count = if subop.max_repeat <= 1 {
                1
            } else {
                rng.random_range(1..=subop.max_repeat)
            };

            let callee_op = &topo.operations[subop.to_operation_idx];
            let callee_svc = &topo.services[callee_op.service_idx];

            for _ in 0..repeat_count {
                // Create CLIENT span on the calling service (child of parent).
                // Always uses the caller's trace ID.
                let client_span = make_span(
                    rng,
                    &SpanParams {
                        trace_id: current_trace_id,
                        operation: &callee_op.span_name,
                        span_kind: CALLER_SPAN_KIND,
                        parent: Some(&parent_span),
                        edge_attributes: &subop.caller_attributes,
                        error_rate,
                        str_pool,
                        earliest_start: Some(cursor),
                    },
                );
                cursor = client_span.end_time_unix_nano;

                service_spans.push(ServiceSpan {
                    service_idx: operation.service_idx,
                    span: client_span.clone(),
                });

                // Database services don't run their own tracing — only the
                // client span on the caller is emitted.
                if callee_svc.service_type == Some(ServiceType::Database) {
                    continue;
                }

                // HTTP/gRPC service entries start a new trace; plain services
                // inherit the caller's trace ID.
                let callee_trace_id = match callee_svc.service_type {
                    Some(ServiceType::Http | ServiceType::Grpc) => rng.random(),
                    _ => current_trace_id,
                };

                // Create SERVER span on the callee service (child of client).
                let server_span = make_span(
                    rng,
                    &SpanParams {
                        trace_id: callee_trace_id,
                        operation: &callee_op.span_name,
                        span_kind: CALLEE_SPAN_KIND,
                        parent: Some(&client_span),
                        edge_attributes: &subop.callee_attributes,
                        error_rate,
                        str_pool,
                        earliest_start: None,
                    },
                );

                service_spans.push(ServiceSpan {
                    service_idx: callee_op.service_idx,
                    span: server_span.clone(),
                });

                work.push((
                    subop.to_operation_idx,
                    server_span,
                    depth + 1,
                    callee_trace_id,
                ));
            }
        }
    }

    service_spans
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

impl crate::Serialize for OpentelemetryTraces {
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
    use std::collections::{BTreeSet, HashSet};

    use super::*;
    use crate::Serialize;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    /// A minimal but representative topology for testing.
    ///
    /// Linear chain: api-gateway → user-service → cache → postgres,
    /// using suboperations for the call graph.
    fn test_config() -> Config {
        Config {
            services: vec![
                ServiceConfig {
                    name: "api-gateway".into(),
                    service_type: Some(ServiceType::Http),
                    grpc: None,
                    database: None,
                    operations: vec![OperationConfig {
                        id: "get-users".into(),
                        span_name: None,
                        method: Some("GET".into()),
                        route: Some("/users/{id}".into()),
                        table: None,
                        query: None,
                        suboperations: vec![SuboperationConfig {
                            to: "user-service/get-user".into(),
                            rate: 1.0,
                            max_repeat: 1,
                            attributes: vec![],
                        }],
                        attributes: vec![],
                        root: None,
                    }],
                    resource_attributes: vec![AttributeConfig {
                        key: "deployment.environment".into(),
                        value: AttributeValueConfig::Static("production".into()),
                    }],
                    scope_name: Some("com.example.gateway".into()),
                    scope_version: "1.0.0".into(),
                },
                ServiceConfig {
                    name: "user-service".into(),
                    service_type: Some(ServiceType::Grpc),
                    grpc: Some(GrpcServiceConfig {
                        service: "UserService".into(),
                    }),
                    database: None,
                    operations: vec![OperationConfig {
                        id: "get-user".into(),
                        span_name: None,
                        method: Some("GetUser".into()),
                        route: None,
                        table: None,
                        query: None,
                        suboperations: vec![SuboperationConfig {
                            to: "cache/lookup".into(),
                            rate: 1.0,
                            max_repeat: 1,
                            attributes: vec![],
                        }],
                        attributes: vec![],
                        root: None,
                    }],
                    resource_attributes: vec![],
                    scope_name: Some("com.example.users".into()),
                    scope_version: "1.0.0".into(),
                },
                ServiceConfig {
                    name: "cache".into(),
                    service_type: None,
                    grpc: None,
                    database: None,
                    operations: vec![OperationConfig {
                        id: "lookup".into(),
                        span_name: Some("cache.lookup".into()),
                        method: None,
                        route: None,
                        table: None,
                        query: None,
                        suboperations: vec![SuboperationConfig {
                            to: "postgres/select-users".into(),
                            rate: 1.0,
                            max_repeat: 1,
                            attributes: vec![],
                        }],
                        attributes: vec![],
                        root: None,
                    }],
                    resource_attributes: vec![],
                    scope_name: None,
                    scope_version: "1.0.0".into(),
                },
                ServiceConfig {
                    name: "postgres".into(),
                    service_type: Some(ServiceType::Database),
                    grpc: None,
                    database: Some(DatabaseServiceConfig {
                        system: "postgresql".into(),
                        name: Some("mydb".into()),
                    }),
                    operations: vec![OperationConfig {
                        id: "select-users".into(),
                        span_name: None,
                        method: None,
                        route: None,
                        table: Some("users".into()),
                        query: Some("SELECT * FROM users WHERE id = $1".into()),
                        suboperations: vec![],
                        attributes: vec![],
                        root: None,
                    }],
                    resource_attributes: vec![],
                    scope_name: None,
                    scope_version: "1.0.0".into(),
                },
            ],
            error_rate: 0.02,
        }
    }

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes in 0u16..=u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = OpentelemetryTraces::with_config(&test_config(), &mut rng)
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
            let mut generator = OpentelemetryTraces::with_config(&test_config(), &mut rng)
                .expect("valid config");

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            generator.to_bytes(&mut rng, max_bytes, &mut bytes)
                .expect("failed to serialize");

            if !bytes.is_empty() {
                ExportTraceServiceRequest::decode(bytes.as_slice())
                    .expect("failed to decode protobuf");
            }
        }
    }

    #[test]
    fn deterministic_output() {
        let config = test_config();
        let max_bytes = 65536;

        let mut rng1 = SmallRng::seed_from_u64(42);
        let mut generator1 = OpentelemetryTraces::with_config(&config, &mut rng1).unwrap();
        let mut bytes1 = Vec::new();
        generator1
            .to_bytes(&mut rng1, max_bytes, &mut bytes1)
            .unwrap();

        let mut rng2 = SmallRng::seed_from_u64(42);
        let mut generator2 = OpentelemetryTraces::with_config(&config, &mut rng2).unwrap();
        let mut bytes2 = Vec::new();
        generator2
            .to_bytes(&mut rng2, max_bytes, &mut bytes2)
            .unwrap();

        assert_eq!(bytes1, bytes2, "same seed must produce identical output");
    }

    /// Helper: plain service with a single operation and optional suboperations.
    fn simple_svc(
        name: &str,
        op_id: &str,
        span_name: &str,
        subops: Vec<SuboperationConfig>,
    ) -> ServiceConfig {
        ServiceConfig {
            name: name.into(),
            service_type: None,
            grpc: None,
            database: None,
            operations: vec![OperationConfig {
                id: op_id.into(),
                span_name: Some(span_name.into()),
                method: None,
                route: None,
                table: None,
                query: None,
                suboperations: subops,
                attributes: vec![],
                root: None,
            }],
            resource_attributes: vec![],
            scope_name: None,
            scope_version: "1.0.0".into(),
        }
    }

    /// Helper: a simple suboperation with rate 1.0.
    fn simple_subop(to: &str) -> SuboperationConfig {
        SuboperationConfig {
            to: to.into(),
            rate: 1.0,
            max_repeat: 1,
            attributes: vec![],
        }
    }

    #[test]
    fn config_validation_empty_services() {
        let config = Config {
            services: vec![],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_no_suboperations() {
        let config = Config {
            services: vec![simple_svc("svc", "op", "op", vec![])],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_unknown_service_in_subop() {
        let config = Config {
            services: vec![simple_svc(
                "svc",
                "op",
                "op",
                vec![simple_subop("nonexistent/op")],
            )],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_unknown_operation_in_subop() {
        let config = Config {
            services: vec![
                simple_svc("a", "op", "op", vec![simple_subop("b/nonexistent")]),
                simple_svc("b", "op", "op", vec![]),
            ],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_invalid_operation_id() {
        let config = Config {
            services: vec![ServiceConfig {
                name: "svc".into(),
                service_type: None,
                grpc: None,
                database: None,
                operations: vec![OperationConfig {
                    id: "GetUsers".into(),
                    span_name: Some("GET /users".into()),
                    method: None,
                    route: None,
                    table: None,
                    query: None,
                    suboperations: vec![],
                    attributes: vec![],
                    root: None,
                }],
                resource_attributes: vec![],
                scope_name: None,
                scope_version: "1.0.0".into(),
            }],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_duplicate_operation_id() {
        let config = Config {
            services: vec![ServiceConfig {
                name: "svc".into(),
                service_type: None,
                grpc: None,
                database: None,
                operations: vec![
                    OperationConfig {
                        id: "op".into(),
                        span_name: Some("first".into()),
                        method: None,
                        route: None,
                        table: None,
                        query: None,
                        suboperations: vec![],
                        attributes: vec![],
                        root: None,
                    },
                    OperationConfig {
                        id: "op".into(),
                        span_name: Some("second".into()),
                        method: None,
                        route: None,
                        table: None,
                        query: None,
                        suboperations: vec![],
                        attributes: vec![],
                        root: None,
                    },
                ],
                resource_attributes: vec![],
                scope_name: None,
                scope_version: "1.0.0".into(),
            }],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_missing_slash_in_subop_ref() {
        let config = Config {
            services: vec![simple_svc(
                "svc",
                "op",
                "op",
                vec![SuboperationConfig {
                    to: "svc-op".into(),
                    rate: 1.0,
                    max_repeat: 1,
                    attributes: vec![],
                }],
            )],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_invalid_subop_rate() {
        let config = Config {
            services: vec![
                simple_svc(
                    "a",
                    "op",
                    "a.op",
                    vec![SuboperationConfig {
                        to: "b/op".into(),
                        rate: 1.5,
                        max_repeat: 1,
                        attributes: vec![],
                    }],
                ),
                simple_svc("b", "op", "b.op", vec![]),
            ],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_no_roots() {
        // Cyclic graph where all operations are suboperation targets.
        let config = Config {
            services: vec![
                simple_svc("a", "op", "a.op", vec![simple_subop("b/op")]),
                simple_svc("b", "op", "b.op", vec![simple_subop("a/op")]),
            ],
            error_rate: 0.01,
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_explicit_root() {
        // Cyclic graph but with an explicit root.
        let config = Config {
            services: vec![
                ServiceConfig {
                    name: "a".into(),
                    service_type: None,
                    grpc: None,
                    database: None,
                    operations: vec![OperationConfig {
                        id: "op".into(),
                        span_name: Some("a.op".into()),
                        method: None,
                        route: None,
                        table: None,
                        query: None,
                        suboperations: vec![simple_subop("b/op")],
                        attributes: vec![],
                        root: Some(true),
                    }],
                    resource_attributes: vec![],
                    scope_name: None,
                    scope_version: "1.0.0".into(),
                },
                simple_svc("b", "op", "b.op", vec![simple_subop("a/op")]),
            ],
            error_rate: 0.01,
        };
        assert!(config.valid().is_ok());
    }

    #[test]
    fn spans_have_correct_service_names() {
        let config = test_config();
        let mut rng = SmallRng::seed_from_u64(99);
        let mut generator = OpentelemetryTraces::with_config(&config, &mut rng).unwrap();

        let mut bytes = Vec::new();
        generator.to_bytes(&mut rng, 65536, &mut bytes).unwrap();

        let request = ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap();

        let expected_names: HashSet<&str> = ["api-gateway", "user-service", "cache"].into();
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
        let generator = OpentelemetryTraces::with_config(&config, &mut rng).unwrap();

        let request = generator.generate(&mut rng).unwrap();

        let mut all_span_ids: HashSet<Vec<u8>> = HashSet::new();
        let mut all_spans: Vec<&v1::Span> = Vec::new();
        let mut trace_ids: HashSet<Vec<u8>> = HashSet::new();

        for rs in &request.resource_spans {
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    all_span_ids.insert(span.span_id.clone());
                    all_spans.push(span);
                    trace_ids.insert(span.trace_id.clone());
                }
            }
        }

        // The test topology has HTTP → gRPC → plain → database. Entering the
        // gRPC service starts a new trace, so we expect exactly 2 trace IDs.
        assert_eq!(
            trace_ids.len(),
            2,
            "expected 2 trace IDs (one per HTTP/gRPC service entry), got {trace_ids:?}"
        );

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
        let config = test_config();
        let mut rng = SmallRng::seed_from_u64(77);
        let generator = OpentelemetryTraces::with_config(&config, &mut rng).unwrap();

        let request = generator.generate(&mut rng).unwrap();

        let service_names: BTreeSet<String> = request
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
            service_names.contains("cache"),
            "trace must include cache, got: {service_names:?}"
        );
        // Database services don't emit server spans, so postgres should not
        // appear in the resource spans.
        assert!(
            !service_names.contains("postgres"),
            "database service should not have server spans, got: {service_names:?}"
        );
    }

    #[test]
    fn subop_rate_zero_never_fires() {
        let config = Config {
            services: vec![
                simple_svc(
                    "a",
                    "op",
                    "a.op",
                    vec![SuboperationConfig {
                        to: "b/op".into(),
                        rate: 0.0,
                        max_repeat: 1,
                        attributes: vec![],
                    }],
                ),
                simple_svc("b", "op", "b.op", vec![]),
            ],
            error_rate: 0.0,
        };

        let mut rng = SmallRng::seed_from_u64(42);
        let generator = OpentelemetryTraces::with_config(&config, &mut rng).unwrap();

        for _ in 0..100 {
            let request = generator.generate(&mut rng).unwrap();
            let service_names: BTreeSet<String> = request
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
                !service_names.contains("b"),
                "rate=0.0 should never fire, but found service 'b'"
            );
        }
    }

    #[test]
    fn subop_rate_one_always_fires() {
        let config = Config {
            services: vec![
                simple_svc("a", "op", "a.op", vec![simple_subop("b/op")]),
                simple_svc("b", "op", "b.op", vec![]),
            ],
            error_rate: 0.0,
        };

        let mut rng = SmallRng::seed_from_u64(42);
        let generator = OpentelemetryTraces::with_config(&config, &mut rng).unwrap();

        for _ in 0..100 {
            let request = generator.generate(&mut rng).unwrap();
            let service_names: BTreeSet<String> = request
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
                service_names.contains("b"),
                "rate=1.0 should always fire, but service 'b' was missing"
            );
        }
    }

    #[test]
    fn suboperations_are_sequential() {
        // Two suboperations from the same parent: their client spans
        // should not overlap.
        let config = Config {
            services: vec![
                simple_svc(
                    "a",
                    "op",
                    "a.op",
                    vec![simple_subop("b/op"), simple_subop("c/op")],
                ),
                simple_svc("b", "op", "b.op", vec![]),
                simple_svc("c", "op", "c.op", vec![]),
            ],
            error_rate: 0.0,
        };

        let mut rng = SmallRng::seed_from_u64(42);
        let generator = OpentelemetryTraces::with_config(&config, &mut rng).unwrap();

        for _ in 0..50 {
            let request = generator.generate(&mut rng).unwrap();

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
        let json = r#"{"key": "http.method", "value": "GET"}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(attr.key, "http.method");
        assert_eq!(attr.value, AttributeValueConfig::Static("GET".into()));

        let json = r#"{"key": "http.status_code", "value": 200}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(attr.key, "http.status_code");
        assert_eq!(attr.value, AttributeValueConfig::StaticInt(200));

        let json = r#"{"key": "env", "value": {"static": "production"}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::Static("production".into())
        );

        let json = r#"{"key": "region", "value": {"dictionary": "cloud_regions"}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::Dictionary(DictionaryName::CloudRegions)
        );

        let json = r#"{"key": "req_id", "value": {"random_string": {"length": 16}}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::RandomString { length: 16 }
        );

        let json = r#"{"key": "color", "value": {"one_of": ["red", "green", "blue"]}}"#;
        let attr: AttributeConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.value,
            AttributeValueConfig::OneOf(vec!["red".into(), "green".into(), "blue".into()])
        );
    }
}
