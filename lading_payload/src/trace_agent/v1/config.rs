//! Configuration and validation for v1.0 trace payloads.
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use super::AttributeValue;
use crate::Error;

/// Controls the time range used for generated trace spans.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum TimestampMode {
    /// Capture wall-clock time once when the payload generator is constructed.
    Realtime,
    /// Use a configured Unix timestamp, in nanoseconds, as the upper bound.
    Fixed {
        /// Unix timestamp in nanoseconds used as the latest possible span end.
        anchor_unix_nanos: u64,
    },
}

impl Default for TimestampMode {
    fn default() -> Self {
        Self::Fixed {
            // 2025-01-01T00:00:00Z. Keeping the default fixed preserves the same-seed
            // byte-identical contract for existing configurations.
            anchor_unix_nanos: 1_735_689_600_000_000_000,
        }
    }
}

/// A statically declared attribute value.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(untagged)]
pub enum ConfigAttributeValue {
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// A double-precision float.
    Double(f64),
    /// A string.
    String(String),
}

impl ConfigAttributeValue {
    pub(super) fn to_attribute_value(&self) -> AttributeValue {
        match self {
            Self::Bool(v) => AttributeValue::Bool(*v),
            Self::Int(v) => AttributeValue::Int(*v),
            Self::Double(v) => AttributeValue::Double(*v),
            Self::String(v) => AttributeValue::String(v.clone()),
        }
    }
}

/// A call from one operation to an operation on another service.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct SubOperation {
    /// The called operation, as `service-name/operation-id`.
    pub to: String,
    /// Probability in `0.0..=1.0` that a generated trace follows this call. Defaults to `1.0`.
    #[serde(default = "default_rate")]
    pub rate: f64,
}

fn default_rate() -> f64 {
    1.0
}

/// An operation a service can perform, producing one span.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct Operation {
    /// Identifier used to reference this operation from a [`SubOperation`]. Unique within a
    /// service.
    pub id: String,
    /// Span operation name, for example `http.request`.
    pub name: String,
    /// Span resource, for example `GET /api/v1/products/{id}`.
    pub resource: String,
    /// Span type, for example `web`, `sql`, or `redis`. Empty by default.
    #[serde(default)]
    pub span_type: String,
    /// Span kind, following the OpenTelemetry enumeration. Zero, meaning unspecified, by default.
    #[serde(default)]
    pub kind: u32,
    /// Instrumentation component that produced the span. Empty by default.
    #[serde(default)]
    pub component: String,
    /// Attributes attached to every span this operation produces.
    #[serde(default)]
    pub attributes: FxHashMap<String, ConfigAttributeValue>,
    /// Operations this one calls, each producing a child span.
    #[serde(default)]
    pub suboperations: Vec<SubOperation>,
}

/// A service in the generated graph.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct Service {
    /// Service name, used verbatim as the span's service.
    pub name: String,
    /// Operations the service can perform.
    pub operations: Vec<Operation>,
}

/// Configuration for v1.0 trace payload generation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Time source for span timestamps.
    ///
    /// Fixed mode is deterministic and is the default. Realtime mode captures the wall clock once
    /// during generator construction, so a prebuilt cache remains internally consistent.
    pub timestamp_mode: TimestampMode,

    /// Probability in `0.0..=1.0` that a generated span is marked as an error. Defaults to `0.0`,
    /// meaning no span is ever marked as an error.
    pub error_rate: f64,

    /// Probability in `0.0..=1.0` that a generated span carries one link to another trace.
    /// Defaults to `0.0`, meaning no span carries a link.
    pub link_rate: f64,

    /// Probability in `0.0..=1.0` that a generated span carries one timestamped event.
    /// Defaults to `0.0`, meaning no span carries an event.
    pub event_rate: f64,

    /// Exact number of trace chunks packed into each payload.
    ///
    /// A block contains one payload. If it cannot fit, the block is rejected; chunks are never
    /// added or removed to fill a byte budget.
    ///
    /// Defaults to `1`.
    pub chunks_per_payload: usize,

    /// Tracer language name reported in the payload, for example `go`. Empty by default.
    pub language_name: String,

    /// Tracer language runtime version reported in the payload. Empty by default.
    pub language_version: String,

    /// Tracer library version reported in the payload. Empty by default.
    pub tracer_version: String,

    /// Environment reported in the payload, for example `production`. Empty by default.
    pub env: String,

    /// Version of the traced application reported in the payload. Empty by default.
    pub app_version: String,

    /// Sampling priority set on every chunk.
    ///
    /// Defaults to `1`, the tracer's automatic keep decision, ensuring the agent keeps the chunk.
    pub priority: i32,

    /// Services in the graph. The operations of the **first** service are the entry points: every
    /// generated trace starts at one of them.
    pub services: Vec<Service>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            timestamp_mode: TimestampMode::default(),
            error_rate: 0.0,
            link_rate: 0.0,
            event_rate: 0.0,
            chunks_per_payload: 1,
            language_name: String::new(),
            language_version: String::new(),
            tracer_version: String::new(),
            env: String::new(),
            app_version: String::new(),
            priority: 1,
            services: Vec::new(),
        }
    }
}

impl Config {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if a rate falls outside `0.0..=1.0`, if there are no services or no
    /// operations to start from, if the same operation is declared more than once, if any span
    /// field the trace-agent's normalizer would rewrite is left empty, or if a suboperation
    /// references an operation that does not exist.
    pub fn valid(&self) -> Result<(), Error> {
        if let TimestampMode::Fixed { anchor_unix_nanos } = self.timestamp_mode {
            super::validate_timestamp_anchor(anchor_unix_nanos)?;
        }

        for (name, rate) in [
            ("error_rate", self.error_rate),
            ("link_rate", self.link_rate),
            ("event_rate", self.event_rate),
        ] {
            if !(0.0..=1.0).contains(&rate) {
                return Err(Error::Validation(format!(
                    "{name} must be between 0.0 and 1.0, got {rate}."
                )));
            }
        }

        if self.chunks_per_payload == 0 {
            return Err(Error::Validation(
                "chunks_per_payload must be at least 1.".to_string(),
            ));
        }

        if self.services.is_empty() {
            return Err(Error::Validation(
                "At least one service must be configured.".to_string(),
            ));
        }

        let mut operation_keys = FxHashSet::default();
        for service in &self.services {
            if service.name.is_empty() {
                return Err(Error::Validation(
                    "Every service must have a non-empty name.".to_string(),
                ));
            }

            for operation in &service.operations {
                // The trace-agent's normalizer requires both of these to be set.
                if operation.name.is_empty() || operation.resource.is_empty() {
                    return Err(Error::Validation(format!(
                        "Operation '{}/{}' must have a non-empty name and resource.",
                        service.name, operation.id
                    )));
                }

                let key = format!("{}/{}", service.name, operation.id);
                if !operation_keys.insert(key.clone()) {
                    return Err(Error::Validation(format!(
                        "Operation '{key}' is declared more than once."
                    )));
                }
            }
        }

        if self.services[0].operations.is_empty() {
            return Err(Error::Validation(format!(
                "The first service ('{}') declares the entry-point operations, so it must have at least one.",
                self.services[0].name
            )));
        }

        for service in &self.services {
            for operation in &service.operations {
                for suboperation in &operation.suboperations {
                    if !(0.0..=1.0).contains(&suboperation.rate) {
                        return Err(Error::Validation(format!(
                            "Suboperation rate for '{}' must be between 0.0 and 1.0, got {}.",
                            suboperation.to, suboperation.rate
                        )));
                    }

                    if !operation_keys.contains(&suboperation.to) {
                        return Err(Error::Validation(format!(
                            "Operation '{}/{}' references unknown suboperation '{}'.",
                            service.name, operation.id, suboperation.to
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}
