//! OpenTelemetry OTLP log payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

// Alright, to summarize this payload generator's understanding of the
// data-model described above:
//
// The central concern is a `LogRecord`. That has:
//
// * `time_unit_nano`: time the event occurred
// * `observed_time_unix_nano`: time the event was observed by the collection system
// * `severity_number`: severity (INFO etc) defined from an enum of options
// * `severity_text`: same, but text and optional so we do not set this
// * `body`: the actual content of the log record
// * `attributes`: key/value tag pairs
// * `dropped_attributes_count`: always 0 in this implementation
// * `flags`: optional and unset by this implementation
// * `trace-id`: the unique identifier of a trace and all logs from a trace share the same id
// * `span-id`: the unique id for a span in a trace, optional but if set
//    trace-id must be set. Unused in this implementation.
// * `event_name`: optional, unset by this implementation
//
// The `LogRecord` is stored inside `ScopeLogs` which has its own
// InstrumentationScope similarly to how metrics works, and a `schema_url` which
// we leave unset in this implementation. `ScopeLogs` are stored inside
// `ResourceLogs` which again has a `schema_url` we do not set and an optional
// `Resource` which is like to the metrics Resource.
//
// A 'context' in this implementation is understood as the product of attributes
// per resource, scope, log records but NOT trace or span IDs. We do however
// expose a trace_cardinality configuration option to generate a limited pool of
// trace-ids that will be assigned to `LogRecord` instances but DO NOT count for
// context cardinality.

mod templates;

use self::templates::{Pool, ResourceTemplateGenerator};
use crate::{
    Error, SizedGenerator,
    common::{config::ConfRange, strings},
    opentelemetry::common::templates::PoolError,
};
use bytes::BytesMut;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest, logs::v1::ResourceLogs,
};
use prost::Message;
use rand::Rng;
use serde::Deserialize;
use std::{cell::RefCell, io::Write, rc::Rc};

// smallest useful protobuf, determined by experimentation and enforced in
// smallest_protobuf test
const SMALLEST_PROTOBUF: usize = 12;

/// Time increment per tick in nanoseconds (1 millisecond)
const TIME_INCREMENT_NANOS: u64 = 1_000_000;

/// Configure the OpenTelemetry log contexts
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Contexts {
    /// The range of contexts that will be generated.
    pub total_contexts: ConfRange<u32>,

    /// The range of attributes for resources.
    pub attributes_per_resource: ConfRange<u8>,
    /// The range of scopes that will be generated per resource.
    pub scopes_per_resource: ConfRange<u8>,
    /// The range of attributes for each scope.
    pub attributes_per_scope: ConfRange<u8>,
    /// The range of logs that will be generated per scope.
    pub logs_per_scope: ConfRange<u8>,
    /// The range of attributes for each log record.
    pub attributes_per_log: ConfRange<u8>,
}

impl Default for Contexts {
    fn default() -> Self {
        Self {
            total_contexts: ConfRange::Constant(100),
            attributes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            scopes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_scope: ConfRange::Constant(0),
            logs_per_scope: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_log: ConfRange::Inclusive { min: 0, max: 10 },
        }
    }
}

/// Defines log severity levels
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct SeverityWeights {
    /// Relative weight for TRACE severity
    pub trace: u8,
    /// Relative weight for DEBUG severity
    pub debug: u8,
    /// Relative weight for INFO severity
    pub info: u8,
    /// Relative weight for WARN severity
    pub warn: u8,
    /// Relative weight for ERROR severity
    pub error: u8,
    /// Relative weight for FATAL severity
    pub fatal: u8,
}

impl Default for SeverityWeights {
    fn default() -> Self {
        Self {
            trace: 5,
            debug: 10,
            info: 50,
            warn: 20,
            error: 10,
            fatal: 5,
        }
    }
}

/// Configure the OpenTelemetry log payload.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Defines the relative probability of each severity level
    pub severity_weights: SeverityWeights,
    /// Define the contexts available when generating logs
    pub contexts: Contexts,
    /// Number of unique trace IDs to generate. 0 means no trace context.
    pub trace_cardinality: ConfRange<u32>,
    /// The range of body sizes (in words) for log messages
    pub body_size: ConfRange<u16>,
}

impl Config {
    /// Determine whether the passed configuration obeys validation criteria.
    ///
    /// # Errors
    /// Function will error if the configuration is invalid
    #[allow(clippy::too_many_lines)]
    pub fn valid(&self) -> Result<(), String> {
        // Validate severity weights - at least one must be non-zero
        if self.severity_weights.trace == 0
            && self.severity_weights.debug == 0
            && self.severity_weights.info == 0
            && self.severity_weights.warn == 0
            && self.severity_weights.error == 0
            && self.severity_weights.fatal == 0
        {
            return Err("At least one severity weight must be non-zero".to_string());
        }

        // Validate total_contexts - we need at least one context
        match self.contexts.total_contexts {
            ConfRange::Constant(0) => return Err("total_contexts cannot be zero".to_string()),
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("total_contexts minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err("total_contexts minimum cannot be greater than maximum".to_string());
                }
            }
        }

        // Validate scopes_per_resource
        match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => return Err("scopes_per_resource cannot be zero".to_string()),
            ConfRange::Constant(_) => (),
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

        // Validate logs_per_scope
        match self.contexts.logs_per_scope {
            ConfRange::Constant(0) => return Err("logs_per_scope cannot be zero".to_string()),
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("logs_per_scope minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err("logs_per_scope minimum cannot be greater than maximum".to_string());
                }
            }
        }

        // Validate attributes_per_resource
        match self.contexts.attributes_per_resource {
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min > max {
                    return Err(
                        "attributes_per_resource minimum cannot be greater than maximum"
                            .to_string(),
                    );
                }
            }
        }

        // Validate attributes_per_scope
        match self.contexts.attributes_per_scope {
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min > max {
                    return Err(
                        "attributes_per_scope minimum cannot be greater than maximum".to_string(),
                    );
                }
            }
        }

        // Validate attributes_per_log
        match self.contexts.attributes_per_log {
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min > max {
                    return Err(
                        "attributes_per_log minimum cannot be greater than maximum".to_string()
                    );
                }
            }
        }

        // Validate trace_cardinality
        match self.trace_cardinality {
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min > max {
                    return Err(
                        "trace_cardinality minimum cannot be greater than maximum".to_string()
                    );
                }
            }
        }

        // Validate body_size
        match self.body_size {
            ConfRange::Constant(0) => return Err("body_size cannot be zero".to_string()),
            ConfRange::Constant(_) => (),
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("body_size minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err("body_size minimum cannot be greater than maximum".to_string());
                }
            }
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            severity_weights: SeverityWeights::default(),
            contexts: Contexts::default(),
            trace_cardinality: ConfRange::Constant(0),
            body_size: ConfRange::Inclusive { min: 1, max: 16 },
        }
    }
}

#[derive(Debug, Clone)]
/// OTLP log payload
pub struct OpentelemetryLogs {
    pool: Pool,
    scratch: RefCell<BytesMut>,
    tick: u64,
    log_records_per_resource: u64,
    log_records_per_block: u64,
}

impl OpentelemetryLogs {
    /// Construct a new instance of `OpentelemetryLogs`
    ///
    /// # Errors
    /// Function will error if the configuration is invalid
    pub fn new<R>(config: Config, max_overhead_bytes: usize, rng: &mut R) -> Result<Self, Error>
    where
        R: rand::Rng + ?Sized,
    {
        config.valid().map_err(Error::Validation)?;

        let context_cap = config.contexts.total_contexts.sample(rng);
        let str_pool = Rc::new(strings::Pool::with_size(rng, 128_000));

        let trace_count = config.trace_cardinality.sample(rng) as usize;
        let trace_pool = Rc::new(templates::TraceIdPool::new(trace_count, rng));

        let rt_gen = ResourceTemplateGenerator::new(&config, &str_pool, &trace_pool, rng)?;

        Ok(Self {
            pool: Pool::new(context_cap, max_overhead_bytes, rt_gen),
            scratch: RefCell::new(BytesMut::with_capacity(4096)),
            tick: 0,
            log_records_per_resource: 0,
            log_records_per_block: 0,
        })
    }
}

impl<'a> SizedGenerator<'a> for OpentelemetryLogs {
    type Output = ResourceLogs;
    type Error = Error;

    fn generate<R>(&'a mut self, rng: &mut R, budget: &mut usize) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        self.tick += rng.random_range(1..=60);

        let mut tpl: ResourceLogs = match self.pool.fetch(rng, budget) {
            Ok(t) => t.to_owned(),
            Err(PoolError::EmptyChoice) => Err(PoolError::EmptyChoice)?,
            Err(e) => Err(e)?,
        };

        let mut data_points_count = 0;
        for scope_logs in &mut tpl.scope_logs {
            for log_record in &mut scope_logs.log_records {
                log_record.time_unix_nano = self.tick * TIME_INCREMENT_NANOS;
                data_points_count += 1;
            }
        }

        self.log_records_per_resource = data_points_count;

        Ok(tpl)
    }
}

impl crate::Serialize for OpentelemetryLogs {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        let mut request = ExportLogsServiceRequest {
            resource_logs: Vec::with_capacity(8),
        };

        let mut total_log_records = 0;

        while bytes_remaining >= SMALLEST_PROTOBUF {
            if let Ok(rl) = self.generate(&mut rng, &mut bytes_remaining) {
                total_log_records += self.log_records_per_resource;
                request.resource_logs.push(rl);

                let required_bytes = request.encoded_len();
                if required_bytes > max_bytes {
                    total_log_records -= self.log_records_per_resource;
                    drop(request.resource_logs.pop());
                    break;
                }
                bytes_remaining = max_bytes.saturating_sub(required_bytes);
            } else {
                break;
            }
        }

        let needed = request.encoded_len();
        {
            let mut buf = self.scratch.borrow_mut();
            buf.clear();
            let capacity = buf.capacity();
            let diff = capacity.saturating_sub(needed);
            if buf.capacity() < needed {
                buf.reserve(diff);
            }
            request.encode(&mut *buf)?;
            writer.write_all(&buf)?;
        }

        self.log_records_per_block = total_log_records;

        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.log_records_per_block)
    }
}

#[cfg(test)]
mod test {
    use super::{
        Config, Contexts, ExportLogsServiceRequest, OpentelemetryLogs, SMALLEST_PROTOBUF,
        SeverityWeights,
    };
    use crate::{Serialize, SizedGenerator, common::config::ConfRange};
    use proptest::prelude::*;
    use prost::Message;
    use rand::{SeedableRng, rngs::SmallRng};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let config = Config::default();
            let mut logs = OpentelemetryLogs::new(config, max_bytes, &mut rng)?;

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL LogRecords.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let config = Config::default();
            let mut logs = OpentelemetryLogs::new(config, max_bytes, &mut rng)?;

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            ExportLogsServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
        }
    }

    // Generation of logs must be deterministic
    proptest! {
        #[test]
        fn is_deterministic(
            seed: u64,
            total_contexts in 1..100_u32,
            steps in 1..10_u8,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    ..Default::default()
                },
                ..Default::default()
            };

            let mut b1 = budget;
            let mut rng1 = SmallRng::seed_from_u64(seed);
            let mut logs1 = OpentelemetryLogs::new(config, budget, &mut rng1)?;
            let mut b2 = budget;
            let mut rng2 = SmallRng::seed_from_u64(seed);
            let mut logs2 = OpentelemetryLogs::new(config, budget, &mut rng2)?;

            for _ in 0..steps {
                if let Ok(gen_1) = logs1.generate(&mut rng1, &mut b1) {
                    let gen_2 = logs2.generate(&mut rng2, &mut b2).expect("gen_2 was not Ok");
                    prop_assert_eq!(gen_1, gen_2);
                    prop_assert_eq!(b1, b2);
                } else {
                    break;
                }
            }
        }
    }

    #[test]
    fn config_validation() {
        // Valid configuration
        let valid = Config::default();
        assert!(valid.valid().is_ok());

        // Invalid: all severity weights are zero
        let zero_severities = Config {
            severity_weights: SeverityWeights {
                trace: 0,
                debug: 0,
                info: 0,
                warn: 0,
                error: 0,
                fatal: 0,
            },
            ..Default::default()
        };
        assert!(zero_severities.valid().is_err());

        // Invalid: zero total_contexts
        let zero_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(0),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(zero_contexts.valid().is_err());

        // Invalid: zero logs_per_scope
        let zero_logs = Config {
            contexts: Contexts {
                logs_per_scope: ConfRange::Constant(0),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(zero_logs.valid().is_err());

        // Invalid: zero body_size
        let zero_body = Config {
            body_size: ConfRange::Constant(0),
            ..Default::default()
        };
        assert!(zero_body.valid().is_err());
    }

    // Budget always decreases equivalent to the size of the returned value
    proptest! {
        #[test]
        fn generate_decrease_budget(
            seed: u64,
            total_contexts in 1..100_u32,
            steps in 1..10_u8,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    ..Default::default()
                },
                ..Default::default()
            };

            let mut budget = 10_000_000;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut logs = OpentelemetryLogs::new(config, budget, &mut rng)?;

            for _ in 0..steps {
                let prev = budget;
                match logs.generate(&mut rng, &mut budget) {
                    Ok(_rl) => {
                        // The pool manages its own budget tracking based on cached sizes
                        // Just verify that budget decreased
                        prop_assert!(budget < prev);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    // This test verifies:
    //
    // * Timestamps actually increase, are not all a constant value
    // * Each timestamp is a monotonic increase
    proptest! {
        #[test]
        fn timestamps_increase_monotonically(
            seed: u64,
            steps in 2..20_u8,
            budget in SMALLEST_PROTOBUF..2048_usize,
        ) {
            let config = Config::default();
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut logs = OpentelemetryLogs::new(config, budget, &mut rng)?;

            let mut timestamps = Vec::new();

            for _ in 0..steps {
                let mut step_budget = budget;
                if let Ok(resource_logs) = logs.generate(&mut rng, &mut step_budget) {
                    // All logs within a single generate() call have the same timestamp
                    // We just need to verify one
                    if let Some(scope_logs) = resource_logs.scope_logs.first() {
                        if let Some(log) = scope_logs.log_records.first() {
                            timestamps.push(log.time_unix_nano);
                        }
                    }
                } else {
                    break;
                }
            }

            if timestamps.len() > 1 {
                for i in 1..timestamps.len() {
                    prop_assert!(
                        timestamps[i] > timestamps[i-1],
                        "Timestamps did not increase monotonically: {} <= {}",
                        timestamps[i], timestamps[i-1]
                    );
                }
            }
        }
    }

    // The goal of the test is to experimentally determine the value of
    // SMALLEST_PROTOBUF. Anything that encode/decodes and obeys the semantics
    // of the OTLP spec is acceptable. It's possible that as the protobuf
    // definition changes the value demonstrated by this test and
    // SMALLEST_PROTOBUF will differ.
    #[test]
    fn smallest_protobuf() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest,
            common::v1::{AnyValue, any_value},
            logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        };

        let log_record = LogRecord {
            time_unix_nano: 0,
            observed_time_unix_nano: 0,
            severity_number: SeverityNumber::Info as i32,
            severity_text: String::new(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(String::new())),
            }),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            event_name: String::new(),
        };

        let scope_logs = ScopeLogs {
            scope: None,
            log_records: vec![log_record],
            schema_url: String::new(),
        };

        let resource_logs = ResourceLogs {
            resource: None,
            scope_logs: vec![scope_logs],
            schema_url: String::new(),
        };

        // The most minimal request we care to support, just one single log record
        let minimal_request = ExportLogsServiceRequest {
            resource_logs: vec![resource_logs],
        };

        let encoded_size = minimal_request.encoded_len();

        assert!(
            encoded_size == SMALLEST_PROTOBUF,
            "Minimal useful request size ({encoded_size}) should be == SMALLEST_PROTOBUF ({SMALLEST_PROTOBUF})"
        );
    }
}
