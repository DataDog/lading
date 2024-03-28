//! OpenTelemetry OTLP log payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use crate::{block::SplitStrategy, common::strings, Error};
use opentelemetry_proto::tonic::{
    common::v1::{any_value, AnyValue},
    logs::v1,
};
use prost::Message;
use rand::Rng;
use std::io::Write;

use super::Generator;

/// Wrapper to generate arbitrary OpenTelemetry [`ExportLogsServiceRequests`](opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest)
struct ExportLogsServiceRequest(Vec<LogRecord>);

impl ExportLogsServiceRequest {
    fn into_prost_type(
        self,
    ) -> opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest {
        opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest {
            resource_logs: vec![v1::ResourceLogs {
                resource: None,
                instrumentation_library_logs: vec![v1::InstrumentationLibraryLogs {
                    instrumentation_library: None,
                    log_records: self.0.into_iter().map(|log| log.0).collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}

pub(crate) struct LogRecord(v1::LogRecord);

#[derive(Debug, Clone)]
/// OTLP log payload
pub struct OpentelemetryLogs {
    str_pool: strings::Pool,
}

impl OpentelemetryLogs {
    /// Construct a new instance of `OpentelemetryLogs`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for OpentelemetryLogs {
    type Output = LogRecord;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let body: String = String::from(
            self.str_pool
                .of_size_range(&mut rng, 1_u16..16_u16)
                .ok_or(Error::StringGenerate)?,
        );

        Ok(
            #[allow(deprecated)]
            LogRecord(v1::LogRecord {
                time_unix_nano: rng.gen(),
                observed_time_unix_nano: rng.gen(),
                severity_number: rng.gen_range(1..=24),
                severity_text: String::new(),
                name: String::new(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(body)),
                }),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: Vec::new(),
                span_id: Vec::new(),
            }),
        )
    }
}

impl crate::Serialize for OpentelemetryLogs {
    fn to_bytes<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<SplitStrategy, Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // An Export*ServiceRequest message has 5 bytes of fixed values plus
        // a varint-encoded message length field. The worst case for the message
        // length field is the max message size divided by 0x7F.
        let bytes_remaining = max_bytes.checked_sub(5 + super::div_ceil(max_bytes, 0x7F));
        let Some(mut bytes_remaining) = bytes_remaining else {
            return Ok(SplitStrategy::None);
        };

        let mut acc = ExportLogsServiceRequest(Vec::new());
        loop {
            let member: LogRecord = self.generate(&mut rng)?;
            // Note: this 2 is a guessed value for an unknown size factor.
            let len = member.0.encoded_len() + 2;
            match bytes_remaining.checked_sub(len) {
                Some(remainder) => {
                    acc.0.push(member);
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }

        let buf = acc.into_prost_type().encode_to_vec();
        writer.write_all(&buf)?;
        Ok(SplitStrategy::None)
    }
}

#[cfg(test)]
mod test {
    use super::OpentelemetryLogs;
    use crate::Serialize;
    use proptest::prelude::*;
    use prost::Message;
    use rand::{rngs::SmallRng, SeedableRng};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryLogs::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // We want to be sure that the payloads are not being left empty.
    proptest! {
        #[test]
        fn payload_is_at_least_half_of_max_bytes(seed: u64, max_bytes in 16u16..u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryLogs::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            assert!(!bytes.is_empty());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL LogRecords.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryLogs::new(&mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
        }
    }
}
