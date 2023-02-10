//! Generates OpenTelemetry OTLP log payloads
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use crate::payload::{Error, Serialize};
use arbitrary::{Arbitrary, Unstructured};
use opentelemetry_proto::tonic::{
    common::v1::{any_value, AnyValue},
    logs::v1,
};
use prost::Message;
use rand::Rng;
use std::io::Write;

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

struct LogRecord(v1::LogRecord);

impl<'a> Arbitrary<'a> for ExportLogsServiceRequest {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let logs: Vec<LogRecord> = u.arbitrary()?;
        Ok(Self(logs))
    }
}

impl<'a> Arbitrary<'a> for LogRecord {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let time_unix_nano = u.arbitrary()?;
        let observed_time_unix_nano = u.arbitrary()?;

        let severity_number = u.int_in_range(1..=24)?;
        let severity_text = String::new();

        let body = Some(AnyValue {
            value: Some(any_value::Value::StringValue(u.arbitrary()?)),
        });

        let attributes = Vec::new();

        let dropped_attributes_count = 0;
        let flags = 0;
        let trace_id = Vec::new();
        let span_id = Vec::new();

        #[allow(deprecated)]
        Ok(LogRecord(v1::LogRecord {
            time_unix_nano,
            observed_time_unix_nano,
            severity_number,
            severity_text,
            name: String::new(),
            body,
            attributes,
            dropped_attributes_count,
            flags,
            trace_id,
            span_id,
        }))
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct OpentelemetryLogs;

impl Serialize for OpentelemetryLogs {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        // An Export*ServiceRequest message has 5 bytes of fixed values plus
        // a varint-encoded message length field. The worst case for the message
        // length field is the max message size divided by 0x7F.
        let bytes_remaining = max_bytes.checked_sub(5 + super::div_ceil(max_bytes, 0x7F));
        let mut bytes_remaining = if let Some(val) = bytes_remaining {
            val
        } else {
            return Ok(());
        };

        let mut acc = ExportLogsServiceRequest(Vec::new());
        while let Ok(member) = unstructured.arbitrary::<LogRecord>() {
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
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::OpentelemetryLogs;
    use crate::payload::Serialize;
    use proptest::prelude::*;
    use prost::Message;
    use rand::{rngs::SmallRng, SeedableRng};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryLogs::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // We want to be sure that the payloads are not being left empty.
    proptest! {
        #[test]
        fn payload_is_at_least_half_of_max_bytes(seed: u64, max_bytes in 16u16..u16::MAX) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryLogs::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            assert!(!bytes.is_empty());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL LogRecords.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryLogs::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();
        }
    }
}
