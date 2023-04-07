//! Generates OpenTelemetry OTLP trace payloads
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use crate::payload::{Error, Serialize};
use opentelemetry_proto::tonic::trace::v1;
use prost::Message;
use rand::{distributions::Standard, prelude::Distribution, Rng};
use std::io::Write;

use super::{common::AsciiString, Generator};

/// Wrapper to generate arbitrary OpenTelemetry [`ExportTraceServiceRequest`](opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest)
struct ExportTraceServiceRequest(Vec<Span>);

impl ExportTraceServiceRequest {
    fn into_prost_type(
        self,
    ) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: [v1::ResourceSpans {
                resource: None,
                instrumentation_library_spans: [v1::InstrumentationLibrarySpans {
                    instrumentation_library: None,
                    spans: self.0.into_iter().map(|span| span.0).collect(),
                    schema_url: String::new(),
                }]
                .to_vec(),
                schema_url: String::new(),
            }]
            .to_vec(),
        }
    }
}
struct Span(v1::Span);

impl Distribution<Span> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Span
    where
        R: Rng + ?Sized,
    {
        let trace_id = Standard.sample_iter(&mut rng).take(16).collect();
        let span_id = Standard.sample_iter(&mut rng).take(8).collect();

        // Some collectors may immediately drop old/future spans. Consider
        // constraining these to recent timestamps.
        let start_time_unix_nano: u64 = rng.gen();
        // end time is expected to be greater than or equal to start time
        let end_time_unix_nano: u64 = rng.gen_range(start_time_unix_nano..=u64::MAX);

        Span(v1::Span {
            trace_id,
            span_id,
            // https://www.w3.org/TR/trace-context/#tracestate-header
            trace_state: String::new(),
            // zeros: root span
            parent_span_id: vec![0; 8],
            name: AsciiString::default().generate(rng).unwrap(),
            kind: rng.gen_range(0..=5),
            start_time_unix_nano,
            end_time_unix_nano,
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: None,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct OpentelemetryTraces;

impl Serialize for OpentelemetryTraces {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // An Export*ServiceRequest message has 5 bytes of fixed values plus
        // a varint-encoded message length field. The worst case for the message
        // length field is the max message size divided by 0x7F.
        let bytes_remaining = max_bytes.checked_sub(5 + super::div_ceil(max_bytes, 0x7F));
        let Some(mut bytes_remaining) = bytes_remaining else {
            return Ok(());
        };

        let mut acc = ExportTraceServiceRequest(Vec::new());
        loop {
            let member: Span = rng.gen();
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
    use super::OpentelemetryTraces;
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
            let traces = OpentelemetryTraces::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // We want to be sure that the payloads are not being left empty.
    proptest! {
        #[test]
        fn payload_is_at_least_half_of_max_bytes(seed: u64, max_bytes in 16u16..u16::MAX) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryTraces::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            assert!(!bytes.is_empty());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL Spans.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let traces = OpentelemetryTraces::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap();
        }
    }
}
