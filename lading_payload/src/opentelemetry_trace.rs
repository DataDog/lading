//! OpenTelemetry OTLP trace payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use crate::{Error, common::strings};
use opentelemetry_proto::tonic::trace::v1;
use prost::Message;
use rand::{Rng, distr::StandardUniform, prelude::Distribution};
use std::io::Write;

use super::Generator;

/// Wrapper to generate arbitrary OpenTelemetry [`ExportTraceServiceRequest`](opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest)
struct ExportTraceServiceRequest(Vec<Span>);

impl ExportTraceServiceRequest {
    fn into_prost_type(
        self,
    ) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: [v1::ResourceSpans {
                resource: None,
                scope_spans: [v1::ScopeSpans {
                    scope: None,
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

/// An OTLP span
#[derive(Debug)]
pub struct Span(v1::Span);

#[derive(Debug, Clone)]
/// OTLP trace payload
pub struct OpentelemetryTraces {
    str_pool: strings::Pool,
}

impl OpentelemetryTraces {
    /// Construct a new instance of `OpentelemetryTraces`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for OpentelemetryTraces {
    type Output = Span;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let trace_id = StandardUniform.sample_iter(&mut rng).take(16).collect();
        let span_id = StandardUniform.sample_iter(&mut rng).take(8).collect();

        // Some collectors may immediately drop old/future spans. Consider
        // constraining these to recent timestamps.
        let start_time_unix_nano: u64 = rng.random();
        // end time is expected to be greater than or equal to start time
        let end_time_unix_nano: u64 = rng.random_range(start_time_unix_nano..=u64::MAX);

        let name = self
            .str_pool
            .of_size_range(&mut rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;

        Ok(Span(v1::Span {
            trace_id,
            span_id,
            // https://www.w3.org/TR/trace-context/#tracestate-header
            trace_state: String::new(),
            // zeros: root span
            parent_span_id: vec![0; 8],
            name: String::from(name),
            kind: rng.random_range(0..=5),
            start_time_unix_nano,
            end_time_unix_nano,
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: None,
            flags: 0,
        }))
    }
}

impl crate::Serialize for OpentelemetryTraces {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // An Export*ServiceRequest message has 5 bytes of fixed values plus
        // a varint-encoded message length field. The worst case for the message
        // length field is the max message size divided by 0x7F.
        let bytes_remaining = max_bytes.checked_sub(5 + max_bytes.div_ceil(0x7F));
        let Some(mut bytes_remaining) = bytes_remaining else {
            return Ok(());
        };

        let mut acc = ExportTraceServiceRequest(Vec::new());
        loop {
            let member: Span = self.generate(&mut rng)?;
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
    use crate::Serialize;
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
            let traces = OpentelemetryTraces::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            prop_assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // We want to be sure that the payloads are not being left empty.
    proptest! {
        #[test]
        fn payload_is_at_least_half_of_max_bytes(seed: u64, max_bytes in 16u16..u16::MAX) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let traces = OpentelemetryTraces::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            prop_assert!(!bytes.is_empty());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL Spans.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let traces = OpentelemetryTraces::new(&mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
        }
    }
}
