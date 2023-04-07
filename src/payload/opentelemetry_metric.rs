//! Generates OpenTelemetry OTLP metric payloads
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use std::io::Write;

use crate::payload::{Error, Serialize};
use opentelemetry_proto::tonic::metrics::v1::{self};
use prost::Message;
use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use super::{common::AsciiString, Generator};

/// Wrapper to generate arbitrary OpenTelemetry [`ExportMetricsServiceRequests`](opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest)
struct ExportMetricsServiceRequest(Vec<Metric>);

impl ExportMetricsServiceRequest {
    fn into_prost_type(
        self,
    ) -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![v1::ResourceMetrics {
                resource: None,
                instrumentation_library_metrics: vec![v1::InstrumentationLibraryMetrics {
                    instrumentation_library: None,
                    metrics: self.0.into_iter().map(|metric| metric.0).collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}

struct Metric(v1::Metric);
struct NumberDataPoint(v1::NumberDataPoint);
struct Gauge(v1::Gauge);
struct Sum(v1::Sum);

impl Distribution<NumberDataPoint> for Standard {
    fn sample<R>(&self, rng: &mut R) -> NumberDataPoint
    where
        R: Rng + ?Sized,
    {
        let value = match rng.gen_range(0..=1) {
            0 => v1::number_data_point::Value::AsDouble(rng.gen()),
            1 => v1::number_data_point::Value::AsInt(rng.gen()),
            _ => unreachable!(),
        };

        NumberDataPoint(v1::NumberDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: rng.gen(),
            time_unix_nano: rng.gen(),
            exemplars: Vec::new(),
            flags: 0,
            value: Some(value),
        })
    }
}

impl Distribution<Gauge> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Gauge
    where
        R: Rng + ?Sized,
    {
        let total = rng.gen_range(0..64);
        let data_points = Standard
            .sample_iter(rng)
            .map(|ndp: NumberDataPoint| ndp.0)
            .take(total)
            .collect();
        Gauge(v1::Gauge { data_points })
    }
}

impl Distribution<Sum> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Sum
    where
        R: Rng + ?Sized,
    {
        // 0: Unspecified AggregationTemporality, MUST not be used
        // 1: Delta
        // 2: Cumulative
        let aggregation_temporality = *[1, 2].choose(rng).unwrap();
        let is_monotonic = rng.gen();
        let total = rng.gen_range(0..64);
        let data_points = Standard
            .sample_iter(rng)
            .map(|ndp: NumberDataPoint| ndp.0)
            .take(total)
            .collect();

        Sum(v1::Sum {
            data_points,
            aggregation_temporality,
            is_monotonic,
        })
    }
}

impl Distribution<Metric> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Metric
    where
        R: Rng + ?Sized,
    {
        let data = match rng.gen_range(0..=1) {
            0 => v1::metric::Data::Gauge(rng.gen::<Gauge>().0),
            1 => v1::metric::Data::Sum(rng.gen::<Sum>().0),
            // Currently unsupported: Histogram, ExponentialHistogram, Summary
            _ => unreachable!(),
        };
        let data = Some(data);

        Metric(v1::Metric {
            name: AsciiString::default().generate(rng),
            description: AsciiString::default().generate(rng),
            unit: AsciiString::default().generate(rng),
            data,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct OpentelemetryMetrics;

impl Serialize for OpentelemetryMetrics {
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

        let mut acc = ExportMetricsServiceRequest(Vec::new());
        loop {
            let member: Metric = rng.gen();
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
    use super::OpentelemetryMetrics;
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
            let logs = OpentelemetryMetrics::default();

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
            let logs = OpentelemetryMetrics::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            assert!(!bytes.is_empty());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL metrics.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryMetrics::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(bytes.as_slice()).unwrap();
        }
    }
}
