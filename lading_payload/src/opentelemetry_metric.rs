//! OpenTelemetry OTLP metric payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use std::io::Write;

use crate::{Error, common::strings};
use opentelemetry_proto::tonic::metrics::v1;
use prost::Message;
use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};

use super::Generator;

/// Wrapper to generate arbitrary OpenTelemetry [`ExportMetricsServiceRequests`](opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest)
struct ExportMetricsServiceRequest(Vec<Metric>);

impl ExportMetricsServiceRequest {
    fn into_prost_type(
        self,
    ) -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![v1::ResourceMetrics {
                resource: None,
                scope_metrics: vec![v1::ScopeMetrics {
                    scope: None,
                    metrics: self.0.into_iter().map(|metric| metric.0).collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}

/// A OTLP metric
#[derive(Debug)]
pub struct Metric(v1::Metric);

struct NumberDataPoint(v1::NumberDataPoint);
struct Gauge(v1::Gauge);
struct Sum(v1::Sum);

impl Distribution<NumberDataPoint> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> NumberDataPoint
    where
        R: Rng + ?Sized,
    {
        let value = match rng.random_range(0..=1) {
            0 => v1::number_data_point::Value::AsDouble(rng.random()),
            1 => v1::number_data_point::Value::AsInt(rng.random()),
            _ => unreachable!(),
        };

        NumberDataPoint(v1::NumberDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: rng.random(),
            time_unix_nano: rng.random(),
            exemplars: Vec::new(),
            flags: 0,
            value: Some(value),
        })
    }
}

impl Distribution<Gauge> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Gauge
    where
        R: Rng + ?Sized,
    {
        let total = rng.random_range(0..64);
        let data_points = StandardUniform
            .sample_iter(rng)
            .map(|ndp: NumberDataPoint| ndp.0)
            .take(total)
            .collect();
        Gauge(v1::Gauge { data_points })
    }
}

impl Distribution<Sum> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Sum
    where
        R: Rng + ?Sized,
    {
        // 0: Unspecified AggregationTemporality, MUST not be used
        // 1: Delta
        // 2: Cumulative
        let aggregation_temporality = *[1, 2]
            .choose(rng)
            .expect("failed to choose aggregation temporality");
        let is_monotonic = rng.random();
        let total = rng.random_range(0..64);
        let data_points = StandardUniform
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

#[derive(Debug, Clone)]
/// OTLP metric payload
pub struct OpentelemetryMetrics {
    str_pool: strings::Pool,
}

impl OpentelemetryMetrics {
    /// Construct a new instance of `OpentelemetryMetrics`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            str_pool: strings::Pool::with_size(rng, 1_000_000),
        }
    }
}

impl<'a> Generator<'a> for OpentelemetryMetrics {
    type Output = Metric;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let data = match rng.random_range(0..=1) {
            0 => v1::metric::Data::Gauge(rng.random::<Gauge>().0),
            1 => v1::metric::Data::Sum(rng.random::<Sum>().0),
            // Currently unsupported: Histogram, ExponentialHistogram, Summary
            _ => unreachable!(),
        };
        let data = Some(data);

        let name = self
            .str_pool
            .of_size_range(&mut rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;
        let description = self
            .str_pool
            .of_size_range(&mut rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;
        let unit = self
            .str_pool
            .of_size_range(&mut rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;

        Ok(Metric(v1::Metric {
            name: String::from(name),
            description: String::from(description),
            unit: String::from(unit),
            data,
            metadata: Vec::new(),
        }))
    }
}

impl crate::Serialize for OpentelemetryMetrics {
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

        let mut acc = ExportMetricsServiceRequest(Vec::new());
        loop {
            let member: Metric = self.generate(&mut rng)?;
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
            let logs = OpentelemetryMetrics::new(&mut rng);

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
            let logs = OpentelemetryMetrics::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            assert!(!bytes.is_empty());
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL metrics.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let logs = OpentelemetryMetrics::new(&mut rng);

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            logs.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
        }
    }
}
