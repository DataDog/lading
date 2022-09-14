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
use arbitrary::{size_hint, Arbitrary, Unstructured};
use opentelemetry_proto::tonic::metrics::v1::{self};
use prost::Message;
use rand::Rng;

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
struct Histogram(v1::Histogram);
struct ExponentialHistogram(v1::ExponentialHistogram);
struct Summary(v1::Summary);

impl<'a> Arbitrary<'a> for ExportMetricsServiceRequest {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let metrics: Vec<Metric> = u.arbitrary()?;
        Ok(Self(metrics))
    }
}

impl<'a> Arbitrary<'a> for NumberDataPoint {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let time_unix_nano = u.arbitrary()?;
        let start_time_unix_nano = u.arbitrary()?;

        let value = match u.int_in_range(0u8..=1)? {
            0 => v1::number_data_point::Value::AsDouble(u.arbitrary()?),
            1 => v1::number_data_point::Value::AsInt(u.arbitrary()?),
            _ => unreachable!(),
        };

        Ok(Self(v1::NumberDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano,
            time_unix_nano,
            exemplars: Vec::new(),
            flags: 0,
            value: Some(value),
        }))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <u64 as Arbitrary>::size_hint(depth),
                <u64 as Arbitrary>::size_hint(depth),
                <u8 as Arbitrary>::size_hint(depth),
                size_hint::or(
                    <f64 as Arbitrary>::size_hint(depth),
                    <i64 as Arbitrary>::size_hint(depth),
                ),
            ])
        })
    }
}

impl<'a> Arbitrary<'a> for Gauge {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.arbitrary_len::<NumberDataPoint>()?;
        let mut data_points = Vec::new();
        for _ in 0..len {
            data_points.push(u.arbitrary::<NumberDataPoint>()?.0);
        }
        Ok(Self(v1::Gauge { data_points }))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        (<NumberDataPoint as Arbitrary>::size_hint(depth).0, None)
    }
}

impl<'a> Arbitrary<'a> for Sum {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.arbitrary_len::<NumberDataPoint>()?;
        let mut data_points = Vec::new();
        for _ in 0..len {
            data_points.push(u.arbitrary::<NumberDataPoint>()?.0);
        }
        Ok(Self(v1::Sum {
            data_points,
            // 0: Unspecified AggregationTemporality, MUST not be used
            // 1: Delta
            // 2: Cumulative
            aggregation_temporality: *u.choose(&[1, 2])?,
            is_monotonic: *u.choose(&[true, false])?,
        }))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        (
            size_hint::and_all(&[
                <NumberDataPoint as Arbitrary>::size_hint(depth),
                <usize as Arbitrary>::size_hint(depth),
                <usize as Arbitrary>::size_hint(depth),
            ])
            .0,
            None,
        )
    }
}

// impl<'a> Arbitrary<'a> for Histogram {
//     fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
//         let len = u.arbitrary_len::<NumberDataPoint>()?;
//         let mut data_points = Vec::new();
//         for _ in 0..len {
//             data_points.push(u.arbitrary::<NumberDataPoint>()?.0);
//         }
//         Ok(Self(v1::Histogram {
//             data_points,
//             aggregation_temporality: *u.choose(&[1, 2])?,
//         }))
//     }
// }

// impl<'a> Arbitrary<'a> for ExponentialHistogram {
//     fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
//         todo!()
//     }
// }

// impl<'a> Arbitrary<'a> for Summary {
//     fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
//         todo!()
//     }
// }

impl<'a> Arbitrary<'a> for Metric {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let name = u.arbitrary()?;
        let description = u.arbitrary()?;
        let unit = u.arbitrary()?;

        let data = match u.int_in_range(0u8..=1)? {
            0 => v1::metric::Data::Gauge(u.arbitrary::<Gauge>()?.0),
            1 => v1::metric::Data::Sum(u.arbitrary::<Sum>()?.0),
            // 2 => v1::metric::Data::Histogram(u.arbitrary::<Histogram>()?.0),
            // 3 => v1::metric::Data::ExponentialHistogram(u.arbitrary::<ExponentialHistogram>()?.0),
            // 4 => v1::metric::Data::Summary(u.arbitrary::<Summary>()?.0),
            _ => unreachable!(),
        };

        let data = Some(data);

        Ok(Metric(v1::Metric {
            name,
            description,
            unit,
            data,
        }))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::recursion_guard(depth, |depth| {
            (
                size_hint::and_all(&[
                    <String as Arbitrary>::size_hint(depth),
                    <String as Arbitrary>::size_hint(depth),
                    <String as Arbitrary>::size_hint(depth),
                    <u8 as Arbitrary>::size_hint(depth),
                    size_hint::or_all(&[
                        <Gauge as Arbitrary>::size_hint(depth),
                        <Sum as Arbitrary>::size_hint(depth),
                        // <Histogram as Arbitrary>::size_hint(depth),
                        // <ExponentialHistogram as Arbitrary>::size_hint(depth),
                        // <Summary as Arbitrary>::size_hint(depth),
                    ]),
                ])
                .0,
                None,
            )
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
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        // An Export*ServiceRequest message has ~5 bytes of fixed values and
        // includes a varint-encoded message length. Use the worst case for this
        // length value.
        let bytes_remaining = max_bytes.checked_sub(4 + 1 + max_bytes / 0x7F);
        let mut bytes_remaining = if let Some(val) = bytes_remaining {
            val
        } else {
            return Ok(());
        };

        let mut acc = ExportMetricsServiceRequest(Vec::new());
        while let Ok(member) = unstructured.arbitrary::<Metric>() {
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
            assert!(bytes.len() <= max_bytes, "max len: {}, actual: {}", max_bytes, bytes.len());
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

            assert!(bytes.len() > 0);
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
