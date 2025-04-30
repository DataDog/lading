use opentelemetry_proto::tonic::{
    common::v1,
    metrics::{
        self,
        v1::{Metric, NumberDataPoint, metric::Data},
    },
    resource,
};
use rand::{
    Rng,
    distr::{Distribution, StandardUniform},
};

struct Ndp(NumberDataPoint);
impl Distribution<Ndp> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Ndp
    where
        R: Rng + ?Sized,
    {
        let value = match rng.random_range(0..=1) {
            0 => metrics::v1::number_data_point::Value::AsDouble(rng.random()),
            1 => metrics::v1::number_data_point::Value::AsInt(rng.random()),
            _ => unreachable!(),
        };

        Ndp(NumberDataPoint {
            // NOTE absent a reason to set attributes to not-empty, it's unclear
            // that we should.
            attributes: Vec::new(),
            start_time_unix_nano: 0, // epoch instant
            time_unix_nano: rng.random(),
            // Unclear that this needs to be set.
            exemplars: Vec::new(),
            // Equivalent to DoNotUse, the flag is ignored. If we ever set
            // `value` to None this must be set to
            // DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
            flags: 0,
            value: Some(value),
        })
    }
}

// struct Gauge(v1::Gauge);
// impl Distribution<Gauge> for StandardUniform {
//     fn sample<R>(&self, rng: &mut R) -> Gauge
//     where
//         R: Rng + ?Sized,
//     {
//         let total = rng.random_range(0..64);
//         let data_points = StandardUniform
//             .sample_iter(rng)
//             .map(|ndp: NumberDataPoint| ndp.0)
//             .take(total)
//             .collect();
//         Gauge(v1::Gauge { data_points })
//     }
// }

// struct Sum(v1::Sum);
// impl Distribution<Sum> for StandardUniform {
//     fn sample<R>(&self, rng: &mut R) -> Sum
//     where
//         R: Rng + ?Sized,
//     {
//         // 0: Unspecified AggregationTemporality, MUST not be used
//         // 1: Delta
//         // 2: Cumulative
//         let aggregation_temporality = *[1, 2]
//             .choose(rng)
//             .expect("failed to choose aggregation temporality");
//         let is_monotonic = rng.random();
//         let total = rng.random_range(0..64);
//         let data_points = StandardUniform
//             .sample_iter(rng)
//             .map(|ndp: NumberDataPoint| ndp.0)
//             .take(total)
//             .collect();

//         Sum(v1::Sum {
//             data_points,
//             aggregation_temporality,
//             is_monotonic,
//         })
//     }
// }

/// Static description of a Metric (everything that defines a context).
#[derive(Debug, Clone)]
pub(crate) struct MetricTemplate {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metadata: Vec<v1::KeyValue>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Kind {
    Gauge,
    Sum {
        aggregation_temporality: i32,
        is_monotonic: bool,
    },
}

impl MetricTemplate {
    /// Instantiate the template into a concrete `v1::Metric`
    pub(crate) fn instantiate<R: Rng + ?Sized>(&self, rng: &mut R) -> Metric {
        let total_data_points = rng.random_range(0..60);
        let data_points = (0..total_data_points)
            .map(|_| rng.random::<Ndp>().0)
            .collect();
        let data = match self.kind {
            Kind::Gauge => Data::Gauge(metrics::v1::Gauge { data_points }),
            Kind::Sum {
                aggregation_temporality,
                is_monotonic,
            } => Data::Sum(metrics::v1::Sum {
                data_points,
                aggregation_temporality,
                is_monotonic,
            }),
        };
        Metric {
            name: self.name.clone(),
            description: self.description.clone(),
            unit: self.unit.clone(),
            data: Some(data),
            metadata: self.metadata.clone(),
        }
    }
}

/// Static description of a Scope and its metrics.
#[derive(Debug, Clone)]
pub(crate) struct ScopeTemplate {
    pub scope: v1::InstrumentationScope,
    pub metrics: Vec<MetricTemplate>,
}

/// Static description of an OTLP Resource (context top-level).
#[derive(Debug, Clone)]
pub(crate) struct ResourceTemplate {
    pub resource: Option<resource::v1::Resource>,
    pub scopes: Vec<ScopeTemplate>,
}
