//! Benchmarks for OpenTelemetry metric payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::common::config::ConfRange;
use lading_payload::{
    OpentelemetryMetrics, Serialize,
    opentelemetry::metric::{Config, Contexts, MetricWeights},
};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn opentelemetry_metric_setup(c: &mut Criterion) {
    c.bench_function("opentelemetry_metric_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let config = Config {
                metric_weights: MetricWeights {
                    gauge: 50,
                    sum_delta: 25,
                    sum_cumulative: 25,
                },
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(100),
                    attributes_per_resource: ConfRange::Inclusive { min: 1, max: 64 },
                    scopes_per_resource: ConfRange::Inclusive { min: 1, max: 32 },
                    attributes_per_scope: ConfRange::Inclusive { min: 0, max: 4 },
                    metrics_per_scope: ConfRange::Inclusive { min: 1, max: 128 },
                    attributes_per_metric: ConfRange::Inclusive { min: 0, max: 255 },
                },
            };
            let _ot = OpentelemetryMetrics::new(config, MIB, &mut rng)
                .expect("failed to create metrics generator");
        });
    });
}

fn opentelemetry_metric_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("opentelemetry_metric_throughput");
    for size in &[MIB, 10 * MIB, 100 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let config = Config {
                        metric_weights: MetricWeights {
                            gauge: 50,
                            sum_delta: 25,
                            sum_cumulative: 25,
                        },
                        contexts: Contexts {
                            total_contexts: ConfRange::Constant(100),
                            attributes_per_resource: ConfRange::Inclusive { min: 1, max: 64 },
                            scopes_per_resource: ConfRange::Inclusive { min: 1, max: 32 },
                            attributes_per_scope: ConfRange::Inclusive { min: 0, max: 4 },
                            metrics_per_scope: ConfRange::Inclusive { min: 1, max: 128 },
                            attributes_per_metric: ConfRange::Inclusive { min: 0, max: 255 },
                        },
                    };
                    let ot = OpentelemetryMetrics::new(config, size, &mut rng)
                        .expect("failed to create metrics generator");
                    (rng, ot, Vec::with_capacity(size))
                },
                |(rng, mut ot, mut writer)| {
                    ot.to_bytes(rng, size, &mut writer)
                        .expect("failed to convert to bytes");
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(
    name = setup_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1));
    targets = opentelemetry_metric_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = opentelemetry_metric_throughput,
);

criterion_main!(setup_benches, throughput_benches);
