//! Benchmarks for OpenTelemetry metric payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use lading_payload::common::config::ConfRange;
use lading_payload::{
    OpentelemetryMetrics, Serialize,
    opentelemetry::metric::{Config, Contexts, MetricWeights},
};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

fn opentelemetry_metric_setup(c: &mut Criterion) {
    c.bench_function("opentelemetry_metric_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
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
            let _ot = OpentelemetryMetrics::new(config, 1_000_000, &mut rng)
                .expect("failed to create metrics generator");
        })
    });
}

fn opentelemetry_metric_all(c: &mut Criterion) {
    let mb = 1_000_000; // 1 MiB

    let mut group = c.benchmark_group("opentelemetry_metric_all");
    for size in &[mb, 10 * mb, 100 * mb, 1_000 * mb] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
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
                let mut ot = OpentelemetryMetrics::new(config, size, &mut rng)
                    .expect("failed to create metrics generator");
                let mut writer = Vec::with_capacity(size);

                ot.to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = opentelemetry_metric_setup, opentelemetry_metric_all
);
criterion_main!(benches);
