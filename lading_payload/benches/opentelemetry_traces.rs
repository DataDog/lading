//! Benchmarks for OpenTelemetry trace payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::{OpentelemetryTraces, Serialize, opentelemetry::trace::Config};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn opentelemetry_traces_setup(c: &mut Criterion) {
    c.bench_function("opentelemetry_traces_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
            let _ot = OpentelemetryTraces::with_config(Config::default(), &mut rng)
                .expect("failed to create trace generator");
        })
    });
}

fn opentelemetry_traces_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("opentelemetry_traces_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
                let mut ot = OpentelemetryTraces::with_config(Config::default(), &mut rng)
                    .expect("failed to create trace generator");
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
    targets = opentelemetry_traces_setup, opentelemetry_traces_all
);
