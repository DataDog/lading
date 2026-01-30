//! Benchmarks for OpenTelemetry log payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::{OpentelemetryLogs, Serialize, opentelemetry::log::Config};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn opentelemetry_log_setup(c: &mut Criterion) {
    c.bench_function("opentelemetry_log_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _ot = OpentelemetryLogs::new(Config::default(), MIB, &mut rng)
                .expect("failed to create log generator");
        })
    });
}

fn opentelemetry_log_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("opentelemetry_log_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19_690_716);
                let mut ot = OpentelemetryLogs::new(Config::default(), size, &mut rng)
                    .expect("failed to create log generator");
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
    targets = opentelemetry_log_setup, opentelemetry_log_all
);
