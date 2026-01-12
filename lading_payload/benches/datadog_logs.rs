//! Benchmarks for Datadog Logs payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::{DatadogLog, Serialize};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn datadog_logs_setup(c: &mut Criterion) {
    c.bench_function("datadog_logs_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
            let _dd = DatadogLog::new(&mut rng);
        })
    });
}

fn datadog_logs_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("datadog_logs_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
                let mut dd = DatadogLog::new(&mut rng);
                let mut writer = Vec::with_capacity(size);

                dd.to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = datadog_logs_setup, datadog_logs_all
);
