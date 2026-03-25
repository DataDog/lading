//! Benchmarks for OpenTelemetry log payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
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
        });
    });
}

fn opentelemetry_log_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("opentelemetry_log_throughput");
    // Benching 100+ MiB pushes the benchmark runtime to >60 minutes
    for size in &[MIB, 10 * MIB, 100 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let ot = OpentelemetryLogs::new(Config::default(), size, &mut rng)
                        .expect("failed to create log generator");
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
    targets = opentelemetry_log_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = opentelemetry_log_throughput,
);

criterion_main!(setup_benches, throughput_benches);
