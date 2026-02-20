//! Benchmarks for Datadog Logs payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{DatadogLog, Serialize, datadog_logs};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn datadog_logs_setup(c: &mut Criterion) {
    c.bench_function("datadog_logs_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _dd = DatadogLog::new(&datadog_logs::Config::default(), &mut rng);
        });
    });
}

fn datadog_logs_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("datadog_logs_throughput");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let dd = DatadogLog::new(&datadog_logs::Config::default(), &mut rng);
                    (rng, dd, Vec::with_capacity(size))
                },
                |(rng, mut dd, mut writer)| {
                    dd.to_bytes(rng, size, &mut writer)
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
    targets = datadog_logs_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = datadog_logs_throughput,
);

criterion_main!(setup_benches, throughput_benches);
