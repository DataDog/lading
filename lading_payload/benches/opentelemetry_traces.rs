use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};

use lading_payload::{OpentelemetryTraces, Serialize};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

fn opentelemetry_traces_setup(c: &mut Criterion) {
    c.bench_function("opentelemetry_traces_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
            let _ot = OpentelemetryTraces::new(&mut rng);
        })
    });
}

fn opentelemetry_traces_all(c: &mut Criterion) {
    let mb = 1_000_000; // 1 MiB

    let mut group = c.benchmark_group("opentelemetry_traces_all");
    for size in &[mb, 10 * mb, 100 * mb, 1_000 * mb] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
                let ot = OpentelemetryTraces::new(&mut rng);
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
