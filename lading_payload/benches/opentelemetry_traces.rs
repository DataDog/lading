//! Benchmarks for OpenTelemetry trace payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{OpentelemetryTraces, Serialize, opentelemetry::trace::Config};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn opentelemetry_traces_setup(c: &mut Criterion) {
    c.bench_function("opentelemetry_traces_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _ot = OpentelemetryTraces::with_config(Config::default(), &mut rng)
                .expect("failed to create trace generator");
        });
    });
}

fn opentelemetry_traces_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("opentelemetry_traces_throughput");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let ot = OpentelemetryTraces::with_config(Config::default(), &mut rng)
                        .expect("failed to create trace generator");
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
    targets = opentelemetry_traces_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = opentelemetry_traces_throughput,
);

criterion_main!(setup_benches, throughput_benches);
