//! Benchmarks for Datadog trace agent payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{Serialize, trace_agent::v04};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn trace_agent_setup(c: &mut Criterion) {
    c.bench_function("trace_agent_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _ta = v04::V04::with_config(v04::Config::default(), &mut rng)
                .expect("failed to create trace agent");
        });
    });
}

fn trace_agent_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("trace_agent_throughput");
    for size in &[MIB, 10 * MIB, 100 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let ta = v04::V04::with_config(v04::Config::default(), &mut rng)
                        .expect("failed to create trace agent");
                    (rng, ta, Vec::with_capacity(size))
                },
                |(rng, mut ta, mut writer)| {
                    ta.to_bytes(rng, size, &mut writer)
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
    targets = trace_agent_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = trace_agent_throughput,
);

criterion_main!(setup_benches, throughput_benches);
