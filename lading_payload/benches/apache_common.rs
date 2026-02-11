//! Benchmarks for Apache Common log payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{Serialize, apache_common};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn apache_common_setup(c: &mut Criterion) {
    c.bench_function("apache_common_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _ac = apache_common::ApacheCommon::new(&mut rng);
        });
    });
}

fn apache_common_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("apache_common_throughput");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let ac = apache_common::ApacheCommon::new(&mut rng);
                    (rng, ac, Vec::with_capacity(size))
                },
                |(rng, mut ac, mut writer)| {
                    ac.to_bytes(rng, size, &mut writer)
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
    targets = apache_common_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = apache_common_throughput,
);

criterion_main!(setup_benches, throughput_benches);
