use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

use lading_payload::{dogstatsd, Serialize};
use rand::{rngs::SmallRng, SeedableRng};
use std::time::Duration;

fn dogstatsd_setup(c: &mut Criterion) {
    c.bench_function("dogstatsd_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
            let _dd = dogstatsd::DogStatsD::default(&mut rng);
        })
    });
}

fn dogstatsd_all(c: &mut Criterion) {
    let mb = 1_000_000; // 1 MiB

    let mut group = c.benchmark_group("dogstatsd_all");
    for size in &[mb, 10 * mb, 100 * mb, 1_000 * mb] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
                let dd =
                    dogstatsd::DogStatsD::default(&mut rng).expect("failed to create DogStatsD");
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
    targets = dogstatsd_setup, dogstatsd_all
);
