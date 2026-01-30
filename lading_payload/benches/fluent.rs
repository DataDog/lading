//! Benchmarks for Fluent payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::{Fluent, Serialize};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn fluent_setup(c: &mut Criterion) {
    c.bench_function("fluent_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _f = Fluent::new(&mut rng);
        })
    });
}

fn fluent_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("fluent_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19_690_716);
                let mut ta = Fluent::new(&mut rng);
                let mut writer = Vec::with_capacity(size);

                ta.to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = fluent_setup, fluent_all,
);
