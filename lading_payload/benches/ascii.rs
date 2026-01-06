//! Benchmarks for ASCII payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};

use lading_payload::{Serialize, ascii};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

fn ascii_setup(c: &mut Criterion) {
    c.bench_function("ascii_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
            let _dd = ascii::Ascii::new(&mut rng);
        })
    });
}

fn ascii_all(c: &mut Criterion) {
    let mb = 1_000_000; // 1 MiB

    let mut group = c.benchmark_group("ascii_all");
    for size in &[mb, 10 * mb, 100 * mb, 1_000 * mb] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
                let mut asc = ascii::Ascii::new(&mut rng);
                let mut writer = Vec::with_capacity(size);

                asc.to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = ascii_setup, ascii_all
);
