//! Benchmarks for JSON payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::{Json, Serialize};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn json_setup(c: &mut Criterion) {
    c.bench_function("json_setup", |b| {
        b.iter(|| {
            let _json = Json::default();
        })
    });
}

fn json_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let rng = SmallRng::seed_from_u64(19_690_716);
                let mut json = Json::default();
                let mut writer = Vec::with_capacity(size);

                json.to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = json_setup, json_all
);
