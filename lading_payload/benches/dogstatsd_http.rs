//! Benchmarks for `DogStatsDHttp` payload generation.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{Serialize, dogstatsd, dogstatsd_http::DogStatsDHttp};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn dogstatsd_http_setup(c: &mut Criterion) {
    c.bench_function("dogstatsd_http_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let config = dogstatsd::Config::default();
            let _dd = DogStatsDHttp::new(&config, &mut rng);
        });
    });
}

fn dogstatsd_http_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("dogstatsd_http_throughput");
    for size in &[MIB, 10 * MIB, 100 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rng = SmallRng::seed_from_u64(19_690_716);
                    let config = dogstatsd::Config::default();
                    let dd = DogStatsDHttp::new(&config, &mut rng)
                        .expect("failed to create DogStatsDHttp");
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
    targets = dogstatsd_http_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = dogstatsd_http_throughput,
);

criterion_main!(setup_benches, throughput_benches);
