//! Benchmarks for Syslog 5424 payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{Serialize, Syslog5424};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

fn syslog_setup(c: &mut Criterion) {
    c.bench_function("syslog_setup", |b| {
        b.iter(|| {
            let _syslog = Syslog5424::default();
        })
    });
}

fn syslog_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("syslog_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let rng = SmallRng::seed_from_u64(19_690_716);
                let mut syslog = Syslog5424::default();
                let mut writer = Vec::with_capacity(size);

                syslog
                    .to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = setup_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(1));
    targets = syslog_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = syslog_all,
);

criterion_main!(setup_benches, throughput_benches);
