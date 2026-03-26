//! Benchmarks for templated JSON payload generation.
//!
//! Two scenarios are covered:
//!
//! - `setup`: measures `TemplatedJson::from_path` (YAML parse + validation).
//! - `throughput`: measures `to_bytes` across a range of block sizes.
//!
//! Both use `examples/templates/bench_templated_json.yaml` from the repository root,
//! which exercises every `ValueGenerator` tag so the numbers reflect realistic
//! template complexity.

use std::path::Path;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lading_payload::{Serialize, TemplatedJson};
use rand::{SeedableRng, rngs::SmallRng};

const MIB: usize = 1_048_576;

const TEMPLATE_BUILTIN_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../examples/templates/templated_json_builtin.yaml"
);

const TEMPLATE_FEATURES_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../examples/templates/templated_json_features.yaml"
);

fn templated_json_setup(c: &mut Criterion) {
    let path = Path::new(TEMPLATE_FEATURES_PATH);
    c.bench_function("templated_json_setup", |b| {
        b.iter(|| TemplatedJson::from_path(path).expect("failed to load template"));
    });
}

fn templated_json_throughput(c: &mut Criterion, path: &str, group_name: &str) {
    let path = Path::new(path);
    let mut group = c.benchmark_group(group_name);
    for size in &[MIB, 10 * MIB, 100 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let rng = SmallRng::seed_from_u64(19_690_716);
                    let tj = TemplatedJson::from_path(path).expect("failed to load template");
                    (rng, tj, Vec::with_capacity(size))
                },
                |(rng, mut tj, mut writer)| {
                    tj.to_bytes(rng, size, &mut writer)
                        .expect("failed to convert to bytes");
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn templated_json_builtin_throughput(c: &mut Criterion) {
    templated_json_throughput(
        c,
        TEMPLATE_BUILTIN_PATH,
        "templated_json_builtin_throughput",
    );
}

fn templated_json_features_throughput(c: &mut Criterion) {
    templated_json_throughput(
        c,
        TEMPLATE_FEATURES_PATH,
        "templated_json_features_throughput",
    );
}

criterion_group!(
    name = setup_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1));
    targets = templated_json_setup,
);

criterion_group!(
    name = throughput_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(1));
    targets = templated_json_builtin_throughput, templated_json_features_throughput,
);

criterion_main!(setup_benches, throughput_benches);
