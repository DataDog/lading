use criterion::{criterion_group, BenchmarkId, Criterion};

use lading_payload::common::strings;
use lading_payload::dogstatsd::common::tags;
use lading_payload::dogstatsd::ConfRange;
use lading_payload::Generator;
use rand::{rngs::SmallRng, SeedableRng};
use std::time::Duration;

fn tag_setup(c: &mut Criterion) {
    c.bench_function("tag_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19690716);
            let _tag_generator = tags::Generator::new(
                19690716,
                ConfRange::Inclusive { min: 0, max: 255 },
                ConfRange::Inclusive { min: 1, max: 64 },
                ConfRange::Inclusive { min: 1, max: 64 },
                strings::Pool::with_size(&mut rng, 8_000_000).into(),
                1_000,
            );
        })
    });
}

fn tag_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("tag_all");
    for size in &[1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19690716);
                let tag_generator = tags::Generator::new(
                    19690716,
                    ConfRange::Inclusive { min: 0, max: 255 },
                    ConfRange::Inclusive { min: 1, max: 64 },
                    ConfRange::Inclusive { min: 1, max: 64 },
                    strings::Pool::with_size(&mut rng, 8_000_000).into(),
                    size,
                );

                let tagset = tag_generator
                    .generate(&mut rng)
                    .expect("failed to generate tags");

                tagset
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(19)).sample_size(50);
    targets = tag_setup, tag_all
);
