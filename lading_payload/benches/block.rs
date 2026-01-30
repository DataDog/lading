//! Benchmarks for Block Cache operations.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::Config;
use lading_payload::block::Cache;
use rand::{SeedableRng, rngs::SmallRng};
use std::hint::black_box;
use std::num::NonZeroU32;
use std::time::Duration;

const MIB: u32 = 1_048_576;

/// Benchmark Cache construction with different total sizes
fn cache_setup(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_setup");

    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(u64::from(*size)));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19_690_716);
                let total_bytes = NonZeroU32::new(size).expect("size is non-zero");
                let max_block_size = 256 * 1024; // 256 KiB blocks

                Cache::fixed_with_max_overhead(
                    &mut rng,
                    total_bytes,
                    max_block_size,
                    &Config::Json,
                    0,
                )
                .expect("failed to create cache")
            });
        });
    }
    group.finish();
}

/// Benchmark the `advance()` hot path - the most critical operation
fn cache_advance(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_advance");

    for size in &[MIB, 10 * MIB, 100 * MIB] {
        group.throughput(Throughput::Bytes(u64::from(*size)));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let total_bytes = NonZeroU32::new(size).expect("size is non-zero");
            let cache =
                Cache::fixed_with_max_overhead(&mut rng, total_bytes, 256 * 1024, &Config::Json, 0)
                    .expect("failed to create cache");

            b.iter(|| {
                let mut handle = cache.handle();
                let mut bytes_read = 0u64;

                // Simulate reading through the entire cache
                while bytes_read < u64::from(size) {
                    let block = black_box(cache.advance(&mut handle));
                    bytes_read += u64::from(block.total_bytes.get());
                }
                bytes_read
            });
        });
    }
    group.finish();
}

/// Benchmark peek operations (`peek_next_size`, `peek_next_metadata`)
fn cache_peek(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(19_690_716);
    let cache = Cache::fixed_with_max_overhead(
        &mut rng,
        NonZeroU32::new(10 * MIB).expect("10 * MIB is non-zero"),
        256 * 1024,
        &Config::Json,
        0,
    )
    .expect("failed to create cache");

    c.bench_function("cache_peek_next_size", |b| {
        let handle = cache.handle();
        b.iter(|| black_box(cache.peek_next_size(&handle)));
    });

    c.bench_function("cache_peek_next_metadata", |b| {
        let handle = cache.handle();
        b.iter(|| black_box(cache.peek_next_metadata(&handle)));
    });
}

/// Benchmark `total_size()` method
fn cache_total_size(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(19_690_716);
    let cache = Cache::fixed_with_max_overhead(
        &mut rng,
        NonZeroU32::new(100 * MIB).expect("100 * MIB is non-zero"),
        256 * 1024,
        &Config::Json,
        0,
    )
    .expect("failed to create cache");

    c.bench_function("cache_total_size", |b| {
        b.iter(|| black_box(cache.total_size()));
    });
}

/// Benchmark `read_at()` for random access patterns
fn cache_read_at(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_read_at");

    let mut rng = SmallRng::seed_from_u64(19_690_716);
    let cache = Cache::fixed_with_max_overhead(
        &mut rng,
        NonZeroU32::new(100 * MIB).expect("100 * MIB is non-zero"),
        256 * 1024,
        &Config::Json,
        0,
    )
    .expect("failed to create cache");

    // Test different read sizes
    for read_size in &[1024, 64 * 1024, MIB] {
        group.throughput(Throughput::Bytes(u64::from(*read_size)));
        group.bench_with_input(
            BenchmarkId::from_parameter(read_size),
            read_size,
            |b, &size| {
                b.iter(|| {
                    // Read from different offsets to avoid cache effects
                    let offset = u64::from(size * 10);
                    black_box(cache.read_at(offset, size as usize))
                });
            },
        );
    }
    group.finish();
}

/// Benchmark `handle()` creation
fn cache_handle_creation(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(19_690_716);
    let cache = Cache::fixed_with_max_overhead(
        &mut rng,
        NonZeroU32::new(10 * MIB).expect("10 * MIB is non-zero"),
        256 * 1024,
        &Config::Json,
        0,
    )
    .expect("failed to create cache");

    c.bench_function("cache_handle", |b| {
        b.iter(|| black_box(cache.handle()));
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = cache_setup, cache_advance, cache_peek, cache_total_size,
              cache_read_at, cache_handle_creation
);
