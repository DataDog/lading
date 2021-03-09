use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use file_gen::buffer;
use rand::SeedableRng;
use rand_xoshiro::SplitMix64;
use std::fmt;

struct Parameters {
    seed: u64,
    data_size: usize,
}

static PARAMETERS: [Parameters; 4] = [
    Parameters {
        seed: 1010101,
        data_size: 1_000,
    },
    Parameters {
        seed: 1010101,
        data_size: 10_000,
    },
    Parameters {
        seed: 1010101,
        data_size: 100_000,
    },
    Parameters {
        seed: 1010101,
        data_size: 1_000_000,
    },
];

impl fmt::Display for Parameters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "seed: {}, data_size: {}", self.seed, self.data_size)
    }
}

fn fill_ascii_buffer_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("fill_ascii_buffer");

    for param in &PARAMETERS {
        group.throughput(Throughput::Bytes(param.data_size as u64));

        let mut rng = SplitMix64::from_seed(param.seed.to_be_bytes());
        let mut data: Vec<u8> = vec![0; param.data_size];
        group.bench_with_input(BenchmarkId::from_parameter(&param), &param, |b, _| {
            b.iter(|| buffer::fill_ascii(&mut rng, &mut data[0..]))
        });
    }
}

fn fill_json_buffer_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("fill_json_buffer");

    for param in &PARAMETERS {
        group.throughput(Throughput::Bytes(param.data_size as u64));

        let mut rng = SplitMix64::from_seed(param.seed.to_be_bytes());
        let mut data: Vec<u8> = vec![0; param.data_size];
        group.bench_with_input(BenchmarkId::from_parameter(&param), &param, |b, _| {
            b.iter(|| buffer::fill_json(&mut rng, &mut data[0..]))
        });
    }
}

fn fill_constant_buffer_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("fill_constant_buffer");

    for param in &PARAMETERS {
        group.throughput(Throughput::Bytes(param.data_size as u64));

        let mut rng = SplitMix64::from_seed(param.seed.to_be_bytes());
        let mut data: Vec<u8> = vec![0; param.data_size];
        group.bench_with_input(BenchmarkId::from_parameter(&param), &param, |b, _| {
            b.iter(|| buffer::fill_constant(&mut rng, &mut data[0..]))
        });
    }
}

criterion_group!(name = benches;
                 config = Criterion::default();
                 targets = fill_json_buffer_bench, fill_ascii_buffer_bench, fill_constant_buffer_bench);
criterion_main!(benches);
