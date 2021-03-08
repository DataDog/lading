use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fastrand::Rng;
use file_gen::buffer;
use std::fmt;

struct Parameters {
    seed: u64,
    data_size: usize,
}

static PARAMETERS: [Parameters; 5] = [
    Parameters {
        seed: 1010101,
        data_size: 1,
    },
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
        write!(f, "{}|{}", self.seed, self.data_size)
    }
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fill_ascii_buffer");

    for param in &PARAMETERS {
        group.throughput(Throughput::Bytes(param.data_size as u64));

        let rng: Rng = Rng::with_seed(param.seed);
        let mut data: Vec<u8> = vec![0; param.data_size];
        group.bench_with_input(BenchmarkId::from_parameter(&param), &param, |b, _| {
            b.iter(|| buffer::fill_ascii_buffer(&rng, &mut data[0..]))
        });
    }
}

criterion_group!(name = benches;
                 config = Criterion::default();
                 targets = benchmark);
criterion_main!(benches);
