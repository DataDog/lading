use criterion::criterion_main;

mod ascii;
mod dogstatsd;

criterion_main!(ascii::benches, dogstatsd::benches,);
