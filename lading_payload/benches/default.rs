use criterion::criterion_main;

mod dogstatsd;
criterion_main!(dogstatsd::benches,);
