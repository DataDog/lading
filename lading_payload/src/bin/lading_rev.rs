/// A program whose sole purpose is to generate DogStatsD load and then
/// exit.
///
/// I'm not sure how this will interact with the release process. We probably
/// should disable this binary at some point.
use lading_payload::dogstatsd;
use rand::{rngs::SmallRng, SeedableRng};

use std::io::{BufWriter, Write};

fn main() {
    let seed: u64 = 19690616;
    let mut rng = SmallRng::seed_from_u64(seed);
    let dg = dogstatsd::DogStatsD::default(&mut rng);

    // NOTE we lock stdout and wrap it in a buffered writer to avoid needing to
    // reacquire the stdio lock each write and, also, to elide as many writes as
    // possible.
    let stdout = std::io::stdout();
    let mut fp = BufWriter::with_capacity(10_000_000, stdout.lock());
    for _ in 0..1_000_000 {
        let member = dg.generate(&mut rng);
        writeln!(fp, "{member}").unwrap();
    }
    fp.flush().unwrap();
}
