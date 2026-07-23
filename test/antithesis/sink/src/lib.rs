//! TCP byte-counting sink oracle for the lading Antithesis harness.
//!
//! The sink is the observer in the Antithesis "general" scenario: it receives
//! the load lading pushes and owns the claim that load arrived. The pure
//! byte-accounting lives here so it can be tested conventionally; the network
//! loop and the SDK assertion live in the binary.

use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe accumulator for bytes the sink has received.
#[derive(Debug, Default)]
pub struct Counter {
    bytes: AtomicU64,
}

impl Counter {
    /// Create an empty counter.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
        }
    }

    /// Record `n` received bytes and return the new running total.
    ///
    /// The total saturates at `u64::MAX` rather than wrapping.
    pub fn record(&self, n: u64) -> u64 {
        let mut current = self.bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(n);
            match self.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(actual) => current = actual,
            }
        }
    }

    /// The running total of recorded bytes.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::Counter;
    use proptest::prelude::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_counter_is_empty() {
        let c = Counter::new();
        assert_eq!(c.total(), 0);
    }

    #[test]
    fn record_accumulates_and_returns_total() {
        let c = Counter::new();
        assert_eq!(c.record(10), 10);
        assert_eq!(c.record(5), 15);
        assert_eq!(c.total(), 15);
    }

    #[test]
    fn record_zero_leaves_total_unchanged() {
        let c = Counter::new();
        assert_eq!(c.record(0), 0);
        assert_eq!(c.record(7), 7);
        assert_eq!(c.record(0), 7);
    }

    #[test]
    fn record_saturates_at_max() {
        let c = Counter::new();
        assert_eq!(c.record(u64::MAX), u64::MAX);
        assert_eq!(c.record(1), u64::MAX);
        assert_eq!(c.total(), u64::MAX);
    }

    #[test]
    fn concurrent_records_sum_exactly() {
        let c = Arc::new(Counter::new());
        let threads: u64 = 8;
        let per_thread: u64 = 10_000;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let c = Arc::clone(&c);
                thread::spawn(move || {
                    for _ in 0..per_thread {
                        c.record(1);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread panicked");
        }
        assert_eq!(c.total(), threads * per_thread);
    }

    proptest! {
        #[test]
        fn total_equals_saturating_sum(
            increments in proptest::collection::vec(any::<u64>(), 0..64)
        ) {
            let c = Counter::new();
            let mut expected: u64 = 0;
            for &n in &increments {
                expected = expected.saturating_add(n);
                prop_assert_eq!(c.record(n), expected);
            }
            prop_assert_eq!(c.total(), expected);
        }
    }
}
