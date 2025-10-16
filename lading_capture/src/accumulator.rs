//! Accumulator logic for time-sliced metric capture
//!
//! This module solves an important problem for lading. We need metrics-rs
//! metrics to reach our capture file, prometheus export depending on how users
//! configure it. This has worked well since the beginning of the
//! project. However, we need also to support "historical" or "delayed"
//! metrics. Consider a generator that mimics the Datadog Intake API. Metrics
//! sent to this intake are batched in 10 second intervals, meaning the
//! metrics-rs understanding of all metrics arriving 'now' no longer serves all
//! needed cases.
//!
//! To that end this module supports a Counter and Gauge metric in a 60-tick
//! rolling accumulation. The core structure is [`Accumulator`]. Calling code is
//! responsible for defining the real-clock duration of a 'tick'. Conceptually,
//! every tick the [`Accumulator`]creates a new 0th interval where 'now' writes
//! are stored. The previous 0th interval becomes the 1st, the 1st the 2nd and
//! so forth. This is called a 'roll'. After 60 ticks the 60th tick becomes
//! 'flushable', that is, the metrics stored in that interval will be returned
//! to the caller should they be requested. Tick time advances independently of
//! flushes and metrics are _not_ stored to the 61st interval. [`Accumulator`]
//! supports a `flush_all` operation that consumes the structure, allowing for
//! metrics to be exfiltrated on shutdown without delay.
//
//! # Semantics
//!
//! The [`Accumulator`] allows writes from tick 0 to tick 59. Writes do not
//! expire, that is, conceptually when a roll occurs the data in the previous
//! 0th interval is copied forward into the new 0th interval. This will be
//! changed in the future.
//!
//! ## Counters
//!
//! A `Counter` write may be either `Increment(k, T, i)` or `Absolute(k, T, i)`,
//! where `T` is the tick interval of the write, `k` is the identifying key of
//! the metric and `i` is the counter value. If `T==0` then the value i is
//! summed to all intervals T..=0. An absolute value writes `i` to T..=0
//! irrespective of what value existed previously in that interval.
//!
//! By way of example, the following orderings of writes must resolve to the
//! same outcome:
//!
//! `[Increment(k, 0, 1), Increment(k, 1, 1), Icrement(k, 1, 2)]`
//! `[Increment(k, 1, 2), Increment(k, 1, 1), Icrement(k, 0, 1)]`
//! -> [(k, 0, 1+1+2), (k, 1, 1+2)]
//!
//! Or consider the mixture of absolute and increment writes. Allow that the
//! current tick is 5 with no writes. The conceptual state is thus empty. At
//! tick 5 we receive writes. These orderings of writes will not resolve to the same
//! outcome:
//!
//! `[Increment(k, 0, 10), Absolute(k, 2, 100)]`
//! -> [(k, 0, 100), (k, 1, 100), (k, 2, 100)]
//! `[Absolute(k, 2, 100), Increment(k, 0, 10)]`
//! -> [(k, 0, 110), (k, 1, 100), (k, 2, 100)]
//!
//! Consider another scenario in which we have absolute writes at distinct tick
//! offsets. Given that the current tick is 5 and there have been no prior
//! writes -- resulting in an empty state -- at tick 5 we receive writes. These
//! orderings of writes must resolve to the same outcome:
//!
//! `[Absolute(k, 0, 200), Absolute(k, 2, 50)]`
//! `[Absolute(k, 2, 50), Absolute(k, 0, 200)]`
//! -> [(k, 0, 200), (k, 1, 50), (k, 2, 50)]
//!
//! Let us now discuss the passage of time. We introduce the operation TICK to
//! denote that the current tick has advanced. Given that we start from tick 0
//! and with an empty state the following operations resolve as such:
//!
//! `[Absolute(k, 1, 100), TICK, Absolute(k, 0, 25)]`
//! -> 0[Absolute(k, 1, 100)]: [(k, 0, 100), (k, 1, 100)]
//! -> 1[TICK]:                [(k, 0, 100), (k, 1, 100), (k, 2, 100)]
//! -> 1[Absolute(k, 0, 25)]:  [(k, 0, 25), (k, 1, 100), (k, 2, 100)]
//!
//! Note that the last two operations happen "at the same time" but the
//! `Accumulator` does not admit concurrent writes and so we can analyze writes
//! in serial. Consider however these two orderings DO NOT resolve to the same
//! result:
//!
//! `[Absolute(k, 0, 10), Increment(k, 0, 100)]`
//! -> [(k, 0, 110)]
//! `[Increment(k, 0, 100)], Absolute(k, 0, 10)]`
//! -> [(k, 0, 10)]
//!
//! Stated logically:
//!
//!  * `Increment` is associative and commutative.
//!  * `Absolute` is idempotent.
//!  * `Absolute` does not commute with `Increment`.
//!
//! ## Gauges
//!
//! A `Gauge` write may be either `Increment(k, T, i)` or `Decrement(k, T, i)`
//! or `Set(k, T, i)`. The semantic considerations of `Counter` discussed above
//! largely apply to `Gauge`. Logically:
//!
//!  * `Increment` is associative and commutative.
//!  * `Decrement` is associative and commutative.
//!  * `Increment` and `Decrement` commute.
//!  * `Set` is idempotent.
//!  * `Set` does not commute with `Increment` nor `Decrement`.

use metrics::Key;
use rustc_hash::FxHashMap;

use crate::manager;

pub(crate) const INTERVALS: usize = 60;

#[inline]
#[allow(clippy::cast_possible_truncation)]
fn interval_idx(tick: u64) -> usize {
    (tick % (INTERVALS as u64)) as usize
}

/// Errors produced by [`Accumulator`]
#[derive(thiserror::Error, Debug, Copy, Clone)]
pub enum Error {
    /// Metric tick is too old
    #[error("Tick for metric too old: {tick_offset}")]
    TickTooOld { tick_offset: u8 },
    /// Metric tick is from the future
    #[error("Tick for metric from future: {tick_offset}")]
    FutureTick { tick_offset: u8 },
}

/// Iterator that drains all accumulated metrics during shutdown.
///
/// Each iteration flushes metrics from a single interval until no intervals
/// remain.
pub(crate) struct DrainIter<'a> {
    accumulator: &'a mut Accumulator,
    remaining: usize,
}

impl Iterator for DrainIter<'_> {
    type Item = (u64, Vec<(Key, MetricValue, u64)>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        self.remaining -= 1;
        let current_tick = self.accumulator.current_tick;
        let metrics: Vec<_> = self.accumulator.flush().collect();
        self.accumulator.advance_tick();

        Some((current_tick, metrics))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for DrainIter<'_> {}

/// Represents a metric value (counter or gauge)
#[derive(Debug, Clone)]
pub(crate) enum MetricValue {
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
}

/// Accumulator with 60-interval rolling window for metrics
#[derive(Debug)]
pub(crate) struct Accumulator {
    counters: FxHashMap<Key, [u64; INTERVALS]>,
    gauges: FxHashMap<Key, [f64; INTERVALS]>,
    pub(crate) current_tick: u64,
}

impl Accumulator {
    pub(crate) fn new() -> Self {
        Self {
            counters: FxHashMap::default(),
            gauges: FxHashMap::default(),
            current_tick: 0,
        }
    }

    pub(crate) fn counter(&mut self, c: manager::Counter) -> Result<(), Error> {
        if usize::from(c.tick_offset) >= INTERVALS {
            return Err(Error::TickTooOld {
                tick_offset: c.tick_offset,
            });
        }

        if u64::from(c.tick_offset) > self.current_tick {
            return Err(Error::FutureTick {
                tick_offset: c.tick_offset,
            });
        }

        let values = self.counters.entry(c.key).or_insert([0; INTERVALS]);

        for offset in 0..=c.tick_offset {
            let target_tick = self.current_tick - u64::from(offset); // always lands within u64 range
            let interval = interval_idx(target_tick);

            match c.value {
                manager::CounterValue::Absolute(v) => values[interval] = v,
                manager::CounterValue::Increment(v) => values[interval] += v,
            }
        }

        Ok(())
    }

    pub(crate) fn gauge(&mut self, g: manager::Gauge) -> Result<(), Error> {
        if usize::from(g.tick_offset) >= INTERVALS {
            return Err(Error::TickTooOld {
                tick_offset: g.tick_offset,
            });
        }

        if u64::from(g.tick_offset) > self.current_tick {
            return Err(Error::FutureTick {
                tick_offset: g.tick_offset,
            });
        }

        let values = self.gauges.entry(g.key).or_insert([0.0; INTERVALS]);

        for offset in 0..=g.tick_offset {
            let target_tick = self.current_tick - u64::from(offset); // always lands within u64 range
            let interval = interval_idx(target_tick);

            match g.value {
                manager::GaugeValue::Set(v) => values[interval] = v,
                manager::GaugeValue::Increment(v) => values[interval] += v,
                manager::GaugeValue::Decrement(v) => values[interval] -= v,
            }
        }

        Ok(())
    }

    /// Advance tick and copy forward values
    pub(crate) fn advance_tick(&mut self) {
        let old_interval = interval_idx(self.current_tick);
        self.current_tick = self.current_tick.wrapping_add(1);
        let new_interval = interval_idx(self.current_tick);
        for values in self.counters.values_mut() {
            values[new_interval] = values[old_interval];
        }
        for values in self.gauges.values_mut() {
            values[new_interval] = values[old_interval];
        }
    }

    /// Flush T-INTERVALS data, returning an iterator of (Key, `MetricValue`, tick)
    /// tuples.
    ///
    /// Returns metrics from the interval that is INTERVALS ticks old. If the
    /// current tick has not yet reached INTERVALS, returns an empty iterator.
    pub(crate) fn flush(&self) -> impl Iterator<Item = (Key, MetricValue, u64)> + use<> {
        let mut metrics = Vec::new();

        if self.current_tick < INTERVALS as u64 {
            return metrics.into_iter();
        }

        let flush_tick = self.current_tick - INTERVALS as u64;
        let flush_interval = interval_idx(flush_tick);

        for (key, values) in &self.counters {
            let value = values[flush_interval];
            metrics.push((key.clone(), MetricValue::Counter(value), flush_tick));
        }

        for (key, values) in &self.gauges {
            let value = values[flush_interval];
            metrics.push((key.clone(), MetricValue::Gauge(value), flush_tick));
        }

        metrics.into_iter()
    }

    /// Returns an iterator that drains all accumulated metrics.
    pub(crate) fn drain(&mut self) -> DrainIter<'_> {
        DrainIter {
            accumulator: self,
            remaining: INTERVALS,
        }
    }

    #[cfg(test)]
    fn get_counter_value(&self, key: &Key, tick: u64) -> u64 {
        self.counters
            .get(key)
            .map(|intervals| intervals[interval_idx(tick)])
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn get_gauge_value(&self, key: &Key, tick: u64) -> f64 {
        self.gauges
            .get(key)
            .map(|intervals| intervals[interval_idx(tick)])
            .unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Test helpers to reduce duplication
    fn counter_increment(key: Key, tick_offset: u8, value: u64) -> manager::Counter {
        manager::Counter {
            key,
            tick_offset,
            value: manager::CounterValue::Increment(value),
        }
    }

    fn counter_absolute(key: Key, tick_offset: u8, value: u64) -> manager::Counter {
        manager::Counter {
            key,
            tick_offset,
            value: manager::CounterValue::Absolute(value),
        }
    }

    fn gauge_set(key: Key, tick_offset: u8, value: f64) -> manager::Gauge {
        manager::Gauge {
            key,
            tick_offset,
            value: manager::GaugeValue::Set(value),
        }
    }

    fn gauge_increment(key: Key, tick_offset: u8, value: f64) -> manager::Gauge {
        manager::Gauge {
            key,
            tick_offset,
            value: manager::GaugeValue::Increment(value),
        }
    }

    fn gauge_decrement(key: Key, tick_offset: u8, value: f64) -> manager::Gauge {
        manager::Gauge {
            key,
            tick_offset,
            value: manager::GaugeValue::Decrement(value),
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        CounterIncrement(u64),
        CounterAbsolute(u64),
        GaugeIncrement(f64),
        GaugeDecrement(f64),
        GaugeSet(f64),
        AdvanceTick,
    }

    impl Arbitrary for Op {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                (1u64..100u64).prop_map(Op::CounterIncrement),
                (1u64..100u64).prop_map(Op::CounterAbsolute),
                (0.0f64..100.0f64).prop_map(Op::GaugeIncrement),
                (0.0f64..100.0f64).prop_map(Op::GaugeDecrement),
                (0.0f64..100.0f64).prop_map(Op::GaugeSet),
                Just(Op::AdvanceTick),
            ]
            .boxed()
        }
    }

    // NOTE generally speaking in lading project we have a complicated System
    // under Test (SUT) and then a simple, 'obviously correct' model that we do
    // model checking against. The SuT here is already very simple and so I have
    // elected to leave the stub of what a model check might be while leaning
    // more into unit tests. My hope is that as the SuT evolves into something
    // more complex we can pursue the more common project test approach.

    proptest! {
        #[test]
        fn random_operations_maintain_invariants(ops in prop::collection::vec(any::<Op>(), 0..50)) {
            let key = Key::from_name("test");
            let mut acc = Accumulator::new();
            let initial_tick = acc.current_tick;

            for op in ops {
                let old_tick = acc.current_tick;

                match op {
                    Op::CounterIncrement(v) => {
                        let _ = acc.counter(counter_increment(key.clone(), 0, v));
                    }
                    Op::CounterAbsolute(v) => {
                        let _ = acc.counter(counter_absolute(key.clone(), 0, v));
                    }
                    Op::GaugeIncrement(v) => {
                        let _ = acc.gauge(gauge_increment(key.clone(), 0, v));
                    }
                    Op::GaugeDecrement(v) => {
                        let _ = acc.gauge(gauge_decrement(key.clone(), 0, v));
                    }
                    Op::GaugeSet(v) => {
                        let _ = acc.gauge(gauge_set(key.clone(), 0, v));
                    }
                    Op::AdvanceTick => {
                        acc.advance_tick();
                    }
                }

                // Invariants
                assert!(acc.current_tick >= old_tick, "tick must not decrease");
                assert!(
                    acc.current_tick <= old_tick + 1,
                    "tick can only advance by 1 at a time"
                );
                assert!(
                    acc.current_tick >= initial_tick,
                    "tick must be >= initial tick"
                );
            }
        }
    }

    // Counter: Increment is associative and commutative
    #[test]
    fn counter_increment_commutative() {
        let key = Key::from_name("test");

        // [Increment(5), Increment(3)]
        let mut acc1 = Accumulator::new();
        acc1.counter(counter_increment(key.clone(), 0, 5)).unwrap();
        acc1.counter(counter_increment(key.clone(), 0, 3)).unwrap();

        // [Increment(3), Increment(5)]
        let mut acc2 = Accumulator::new();
        acc2.counter(counter_increment(key.clone(), 0, 3)).unwrap();
        acc2.counter(counter_increment(key.clone(), 0, 5)).unwrap();

        assert_eq!(
            acc1.get_counter_value(&key, acc1.current_tick),
            acc2.get_counter_value(&key, acc2.current_tick)
        );
        assert_eq!(acc1.get_counter_value(&key, acc1.current_tick), 8);
    }

    // Counter: Absolute is idempotent
    #[test]
    fn counter_absolute_idempotent() {
        let key = Key::from_name("test");

        // [Absolute(100)]
        let mut acc1 = Accumulator::new();
        acc1.counter(counter_absolute(key.clone(), 0, 100)).unwrap();

        // [Absolute(100), Absolute(100)]
        let mut acc2 = Accumulator::new();
        acc2.counter(counter_absolute(key.clone(), 0, 100)).unwrap();
        acc2.counter(counter_absolute(key.clone(), 0, 100)).unwrap();

        assert_eq!(
            acc1.get_counter_value(&key, acc1.current_tick),
            acc2.get_counter_value(&key, acc2.current_tick)
        );
        assert_eq!(acc1.get_counter_value(&key, acc1.current_tick), 100);
    }

    // Counter: Absolute does NOT commute with Increment
    #[test]
    fn counter_absolute_noncommutative_with_increment() {
        let key = Key::from_name("test");

        // [Increment(10), Absolute(50)]
        let mut acc1 = Accumulator::new();
        acc1.counter(counter_increment(key.clone(), 0, 10)).unwrap();
        acc1.counter(counter_absolute(key.clone(), 0, 50)).unwrap();

        // [Absolute(50), Increment(10)]
        let mut acc2 = Accumulator::new();
        acc2.counter(counter_absolute(key.clone(), 0, 50)).unwrap();
        acc2.counter(counter_increment(key.clone(), 0, 10)).unwrap();

        // Should differ: acc1 is 50, acc2 is 60
        assert_eq!(acc1.get_counter_value(&key, acc1.current_tick), 50);
        assert_eq!(acc2.get_counter_value(&key, acc2.current_tick), 60);
        assert_ne!(
            acc1.get_counter_value(&key, acc1.current_tick),
            acc2.get_counter_value(&key, acc2.current_tick)
        );
    }

    // Gauge: Increment and Decrement commute
    #[test]
    fn gauge_increment_decrement_commute() {
        let key = Key::from_name("test");

        // [Increment(10.0), Decrement(3.0)]
        let mut acc1 = Accumulator::new();
        acc1.gauge(gauge_increment(key.clone(), 0, 10.0)).unwrap();
        acc1.gauge(gauge_decrement(key.clone(), 0, 3.0)).unwrap();

        // [Decrement(3.0), Increment(10.0)]
        let mut acc2 = Accumulator::new();
        acc2.gauge(gauge_decrement(key.clone(), 0, 3.0)).unwrap();
        acc2.gauge(gauge_increment(key.clone(), 0, 10.0)).unwrap();

        assert_eq!(
            acc1.get_gauge_value(&key, acc1.current_tick),
            acc2.get_gauge_value(&key, acc2.current_tick)
        );
        assert!((acc1.get_gauge_value(&key, acc1.current_tick) - 7.0).abs() < 1e-10);
    }

    // Gauge: Set is idempotent
    #[test]
    fn gauge_set_idempotent() {
        let key = Key::from_name("test");

        // [Set(50.0)]
        let mut acc1 = Accumulator::new();
        acc1.gauge(gauge_set(key.clone(), 0, 50.0)).unwrap();

        // [Set(50.0), Set(50.0)]
        let mut acc2 = Accumulator::new();
        acc2.gauge(gauge_set(key.clone(), 0, 50.0)).unwrap();
        acc2.gauge(gauge_set(key.clone(), 0, 50.0)).unwrap();

        assert!(
            (acc1.get_gauge_value(&key, acc1.current_tick)
                - acc2.get_gauge_value(&key, acc2.current_tick))
            .abs()
                < 1e-10
        );
        assert!((acc1.get_gauge_value(&key, acc1.current_tick) - 50.0).abs() < 1e-10);
    }

    // Gauge: Set does NOT commute with Increment/Decrement
    #[test]
    fn gauge_set_noncommutative_with_increment() {
        let key = Key::from_name("test");

        // [Increment(10.0), Set(25.0)]
        let mut acc1 = Accumulator::new();
        acc1.gauge(gauge_increment(key.clone(), 0, 10.0)).unwrap();
        acc1.gauge(gauge_set(key.clone(), 0, 25.0)).unwrap();

        // [Set(25.0), Increment(10.0)]
        let mut acc2 = Accumulator::new();
        acc2.gauge(gauge_set(key.clone(), 0, 25.0)).unwrap();
        acc2.gauge(gauge_increment(key.clone(), 0, 10.0)).unwrap();

        let v1 = acc1.get_gauge_value(&key, acc1.current_tick);
        let v2 = acc2.get_gauge_value(&key, acc2.current_tick);

        // Should differ: acc1 is 25.0, acc2 is 35.0
        assert!((v1 - 25.0).abs() < 1e-10);
        assert!((v2 - 35.0).abs() < 1e-10);
        assert!((v1 - v2).abs() > 1e-10);
    }

    // Tick advancement: values copy forward, no expiration
    #[test]
    fn tick_advancement_copies_forward() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        acc.counter(counter_increment(key.clone(), 0, 42)).unwrap();
        let value_before_advance = acc.get_counter_value(&key, acc.current_tick);

        acc.advance_tick();
        let value_after_advance = acc.get_counter_value(&key, acc.current_tick);

        assert_eq!(value_before_advance, 42);
        assert_eq!(value_after_advance, 42);
    }

    #[test]
    fn advancing_to_tick_intervals_makes_data_flushable() {
        let mut acc = Accumulator::new();

        assert!(acc.current_tick < INTERVALS as u64);

        while acc.current_tick < INTERVALS as u64 {
            acc.advance_tick();
        }

        assert!(acc.current_tick >= INTERVALS as u64);
    }

    // Test that the shutdown pattern (advance + flush loop) drains all data
    // without loss or duplication
    #[test]
    fn shutdown_pattern_drains_all_data() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Add data at various ticks
        acc.counter(counter_increment(key.clone(), 0, 10)).unwrap();

        for _i in 0..5 {
            acc.advance_tick();
        }

        acc.counter(counter_increment(key.clone(), 0, 20)).unwrap();

        for _i in 0..10 {
            acc.advance_tick();
        }

        acc.counter(counter_increment(key.clone(), 0, 30)).unwrap();

        // We're now at tick 15. Simulate shutdown pattern:
        // 1. Advance to tick 60 if not there yet
        while acc.current_tick < 60 {
            acc.advance_tick();
        }

        // 2. Flush in a loop 60 times
        let mut all_results = Vec::new();
        for _ in 0..60 {
            for item in acc.flush() {
                all_results.push(item);
            }
            acc.advance_tick();
        }

        // Extract ticks from results
        let ticks: Vec<u64> = all_results.iter().map(|(_, _, tick)| *tick).collect();

        // Count occurrences of each tick
        let mut tick_counts: FxHashMap<u64, usize> = FxHashMap::default();
        for tick in &ticks {
            *tick_counts.entry(*tick).or_insert(0) += 1;
        }

        // Verify no duplicates
        for (tick, count) in &tick_counts {
            assert_eq!(
                *count, 1,
                "tick {tick} appeared {count} times, expected 1. All ticks: {ticks:?}"
            );
        }

        // Verify we got at least some data (the three metrics we wrote)
        assert!(
            !all_results.is_empty(),
            "shutdown pattern should return data"
        );
    }

    // Test drain() iterator drains all data correctly
    #[test]
    fn drain_iterator_drains_all_data() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Add data at various ticks
        acc.counter(counter_increment(key.clone(), 0, 10)).unwrap();

        for _i in 0..5 {
            acc.advance_tick();
        }

        acc.counter(counter_increment(key.clone(), 0, 20)).unwrap();

        for _i in 0..10 {
            acc.advance_tick();
        }

        acc.counter(counter_increment(key.clone(), 0, 30)).unwrap();

        // Advance to INTERVALS so data becomes flushable
        while acc.current_tick < INTERVALS as u64 {
            acc.advance_tick();
        }

        // Test ExactSizeIterator
        let drain_iter = acc.drain();
        assert_eq!(drain_iter.len(), INTERVALS);

        // Collect all drained data
        let mut all_results = Vec::new();
        for (current_tick, metrics) in drain_iter {
            for (key, value, tick) in metrics {
                all_results.push((key, value, tick, current_tick));
            }
        }

        // Extract ticks from results
        let ticks: Vec<u64> = all_results.iter().map(|(_, _, tick, _)| *tick).collect();

        // Count occurrences of each tick
        let mut tick_counts: FxHashMap<u64, usize> = FxHashMap::default();
        for tick in &ticks {
            *tick_counts.entry(*tick).or_insert(0) += 1;
        }

        // Verify no duplicates
        for (tick, count) in &tick_counts {
            assert_eq!(
                *count, 1,
                "tick {tick} appeared {count} times, expected 1. All ticks: {ticks:?}"
            );
        }

        // Verify we got data
        assert!(!all_results.is_empty(), "drain should return data");

        // Verify current_tick increases monotonically in the results
        let current_ticks: Vec<u64> = all_results.iter().map(|(_, _, _, ct)| *ct).collect();
        for window in current_ticks.windows(2) {
            assert!(
                window[1] >= window[0],
                "current_tick should increase monotonically"
            );
        }
    }

    // Test tick_offset > 0 propagates to multiple intervals for counter
    #[test]
    fn counter_tick_offset_propagates_to_multiple_intervals() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Advance to tick 5
        for _ in 0..5 {
            acc.advance_tick();
        }

        // Write with tick_offset=2, should update intervals at ticks 5, 4, 3
        acc.counter(counter_increment(key.clone(), 2, 100)).unwrap();

        assert_eq!(acc.get_counter_value(&key, 5), 100);
        assert_eq!(acc.get_counter_value(&key, 4), 100);
        assert_eq!(acc.get_counter_value(&key, 3), 100);
        assert_eq!(acc.get_counter_value(&key, 2), 0);
    }

    // Test tick_offset >= 60 returns TickTooOld error for counter
    #[test]
    fn counter_tick_too_old() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        let result = acc.counter(counter_increment(key, 60, 100));
        assert!(matches!(result, Err(Error::TickTooOld { tick_offset: 60 })));
    }

    // Test tick_offset > current_tick returns FutureTick error for counter
    #[test]
    fn counter_future_tick() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Current tick is 0, try to write with tick_offset=1
        let result = acc.counter(counter_increment(key.clone(), 1, 100));
        assert!(matches!(result, Err(Error::FutureTick { tick_offset: 1 })));

        // Advance to tick 5, try tick_offset=6
        for _ in 0..5 {
            acc.advance_tick();
        }
        let result = acc.counter(counter_increment(key, 6, 100));
        assert!(matches!(result, Err(Error::FutureTick { tick_offset: 6 })));
    }

    // Test tick_offset > 0 propagates to multiple intervals for gauge
    #[test]
    fn gauge_tick_offset_propagates_to_multiple_intervals() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Advance to tick 5
        for _ in 0..5 {
            acc.advance_tick();
        }

        // Write with tick_offset=2, should update intervals at ticks 5, 4, 3
        acc.gauge(gauge_set(key.clone(), 2, 42.0)).unwrap();

        assert!((acc.get_gauge_value(&key, 5) - 42.0).abs() < 1e-10);
        assert!((acc.get_gauge_value(&key, 4) - 42.0).abs() < 1e-10);
        assert!((acc.get_gauge_value(&key, 3) - 42.0).abs() < 1e-10);
        assert!((acc.get_gauge_value(&key, 2) - 0.0).abs() < 1e-10);
    }

    // Test tick_offset >= 60 returns TickTooOld error for gauge
    #[test]
    fn gauge_tick_too_old() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        let result = acc.gauge(gauge_set(key, 60, 42.0));
        assert!(matches!(result, Err(Error::TickTooOld { tick_offset: 60 })));
    }

    // Test tick_offset > current_tick returns FutureTick error for gauge
    #[test]
    fn gauge_future_tick() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Current tick is 0, try to write with tick_offset=1
        let result = acc.gauge(gauge_set(key.clone(), 1, 42.0));
        assert!(matches!(result, Err(Error::FutureTick { tick_offset: 1 })));

        // Advance to tick 5, try tick_offset=6
        for _ in 0..5 {
            acc.advance_tick();
        }
        let result = acc.gauge(gauge_set(key, 6, 42.0));
        assert!(matches!(result, Err(Error::FutureTick { tick_offset: 6 })));
    }

    // Test flush includes all values, including zeros
    #[test]
    fn flush_includes_zero_values() {
        let key1 = Key::from_name("counter_zero");
        let key2 = Key::from_name("counter_nonzero");
        let key3 = Key::from_name("gauge_zero");
        let key4 = Key::from_name("gauge_nonzero");
        let mut acc = Accumulator::new();

        // Write zero and non-zero values
        acc.counter(counter_absolute(key1.clone(), 0, 0)).unwrap();
        acc.counter(counter_absolute(key2.clone(), 0, 100)).unwrap();
        acc.gauge(gauge_set(key3.clone(), 0, 0.0)).unwrap();
        acc.gauge(gauge_set(key4.clone(), 0, 42.0)).unwrap();

        // Advance to INTERVALS so data becomes flushable
        while acc.current_tick < INTERVALS as u64 {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();

        // All values should be present, including zeros
        assert_eq!(results.len(), 4);

        let keys: Vec<_> = results.iter().map(|(k, _, _)| k.name()).collect();
        assert!(keys.contains(&"counter_zero"));
        assert!(keys.contains(&"counter_nonzero"));
        assert!(keys.contains(&"gauge_zero"));
        assert!(keys.contains(&"gauge_nonzero"));

        // Verify zero values are actually returned
        for (key, value, _tick) in results {
            match key.name() {
                "counter_zero" => assert!(matches!(value, MetricValue::Counter(0))),
                "counter_nonzero" => assert!(matches!(value, MetricValue::Counter(100))),
                "gauge_zero" => {
                    if let MetricValue::Gauge(v) = value {
                        assert!((v - 0.0).abs() < 1e-10);
                    } else {
                        panic!("Expected gauge value");
                    }
                }
                "gauge_nonzero" => {
                    if let MetricValue::Gauge(v) = value {
                        assert!((v - 42.0).abs() < 1e-10);
                    } else {
                        panic!("Expected gauge value");
                    }
                }
                _ => panic!("Unexpected key"),
            }
        }
    }

    // Test flush handles multiple keys correctly
    #[test]
    fn flush_handles_multiple_keys() {
        let key1 = Key::from_name("counter1");
        let key2 = Key::from_name("counter2");
        let key3 = Key::from_name("gauge1");
        let mut acc = Accumulator::new();

        // Write to multiple keys
        acc.counter(counter_increment(key1.clone(), 0, 10)).unwrap();
        acc.counter(counter_increment(key2.clone(), 0, 20)).unwrap();
        acc.gauge(gauge_set(key3.clone(), 0, 3.14)).unwrap();

        // Advance to INTERVALS so data becomes flushable
        while acc.current_tick < INTERVALS as u64 {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();

        assert_eq!(results.len(), 3);

        // Verify all keys are present
        let keys: Vec<_> = results.iter().map(|(k, _, _)| k.name()).collect();
        assert!(keys.contains(&"counter1"));
        assert!(keys.contains(&"counter2"));
        assert!(keys.contains(&"gauge1"));

        // Verify values
        for (key, value, _tick) in results {
            match key.name() {
                "counter1" => {
                    assert!(matches!(value, MetricValue::Counter(10)));
                }
                "counter2" => {
                    assert!(matches!(value, MetricValue::Counter(20)));
                }
                "gauge1" => {
                    if let MetricValue::Gauge(v) = value {
                        assert!((v - 3.14).abs() < 1e-10);
                    } else {
                        panic!("Expected gauge value");
                    }
                }
                _ => panic!("Unexpected key"),
            }
        }
    }

    // Test interval wrapping beyond INTERVALS
    #[test]
    fn interval_wrapping_beyond_intervals() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Write at tick 0
        acc.counter(counter_increment(key.clone(), 0, 100)).unwrap();

        // Advance beyond INTERVALS
        for _ in 0..70 {
            acc.advance_tick();
        }

        // Write again - should wrap around to reuse intervals
        acc.counter(counter_increment(key.clone(), 0, 50)).unwrap();

        // Value at current tick should be forwarded value (100) + new increment (50)
        assert_eq!(acc.get_counter_value(&key, acc.current_tick), 150);

        // Verify interval_idx wraps correctly
        assert_eq!(interval_idx(0), interval_idx(60));
        assert_eq!(interval_idx(1), interval_idx(61));
        assert_eq!(interval_idx(59), interval_idx(119));
    }

    // Test absolute counter write to historical tick
    #[test]
    fn counter_absolute_with_tick_offset() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Advance to tick 10
        for _ in 0..10 {
            acc.advance_tick();
        }

        // Write absolute value with tick_offset=3
        acc.counter(counter_absolute(key.clone(), 3, 500)).unwrap();

        // Ticks 10, 9, 8, 7 should all have value 500
        assert_eq!(acc.get_counter_value(&key, 10), 500);
        assert_eq!(acc.get_counter_value(&key, 9), 500);
        assert_eq!(acc.get_counter_value(&key, 8), 500);
        assert_eq!(acc.get_counter_value(&key, 7), 500);
        assert_eq!(acc.get_counter_value(&key, 6), 0);
    }

    // Test gauge increment with tick_offset
    #[test]
    fn gauge_operations_with_tick_offset() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Advance to tick 5
        for _ in 0..5 {
            acc.advance_tick();
        }

        // Increment with tick_offset=2
        acc.gauge(gauge_increment(key.clone(), 2, 10.0)).unwrap();
        // Decrement with tick_offset=1
        acc.gauge(gauge_decrement(key.clone(), 1, 3.0)).unwrap();

        // Tick 5: +10.0 -3.0 = 7.0
        assert!((acc.get_gauge_value(&key, 5) - 7.0).abs() < 1e-10);
        // Tick 4: +10.0 -3.0 = 7.0
        assert!((acc.get_gauge_value(&key, 4) - 7.0).abs() < 1e-10);
        // Tick 3: +10.0 = 10.0
        assert!((acc.get_gauge_value(&key, 3) - 10.0).abs() < 1e-10);
        // Tick 2: nothing
        assert!((acc.get_gauge_value(&key, 2) - 0.0).abs() < 1e-10);
    }
}
