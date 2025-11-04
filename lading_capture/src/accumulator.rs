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
//! every tick the [`Accumulator`] creates a new 0th interval where 'now' writes
//! are stored. The previous 0th interval becomes the 1st, the 1st the 2nd and
//! so forth. This is called a 'roll'. After 60 ticks the 60th tick becomes
//! 'flushable', that is, the metrics stored in that interval will be returned
//! to the caller should they be requested. Tick time advances independently of
//! flushes and metrics are _not_ stored to the 61st interval. [`Accumulator`]
//! supports a `drain` operation that consumes the structure, allowing for
//! metrics to be exfiltrated on shutdown without delay.
//
//! # Semantics
//!
//! The [`Accumulator`] accepts writes to any absolute tick `T` such that:
//!
//! * `T <= current_tick` (not in the future)
//! * `current_tick - T < 60` (within the 60-interval window)
//!
//! Writes do not expire. When a tick time advances data in the previous
//! interval is copied forward into the new interval.
//!
//! ## Counters
//!
//! A `Counter` write may be either `Increment(k, T, i)` or `Absolute(k, T, i)`,
//! where `T` is the absolute logical tick when the event occurred, `k` is the
//! identifying key of the metric, and `i` is the counter value (`i >= 0`).
//!
//! The operation `Increment(k, T, i)` sums `i` to the value of `k` in each
//! interval Ti such that T <= Ti <= `current_tick`, or sets the value to `i` if
//! there is no value in the interval for `k`. The operation `Absolute(k, T, i)`
//! sets `i` as the value of `k` in each interval Ti such that T <= Ti <=
//! `current_tick`.
//!
//! As a matter of state notation let's adopt a notation for discussing the
//! evolution of Accumulator state. In what follows we'll describe two
//! increments to `k` in the same tick `0` with no tick advancement:
//!
//! ```
//! [Increment(k, 0, 10), Increment(k, 0, 100)]
//!  -> 0:[-]                    => []
//!  -> 0:[Increment(k, 0, 10)]  => [(k, 10)]
//!  -> 0:[Increment(k, 0, 100)] => [(k, 110)]
//! ```
//!
//! In the above we begin with empty state -- signaled by `0:[-] => []` -- and
//! have two increments at time 0 to k, first of 10 and then
//! 100. `0:[Increment(k, 0, 10)]` denotes the operation `Increment(k, 0, 10)`
//! being applied to the state at tick 0, resulting in state `[(k, 10)]`. We
//! allow a further operation `TICK` which advances the tick interval.
//!
//! ```
//! [Increment(k, 0, 10), TICK, Increment(k, 0, 100)]
//!  -> 0:[-]                    => []
//!  -> 0:[Increment(k, 0, 10)]  => [(k, 10)]
//!  -> 0:[TICK]                 => [(k, 10), (k, 10)]
//!  -> 1:[Increment(k, 0, 100)] => [(k, 110), (k, 110)]]
//! ```
//!
//! Let's examine the mixture of absolute and increment writes. For instance, an
//! increment and absolute write to the same `k`:
//!
//! ```
//! [Increment(k, 0, 10), Absolute(k, 2, 100)]
//!  -> 2:[-]                    => []
//!  -> 2:[Increment(k, 0, 10)]  => [(k, 10), (k, 10), (k, 10)]
//!  -> 2:[Absolute(k, 2, 100)]  => [(k, 10), (k, 10), (k, 100)]
//! ```
//!
//! Absolute writes overwrite any value previously set to `k` in an
//! interval. But, what if the order of writes were different?
//!
//! ```
//! [Absolute(k, 2, 100), Increment(k, 0, 10)]
//!  -> 2:[-]                    => []
//!  -> 2:[Absolute(k, 2, 100)]  => [∅, ∅, (k, 100)]
//!  -> 2:[Increment(k, 0, 10)]  => [(k, 10), (k, 10), (k, 110)]
//! ```
//!
//! Two separate outcomes! The `Accumulator` does not admit concurrent writes
//! and so we analyze writes in serial. The order of writes therefore matters
//! very much in the determination of state. Stated logically:
//!
//!  * `Increment` is associative and commutative.
//!  * `Absolute` is idempotent.
//!  * `Absolute` does not commute with `Increment`.
//!
//! ## Gauges
//!
//! A `Gauge` write may be either `Increment(k, T, i)` or `Decrement(k, T, i)`
//! or `Set(k, T, i)`. The semantic considerations of `Counter` discussed above
//! largely apply to `Gauge`. Loss of associativity compared to `Counter` is a
//! result of the interior f64. Logically:
//!
//!  * `Increment` is commutative.
//!  * `Decrement` is commutative.
//!  * `Increment` and `Decrement` commute.
//!  * `Set` is idempotent.
//!  * `Set` does not commute with `Increment` nor `Decrement`.

use metrics::Key;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::metric::{Counter, CounterValue, Gauge, GaugeValue};

pub(crate) const INTERVALS: usize = 60;
// The actual buffer size is INTERVALS + 1 to prevent accidental overwrite when
// we have exactly INTERVALS unflushed ticks. This extra slot acts as a guard to
// prevent the write-advance-flush pattern from overwriting unflushed data.
const BUFFER_SIZE: usize = INTERVALS + 1;

#[inline]
#[allow(clippy::cast_possible_truncation)]
fn interval_idx(tick: u64) -> usize {
    // Use BUFFER_SIZE for modulo to utilize the extra guard slot
    (tick % (BUFFER_SIZE as u64)) as usize
}

/// Errors produced by [`Accumulator`]
#[derive(thiserror::Error, Debug, Copy, Clone)]
pub enum Error {
    /// Metric tick is too old
    #[error("Tick for metric too old: {tick}")]
    TickTooOld { tick: u64 },
    /// Metric tick is from the future
    #[error("Tick for metric from future: {tick}")]
    FutureTick { tick: u64 },
}

/// Iterator that drains all accumulated metrics during shutdown.
///
/// Each iteration flushes metrics from a single interval until no intervals
/// remain.
pub(crate) struct DrainIter {
    accumulator: Accumulator,
    remaining: usize,
}

impl Iterator for DrainIter {
    type Item = Vec<(Key, MetricValue, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        // Calculate which tick to flush based on what's already been flushed
        let tick_to_flush = if let Some(last) = self.accumulator.last_flushed_tick {
            last + 1
        } else {
            0
        };

        // Directly gather metrics from the tick's interval without going
        // through flush(). flush/advance_tick are intended as a pair of
        // operations and one should not be used without the other.
        let mut metrics = Vec::new();
        let interval = interval_idx(tick_to_flush);

        for (key, values) in &self.accumulator.counters {
            let value = values[interval];
            metrics.push((key.clone(), MetricValue::Counter(value), tick_to_flush));
        }
        for (key, values) in &self.accumulator.gauges {
            let value = values[interval];
            metrics.push((key.clone(), MetricValue::Gauge(value), tick_to_flush));
        }

        self.accumulator.last_flushed_tick = Some(tick_to_flush);
        Some(metrics)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for DrainIter {}

/// Represents a metric value (counter or gauge)
#[derive(Debug, Clone)]
pub(crate) enum MetricValue {
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
}

/// Accumulator with 60-interval rolling window for metrics
///
/// # Critical Design Constraints
///
/// This accumulator uses a circular buffer with `BUFFER_SIZE` (61) slots to
/// store INTERVALS (60) ticks worth of data. The extra slot prevents overwrite
/// when exactly INTERVALS ticks are unflushed.
///
/// This means:
/// - Only the most recent 60 ticks of data can be stored at any time.
/// - When tick N is written, it goes to slot N % 61.
/// - The extra slot ensures the write-advance-flush pattern doesn't overwrite
///   data.
#[derive(Debug)]
pub(crate) struct Accumulator {
    counters: FxHashMap<Key, [u64; BUFFER_SIZE]>,
    gauges: FxHashMap<Key, [f64; BUFFER_SIZE]>,
    pub(crate) current_tick: u64,
    last_flushed_tick: Option<u64>,
}

impl Accumulator {
    pub(crate) fn new() -> Self {
        Self {
            counters: FxHashMap::default(),
            gauges: FxHashMap::default(),
            current_tick: 0,
            last_flushed_tick: None,
        }
    }

    pub(crate) fn counter(&mut self, c: Counter, tick: u64) -> Result<(), Error> {
        if tick > self.current_tick {
            warn!(
                metric_tick = tick,
                current_tick = self.current_tick,
                delta = tick - self.current_tick,
                timestamp = ?c.timestamp,
                key = %c.key.name(),
                "Counter metric tick is from future"
            );
            return Err(Error::FutureTick { tick });
        }

        let tick_offset = self.current_tick.saturating_sub(tick);
        if tick_offset >= INTERVALS as u64 {
            warn!(
                metric_tick = tick,
                current_tick = self.current_tick,
                tick_offset = tick_offset,
                timestamp = ?c.timestamp,
                key = %c.key.name(),
                "Counter metric tick too old"
            );
            return Err(Error::TickTooOld { tick });
        }

        let values = self.counters.entry(c.key).or_insert([0; BUFFER_SIZE]);

        for offset in 0..=tick_offset {
            let target_tick = self.current_tick - offset; // always lands within u64 range
            let interval = interval_idx(target_tick);

            match c.value {
                CounterValue::Absolute(v) => values[interval] = v,
                CounterValue::Increment(v) => values[interval] += v,
            }
        }

        Ok(())
    }

    pub(crate) fn gauge(&mut self, g: Gauge, tick: u64) -> Result<(), Error> {
        if tick > self.current_tick {
            warn!(
                metric_tick = tick,
                current_tick = self.current_tick,
                delta = tick - self.current_tick,
                timestamp = ?g.timestamp,
                key = %g.key.name(),
                "Gauge metric tick is from future"
            );
            return Err(Error::FutureTick { tick });
        }

        let tick_offset = self.current_tick.saturating_sub(tick);
        if tick_offset >= INTERVALS as u64 {
            warn!(
                metric_tick = tick,
                current_tick = self.current_tick,
                tick_offset = tick_offset,
                timestamp = ?g.timestamp,
                key = %g.key.name(),
                "Gauge metric tick too old"
            );
            return Err(Error::TickTooOld { tick });
        }

        let values = self.gauges.entry(g.key).or_insert([0.0; BUFFER_SIZE]);

        for offset in 0..=tick_offset {
            let target_tick = self.current_tick - offset; // always lands within u64 range
            let interval = interval_idx(target_tick);

            match g.value {
                GaugeValue::Set(v) => values[interval] = v,
                GaugeValue::Increment(v) => values[interval] += v,
                GaugeValue::Decrement(v) => values[interval] -= v,
            }
        }

        Ok(())
    }

    /// Advance tick and copy forward values
    ///
    /// This function is intended to be paired with calls to `flush`. It WILL
    /// overwrite data if data is not flushed in a timely fashion.
    pub(crate) fn advance_tick(&mut self) {
        let old_interval = interval_idx(self.current_tick);
        // WARNING: When we hit u64::MAX whether this wrapping_add is correct or
        // not depends on the value of INTERVALS. For instance, u64::MAX % 60 ==
        // 15 so when we wrap to 0 we'll index into the wrong spot. However if
        // an interval is 1 nanosecond we're looking at a continuous runtime of
        // ~500 years before this becomes a problem.
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
    pub(crate) fn flush(&mut self) -> impl Iterator<Item = (Key, MetricValue, u64)> + use<> {
        let mut metrics = Vec::new();

        if self.current_tick < INTERVALS as u64 {
            return metrics.into_iter();
        }

        let flush_tick = self.current_tick - INTERVALS as u64;

        // Assert we never flush the same tick twice
        if let Some(last_flushed) = self.last_flushed_tick {
            assert!(
                flush_tick > last_flushed,
                "Attempted to flush tick {flush_tick} but already flushed tick {last_flushed}"
            );
        }

        let flush_interval = interval_idx(flush_tick);

        for (key, values) in &self.counters {
            let value = values[flush_interval];
            metrics.push((key.clone(), MetricValue::Counter(value), flush_tick));
        }

        for (key, values) in &self.gauges {
            let value = values[flush_interval];
            metrics.push((key.clone(), MetricValue::Gauge(value), flush_tick));
        }

        self.last_flushed_tick = Some(flush_tick);
        metrics.into_iter()
    }

    /// Returns an iterator that drains all accumulated metrics.
    ///
    /// Only flushes ticks that haven't been flushed yet. If the most recent
    /// `flush()` call already wrote tick N, drain will start from tick N+1.
    pub(crate) fn drain(self) -> DrainIter {
        // Calculate how many unflushed ticks remain.
        //
        // After normal operation, current_tick points to the next tick that would
        // be written to. All ticks from 0 to (current_tick - 1) have data.
        let remaining = if let Some(last_flushed) = self.last_flushed_tick {
            // We need to flush from (last_flushed + 1) to (current_tick - 1)
            let next_to_flush = last_flushed + 1;
            let last_with_data = self.current_tick.saturating_sub(1);

            if next_to_flush > last_with_data {
                // All ticks have been flushed
                0
            } else {
                // Number of ticks to flush
                let to_flush = last_with_data - next_to_flush + 1;
                #[allow(clippy::cast_possible_truncation)]
                {
                    to_flush.min(INTERVALS as u64) as usize
                }
            }
        } else {
            // No flushes yet. We can flush all ticks that have data (0 to current_tick-1)
            // Capped at INTERVALS since that's all we can store
            #[allow(clippy::cast_possible_truncation)]
            {
                self.current_tick.min(INTERVALS as u64) as usize
            }
        };

        DrainIter {
            accumulator: self,
            remaining,
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
    use crate::metric::{Counter, CounterValue, Gauge, GaugeValue};
    use proptest::prelude::*;
    use std::time::Instant;

    // Test helpers that handle the full counter/gauge operation
    fn counter_increment(
        acc: &mut Accumulator,
        key: Key,
        tick: u64,
        value: u64,
    ) -> Result<(), Error> {
        let counter = Counter {
            key,
            timestamp: Instant::now(),
            value: CounterValue::Increment(value),
        };
        acc.counter(counter, tick)
    }

    fn counter_absolute(
        acc: &mut Accumulator,
        key: Key,
        tick: u64,
        value: u64,
    ) -> Result<(), Error> {
        let counter = Counter {
            key,
            timestamp: Instant::now(),
            value: CounterValue::Absolute(value),
        };
        acc.counter(counter, tick)
    }

    fn gauge_set(acc: &mut Accumulator, key: Key, tick: u64, value: f64) -> Result<(), Error> {
        let gauge = Gauge {
            key,
            timestamp: Instant::now(),
            value: GaugeValue::Set(value),
        };
        acc.gauge(gauge, tick)
    }

    fn gauge_increment(
        acc: &mut Accumulator,
        key: Key,
        tick: u64,
        value: f64,
    ) -> Result<(), Error> {
        let gauge = Gauge {
            key,
            timestamp: Instant::now(),
            value: GaugeValue::Increment(value),
        };
        acc.gauge(gauge, tick)
    }

    fn gauge_decrement(
        acc: &mut Accumulator,
        key: Key,
        tick: u64,
        value: f64,
    ) -> Result<(), Error> {
        let gauge = Gauge {
            key,
            timestamp: Instant::now(),
            value: GaugeValue::Decrement(value),
        };
        acc.gauge(gauge, tick)
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
                        let _ = counter_increment(&mut acc, key.clone(), 0, v);
                    }
                    Op::CounterAbsolute(v) => {
                        let _ = counter_absolute(&mut acc, key.clone(), 0, v);
                    }
                    Op::GaugeIncrement(v) => {
                        let _ = gauge_increment(&mut acc, key.clone(), 0, v);
                    }
                    Op::GaugeDecrement(v) => {
                        let _ = gauge_decrement(&mut acc, key.clone(), 0, v);
                    }
                    Op::GaugeSet(v) => {
                        let _ = gauge_set(&mut acc, key.clone(), 0, v);
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
        counter_increment(&mut acc1, key.clone(), 0, 5).unwrap();
        counter_increment(&mut acc1, key.clone(), 0, 3).unwrap();

        // [Increment(3), Increment(5)]
        let mut acc2 = Accumulator::new();
        counter_increment(&mut acc2, key.clone(), 0, 3).unwrap();
        counter_increment(&mut acc2, key.clone(), 0, 5).unwrap();

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
        counter_absolute(&mut acc1, key.clone(), 0, 100).unwrap();

        // [Absolute(100), Absolute(100)]
        let mut acc2 = Accumulator::new();
        counter_absolute(&mut acc2, key.clone(), 0, 100).unwrap();
        counter_absolute(&mut acc2, key.clone(), 0, 100).unwrap();

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
        counter_increment(&mut acc1, key.clone(), 0, 10).unwrap();
        counter_absolute(&mut acc1, key.clone(), 0, 50).unwrap();

        // [Absolute(50), Increment(10)]
        let mut acc2 = Accumulator::new();
        counter_absolute(&mut acc2, key.clone(), 0, 50).unwrap();
        counter_increment(&mut acc2, key.clone(), 0, 10).unwrap();

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
        gauge_increment(&mut acc1, key.clone(), 0, 10.0).unwrap();
        gauge_decrement(&mut acc1, key.clone(), 0, 3.0).unwrap();

        // [Decrement(3.0), Increment(10.0)]
        let mut acc2 = Accumulator::new();
        gauge_decrement(&mut acc2, key.clone(), 0, 3.0).unwrap();
        gauge_increment(&mut acc2, key.clone(), 0, 10.0).unwrap();

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
        gauge_set(&mut acc1, key.clone(), 0, 50.0).unwrap();

        // [Set(50.0), Set(50.0)]
        let mut acc2 = Accumulator::new();
        gauge_set(&mut acc2, key.clone(), 0, 50.0).unwrap();
        gauge_set(&mut acc2, key.clone(), 0, 50.0).unwrap();

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
        gauge_increment(&mut acc1, key.clone(), 0, 10.0).unwrap();
        gauge_set(&mut acc1, key.clone(), 0, 25.0).unwrap();

        // [Set(25.0), Increment(10.0)]
        let mut acc2 = Accumulator::new();
        gauge_set(&mut acc2, key.clone(), 0, 25.0).unwrap();
        gauge_increment(&mut acc2, key.clone(), 0, 10.0).unwrap();

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

        counter_increment(&mut acc, key.clone(), 0, 42).unwrap();
        let value_before_advance = acc.get_counter_value(&key, acc.current_tick);

        acc.advance_tick();
        let value_after_advance = acc.get_counter_value(&key, acc.current_tick);

        assert_eq!(value_before_advance, 42);
        assert_eq!(value_after_advance, 42);
    }

    // Test that the Accumulator is not flushable until INTERVAL ticks have
    // passed.
    #[test]
    fn advancing_to_tick_intervals_makes_data_flushable() {
        let key1 = Key::from_name("counter");
        let key2 = Key::from_name("gauge");
        let mut acc = Accumulator::new();

        counter_increment(&mut acc, key1.clone(), 0, 42).unwrap();
        gauge_set(&mut acc, key2.clone(), 0, 3.14).unwrap();

        assert!(
            acc.flush().count() == 0,
            "flush should return no data before INTERVALS ticks"
        );
        while acc.current_tick < (INTERVALS as u64 - 1) {
            acc.advance_tick();
        }
        assert!(
            acc.flush().count() == 0,
            "flush should return no data at INTERVALS-1 ticks"
        );

        // Advance one more tick to reach INTERVALS, accumulator is now
        // flushable
        acc.advance_tick();
        assert_eq!(acc.current_tick, INTERVALS as u64);

        let results: Vec<_> = acc.flush().collect();
        assert_eq!(results.len(), 2, "flush should return both metrics");

        // Verify the returned data
        for (key, value, tick) in results {
            assert_eq!(tick, 0, "flushed data should be from tick 0");
            match key.name() {
                "counter" => {
                    assert!(matches!(value, MetricValue::Counter(42)));
                }
                "gauge" => {
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

    // Test that the shutdown pattern (advance + flush loop) drains all data
    // without loss or duplication
    #[test]
    fn shutdown_pattern_drains_all_data() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Add data at various ticks
        counter_increment(&mut acc, key.clone(), 0, 10).unwrap();

        for _i in 0..5 {
            acc.advance_tick();
        }

        counter_increment(&mut acc, key.clone(), 0, 20).unwrap();

        for _i in 0..10 {
            acc.advance_tick();
        }

        counter_increment(&mut acc, key.clone(), 0, 30).unwrap();

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

    // Test that drain() after normal flushes doesn't re-flush ticks
    #[test]
    fn drain_after_normal_flush_no_duplicates() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Simulate normal operation: write and flush repeatedly
        for i in 0..100 {
            counter_increment(&mut acc, key.clone(), i, i + 1).unwrap();
            acc.advance_tick();

            // Once we have INTERVALS ticks, start flushing
            if acc.current_tick >= INTERVALS as u64 {
                let _ = acc.flush().collect::<Vec<_>>();
            }
        }

        // At this point we've flushed many ticks during normal operation
        // Now simulate shutdown: drain remaining data
        let mut all_ticks = Vec::new();
        for metrics in acc.drain() {
            for (_, _, tick) in metrics {
                all_ticks.push(tick);
            }
        }

        // Count occurrences - should have no duplicates
        let mut tick_counts: FxHashMap<u64, usize> = FxHashMap::default();
        for tick in &all_ticks {
            *tick_counts.entry(*tick).or_insert(0) += 1;
        }

        for (tick, count) in &tick_counts {
            assert_eq!(
                *count, 1,
                "tick {tick} appeared {count} times during drain, expected 1 (no re-flushes)"
            );
        }

        // Verify we got some data
        assert!(!all_ticks.is_empty(), "drain should return unflushed data");
    }

    // Test drain() iterator drains all data correctly
    #[test]
    fn drain_iterator_drains_all_data() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Add data at various ticks
        counter_increment(&mut acc, key.clone(), 0, 10).unwrap();

        for _i in 0..5 {
            acc.advance_tick();
        }

        counter_increment(&mut acc, key.clone(), 0, 20).unwrap();

        for _i in 0..10 {
            acc.advance_tick();
        }

        counter_increment(&mut acc, key.clone(), 0, 30).unwrap();

        // Advance to INTERVALS so data becomes flushable
        while acc.current_tick < INTERVALS as u64 {
            acc.advance_tick();
        }

        // Test ExactSizeIterator
        let drain_iter = acc.drain();
        assert_eq!(drain_iter.len(), INTERVALS);

        // Collect all drained data
        let mut all_results = Vec::new();
        for metrics in drain_iter {
            for (key, value, tick) in metrics {
                all_results.push((key, value, tick));
            }
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

        // Verify we got data
        assert!(!all_results.is_empty(), "drain should return data");
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

        // Write event from tick 3, should update intervals at ticks 3, 4, 5
        counter_increment(&mut acc, key.clone(), 3, 100).unwrap();

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

        // Advance to tick 60
        for _ in 0..60 {
            acc.advance_tick();
        }
        assert_eq!(acc.current_tick, 60);

        // Try to write to tick 0 (60 ticks ago, exactly at boundary)
        let result = counter_increment(&mut acc, key, 0, 100);
        assert!(matches!(result, Err(Error::TickTooOld { tick: 0 })));
    }

    // Test tick_offset > current_tick returns FutureTick error for counter
    #[test]
    fn counter_future_tick() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Current tick is 0, try to write with tick=1 (future)
        let result = counter_increment(&mut acc, key.clone(), 1, 100);
        assert!(matches!(result, Err(Error::FutureTick { tick: 1 })));

        // Advance to tick 5, try tick=6 (future)
        for _ in 0..5 {
            acc.advance_tick();
        }
        let result = counter_increment(&mut acc, key, 6, 100);
        assert!(matches!(result, Err(Error::FutureTick { tick: 6 })));
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

        // Write event from tick 3, should update intervals at ticks 3, 4, 5
        gauge_set(&mut acc, key.clone(), 3, 42.0).unwrap();

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

        // Advance to tick 60
        for _ in 0..60 {
            acc.advance_tick();
        }
        assert_eq!(acc.current_tick, 60);

        // Try to write to tick 0 (60 ticks ago, exactly at boundary)
        let result = gauge_set(&mut acc, key, 0, 42.0);
        assert!(matches!(result, Err(Error::TickTooOld { tick: 0 })));
    }

    // Test tick_offset > current_tick returns FutureTick error for gauge
    #[test]
    fn gauge_future_tick() {
        let key = Key::from_name("test");
        let mut acc = Accumulator::new();

        // Current tick is 0, try to write with tick=1 (future)
        let result = gauge_set(&mut acc, key.clone(), 1, 42.0);
        assert!(matches!(result, Err(Error::FutureTick { tick: 1 })));

        // Advance to tick 5, try tick=6 (future)
        for _ in 0..5 {
            acc.advance_tick();
        }
        let result = gauge_set(&mut acc, key, 6, 42.0);
        assert!(matches!(result, Err(Error::FutureTick { tick: 6 })));
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
        counter_absolute(&mut acc, key1.clone(), 0, 0).unwrap();
        counter_absolute(&mut acc, key2.clone(), 0, 100).unwrap();
        gauge_set(&mut acc, key3.clone(), 0, 0.0).unwrap();
        gauge_set(&mut acc, key4.clone(), 0, 42.0).unwrap();

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
        counter_increment(&mut acc, key1.clone(), 0, 10).unwrap();
        counter_increment(&mut acc, key2.clone(), 0, 20).unwrap();
        gauge_set(&mut acc, key3.clone(), 0, 3.14).unwrap();

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
        counter_increment(&mut acc, key.clone(), 0, 100).unwrap();

        // Advance beyond INTERVALS
        for _ in 0..70 {
            acc.advance_tick();
        }

        // Write again at current tick (70) - intervals wrap, so interval[70%61=9] gets reused
        counter_increment(&mut acc, key.clone(), 70, 50).unwrap();

        // Value at current tick should be forwarded value (100) + new increment (50)
        assert_eq!(acc.get_counter_value(&key, acc.current_tick), 150);

        // Verify interval_idx wraps correctly with 61-slot buffer
        // Now indices wrap at 61, not 60
        assert_eq!(interval_idx(0), interval_idx(61));
        assert_eq!(interval_idx(1), interval_idx(62));
        assert_eq!(interval_idx(60), interval_idx(121));
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

        // Write absolute value from tick 7
        counter_absolute(&mut acc, key.clone(), 7, 500).unwrap();

        // Ticks 7, 8, 9, 10 should all have value 500
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

        // Increment from tick 3
        gauge_increment(&mut acc, key.clone(), 3, 10.0).unwrap();
        // Decrement from tick 4
        gauge_decrement(&mut acc, key.clone(), 4, 3.0).unwrap();

        // Tick 5: +10.0 -3.0 = 7.0
        assert!((acc.get_gauge_value(&key, 5) - 7.0).abs() < 1e-10);
        // Tick 4: +10.0 -3.0 = 7.0
        assert!((acc.get_gauge_value(&key, 4) - 7.0).abs() < 1e-10);
        // Tick 3: +10.0 = 10.0
        assert!((acc.get_gauge_value(&key, 3) - 10.0).abs() < 1e-10);
        // Tick 2: nothing
        assert!((acc.get_gauge_value(&key, 2) - 0.0).abs() < 1e-10);
    }

    // Test documentation example 1:
    // [Increment(k, 0, 10), Increment(k, 0, 100)]
    //  -> 0:[-]                    => []
    //  -> 0:[Increment(k, 0, 10)]  => [(k, 10)]
    //  -> 0:[Increment(k, 0, 100)] => [(k, 110)]
    #[test]
    fn doc_example_1_two_increments_same_tick() {
        let key = Key::from_name("k");
        let mut acc = Accumulator::new();

        // Current tick is 0
        assert_eq!(acc.current_tick, 0);

        // Increment(k, 0, 10)
        counter_increment(&mut acc, key.clone(), 0, 10).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 10);

        // Increment(k, 0, 100)
        counter_increment(&mut acc, key.clone(), 0, 100).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 110);
    }

    // Test documentation example 2:
    // [Increment(k, 0, 10), TICK, Increment(k, 0, 100)]
    //  -> 0:[-]                    => []
    //  -> 0:[Increment(k, 0, 10)]  => [(k, 10)]
    //  -> 0:[TICK]                 => [(k, 10), (k, 10)]
    //  -> 1:[Increment(k, 0, 100)] => [(k, 110), (k, 110)]
    #[test]
    fn doc_example_2_increment_tick_historical_increment() {
        let key = Key::from_name("k");
        let mut acc = Accumulator::new();

        // Current tick is 0
        assert_eq!(acc.current_tick, 0);

        // Increment(k, 0, 10)
        counter_increment(&mut acc, key.clone(), 0, 10).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 10);

        // TICK
        acc.advance_tick();
        assert_eq!(acc.current_tick, 1);
        assert_eq!(acc.get_counter_value(&key, 0), 10);
        assert_eq!(acc.get_counter_value(&key, 1), 10);

        // Increment(k, 0, 100) - historical write to tick 0
        counter_increment(&mut acc, key.clone(), 0, 100).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 110);
        assert_eq!(acc.get_counter_value(&key, 1), 110);
    }

    // Test documentation example 3:
    // [Increment(k, 0, 10), Absolute(k, 2, 100)]
    //  -> 2:[-]                    => []
    //  -> 2:[Increment(k, 0, 10)]  => [(k, 10), (k, 10), (k, 10)]
    //  -> 2:[Absolute(k, 2, 100)]  => [(k, 10), (k, 10), (k, 100)]
    #[test]
    fn doc_example_3_increment_then_absolute() {
        let key = Key::from_name("k");
        let mut acc = Accumulator::new();

        // Advance to tick 2
        acc.advance_tick();
        acc.advance_tick();
        assert_eq!(acc.current_tick, 2);

        // Increment(k, 0, 10) - writes to ticks [0, 1, 2]
        counter_increment(&mut acc, key.clone(), 0, 10).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 10);
        assert_eq!(acc.get_counter_value(&key, 1), 10);
        assert_eq!(acc.get_counter_value(&key, 2), 10);

        // Absolute(k, 2, 100) - writes to tick [2]
        counter_absolute(&mut acc, key.clone(), 2, 100).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 10);
        assert_eq!(acc.get_counter_value(&key, 1), 10);
        assert_eq!(acc.get_counter_value(&key, 2), 100);
    }

    // Test documentation example 4:
    // [Absolute(k, 2, 100), Increment(k, 0, 10)]
    //  -> 2:[-]                    => []
    //  -> 2:[Absolute(k, 2, 100)]  => [∅, ∅, (k, 100)]
    //  -> 2:[Increment(k, 0, 10)]  => [(k, 10), (k, 10), (k, 110)]
    #[test]
    fn doc_example_4_absolute_then_increment() {
        let key = Key::from_name("k");
        let mut acc = Accumulator::new();

        // Advance to tick 2
        acc.advance_tick();
        acc.advance_tick();
        assert_eq!(acc.current_tick, 2);

        // Absolute(k, 2, 100) - writes to tick [2]
        counter_absolute(&mut acc, key.clone(), 2, 100).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 0);
        assert_eq!(acc.get_counter_value(&key, 1), 0);
        assert_eq!(acc.get_counter_value(&key, 2), 100);

        // Increment(k, 0, 10) - writes to ticks [0, 1, 2]
        counter_increment(&mut acc, key.clone(), 0, 10).unwrap();
        assert_eq!(acc.get_counter_value(&key, 0), 10);
        assert_eq!(acc.get_counter_value(&key, 1), 10);
        assert_eq!(acc.get_counter_value(&key, 2), 110);
    }
}
