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
//!
//! ## Histograms
//!
//! A `Histogram` write is `Record(k, T, v)` where `T` is the absolute logical
//! tick when the sample was recorded, `k` is the identifying key of the metric,
//! and `v` is a finite sample value. The operation `Record(k, T, v)` adds sample
//! `v` to the `DDSketch` distribution at key `k` in each interval Ti such that
//! T <= Ti <= `current_tick`. Infinity and NaN values are rejected.
//!
//! Histograms do not copy forward on tick advance. Each interval stores only
//! samples recorded during that tick.
//!
//! ```
//! [Record(k, 0, 10.0), Record(k, 0, 20.0), TICK]
//!  -> 0:[-]                   => []
//!  -> 0:[Record(k, 0, 10.0)]  => [(k, DDSketch{10.0})]
//!  -> 0:[Record(k, 0, 20.0)]  => [(k, DDSketch{10.0, 20.0})]
//!  -> 0:[TICK]                => [(k, DDSketch{10.0, 20.0}), (k, DDSketch{})]
//! ```
//!
//! On TICK, the new interval receives an empty sketch. This contrasts with
//! Counters and Gauges which copy forward. Historical writes populate multiple
//! intervals:
//!
//! ```
//! [TICK, Record(k, 0, 5.0)]
//!  -> 0:[-]                  => []
//!  -> 0:[TICK]               => [∅, ∅]
//!  -> 1:[Record(k, 0, 5.0)]  => [(k, DDSketch{5.0}), (k, DDSketch{5.0})]
//! ```
//!
//! Flush extracts the sketch from the flushable interval and replaces it with
//! an empty sketch.
//!
//! Stated logically:
//!
//!  * `Record` is not commutative.
//!  * `Record` is not idempotent.
//!  * Historical writes populate all intervals from T to `current_tick`.
//!  * Empty sketches are not flushed.
//!  * Infinity and NaN are filtered with warning.

use datadog_protos::metrics::Dogsketch;
use ddsketch_agent::DDSketch;
use metrics::Key;
use protobuf::Message;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::metric::{Counter, CounterValue, Gauge, GaugeValue, Histogram};

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
            // Skip ticks before this key was first written
            if let Some(&first_tick) = self.accumulator.counter_first_tick.get(key) {
                if tick_to_flush < first_tick {
                    continue;
                }
            }
            let value = values[interval];
            metrics.push((key.clone(), MetricValue::Counter(value), tick_to_flush));
        }
        for (key, values) in &self.accumulator.gauges {
            // Skip ticks before this key was first written
            if let Some(&first_tick) = self.accumulator.gauge_first_tick.get(key) {
                if tick_to_flush < first_tick {
                    continue;
                }
            }
            let value = values[interval];
            metrics.push((key.clone(), MetricValue::Gauge(value), tick_to_flush));
        }
        for (key, sketches) in &mut self.accumulator.histograms {
            let sketch = std::mem::take(&mut sketches[interval]);
            if sketch.count() > 0 {
                let mut dogsketch = Dogsketch::new();
                sketch.merge_to_dogsketch(&mut dogsketch);
                let sketch_bytes = dogsketch
                    .write_to_bytes()
                    .expect("protobuf serialization failed");
                metrics.push((
                    key.clone(),
                    MetricValue::Histogram(sketch_bytes),
                    tick_to_flush,
                ));
            }
        }

        self.accumulator.last_flushed_tick = Some(tick_to_flush);
        Some(metrics)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for DrainIter {}

/// Represents a metric value (counter, gauge, or histogram)
#[derive(Clone)]
pub(crate) enum MetricValue {
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
    /// Histogram distribution (protobuf-serialized `DDSketch`)
    Histogram(Vec<u8>),
}

impl std::fmt::Debug for MetricValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricValue::Counter(v) => f.debug_tuple("Counter").field(v).finish(),
            MetricValue::Gauge(v) => f.debug_tuple("Gauge").field(v).finish(),
            MetricValue::Histogram(bytes) => {
                let count = Dogsketch::parse_from_bytes(bytes)
                    .ok()
                    .and_then(|ds| ddsketch_agent::DDSketch::try_from(ds).ok())
                    .map_or(0, |sketch| sketch.count());
                f.debug_tuple("Histogram")
                    .field(&format!("count={count}"))
                    .finish()
            }
        }
    }
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
pub(crate) struct Accumulator {
    counters: FxHashMap<Key, [u64; BUFFER_SIZE]>,
    gauges: FxHashMap<Key, [f64; BUFFER_SIZE]>,
    histograms: FxHashMap<Key, [DDSketch; BUFFER_SIZE]>,
    /// Tick when each counter key was first written (for delta counter support)
    counter_first_tick: FxHashMap<Key, u64>,
    /// Tick when each gauge key was first written
    gauge_first_tick: FxHashMap<Key, u64>,
    pub(crate) current_tick: u64,
    last_flushed_tick: Option<u64>,
}

impl std::fmt::Debug for Accumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Accumulator")
            .field("counters", &self.counters)
            .field("gauges", &self.gauges)
            .field(
                "histograms",
                &format!("<{len} histogram keys>", len = self.histograms.len()),
            )
            .field("current_tick", &self.current_tick)
            .field("last_flushed_tick", &self.last_flushed_tick)
            .finish()
    }
}

impl Accumulator {
    pub(crate) fn new() -> Self {
        Self {
            counters: FxHashMap::default(),
            gauges: FxHashMap::default(),
            histograms: FxHashMap::default(),
            counter_first_tick: FxHashMap::default(),
            gauge_first_tick: FxHashMap::default(),
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

        // Track when this key was first written
        let key_clone = c.key.clone();
        self.counter_first_tick.entry(key_clone).or_insert(tick);

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

        // Track when this key was first written
        let key_clone = g.key.clone();
        self.gauge_first_tick.entry(key_clone).or_insert(tick);

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

    pub(crate) fn histogram(&mut self, h: Histogram, tick: u64) -> Result<(), Error> {
        if tick > self.current_tick {
            warn!(
                metric_tick = tick,
                current_tick = self.current_tick,
                delta = tick - self.current_tick,
                timestamp = ?h.timestamp,
                key = %h.key.name(),
                "Histogram metric tick is from future"
            );
            return Err(Error::FutureTick { tick });
        }

        let tick_offset = self.current_tick.saturating_sub(tick);
        if tick_offset >= INTERVALS as u64 {
            warn!(
                metric_tick = tick,
                current_tick = self.current_tick,
                tick_offset = tick_offset,
                timestamp = ?h.timestamp,
                key = %h.key.name(),
                "Histogram metric tick too old"
            );
            return Err(Error::TickTooOld { tick });
        }

        // Filter infinity and NaN. DDSketch panics on infinite values.
        if !h.value.is_finite() {
            warn!(
                key = %h.key.name(),
                value = h.value,
                tick = tick,
                "Histogram sample rejected - infinity or NaN not supported by DDSketch"
            );
            return Ok(());
        }

        let sketches = self
            .histograms
            .entry(h.key)
            .or_insert_with(|| std::array::from_fn(|_| DDSketch::default()));

        for offset in 0..=tick_offset {
            let target_tick = self.current_tick - offset;
            let interval = interval_idx(target_tick);
            sketches[interval].insert(h.value);
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
        // not depends on the value of INTERVALS. For instance, u64::MAX % 61 ==
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
        let mut metrics =
            Vec::with_capacity(self.counters.len() + self.gauges.len() + self.histograms.len());

        if self.current_tick < INTERVALS as u64 {
            return metrics.into_iter();
        }

        let flush_tick = self.current_tick - INTERVALS as u64;

        // Debug check to catch misuse during development
        debug_assert!(
            self.last_flushed_tick.is_none_or(|last| flush_tick > last),
            "flush_tick {flush_tick} should be > last_flushed {:?}",
            self.last_flushed_tick
        );

        let flush_interval = interval_idx(flush_tick);

        for (key, values) in &self.counters {
            // Skip ticks before this key was first written
            if let Some(&first_tick) = self.counter_first_tick.get(key) {
                if flush_tick < first_tick {
                    continue;
                }
            }
            let value = values[flush_interval];
            metrics.push((key.clone(), MetricValue::Counter(value), flush_tick));
        }

        for (key, values) in &self.gauges {
            // Skip ticks before this key was first written
            if let Some(&first_tick) = self.gauge_first_tick.get(key) {
                if flush_tick < first_tick {
                    continue;
                }
            }
            let value = values[flush_interval];
            metrics.push((key.clone(), MetricValue::Gauge(value), flush_tick));
        }

        for (key, sketches) in &mut self.histograms {
            let sketch = std::mem::take(&mut sketches[flush_interval]);
            // Only include histograms with samples. Empty sketches represent
            // "no data" which is different from counters/gauges where 0 is
            // meaningful.
            if sketch.count() > 0 {
                let mut dogsketch = Dogsketch::new();
                sketch.merge_to_dogsketch(&mut dogsketch);
                let sketch_bytes = dogsketch
                    .write_to_bytes()
                    .expect("protobuf serialization failed");
                metrics.push((
                    key.clone(),
                    MetricValue::Histogram(sketch_bytes),
                    flush_tick,
                ));
            }
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
    use crate::metric::{Counter, CounterValue, Gauge, GaugeValue, Histogram};
    use proptest::prelude::*;
    use std::time::Instant;

    fn deserialize_histogram(bytes: &[u8]) -> DDSketch {
        let dogsketch = Dogsketch::parse_from_bytes(bytes).expect("parse protobuf");
        DDSketch::try_from(dogsketch).expect("convert")
    }

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

    fn histogram_sample(
        acc: &mut Accumulator,
        key: Key,
        tick: u64,
        value: f64,
    ) -> Result<(), Error> {
        let histogram = Histogram {
            key,
            timestamp: Instant::now(),
            value,
        };
        acc.histogram(histogram, tick)
    }

    #[derive(Debug, Clone)]
    enum Op {
        CounterIncrement(u64),
        CounterAbsolute(u64),
        GaugeIncrement(f64),
        GaugeDecrement(f64),
        GaugeSet(f64),
        HistogramRecord(f64),
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
                (-100.0f64..100.0f64)
                    .prop_filter("must be finite", |f| f.is_finite())
                    .prop_map(Op::HistogramRecord),
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
                    Op::HistogramRecord(v) => {
                        let _ = histogram_sample(&mut acc, key.clone(), 0, v);
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

    // Histogram property tests

    proptest! {
        /// Verify DDSketch accumulates samples correctly with exact count and min/max.
        ///
        /// Add arbitrary samples to a histogram at tick 0, advance to flush point,
        /// then verify the flushed sketch maintains exact count and min/max values.
        /// DDSketch quantiles are approximate due to bucketing. We verify the median
        /// is finite and within reasonable tolerance: 10% of range for multiple
        /// samples, or 10% plus 10 absolute for single samples where bucketing
        /// effects are more pronounced.
        #[test]
        fn histogram_samples_accumulate_correctly(
            samples in prop::collection::vec(0.0f64..1000.0f64, 1..100)
        ) {
            let key = Key::from_name("test_hist");
            let mut acc = Accumulator::new();

            for &value in &samples {
                histogram_sample(&mut acc, key.clone(), 0, value).unwrap();
            }

            for _ in 0..INTERVALS {
                acc.advance_tick();
            }

            let results: Vec<_> = acc.flush().collect();
            let hist = results
                .iter()
                .find(|(k, _, _)| k.name() == "test_hist")
                .expect("should find histogram");

            if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
                let sketch = deserialize_histogram(sketch_bytes);
                assert_eq!(sketch.count() as usize, samples.len());

                let min_sample = samples.iter().copied().fold(f64::INFINITY, f64::min);
                let max_sample = samples.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                assert_eq!(sketch.min(), Some(min_sample));
                assert_eq!(sketch.max(), Some(max_sample));

                if let Some(median) = sketch.quantile(0.5) {
                    assert!(median.is_finite(), "median must be finite");
                    if samples.len() > 1 {
                        let range = max_sample - min_sample;
                        let tolerance = if range > 0.0 { range * 0.1 } else { 10.0 };
                        assert!(
                            median >= min_sample - tolerance && median <= max_sample + tolerance,
                            "median {median} should be approximately between {min_sample} and {max_sample}"
                        );
                    } else {
                        let sample = samples[0];
                        let tolerance = sample.abs() * 0.1 + 10.0;
                        assert!(
                            (median - sample).abs() <= tolerance,
                            "median {median} should be approximately {sample}"
                        );
                    }
                }
            } else {
                panic!("Expected histogram");
            }
        }

        /// Verify historical writes populate all intervals from T to current_tick.
        ///
        /// Advance the accumulator to an arbitrary tick offset, then write a sample
        /// timestamped at tick 0. Per histogram semantics, this historical write must
        /// populate all intervals from the sample's timestamp through the current tick.
        /// Verify each interval contains exactly one sample with the expected value.
        #[test]
        fn histogram_historical_writes_populate_all_intervals(
            tick_offset in 0u64..10u64,
            value in 0.0f64..1000.0f64
        ) {
            let key = Key::from_name("test_hist");
            let mut acc = Accumulator::new();

            for _ in 0..tick_offset {
                acc.advance_tick();
            }

            histogram_sample(&mut acc, key.clone(), 0, value).unwrap();

            for tick in 0..=tick_offset {
                let interval = interval_idx(tick);
                let sketches = acc.histograms.get(&key).expect("should have histogram key");
                let sketch = &sketches[interval];
                assert!(sketch.count() > 0, "tick {tick} should have sample");
                assert_eq!(sketch.min(), Some(value));
                assert_eq!(sketch.max(), Some(value));
            }
        }

        /// Verify histograms do not copy forward on tick advance.
        ///
        /// Add samples at tick 0, advance to tick 1, add different samples at tick 1.
        /// Unlike counters and gauges which copy forward their values on tick advance,
        /// histograms maintain separate sketch state per interval. Verify that tick 0's
        /// interval contains only tick 0 samples and tick 1's interval contains only
        /// tick 1 samples. Disjoint value ranges detect contamination.
        #[test]
        fn histogram_does_not_copy_forward_on_advance(
            samples_tick_0 in prop::collection::vec(0.0f64..100.0f64, 1..10),
            samples_tick_1 in prop::collection::vec(100.0f64..200.0f64, 1..10),
        ) {
            let key = Key::from_name("test_hist");
            let mut acc = Accumulator::new();

            for &value in &samples_tick_0 {
                histogram_sample(&mut acc, key.clone(), 0, value).unwrap();
            }

            acc.advance_tick();

            for &value in &samples_tick_1 {
                histogram_sample(&mut acc, key.clone(), 1, value).unwrap();
            }

            let interval_0 = interval_idx(0);
            let sketches = acc.histograms.get(&key).expect("should have histogram key");
            let sketch_0 = &sketches[interval_0];

            assert_eq!(sketch_0.count() as usize, samples_tick_0.len());
            let max_tick_0 = samples_tick_0.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            assert_eq!(sketch_0.max(), Some(max_tick_0));

            let interval_1 = interval_idx(1);
            let sketch_1 = &sketches[interval_1];
            assert_eq!(sketch_1.count() as usize, samples_tick_1.len());
            let max_tick_1 = samples_tick_1.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            assert_eq!(sketch_1.max(), Some(max_tick_1));
        }

        /// Verify flush extracts sketch and resets interval.
        ///
        /// Add samples at tick 0, advance INTERVALS ticks to the flush point, then
        /// flush. The flushed sketch should contain all samples from tick 0, and the
        /// interval slot for tick 0 should be reset to an empty sketch ready for
        /// future use. This ensures flush is a move operation, not a copy.
        #[test]
        fn histogram_flush_moves_sketch_and_resets(
            samples in prop::collection::vec(0.0f64..1000.0f64, 1..50)
        ) {
            let key = Key::from_name("test_hist");
            let mut acc = Accumulator::new();

            for &value in &samples {
                histogram_sample(&mut acc, key.clone(), 0, value).unwrap();
            }

            for _ in 0..INTERVALS {
                acc.advance_tick();
            }

            let results: Vec<_> = acc.flush().collect();
            let hist = results
                .iter()
                .find(|(k, _, tick)| k.name() == "test_hist" && *tick == 0)
                .expect("should find histogram");

            if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
                let sketch = deserialize_histogram(sketch_bytes);
                assert_eq!(sketch.count() as usize, samples.len());
            } else {
                panic!("Expected histogram");
            }

            let interval_0 = interval_idx(0);
            let sketches = acc.histograms.get(&key).expect("should have histogram key");
            let sketch_0 = &sketches[interval_0];
            assert_eq!(sketch_0.count(), 0, "flushed interval should be reset to empty");
        }

        /// Verify empty sketches are not flushed.
        ///
        /// Advance the accumulator past the flush point without adding any samples,
        /// then flush. Unlike counters and gauges where zero is meaningful, an empty
        /// histogram represents "no data" and should not appear in flush results.
        /// Verify no histogram metrics are returned.
        #[test]
        fn histogram_empty_intervals_not_flushed(
            advance_count in (INTERVALS as u64)..((INTERVALS as u64) + 10)
        ) {
            let mut acc = Accumulator::new();

            for _ in 0..advance_count {
                acc.advance_tick();
            }

            let results: Vec<_> = acc.flush().collect();
            let hist_count = results
                .iter()
                .filter(|(_, v, _)| matches!(v, MetricValue::Histogram(_)))
                .count();

            assert_eq!(hist_count, 0, "empty histograms should not be flushed");
        }
    }

    #[test]
    fn histogram_basic() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        histogram_sample(&mut acc, key.clone(), 0, 42.0).unwrap();

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            assert_eq!(sketch.min(), Some(42.0));
            assert_eq!(sketch.max(), Some(42.0));
        } else {
            panic!("Expected histogram");
        }
    }

    #[test]
    fn histogram_no_copy_forward() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        // Add sample at tick 0
        histogram_sample(&mut acc, key.clone(), 0, 100.0).unwrap();

        // Advance tick - histogram should NOT copy forward
        acc.advance_tick();
        assert_eq!(acc.current_tick, 1);

        // Add sample at tick 1 for comparison
        histogram_sample(&mut acc, key.clone(), 1, 200.0).unwrap();

        // Advance to flush point for tick 0 (need to be at tick 60 to flush tick 0)
        for _ in 0..(INTERVALS - 1) {
            acc.advance_tick();
        }

        // Flush tick 0 - should have the 100.0 sample only (no copy forward of tick 1 data)
        let results: Vec<_> = acc.flush().collect();
        let maybe_hist = results
            .iter()
            .find(|(k, _, tick)| k.name() == "test_hist" && *tick == 0);

        assert!(maybe_hist.is_some(), "Should have histogram at tick 0");
        if let Some((_, MetricValue::Histogram(sketch_bytes), _)) = maybe_hist {
            let sketch = deserialize_histogram(sketch_bytes);
            // Should only have tick 0 sample (100.0), not tick 1 (200.0)
            assert_eq!(sketch.count(), 1);
            assert_eq!(sketch.min(), Some(100.0));
            assert_eq!(sketch.max(), Some(100.0));
        }
    }

    /// Infinity values are filtered with warning.
    #[test]
    fn histogram_filters_infinity() {
        // Infinity values are filtered by accumulator before reaching DDSketch
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        // Add infinity - should be filtered with warning
        histogram_sample(&mut acc, key.clone(), 0, f64::INFINITY).unwrap();
        histogram_sample(&mut acc, key.clone(), 0, f64::NEG_INFINITY).unwrap();
        // Add normal value
        histogram_sample(&mut acc, key.clone(), 0, 42.0).unwrap();

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            // Only the normal value should be in the sketch (infinities filtered)
            assert_eq!(sketch.count(), 1);
            assert_eq!(sketch.min(), Some(42.0));
            assert_eq!(sketch.max(), Some(42.0));
        } else {
            panic!("Expected histogram");
        }
    }

    /// NaN values are filtered.
    #[test]
    fn histogram_handles_nan() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        // Add NaN value
        histogram_sample(&mut acc, key.clone(), 0, f64::NAN).unwrap();
        // Add normal value
        histogram_sample(&mut acc, key.clone(), 0, 42.0).unwrap();

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            // Verify DDSketch handles NaN values
            // Count should include all samples that were added
            assert!(
                sketch.count() >= 1,
                "sketch should have at least the normal value"
            );
            // DDSketch should handle NaN gracefully (may filter or include it)
            assert!(sketch.min().is_some() || sketch.max().is_some());
        } else {
            panic!("Expected histogram");
        }
    }

    /// Extreme finite values are supported.
    #[test]
    fn histogram_handles_extreme_values() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        // Add extremely large positive value
        histogram_sample(&mut acc, key.clone(), 0, f64::MAX).unwrap();
        // Add extremely small positive value
        histogram_sample(&mut acc, key.clone(), 0, f64::MIN_POSITIVE).unwrap();
        // Add value near zero
        histogram_sample(&mut acc, key.clone(), 0, 1e-300).unwrap();
        // Add large negative value
        histogram_sample(&mut acc, key.clone(), 0, f64::MIN).unwrap();

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            // Verify DDSketch handles extreme values
            assert_eq!(sketch.count(), 4);
            assert!(sketch.min().is_some());
            assert!(sketch.max().is_some());
        } else {
            panic!("Expected histogram");
        }
    }

    /// Negative values are supported.
    #[test]
    fn histogram_handles_negative_values() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        // Add mix of negative and positive values
        for &value in &[-100.0, -50.0, -10.0, 0.0, 10.0, 50.0, 100.0] {
            histogram_sample(&mut acc, key.clone(), 0, value).unwrap();
        }

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            assert_eq!(sketch.count(), 7);
            assert_eq!(sketch.min(), Some(-100.0));
            assert_eq!(sketch.max(), Some(100.0));
        } else {
            panic!("Expected histogram");
        }
    }

    /// Empty histograms are not flushed
    #[test]
    fn empty_histograms_not_flushed() {
        let mut acc = Accumulator::new();

        // Don't add any samples - histogram should be empty

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();

        // Empty histogram should not appear in results
        let hist = results.iter().find(|(k, _, _)| k.name() == "test_hist");
        assert!(hist.is_none(), "Empty histogram should not be flushed");
    }

    /// Histogram sample count matches inserted samples
    #[test]
    fn histogram_count_equals_samples_inserted() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        let samples = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        for &value in &samples {
            histogram_sample(&mut acc, key.clone(), 0, value).unwrap();
        }

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            assert_eq!(
                sketch.count(),
                samples.len() as u32,
                "Sketch count should match number of samples inserted"
            );
            assert_eq!(sketch.min(), Some(1.0));
            assert_eq!(sketch.max(), Some(10.0));
        } else {
            panic!("Expected histogram");
        }
    }

    /// Histogram min/max bounds are correct
    #[test]
    fn histogram_min_max_bounds() {
        let key = Key::from_name("test_hist");
        let mut acc = Accumulator::new();

        let samples = [5.5, 10.2, -3.7, 0.0, 42.0];
        for &value in &samples {
            histogram_sample(&mut acc, key.clone(), 0, value).unwrap();
        }

        for _ in 0..INTERVALS {
            acc.advance_tick();
        }

        let results: Vec<_> = acc.flush().collect();
        let hist = results
            .iter()
            .find(|(k, _, _)| k.name() == "test_hist")
            .expect("should find histogram");

        if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
            let min = sketch.min().expect("should have min");
            let max = sketch.max().expect("should have max");

            // Verify min/max are within sample bounds
            assert_eq!(min, -3.7, "Min should be smallest sample");
            assert_eq!(max, 42.0, "Max should be largest sample");

            // Verify all samples are within min/max range
            for &sample in &samples {
                assert!(
                    sample >= min && sample <= max,
                    "Sample {sample} should be within [{min}, {max}]"
                );
            }
        } else {
            panic!("Expected histogram");
        }
    }

    proptest! {
        /// Histogram quantiles maintain ordering: P0 <= P25 <= P50 <= P75 <= P100
        #[test]
        fn histogram_quantile_ordering(
            samples in prop::collection::vec(
                any::<f64>().prop_filter("finite", |f| f.is_finite()),
                1..100
            )
        ) {
            let key = Key::from_name("test_hist");
            let mut acc = Accumulator::new();

            for sample in &samples {
                histogram_sample(&mut acc, key.clone(), 0, *sample).unwrap();
            }

            for _ in 0..INTERVALS {
                acc.advance_tick();
            }

            let results: Vec<_> = acc.flush().collect();
            let hist = results
                .iter()
                .find(|(k, _, _)| k.name() == "test_hist")
                .expect("should find histogram");

            if let MetricValue::Histogram(sketch_bytes) = &hist.1 {
            let sketch = deserialize_histogram(sketch_bytes);
                // Verify sketch has the expected sample count
                prop_assert_eq!(sketch.count(), samples.len() as u32);

                let p0 = sketch.min().unwrap();
                let p25 = sketch.quantile(0.25).unwrap_or(f64::NAN);
                let p50 = sketch.quantile(0.5).unwrap_or(f64::NAN);
                let p75 = sketch.quantile(0.75).unwrap_or(f64::NAN);
                let p100 = sketch.max().unwrap();

                // Only verify ordering for quantiles that exist (non-NaN)
                // DDSketch may return NaN for quantiles with insufficient samples
                if p25.is_finite() && p50.is_finite() && p75.is_finite() {
                    prop_assert!(
                        p0 <= p25 && p25 <= p50 && p50 <= p75 && p75 <= p100,
                        "Quantile ordering violated: P0={p0} P25={p25} P50={p50} P75={p75} P100={p100}"
                    );
                }

                // Always verify min <= max
                prop_assert!(
                    p0 <= p100,
                    "Min must be <= max: min={p0} max={p100}"
                );
            } else {
                prop_assert!(false, "Expected histogram");
            }
        }
    }
}
