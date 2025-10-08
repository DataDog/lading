//! Time-series for capture data.

// This module provides an in-memory time-series database that buffers metric
// points for periodic flushing. Much like the way logrotate_fs works this
// implementation does not handle time directly, it is passed in from the
// outside world. This allows the implementation to be conveniently property
// tested.
//
// Unlike logrotate_fs the caller does not pass 'now' at every operation. Time
// is advanced by the `flush` operation. Actions on this State are discrete
// whereas logrotate_fs is continuous, simplifying this implementation.
//
// Operations on indexes are saturating. We presume that 1 Tick ~= 1 second,
// meaning this implemenation will misbehave after 2**64 seconds of continuous
// operation. We strongly suspect that the self-consumption of the sun will
// intercede to terminate this program prior to that point.

#![allow(dead_code)] // These will be used when integrated with captures

use std::fmt;

use ustr::Ustr;

pub(crate) const MAX_LABELS: u8 = 12;
pub(crate) const MAX_INTERVALS: u64 = 60;
pub(crate) type Tick = u64;

/// A single metric point.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Point {
    Counter {
        name: Ustr,
        labels: LabelSet,
        value: u64,
    },
    Gauge {
        name: Ustr,
        labels: LabelSet,
        value: f64,
    },
}

/// Manually implement Debug to avoid splatting the `LabelSet` out in tests if
/// it's otherwise empty.
impl fmt::Debug for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Point::Counter {
                name,
                labels,
                value,
            } => f
                .debug_struct("Counter")
                .field("name", name)
                .field("labels", &format_args!("{}", DebugLabelSet(labels)))
                .field("value", value)
                .finish(),
            Point::Gauge {
                name,
                labels,
                value,
            } => f
                .debug_struct("Gauge")
                .field("name", name)
                .field("labels", &format_args!("{}", DebugLabelSet(labels)))
                .field("value", value)
                .finish(),
        }
    }
}

struct DebugLabelSet<'a>(&'a LabelSet);

impl std::fmt::Display for DebugLabelSet<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(k, v)| format!("{k}={v}")))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct LabelSet {
    /// Fixed-size array of label pairs.
    labels: [(Option<Ustr>, Option<Ustr>); MAX_LABELS as usize],
    /// Number of labels currently stored.
    count: u8,
}

impl LabelSet {
    /// Create a new empty label set.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Add a label key-value pair.
    ///
    /// # Errors
    ///
    /// Returns error if already at max capacity.
    pub(crate) fn add(&mut self, key: &str, value: &str) -> Result<(), LabelError> {
        if self.count >= MAX_LABELS {
            return Err(LabelError::TooManyLabels);
        }
        let idx = self.count as usize;
        self.labels[idx] = (Some(Ustr::from(key)), Some(Ustr::from(value)));
        self.count += 1;
        Ok(())
    }

    /// Iterate over label pairs.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Ustr, &Ustr)> + '_ {
        self.labels[..self.count as usize]
            .iter()
            .filter_map(|(k, v)| match (k, v) {
                (Some(key), Some(val)) => Some((key, val)),
                _ => None,
            })
    }
}

/// A time interval containing points.
#[derive(Debug, Clone)]
struct Interval<T> {
    /// Points stored in this interval.
    points: Vec<T>,
}

#[inline]
fn mapped_idx(idx: u64) -> usize {
    (idx % MAX_INTERVALS) as usize
}

/// The time-series database.
///
/// This is a pure model with no internal time concepts. All progression
/// happens through external tick advances. The model maintains a fixed-size
/// ring buffer of `MAX_INTERVALS` intervals.
#[derive(Debug)]
pub(crate) struct State<T> {
    intervals: Vec<Interval<T>>,
    /// Current write position in the ring buffer. This is also 'now', that is, the
    /// current tick.
    write_idx: u64, // intentionally not usize to avoid indexing with this value
    /// Current read position in the ring buffer. Always `<= write_idx`.
    read_idx: u64,
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> State<T> {
    #[must_use]
    pub(crate) fn new() -> Self {
        let mut intervals = Vec::with_capacity(MAX_INTERVALS as usize);
        for _ in 0..MAX_INTERVALS {
            intervals.push(Interval {
                points: Vec::with_capacity(8), // magic constant, lowish but not too low
            });
        }

        Self {
            intervals,
            write_idx: 0,
            read_idx: 0,
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn now(&self) -> Tick {
        self.write_idx
    }

    /// Add a point to the current interval.
    #[inline]
    pub(crate) fn add_point(&mut self, point: T) {
        self.add_historical_point(point, self.now())
            .expect("must not fail, catastrophic programming error");
    }

    /// Add a historical point to a specific tick.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Tick is in the future (tick > now)
    /// - Tick is too old (more than MAX_INTERVALS-1 ticks ago)
    pub(crate) fn add_historical_point(&mut self, point: T, tick: Tick) -> Result<(), StateError> {
        // Determine that `tick` is within the read and write index interval,
        // meaning that the historical point is neither too old or too in the
        // future.
        assert!(self.read_idx <= self.write_idx);
        if tick > self.write_idx {
            return Err(StateError::TickInFuture);
        }
        if tick < self.read_idx {
            return Err(StateError::TickTooOld);
        }
        if self.write_idx > tick && self.write_idx - tick > MAX_INTERVALS {
            return Err(StateError::TickTooOld);
        }

        let target_idx = mapped_idx(tick);
        let interval: &mut Interval<T> = &mut self.intervals[target_idx];
        interval.points.push(point);
        Ok(())
    }

    /// Flush the currently expiring interval into the provided buffer.
    ///
    /// Returns the tick if data was flushed, or None if no intervals have
    /// expired. The caller's buffer receives the drained points from the
    /// interval.
    ///
    /// Advances the read index to the write index.
    ///
    /// Passed `buffer` must be cleared by the caller.
    pub(crate) fn flush(&mut self, buffer: &mut Vec<T>) -> Option<Tick> {
        // Flushing advances the write_idx by 1. When write_idx - read_idx >
        // MAX_INTERVALS the interval at read_idx will be flushed into buffer
        // and then read_idx will be advanced.
        //
        // Properties:
        //
        // * (write_idx - read_idx) > MAX_INTERVALS (pre/post condition)
        // * write_idx - returned(TICK) == MAX_INTERVALS
        self.write_idx = self.write_idx.saturating_add(1);

        if self.write_idx - self.read_idx == MAX_INTERVALS {
            let interval = &mut self.intervals[mapped_idx(self.read_idx)];
            buffer.append(&mut interval.points);
            let tick = self.read_idx;
            self.read_idx = self.read_idx.saturating_add(1);
            return Some(tick);
        }
        None
    }

    /// Flush all intervals with data into the provided buffer.
    ///
    /// Consumes the State and drains all intervals in order (oldest to newest)
    /// into the buffer. This function is only intended to be called on
    /// shutdown. `State` is consumed by this operation to prevent mis-use.
    ///
    /// Passed `buffer` must be cleared by the caller.
    pub(crate) fn flush_all(mut self, buffer: &mut Vec<T>) {
        while self.read_idx <= self.write_idx {
            let interval = &mut self.intervals[mapped_idx(self.read_idx)];
            buffer.append(&mut interval.points);
            self.read_idx = self.read_idx.saturating_add(1);
        }
    }
}

/// Errors that can occur in state operations.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq)]
pub(crate) enum StateError {
    /// Tick is too old for current window.
    #[error("Tick too old for current window")]
    TickTooOld,
    /// Tick is in the future.
    #[error("Tick is in the future")]
    TickInFuture,
}

/// Errors that can occur with labels.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq)]
pub(crate) enum LabelError {
    /// Too many labels
    #[error("Too many labels (max {MAX_LABELS})")]
    TooManyLabels,
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Proof: When adding a historical point at tick T to State, the point must
    /// be written to buffer slot mapped_idx(T) and no other.
    #[kani::proof]
    #[kani::unwind(65)]
    fn historical_point_correct_buffer_slot() {
        let mut state: State<u32> = State::new();
        let write_idx: u64 = kani::any();
        let read_idx: u64 = kani::any();

        // Constrain to valid State invariants.
        kani::assume(read_idx <= write_idx);
        kani::assume(write_idx - read_idx < MAX_INTERVALS);
        state.write_idx = write_idx;
        state.read_idx = read_idx;

        // Choose a tick in a valid range for historical points.
        let tick: u64 = kani::any();
        kani::assume(tick >= read_idx);
        kani::assume(tick <= write_idx);

        let point = kani::any();

        // Record the state of all buffer slots before adding the point,
        // calculate the interval the point should be going into.
        let mut was_empty = [false; MAX_INTERVALS as usize];
        for i in 0..MAX_INTERVALS as usize {
            was_empty[i] = state.intervals[i].points.is_empty();
        }
        let expected_interval = mapped_idx(tick);
        kani::assert(
            was_empty[expected_interval],
            "Interval {expected_interval} was not empty",
        );

        let result = state.add_historical_point(point, tick);
        kani::assert(
            result.is_ok(),
            "Addition of historical point should have been a success, was not",
        );
        if result.is_ok() {
            // Point should appear where we expect it.
            let point_in_expected_interval = !state.intervals[expected_interval].points.is_empty();
            kani::assert(
                point_in_expected_interval,
                "Point added at tick T must be in buffer interval mapped_idx(T)",
            );
            // Point should not appear anywhere else.
            for i in 0..MAX_INTERVALS as usize {
                if i != expected_interval && was_empty[i] {
                    kani::assert(
                        state.intervals[i].points.is_empty(),
                        "Point should only be in the expected slot",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    impl Arbitrary for LabelSet {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            // Generate between 0 and MAX_LABELS-1 labels to ensure we never overflow
            prop::collection::vec(("[a-z]{1,5}", "[a-z]{1,5}"), 0..(MAX_LABELS as usize - 1))
                .prop_map(|labels| {
                    let mut label_set = LabelSet::new();
                    for (k, v) in labels {
                        let _ = label_set.add(&k, &v);
                    }
                    label_set
                })
                .boxed()
        }
    }

    impl Arbitrary for Point {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                ("[a-z]{1,10}", any::<LabelSet>(), 0u64..1000u64).prop_map(
                    |(name, labels, value)| Point::Counter {
                        name: Ustr::from(&name),
                        labels,
                        value,
                    }
                ),
                ("[a-z]{1,10}", any::<LabelSet>(), 0f64..1000f64).prop_map(
                    |(name, labels, value)| Point::Gauge {
                        name: Ustr::from(&name),
                        labels,
                        value,
                    }
                ),
            ]
            .boxed()
        }
    }

    /// Operations that can be performed on the State.
    #[derive(Debug, Clone)]
    enum Operation {
        /// Add a point to current interval
        AddCurrent { point: Point },
        /// Add a historical point
        AddHistorical { point: Point, ticks_ago: u8 },
        /// Flush, if an interval is ready to be flushed
        Flush,
        /// Flush all intervals
        FlushAll,
    }

    impl Arbitrary for Operation {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                3 => any::<Point>().prop_map(|point| Operation::AddCurrent { point }),
                2 => Just(Operation::FlushAll),
                2 => (any::<Point>(), 0u8..70u8)
                    .prop_map(|(point, ticks_ago)| Operation::AddHistorical { point, ticks_ago }),
                1 => Just(Operation::Flush),
            ]
            .boxed()
        }
    }

    /// Context for tracking operation counts and data.
    struct Context {
        added_count: usize,
        flushed_count: usize,
        previous_tick: Option<Tick>,
        // Track all points we've added so we can verify they're preserved
        added_points: Vec<(Tick, Point)>,
        // Track all points we've flushed to verify against added_points
        flushed_points: Vec<(Tick, Point)>,
        // Track each flush operation separately to verify ordering
        flush_operations: Vec<Vec<(Tick, Point)>>,
        // Track flush operations with their returned tick and state.now() at flush time
        flush_metadata: Vec<(Tick, Tick)>, // (returned_tick, now_at_flush_time)
        // Track flush operations that returned None with state before flush
        flush_none_operations: Vec<(u64, u64)>, // (write_idx, read_idx) when None returned
        // Track AddHistorical operations that should have failed and did fail
        expected_historical_error_ticks: Vec<Tick>,
        actual_historical_error_ticks: Vec<Tick>,
    }

    /// Assert invariant properties of the State.
    ///
    /// This is called before and after every option to ensure the model
    /// maintains its properties. All properties of the model are asserted here.
    fn assert_properties(state: &State<Point>, ctx: &Context) {
        // Property 1: Ticks are monotonic. Time, managed by the caller, is never
        // mapped backward by this implementation.
        if let Some(prev) = ctx.previous_tick {
            assert!(
                state.now() >= prev,
                "Tick went backwards! Previous: {prev}, Current: {now}",
                now = state.now(),
            );
        }

        // Property 2: The write_idx is never more than MAX_INTERVALS apart from
        // the read_idx.
        assert!(
            state.write_idx - state.read_idx < MAX_INTERVALS,
            "{write_idx} - {read_idx} < {MAX_INTERVALS}",
            write_idx = state.write_idx,
            read_idx = state.read_idx,
        );

        // Property 2a: The write_idx is always greater or equal to read_idx.
        assert!(
            state.write_idx >= state.read_idx,
            "{write_idx} < {read_idx}",
            write_idx = state.write_idx,
            read_idx = state.read_idx,
        );

        // Property 3: For flushes that return Some(tick) that `tick` is such
        // that state.now() - tick == MAX_INTERVALS.
        for (returned_tick, now_at_flush) in &ctx.flush_metadata {
            assert!(
                now_at_flush - returned_tick == MAX_INTERVALS,
                "Flush returned tick {returned_tick} but now was {now_at_flush}, difference should be {MAX_INTERVALS} but was {diff}",
                diff = now_at_flush - returned_tick,
            );
        }

        // Property 4: Flush operations flush exactly the same number of points
        // that have previously been added to State.
        assert!(
            ctx.flushed_count <= ctx.added_count,
            "Flushed {flushed} points but only added {added}",
            flushed = ctx.flushed_count,
            added = ctx.added_count
        );

        // Property 5: Each flush operation returns points in tick order.
        for (op_idx, flush_op) in ctx.flush_operations.iter().enumerate() {
            let mut last_tick = 0;
            for (tick, _point) in flush_op {
                assert!(
                    *tick >= last_tick,
                    "Flush operation {op_idx} returned out of order: tick {tick} after {last_tick}",
                );
                assert!(
                    *tick <= state.now(),
                    "Flush operation {op_idx} returned future tick {tick} (now={now})",
                    now = state.now()
                );
                last_tick = *tick;
            }
        }

        // Property 6: All flushed points exactly match what was added.
        //
        // For each (tick, point) pair, verify multiplicity in flushed_points
        // doesn't exceed multiplicity in added_points.
        for (flushed_tick, flushed_point) in &ctx.flushed_points {
            let flushed_count = ctx
                .flushed_points
                .iter()
                .filter(|(t, p)| t == flushed_tick && p == flushed_point)
                .count();
            let added_count = ctx
                .added_points
                .iter()
                .filter(|(t, p)| t == flushed_tick && p == flushed_point)
                .count();
            assert!(
                flushed_count == added_count,
                "Point {flushed_point:?} at tick {flushed_tick} was flushed {flushed_count} times but only added {added_count} times",
            );
        }

        // Property 7: Points do not carry forward, they expire. That is, a
        // point is only present in the interval it is written to.
        //
        // We verify this by ensuring no (tick, point) pair appears in multiple
        // distinct flush operations. Since flush drains the interval, each
        // (tick, point) should appear in at most one flush operation.
        for (i, flush_op_i) in ctx.flush_operations.iter().enumerate() {
            for (tick_i, point_i) in flush_op_i {
                for (j, flush_op_j) in ctx.flush_operations.iter().enumerate().skip(i + 1) {
                    for (tick_j, point_j) in flush_op_j {
                        assert!(
                            !(tick_i == tick_j && point_i == point_j),
                            "Point {point_i:?} at tick {tick_i} appeared in flush operations {i} and {j} - points must not carry forward",
                        );
                    }
                }
            }
        }

        // Property 8: If flush operation returned None then write_idx -
        // read_idx < MAX_INTERVALS.
        for (write_idx, read_idx) in &ctx.flush_none_operations {
            assert!(
                write_idx - read_idx < MAX_INTERVALS,
                "Flush returned None but write_idx - read_idx = {diff} which is >= MAX_INTERVALS",
                diff = write_idx - read_idx,
            );
        }

        // Property 9: AddHistorical operations with invalid ticks are rejected.
        //
        // Operations that attempt to add points with ticks that are too old or
        // in the future must fail. Assert that the specific ticks which failed
        // match our expectations, in addition to a tally of them.
        assert!(
            ctx.actual_historical_error_ticks.len() == ctx.expected_historical_error_ticks.len(),
            "Expected {expected} historical errors but got {actual}",
            expected = ctx.expected_historical_error_ticks.len(),
            actual = ctx.actual_historical_error_ticks.len(),
        );
        for expected_tick in &ctx.expected_historical_error_ticks {
            let found = ctx.actual_historical_error_ticks.contains(expected_tick);
            assert!(
                found,
                "Expected tick {expected_tick} to fail but it did not",
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn state_operations(operations in prop::collection::vec(any::<Operation>(), 0..(MAX_INTERVALS*4) as usize)) {
            let mut state = State::new();
            let mut ctx = Context {
                added_count: 0,
                flushed_count: 0,
                previous_tick: None,
                added_points: Vec::new(),
                flushed_points: Vec::new(),
                flush_operations: Vec::new(),
                flush_metadata: Vec::new(),
                flush_none_operations: Vec::new(),
                expected_historical_error_ticks: Vec::new(),
                actual_historical_error_ticks: Vec::new(),
            };

            for op in operations {
                // Assert properties before operation
                assert_properties(&state, &ctx);

                ctx.previous_tick = Some(state.now());
                match op {
                    Operation::AddCurrent { point } => {
                        // State is initialized, tick 0 is valid
                        ctx.added_points.push((state.now(), point));
                        state.add_point(point);
                        ctx.added_count += 1;
                    }
                    Operation::AddHistorical { point, ticks_ago } => {
                        if state.now() >= u64::from(ticks_ago) {
                            let tick = state.now() - u64::from(ticks_ago);

                            // Determine if this should fail based on state
                            let should_fail = tick < state.read_idx || tick > state.write_idx;

                            let result = state.add_historical_point(point, tick);

                            if should_fail {
                                ctx.expected_historical_error_ticks.push(tick);
                                if result.is_err() {
                                    ctx.actual_historical_error_ticks.push(tick);
                                }
                            } else if result.is_ok() {
                                ctx.added_points.push((tick, point));
                                ctx.added_count += 1;
                            }
                        }
                    }
                    Operation::Flush => {
                        let mut buffer = Vec::new();
                        let prev_write_idx = state.write_idx;
                        let prev_read_idx = state.read_idx;

                        if let Some(tick) = state.flush(&mut buffer) {
                            let now_after_flush = state.now();
                            ctx.flush_metadata.push((tick, now_after_flush));
                            let mut flush_op = Vec::new();
                            for point in &buffer {
                                ctx.flushed_points.push((tick, *point));
                                flush_op.push((tick, *point));
                            }
                            ctx.flushed_count += buffer.len();
                            if !flush_op.is_empty() {
                                ctx.flush_operations.push(flush_op);
                            }
                        } else {
                            ctx.flush_none_operations.push((prev_write_idx, prev_read_idx));
                        }
                    }
                    Operation::FlushAll => {
                        let mut buffer = Vec::new();
                        state.flush_all(&mut buffer);

                        // flush_all doesn't provide tick information, so we
                        // just verify that all points were previously added.
                        for point in &buffer {
                            let was_added = ctx.added_points.iter().any(|(_, added_point)| added_point == point);
                            assert!(was_added, "Flushed point {point:?} was never added");
                        }
                        ctx.flushed_count += buffer.len();
                        return Ok(());
                    }
                }

                // Assert properties after operation
                assert_properties(&state, &ctx);
            }
        }


    }
}
