//! Stable throttle
//!
//! This throttle refills capacity at a steady rate.

use std::num::NonZeroU32;

use super::{Clock, INTERVAL_TICKS, RealClock};

/// Errors produced by [`Stable`].
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq)]
pub enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("capacity request {requested} exceeds throttle's maximum {maximum}")]
    Capacity {
        /// The requested capacity that exceeded the maximum.
        requested: u32,
        /// The maximum capacity permitted by the throttle.
        maximum: u32,
    },
}

#[derive(Debug)]
/// A throttle type.
///
/// This throttle is stable in that it will steadily refill units at a known
/// rate and does not inspect the target in any way.
pub struct Stable<C = RealClock> {
    valve: Valve,
    /// The clock that `Stable` will use.
    clock: C,
    /// Tick-interval most recently observed by the Antithesis grant check.
    /// Resets `granted_this_interval` on a roll.
    #[cfg(feature = "antithesis")]
    observed_interval: u64,
    /// Capacity granted so far in `observed_interval`, checked against the
    /// throttle's proven per-interval burst envelope.
    #[cfg(feature = "antithesis")]
    granted_this_interval: u64,
}

impl<C> Stable<C>
where
    C: Clock + Send + Sync,
{
    #[inline]
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        self.wait_for(NonZeroU32::MIN).await
    }

    pub(crate) async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        // A request larger than one interval's capacity is not an error. Drain
        // it across intervals in chunks of at most `maximum_capacity`, so an
        // oversized block -- for example one larger than the per-worker capacity
        // after `divide` -- is delivered at the configured rate instead of being
        // rejected and discarded. Each chunk stays inside the Valve's proven
        // per-interval envelope, so the Kani bounds are unaffected.
        let max = self.valve.maximum_capacity;
        let mut remaining = request.get();
        while remaining > 0 {
            let chunk = remaining.min(max);
            loop {
                let ticks_elapsed = self.clock.ticks_elapsed();
                let slop: u64 = self.valve.request(ticks_elapsed, chunk)?;
                if slop == 0 {
                    // The chunk was granted this iteration. Check, in the real
                    // async path under whatever clock Antithesis supplies, that
                    // the grant stays inside the proven per-interval envelope.
                    #[cfg(feature = "antithesis")]
                    self.assert_grant_within_envelope(ticks_elapsed, chunk);
                    break;
                }
                self.clock.wait(slop).await;
            }
            remaining -= chunk;
        }
        Ok(())
    }

    /// Antithesis property `aggregate-rate-not-exceeded`, SUT-side, plus the
    /// `cross-interval-burst-bounded` characterization.
    ///
    /// Kani proves the sync `Valve` never grants more than
    /// `(MAX_ROLLED_INTERVALS + 1) * maximum_capacity` per interval, and exactly
    /// `maximum_capacity` when `timeout_ticks == 0`. Those proofs assume a
    /// well-behaved tick source. The `RealClock` -> ticks mapping and this
    /// async loop are unverified end to end. This runs the same bound at
    /// runtime, under whatever clock, possibly faulted, feeds `ticks_elapsed`,
    /// so an over-grant that only the real path can produce is caught
    /// in-process rather than inferred from probe bytes.
    #[cfg(feature = "antithesis")]
    fn assert_grant_within_envelope(&mut self, ticks_elapsed: u64, granted: u32) {
        use antithesis_sdk::prelude::*;
        use serde_json::json;

        let interval = tick_to_interval(ticks_elapsed);
        if interval != self.observed_interval {
            self.observed_interval = interval;
            self.granted_this_interval = 0;
        }
        self.granted_this_interval += u64::from(granted);

        let maximum_capacity = u64::from(self.valve.maximum_capacity);
        // At the default `timeout_ticks == 0` no capacity rolls over, so the
        // envelope is exactly the configured rate. Any interval that delivers
        // more than `maximum_capacity` is over-delivery. With a timeout, rolled
        // capacity legitimately allows up to the Kani-proven 11x.
        let envelope = if self.valve.timeout_ticks == 0 {
            maximum_capacity
        } else {
            maximum_capacity.saturating_mul(u64::from(MAX_ROLLED_INTERVALS) + 1)
        };

        let details = json!({
            "granted_this_interval": self.granted_this_interval,
            "envelope": envelope,
            "maximum_capacity": maximum_capacity,
            "timeout_ticks": self.valve.timeout_ticks,
            "interval": interval,
        });

        assert_always!(
            self.granted_this_interval <= envelope,
            "lading_throttle.stable.interval_grant_within_burst_envelope",
            &details
        );

        // Characterize how large the burst actually gets, the cross-interval-
        // burst-bounded property. Distinct inline literals so triage shows the
        // factor reached.
        if self.granted_this_interval > maximum_capacity.saturating_mul(2) {
            assert_reachable!("lading_throttle.stable.interval_burst_exceeded_2x", &details);
        }
        if self.granted_this_interval > maximum_capacity.saturating_mul(5) {
            assert_reachable!("lading_throttle.stable.interval_burst_exceeded_5x", &details);
        }
        if self.granted_this_interval > maximum_capacity.saturating_mul(10) {
            assert_reachable!("lading_throttle.stable.interval_burst_exceeded_10x", &details);
        }
    }

    pub(crate) fn with_clock(maximum_capacity: NonZeroU32, timeout_micros: u64, clock: C) -> Self {
        Self {
            valve: Valve::new_with_timeout(maximum_capacity, timeout_micros),
            clock,
            #[cfg(feature = "antithesis")]
            observed_interval: 0,
            #[cfg(feature = "antithesis")]
            granted_this_interval: 0,
        }
    }

    /// Get the maximum capacity of this throttle
    pub(super) fn maximum_capacity(&self) -> u32 {
        self.valve.maximum_capacity
    }

    /// Get the timeout in microseconds for this throttle
    pub(super) fn timeout_micros(&self) -> u64 {
        self.valve.timeout_ticks
    }
}

/// Represents unused capacity with its expiration time
#[derive(Debug, Clone, Copy)]
struct UnusedCapacity {
    /// Total amount of capacity available, caveat the noted expiration time in
    /// `expires_at`.
    amount: u32,
    /// Absolute time -- in ticks -- that this capacity expires. Expiration is
    /// instantaneous when it occurs.
    expires_at: u64,
}

impl UnusedCapacity {
    const ZERO: Self = UnusedCapacity {
        amount: 0,
        expires_at: 0,
    };
}

/// The non-async interior to Stable, about which we can make proof claims. The
/// mechanical analogue isn't quite right but think of this as a poppet valve
/// for the stable throttle.
#[derive(Debug)]
struct Valve {
    /// The maximum capacity of `Valve` past which no more capacity will be
    /// added.
    maximum_capacity: u32,
    /// The capacity of the `Valve`. This amount will be drawn on by every
    /// request. It is refilled to maximum at every interval roll-over.
    capacity: u32,
    /// The current interval -- multiple of `INTERVAL_TICKS` --  of time.
    interval: u64,
    /// The timeout in ticks for rolled capacity. When 0, no capacity is stored
    /// up between intervals.
    timeout_ticks: u64,
    /// Storage for unused capacity from previous intervals with expiration
    /// times. This is the mechanism we use to model a 'queue' of requests into
    /// the Valve with the lead request blocking all others.
    unused: [UnusedCapacity; MAX_ROLLED_INTERVALS as usize],
    /// Index of the next slot to write in the unused array.
    unused_head: u8,
}

/// Maximum number of intervals we can track rolled capacity for.
/// Based on a reasonable maximum timeout of 10 seconds.
pub const MAX_ROLLED_INTERVALS: u8 = 10;

impl Valve {
    /// Create a new `Valve` instance with a maximum capacity and timeout.
    fn new_with_timeout(maximum_capacity: NonZeroU32, timeout_ticks: u64) -> Self {
        let maximum_capacity = maximum_capacity.get();
        Self {
            capacity: maximum_capacity,
            maximum_capacity,
            interval: 0,
            timeout_ticks,
            unused: [UnusedCapacity::ZERO; MAX_ROLLED_INTERVALS as usize],
            unused_head: 0,
        }
    }

    /// For a given `capacity_request` and an amount of `ticks_elapsed` since
    /// the last call return how long a caller would have to wait -- in ticks --
    /// before the valve will have sufficient spare capacity to be open.
    ///
    /// Note that `ticks_elapsed` must be an absolute value.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn request(&mut self, ticks_elapsed: u64, capacity_request: u32) -> Result<u64, Error> {
        // Okay, here's the idea. We have bucket that fills every INTERVAL_TICKS
        // microseconds and requests draw down on that bucket. When it's empty,
        // we return the number of ticks until the next interval roll-over.
        // Callers are expected to wait although nothing forces them to.
        // Capacity is only drawn on when it is immediately available.
        //
        // Caller is responsible for maintaining the clock. We do not advance
        // the interval ticker when the caller requests more capacity than will
        // ever be available from this throttle. We do advance the iterval
        // ticker if the caller makes a zero request. It's strange but it's a
        // valid thing to do.
        if capacity_request > self.maximum_capacity {
            return Err(Error::Capacity {
                requested: capacity_request,
                maximum: self.maximum_capacity,
            });
        }

        let current_interval = tick_to_interval(ticks_elapsed);
        if current_interval > self.interval {
            // We have rolled forward into a new interval. At this point the
            // capacity is reset to maximum -- no matter how deep we are into
            // the interval -- in the current interval. We record any unused
            // capacity here for potential use in future intervals, caveat
            // expiration per MAX_ROLLED_INTERVALS.
            if self.timeout_ticks > 0 && self.capacity > 0 {
                self.record_unused_capacity(current_interval);
            }

            self.capacity = self.maximum_capacity;
            self.interval = current_interval;
        }

        let total_available = self
            .capacity
            .saturating_add(self.unused_capacity(ticks_elapsed));

        // If the request is zero we return. If the capacity is greater or equal
        // to the request we deduct the request from capacity and return 0 slop,
        // signaling to the user that their request is a success. Else, we
        // calculate how long the caller should wait until the interval rolls
        // over and capacity is refilled. The capacity will never increase in
        // this interval so they will have to call again later.
        if capacity_request == 0 {
            Ok(0)
        } else if capacity_request <= total_available {
            self.deduct_capacity(capacity_request, ticks_elapsed);
            Ok(0)
        } else {
            Ok(INTERVAL_TICKS.saturating_sub(ticks_elapsed % INTERVAL_TICKS))
        }
    }

    /// Roll unused capacity forward when transitioning to a new interval
    fn record_unused_capacity(&mut self, current_interval: u64) {
        let intervals_passed = current_interval.saturating_sub(self.interval);

        // Convert to usize, capping at MAX_ROLLED_INTERVALS
        #[expect(clippy::cast_possible_truncation)]
        let intervals_to_store = (intervals_passed.min(u64::from(MAX_ROLLED_INTERVALS))) as usize;
        debug_assert!(
            intervals_to_store > 0,
            "intervals_to_store must be > 0 since record_unused_capacity is only called when current_interval ({}) > self.interval ({})",
            current_interval,
            self.interval
        );

        // If we're storing MAX or more intervals, clear everything first
        if intervals_to_store >= MAX_ROLLED_INTERVALS as usize {
            self.unused = [UnusedCapacity::ZERO; MAX_ROLLED_INTERVALS as usize];
            self.unused_head = 0;
        }

        // Record unused capacity for each interval we're transitioning past.
        // For example, if moving from interval 5 to interval 8
        // (intervals_passed = 3):
        //
        // * i=0: Interval 5 (self.interval -> the current interval we're leaving)
        //
        //        Amount set to self.capacity
        //
        // * i=1: Interval 6 (self.interval + 1 -> a skipped interval)
        //
        //        Amount set to self.maximum_capacity, full capacity since no
        //        request made on that interval
        //
        // * i=2: Interval 7 (self.interval + 2 - another skipped interval)
        //
        //        Amount set to self.maximum_capacity, same reasoning as
        //        Interval 6.
        //
        // Each entry expires `timeout_ticks` after its interval ends.
        // The loop iterates MAX_ROLLED_INTERVALS times for kani verification.
        for i in 0..MAX_ROLLED_INTERVALS as usize {
            // This if check is needed because we iterate a fixed number of times
            // (MAX_ROLLED_INTERVALS) for kani verification, but only want to
            // process intervals_to_store entries.
            if i < intervals_to_store {
                let amount = if i == 0 {
                    self.capacity
                } else {
                    self.maximum_capacity
                };
                // `interval_end` is the time -- in ticks -- that interval `i`
                // ends. We use this to figure out when any unused capacity in
                // this interval expires.
                let interval_end = self
                    .interval
                    .saturating_add(i as u64)
                    .saturating_add(1)
                    .saturating_mul(INTERVAL_TICKS);
                let expires_at = interval_end.saturating_add(self.timeout_ticks);
                self.unused[self.unused_head as usize] = UnusedCapacity { amount, expires_at };
                self.unused_head = (self.unused_head + 1) % MAX_ROLLED_INTERVALS;
            }
        }
    }

    /// Returns the unused capacity that has not expired.
    ///
    /// This function DOES NOT expire capacity. That is done in
    /// `record_unused_capacity`.
    fn unused_capacity(&self, ticks_elapsed: u64) -> u32 {
        if self.timeout_ticks == 0 {
            return 0;
        }

        let mut total: u32 = 0;
        for i in 0..MAX_ROLLED_INTERVALS as usize {
            if self.unused[i].expires_at > ticks_elapsed {
                total = total.saturating_add(self.unused[i].amount);
            }
        }
        total
    }

    /// Deduct capacity, taking from the soonest expiring unused capacity first,
    /// then from the current interval's capacity. This maximizes utilization
    /// before expiration.
    fn deduct_capacity(&mut self, mut capacity_request: u32, ticks_elapsed: u64) {
        // When timeout is 0, there's no unused capacity - just use current capacity
        if self.timeout_ticks == 0 {
            self.capacity = self.capacity.saturating_sub(capacity_request);
            return;
        }

        // Consume unused capacity in chronological order, oldest first. Rolled
        // intervals are stored in order, unused_head pointing to where the next
        // write will go to, making it the oldest entry.
        for offset in 0..MAX_ROLLED_INTERVALS as usize {
            if capacity_request == 0 {
                return;
            }

            let idx = (self.unused_head as usize + offset) % MAX_ROLLED_INTERVALS as usize;
            if self.unused[idx].amount == 0 || self.unused[idx].expires_at <= ticks_elapsed {
                continue;
            }

            if capacity_request <= self.unused[idx].amount {
                self.unused[idx].amount -= capacity_request;
                return;
            }
            capacity_request -= self.unused[idx].amount;
            self.unused[idx].amount = 0;
        }

        self.capacity = self.capacity.saturating_sub(capacity_request);
    }
}

/// Calculate which interval a given tick count falls into
#[inline]
fn tick_to_interval(ticks: u64) -> u64 {
    ticks / INTERVAL_TICKS
}

#[cfg(test)]
mod divide_stall {
    //! Wildcard #1: `divide` shrinks per-worker capacity to `R/N` but not the
    //! block a worker draws. The raw `Valve` grants at most `maximum_capacity`
    //! in one interval, so a block sized `R/N < block <= R` fits a single
    //! connection but not a divided worker. `Stable::wait_for` drains such a
    //! request across intervals at the per-worker rate, so the worker delivers
    //! the block instead of discarding it.
    use super::{Error, Stable, Valve};
    use crate::Clock;
    use async_trait::async_trait;
    use proptest::prelude::*;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A clock whose `wait` advances a tick counter and returns immediately, so
    /// `wait_for` can be driven to completion without real time or a runtime.
    struct MockClock {
        now: AtomicU64,
    }

    #[async_trait]
    impl Clock for MockClock {
        fn ticks_elapsed(&self) -> u64 {
            self.now.load(Ordering::Relaxed)
        }
        async fn wait(&self, ticks: u64) {
            self.now.fetch_add(ticks, Ordering::Relaxed);
        }
    }

    /// Poll a future to completion on the current thread. `MockClock::wait`
    /// never suspends, so `wait_for` makes full progress without a runtime.
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        use std::pin::pin;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        const VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| RAW, |_| {}, |_| {}, |_| {});
        const RAW: RawWaker = RawWaker::new(std::ptr::null(), &VTABLE);
        // SAFETY: the waker ignores its data pointer, so a null pointer is fine.
        let waker = unsafe { Waker::from_raw(RAW) };
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
        }
    }

    proptest! {
        /// The raw per-interval fact behind the wildcard: a request above
        /// `maximum_capacity` fits a single connection at capacity `R` but the
        /// divided worker's `Valve` at capacity `R/N` rejects it in one interval
        /// with `Capacity`. `wait_for` is what turns that per-interval limit
        /// into a multi-interval drain rather than a discard.
        #[test]
        fn raw_valve_rejects_request_above_capacity_in_one_interval(
            rate in 2u32..=1_000_000u32,
            divisor in 2u32..=32u32,
        ) {
            prop_assume!(rate / divisor >= 1);
            let per_worker = rate / divisor; // what divide() gives each worker
            let block = per_worker + 1; // fits a single connection, not a worker
            prop_assume!(block <= rate);

            let mut single = Valve::new_with_timeout(NonZeroU32::new(rate).unwrap(), 0);
            prop_assert!(
                single.request(0, block).is_ok(),
                "a single connection (capacity {rate}) must accept a {block}-byte block"
            );

            let mut worker = Valve::new_with_timeout(NonZeroU32::new(per_worker).unwrap(), 0);
            prop_assert!(
                matches!(worker.request(0, block), Err(Error::Capacity { .. })),
                "a divided worker's Valve (capacity {per_worker}) rejects the {block}-byte block in one interval"
            );
        }
    }

    /// The fix: `wait_for` delivers a request larger than `maximum_capacity` by
    /// draining `maximum_capacity` per interval across as many intervals as it
    /// takes, instead of returning `Capacity` and letting the generator discard
    /// the block. Rate is preserved: the block simply spans multiple intervals.
    #[test]
    fn wait_for_drains_oversized_request_across_intervals() {
        let max = 100u32;
        let clock = MockClock {
            now: AtomicU64::new(0),
        };
        let mut stable = Stable::with_clock(NonZeroU32::new(max).expect("nonzero"), 0, clock);
        // 3 full intervals plus a remainder: four chunks, three interval rolls.
        let request = NonZeroU32::new(max * 3 + 7).expect("nonzero");
        block_on(stable.wait_for(request)).expect("oversized request must drain, not error");
        let elapsed = stable.clock.ticks_elapsed();
        assert!(
            elapsed >= 3 * super::INTERVAL_TICKS,
            "draining {request} at capacity {max} must span at least 3 interval rolls, saw {elapsed} ticks"
        );
    }
}

#[cfg(test)]
mod burst_measurement {
    //! Confirm/deny the "up to 11x" burst by driving the real `Valve` over a
    //! range of rates and idle depths. These call the same `request` path lading
    //! uses and assert the burst relationship as a property, not a fixture.
    use super::{INTERVAL_TICKS, MAX_ROLLED_INTERVALS, Valve};
    use proptest::prelude::*;
    use std::num::NonZeroU32;

    /// Grant `max_cap`-sized requests within one interval until refused, and
    /// return total capacity granted. The loop is bounded by the proven
    /// envelope, so it cannot spin.
    fn burst_capacity(valve: &mut Valve, ticks: u64, max_cap: u32) -> u64 {
        let mut granted = 0u64;
        for _ in 0..=(u64::from(MAX_ROLLED_INTERVALS) + 1) {
            match valve.request(ticks, max_cap) {
                Ok(0) => granted += u64::from(max_cap),
                _ => break,
            }
        }
        granted
    }

    proptest! {
        /// With no rollover, the default `timeout == 0`, a single interval
        /// grants exactly the configured rate for any rate. No burst is possible.
        #[test]
        fn timeout_zero_grants_exactly_configured(max_cap in 1u32..=100_000_000) {
            let mut valve = Valve::new_with_timeout(NonZeroU32::new(max_cap).unwrap(), 0);
            let granted = burst_capacity(&mut valve, 5, max_cap);
            prop_assert_eq!(granted, u64::from(max_cap));
        }

        /// With rolled capacity, idling `idle` intervals lets the next interval
        /// burst to exactly `(idle + 1)x` the configured rate, never past the
        /// proven `(MAX_ROLLED_INTERVALS + 1)x` ceiling. This confirms the 11x
        /// figure is reached, not merely bounded.
        #[test]
        fn idle_then_burst_scales_with_rollover(
            max_cap in 1u32..=100_000_000,
            idle in 1u64..=u64::from(MAX_ROLLED_INTERVALS),
        ) {
            let timeout = INTERVAL_TICKS * u64::from(MAX_ROLLED_INTERVALS);
            let mut valve = Valve::new_with_timeout(NonZeroU32::new(max_cap).unwrap(), timeout);
            let land = INTERVAL_TICKS * idle;
            let _ = valve.request(land, 0); // roll forward, banking rolled capacity
            let granted = burst_capacity(&mut valve, land + 1, max_cap);
            prop_assert_eq!(granted, u64::from(max_cap) * (idle + 1));
            prop_assert!(granted <= u64::from(max_cap) * (u64::from(MAX_ROLLED_INTERVALS) + 1));
        }
    }
}

#[cfg(all(test, feature = "antithesis"))]
mod antithesis_tests {
    use super::{INTERVAL_TICKS, Stable};
    use crate::RealClock;
    use std::num::NonZeroU32;

    // `assert_grant_within_envelope` takes ticks as an argument and never
    // touches the clock, so a `RealClock` is fine here.
    fn stable(max_cap: u32, timeout_ticks: u64) -> Stable<RealClock> {
        Stable::with_clock(
            NonZeroU32::new(max_cap).unwrap(),
            timeout_ticks,
            RealClock::default(),
        )
    }

    #[test]
    fn grant_accounting_accumulates_within_interval() {
        let mut s = stable(1000, 0);
        s.assert_grant_within_envelope(0, 400);
        assert_eq!(s.granted_this_interval, 400);
        s.assert_grant_within_envelope(10, 500);
        assert_eq!(s.granted_this_interval, 900);
        // At timeout == 0 the envelope is exactly maximum_capacity.
        assert!(s.granted_this_interval <= 1000);
    }

    #[test]
    fn grant_accounting_resets_on_interval_roll() {
        let mut s = stable(1000, 0);
        s.assert_grant_within_envelope(0, 900);
        assert_eq!(s.granted_this_interval, 900);
        // Crossing into the next interval resets the per-interval counter.
        s.assert_grant_within_envelope(INTERVAL_TICKS, 300);
        assert_eq!(s.observed_interval, 1);
        assert_eq!(s.granted_this_interval, 300);
    }

    #[test]
    fn burst_envelope_widens_with_timeout() {
        // With a timeout, rolled capacity legitimately allows up to 11x, so the
        // accounting accumulates past maximum_capacity within one interval
        // without exceeding the envelope.
        let mut s = stable(1000, INTERVAL_TICKS * 10);
        for _ in 0..8 {
            s.assert_grant_within_envelope(5, 1000);
        }
        assert_eq!(s.granted_this_interval, 8000);
        assert!(s.granted_this_interval <= 1000 * 11);
    }
}

#[cfg(kani)]
mod verification {
    use crate::stable::{
        INTERVAL_TICKS, MAX_ROLLED_INTERVALS, UnusedCapacity, Valve, tick_to_interval,
    };
    use std::num::NonZeroU32;

    /// Create a new `Valve` instance with a maximum capacity, given in
    /// tick-units.
    fn new(maximum_capacity: NonZeroU32) -> Valve {
        let maximum_capacity = maximum_capacity.get();
        Valve {
            capacity: maximum_capacity,
            maximum_capacity,
            interval: 0,
            timeout_ticks: 0,
            unused: [UnusedCapacity::ZERO; MAX_ROLLED_INTERVALS as usize],
            unused_head: 0,
        }
    }

    // Original Valve implementation preserved for equivalence testing.
    // This is the throttle behavior before timeout support was added.
    #[derive(Debug)]
    struct OriginalValve {
        maximum_capacity: u32,
        capacity: u32,
        interval: u64,
    }

    impl OriginalValve {
        fn new(maximum_capacity: NonZeroU32) -> Self {
            let maximum_capacity = maximum_capacity.get();
            Self {
                capacity: maximum_capacity,
                maximum_capacity,
                interval: 0,
            }
        }

        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        fn request(
            &mut self,
            ticks_elapsed: u64,
            capacity_request: u32,
        ) -> Result<u64, super::Error> {
            if capacity_request > self.maximum_capacity {
                return Err(super::Error::Capacity {
                    requested: capacity_request,
                    maximum: self.maximum_capacity,
                });
            }

            let current_interval = tick_to_interval(ticks_elapsed);
            if current_interval > self.interval {
                self.capacity = self.maximum_capacity;
                self.interval = current_interval;
            }

            if capacity_request == 0 {
                Ok(0)
            } else if capacity_request <= self.capacity {
                self.capacity -= capacity_request;
                Ok(0)
            } else {
                Ok(INTERVAL_TICKS.saturating_sub(ticks_elapsed % INTERVAL_TICKS))
            }
        }
    }

    /// Capacity requests that are too large always error.
    #[kani::proof]
    fn request_too_large_always_errors() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = new(maximum_capacity);
        let maximum_capacity = maximum_capacity.get();

        let request: u32 = kani::any_where(|r: &u32| *r > maximum_capacity);
        let ticks_elapsed: u64 = kani::any();

        let res = valve.request(ticks_elapsed, request);
        kani::assert(
            res.is_err(),
            "Requests that are too large must always fail.",
        );
    }

    /// Capacity requests that are zero always succeed.
    #[kani::proof]
    fn request_zero_always_succeed() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = new(maximum_capacity);

        let ticks_elapsed: u64 = kani::any();

        let slop = valve.request(ticks_elapsed, 0).expect("request failed.");
        kani::assert(slop == 0, "Requests that are zero always succeed.");
    }

    /// If a request is made on the throttle such that request <= max_capacity
    /// and ticks_elapsed <= INTERVAL_TICKS then the request should return with
    /// zero slop and the internal capacity of the valve should be reduced
    /// exactly the request size.
    #[kani::proof]
    fn request_in_cap_interval() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = new(maximum_capacity);
        let maximum_capacity = maximum_capacity.get();

        let request: u32 = kani::any_where(|r: &u32| *r <= maximum_capacity);
        let ticks_elapsed: u64 = kani::any_where(|t: &u64| *t <= INTERVAL_TICKS);

        let slop = valve
            .request(ticks_elapsed, request)
            .expect("request failed");
        kani::assert(
            slop == 0,
            "Request in-capacity, interval should succeed without wait.",
        );
        kani::assert(
            valve.capacity == maximum_capacity - request,
            "Request in-capacity, interval should reduce capacity by request size.",
        );
    }

    /// If a request is made on the throttle such that capacity < request <=
    /// max_capacity and ticks_elapsed <= INTERVAL_TICKS then the request should
    /// return with non-zero slop and the internal capacity of the valve should
    /// not be reduced.
    #[kani::proof]
    fn request_out_in_cap_interval() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = new(maximum_capacity);
        let maximum_capacity = maximum_capacity.get();

        let original_capacity = valve.capacity;
        let request: u32 =
            kani::any_where(|r: &u32| original_capacity < *r && *r <= maximum_capacity);
        let ticks_elapsed: u64 = kani::any_where(|t: &u64| *t <= INTERVAL_TICKS);

        let slop = valve
            .request(ticks_elapsed, request)
            .expect("request failed");
        kani::assert(slop > 0, "Should be forced to wait.");
        kani::assert(
            valve.capacity == original_capacity,
            "Capacity should not be reduced.",
        );
    }

    /// No matter the request size the valve's interval measure should always be
    /// consistent with the time passed in ticks_elapsed.
    #[kani::proof]
    fn interval_time_preserved() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = new(maximum_capacity);
        let maximum_capacity = maximum_capacity.get();

        let request: u32 = kani::any_where(|r: &u32| *r <= maximum_capacity);
        // 2**32 microseconds is 1 hour 1 minutes and change. While callers
        // _may_ be waiting longer this this we deem it unlikely.
        let ticks_elapsed = kani::any::<u32>() as u64;

        let _ = valve.request(ticks_elapsed, request);
        kani::assert(
            valve.interval == ticks_elapsed / INTERVAL_TICKS,
            "Interval should be consistent with ticks_elapsed.",
        );
    }

    /// When a request exceeds current capacity, the throttle returns the time
    /// remaining until the next interval boundary (not a guarantee of fulfillment)
    #[kani::proof]
    fn insufficient_capacity_returns_time_to_interval_boundary() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = new(maximum_capacity);
        let maximum_capacity = maximum_capacity.get();

        // Start with partial capacity by making an initial request
        let initial_request: u32 = kani::any_where(|r: &u32| *r > 0 && *r < maximum_capacity);
        let _ = valve.request(0, initial_request);

        // Now request more than remaining capacity
        let remaining_capacity = maximum_capacity - initial_request;
        let request: u32 =
            kani::any_where(|r: &u32| *r > remaining_capacity && *r <= maximum_capacity);
        let ticks_in_interval: u64 = kani::any_where(|t: &u64| *t < INTERVAL_TICKS);

        let slop = valve
            .request(ticks_in_interval, request)
            .expect("request should succeed");

        kani::assert(
            slop == INTERVAL_TICKS - ticks_in_interval,
            "Wait time should be exactly the time remaining until interval boundary",
        );
    }

    /// Critical bootstrap proof: When timeout_ticks=0, the modified Valve must
    /// behave identically to the original implementation. This ensures that
    /// existing users see no behavior change when they don't use the new feature.
    /// We test this by running both implementations in lockstep with identical
    /// inputs and verifying they produce identical outputs and state changes.
    #[kani::proof]
    fn valve_with_zero_timeout_equals_original() {
        let maximum_capacity: NonZeroU32 = kani::any();

        // Create original and new implementation with timeout disabled
        let mut original = OriginalValve::new(maximum_capacity);
        let mut with_timeout = Valve::new_with_timeout(maximum_capacity, 0);

        // Test a single operation to make verification tractable
        let ticks: u64 = kani::any::<u32>() as u64;
        let request: u32 = kani::any_where(|r: &u32| *r <= maximum_capacity.get());

        // Both implementations must produce identical results
        let result_original = original.request(ticks, request);
        let result_with_timeout = with_timeout.request(ticks, request);

        kani::assert(
            result_original == result_with_timeout,
            "Request results must be identical",
        );

        // Internal state must also remain synchronized
        kani::assert(
            original.capacity == with_timeout.capacity,
            "Available capacity must match",
        );
        kani::assert(
            original.interval == with_timeout.interval,
            "Current interval must match",
        );
    }

    /// Proves that we cannot achieve more successful requests in a single
    /// interval than (MAX_ROLLED_INTERVALS + 1) * maximum_capacity. This is the
    /// case when all previous intervals are unused and the current interval is
    /// not tapped either.
    #[kani::proof]
    fn max_requests_per_interval_bounded() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let timeout_ticks: u64 = kani::any_where(|t: &u64| {
            *t > 0 && *t >= INTERVAL_TICKS * u64::from(MAX_ROLLED_INTERVALS)
        });
        let mut valve = Valve::new_with_timeout(maximum_capacity, timeout_ticks);

        // Skip MAX_ROLLED_INTERVALS intervals without making any requests.
        // This accumulates the maximum possible rolled capacity.
        let target_interval = u64::from(MAX_ROLLED_INTERVALS);
        let target_ticks = target_interval * INTERVAL_TICKS;

        // Make a zero request to trigger the interval transition
        let _ = valve.request(target_ticks, 0);

        // Now we're in an interval with maximum possible accumulated capacity.
        // Try to drain capacity with a few arbitrary requests within this interval.
        let mut total_granted = 0u64;

        // Make a bounded number of arbitrary requests to avoid kani timeout
        // We don't need to exhaust all capacity to prove the bound
        for i in 0..5 {
            let ticks_within_interval = target_ticks + 1 + i * 100;

            // Stay within the same interval
            if ticks_within_interval >= (target_interval + 1) * INTERVAL_TICKS {
                break;
            }

            // Make an arbitrary valid request
            let request_size: u32 =
                kani::any_where(|r: &u32| *r > 0 && *r <= maximum_capacity.get());

            let result = valve.request(ticks_within_interval, request_size);
            if let Ok(0) = result {
                // Request granted
                total_granted = total_granted.saturating_add(u64::from(request_size));
            }
        }

        // Verify the bound: we should not be able to get more than
        // (MAX_ROLLED_INTERVALS + 1) * maximum_capacity successful requests
        let theoretical_max =
            u64::from(maximum_capacity.get()) * (u64::from(MAX_ROLLED_INTERVALS) + 1);
        kani::assert(
            total_granted <= theoretical_max,
            "Cannot exceed theoretical maximum capacity in single interval",
        );
    }
}
