//! Linear throttle
//!
//! This throttle increases at a linear rate up to some maximum.

use std::num::NonZeroU32;

use crate::INTERVAL_TICKS;

use super::{Clock, RealClock};

/// Errors produced by [`Stable`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("capacity request {requested} exceeds maximum {maximum}")]
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
/// This throttle is linear in that it refills up to some maximum at a known
/// rate of change per tick.
pub struct Linear<C = RealClock> {
    valve: Valve,
    /// The clock that `Stable` will use.
    clock: C,
}

impl<C> Linear<C>
where
    C: Clock + Send + Sync,
{
    #[inline]
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        self.wait_for(NonZeroU32::MIN).await
    }

    pub(crate) async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        // A request larger than one interval's grantable capacity is not an
        // error. Drain it across intervals in chunks, so an oversized block --
        // for example one larger than the per-worker capacity after `divide` --
        // is delivered at the configured rate instead of being rejected and
        // discarded. This mirrors `Stable::wait_for`.
        //
        // Unlike `Stable`, a `Linear` valve ramps, so the chunk size is the
        // capacity this worker can actually reach in an interval, not
        // `maximum_capacity`. When `rate_of_change` is zero the ramp is flat --
        // `divide` floors a fine rate to zero -- and never climbs past
        // `reset_capacity`, so a `maximum_capacity`-sized chunk would never be
        // granted and this loop would spin forever. Chunk at the reachable
        // ceiling instead.
        let ceiling = if self.valve.rate_of_change == 0 {
            self.valve.reset_capacity
        } else {
            self.valve.maximum_capacity
        }
        .min(self.valve.maximum_capacity);
        let Some(ceiling) = NonZeroU32::new(ceiling) else {
            // A flat ramp with zero deliverable capacity can never grant a
            // positive request -- `divide` can floor a small throttle to
            // `initial == rate == 0`. Returning immediately would let the
            // caller's discard loop, which has no await of its own, busy-spin a
            // core. Yield one interval first so the discard runs at most once
            // per interval, then report a zero deliverable maximum.
            self.clock.wait(INTERVAL_TICKS).await;
            return Err(Error::Capacity {
                requested: request.get(),
                maximum: 0,
            });
        };
        let ceiling = ceiling.get();

        let mut remaining = request.get();
        while remaining > 0 {
            let chunk = remaining.min(ceiling);
            loop {
                let slop: u64 = self.valve.request(self.clock.ticks_elapsed(), chunk)?;
                if slop == 0 {
                    break;
                }
                self.clock.wait(slop).await;
            }
            remaining -= chunk;
        }
        Ok(())
    }

    pub(crate) fn with_clock(
        initial_capacity: u32,
        maximum_capacity: NonZeroU32,
        rate_of_change: u32,
        clock: C,
    ) -> Self {
        Self {
            valve: Valve::new(initial_capacity, maximum_capacity, rate_of_change),
            clock,
        }
    }

    /// Get the maximum capacity of this throttle
    pub(super) fn maximum_capacity(&self) -> u32 {
        self.valve.maximum_capacity
    }

    /// Get the initial capacity for this throttle
    pub(super) fn initial_capacity(&self) -> u32 {
        self.valve.initial_capacity
    }

    /// Get the rate of change for this throttle
    pub(super) fn rate_of_change(&self) -> u32 {
        self.valve.rate_of_change
    }
}

/// The non-async interior to Linear, about which we can make proof claims. The
/// mechanical analogue isn't quite right but think of this as a poppet valve
/// for the linear throttle.
#[derive(Debug)]
struct Valve {
    /// The initial capacity when the throttle was created
    initial_capacity: u32,
    /// The capacity to reset `capacity` to at each interval roll-over. Will
    /// never be less than `initial_capacity`.
    reset_capacity: u32,
    /// The maximum capacity of `Valve` past which no more capacity will be
    /// added.
    maximum_capacity: u32,
    /// The rate at which `maximum_capacity` increases per interval roll-over.
    rate_of_change: u32,
    /// The capacity of the `Valve`. This amount will be drawn on by every
    /// request. It is refilled to maximum at every interval roll-over.
    capacity: u32,
    /// The current interval -- multiple of `INTERVAL_TICKS` --  of time.
    interval: u64,
}

impl Valve {
    /// Create a new `Valve` instance with a maximum capacity, given in
    /// tick-units.
    fn new(initial_capacity: u32, maximum_capacity: NonZeroU32, rate_of_change: u32) -> Self {
        let maximum_capacity = maximum_capacity.get();
        Self {
            initial_capacity,
            reset_capacity: initial_capacity,
            maximum_capacity,
            rate_of_change,
            capacity: initial_capacity,
            interval: 0,
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

        let current_interval = ticks_elapsed / INTERVAL_TICKS;
        if current_interval > self.interval {
            // We have rolled forward into a new interval. At this point the
            // capacity is reset to reset_capacity -- no matter how deep we are
            // into the interval -- and we record the new interval index.
            self.capacity = self.reset_capacity;
            if self.reset_capacity < self.maximum_capacity {
                self.reset_capacity = self
                    .reset_capacity
                    .saturating_add(self.rate_of_change)
                    .min(self.maximum_capacity);
            }
            self.interval = current_interval;
        }

        // If the request is zero we return. If the capacity is greater or equal
        // to the request we deduct the request from capacity and return 0 slop,
        // signaling to the user that their request is a success. Else, we
        // calculate how long the caller should wait until the interval rolls
        // over and capacity is refilled. The capacity will never increase in
        // this interval so they will have to call again later.
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

#[cfg(test)]
mod drain {
    //! `Linear::wait_for` must deliver a request larger than a single interval's
    //! grantable capacity by draining it across intervals, matching
    //! `Stable::wait_for`. Without it a `divide`d Linear worker discards every
    //! block between its per-worker capacity and the single-connection rate. The
    //! flat-ramp case is the hazard: `divide` can floor `rate_of_change` to zero,
    //! so a naive `maximum_capacity`-sized chunk would never be granted and the
    //! drain would spin forever.
    use super::{Error, Linear};
    use crate::{Clock, INTERVAL_TICKS};
    use async_trait::async_trait;
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
    /// never suspends, so `wait_for` makes full progress without a runtime. A
    /// drain that failed to terminate would hang this loop, which is the point:
    /// the flat-ramp test would never return under the old naive chunking.
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        use std::pin::pin;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        const VTABLE: RawWakerVTable = RawWakerVTable::new(|_| RAW, |_| {}, |_| {}, |_| {});
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

    fn linear(initial: u32, max: u32, rate: u32) -> Linear<MockClock> {
        Linear::with_clock(
            initial,
            NonZeroU32::new(max).expect("max nonzero"),
            rate,
            MockClock {
                now: AtomicU64::new(0),
            },
        )
    }

    /// A request larger than one interval's capacity drains across intervals
    /// instead of returning `Capacity`. With `initial == max` the valve refills
    /// to `max` each interval, so four chunks span three interval rolls.
    #[test]
    fn wait_for_drains_oversized_request_across_intervals() {
        let max = 100u32;
        let mut throttle = linear(max, max, 10);
        let request = NonZeroU32::new(max * 3 + 7).expect("nonzero");
        block_on(throttle.wait_for(request)).expect("oversized request must drain, not error");
        let elapsed = throttle.clock.ticks_elapsed();
        assert!(
            elapsed >= 3 * INTERVAL_TICKS,
            "draining {request} at capacity {max} must span at least 3 interval rolls, saw {elapsed}"
        );
    }

    /// The flat-ramp hazard: `divide` floored `rate_of_change` to zero while
    /// `initial < max`, so the ramp never climbs to `max`. A `max`-sized chunk
    /// could never be granted and a naive drain would spin forever. Chunking at
    /// the reachable ceiling (`reset_capacity`, here 1) drains the block one unit
    /// per interval and terminates. If this test hangs, the fix regressed.
    #[test]
    fn wait_for_drains_flat_ramp_without_hanging() {
        let mut throttle = linear(1, 10, 0); // flat: capacity resets to 1 forever
        let request = NonZeroU32::new(5).expect("nonzero");
        block_on(throttle.wait_for(request)).expect("flat ramp must drain, not hang or error");
        let elapsed = throttle.clock.ticks_elapsed();
        assert!(
            elapsed >= 4 * INTERVAL_TICKS,
            "five 1-unit chunks on a flat ramp must span at least 4 interval rolls, saw {elapsed}"
        );
    }

    /// A throttle with zero deliverable capacity (flat ramp, `initial == 0`) can
    /// never grant a positive request. It must yield an interval and then report
    /// `Capacity`, rather than either hang forever or return immediately -- an
    /// immediate error lets the caller's await-free discard loop busy-spin a
    /// core. The yield bounds that to one discard per interval.
    #[test]
    fn wait_for_zero_capacity_flat_ramp_yields_then_errors() {
        let mut throttle = linear(0, 1, 0);
        let err = block_on(throttle.wait_for(NonZeroU32::MIN))
            .expect_err("zero-capacity throttle must error, not hang");
        assert!(matches!(err, Error::Capacity { maximum: 0, .. }));
        assert!(
            throttle.clock.ticks_elapsed() >= INTERVAL_TICKS,
            "must yield at least one interval before erroring so the caller cannot busy-spin, saw {}",
            throttle.clock.ticks_elapsed()
        );
    }
}

#[cfg(kani)]
mod verification {
    use crate::INTERVAL_TICKS;
    use crate::linear::Valve;
    use std::num::NonZeroU32;

    /// Capacity requests that are too large always error.
    #[kani::proof]
    fn request_too_large_always_errors() {
        let initial_capacity: u32 = kani::any();
        let rate_of_change: u32 = kani::any();
        let maximum_capacity: NonZeroU32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);
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
        let initial_capacity: u32 = kani::any();
        let rate_of_change: u32 = kani::any();
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);

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
        let initial_capacity: u32 = kani::any_where(|i: &u32| *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);

        let request: u32 = kani::any_where(|r: &u32| *r <= initial_capacity);
        let ticks_elapsed: u64 = kani::any_where(|t: &u64| *t <= INTERVAL_TICKS);

        let slop = valve
            .request(ticks_elapsed, request)
            .expect("request failed");
        kani::assert(
            slop == 0,
            "Request in-capacity, interval should succeed without wait.",
        );
        kani::assert(
            valve.capacity == initial_capacity - request,
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
        let initial_capacity: u32 = kani::any_where(|i: &u32| *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);
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
        let initial_capacity: u32 = kani::any_where(|i: &u32| *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);
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

    /// Reset capacity must never exceed maximum capacity.
    #[kani::proof]
    fn reset_capacity_bounded() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let initial_capacity: u32 = kani::any_where(|i: &u32| *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);
        let maximum_capacity = maximum_capacity.get();

        // Make multiple requests across intervals -- potentially -- to trigger
        // reset_capacity updates.
        for _ in 0..3 {
            let ticks_elapsed: u64 = kani::any();
            let request: u32 = kani::any_where(|r: &u32| *r <= maximum_capacity);
            let _ = valve.request(ticks_elapsed, request);

            kani::assert(
                valve.reset_capacity <= maximum_capacity,
                "Reset capacity should never exceed maximum capacity",
            );
        }
    }

    /// Capacity should reset to the prior reset_capacity when an interval roll-over
    /// happens.
    #[kani::proof]
    fn capacity_resets_on_interval_change() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let initial_capacity: u32 = kani::any_where(|i: &u32| *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);

        // Interval 0
        let first_request: u32 = kani::any_where(|r: &u32| *r <= initial_capacity);
        let _ = valve.request(0, first_request);

        let prior_reset_capacity = valve.reset_capacity;

        // Interval 1
        let ticks_elapsed = INTERVAL_TICKS + 1;
        let _ = valve.request(ticks_elapsed, 0);

        kani::assert(
            valve.capacity == prior_reset_capacity,
            "Capacity should reset to the reset_capacity value from prior to the interval change",
        );
    }

    /// reset_capacity should increase by rate_of_change each interval.
    #[kani::proof]
    fn rate_of_growth_preserved() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let initial_capacity: u32 = kani::any_where(|i: &u32| *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);

        let original_reset_capacity = valve.reset_capacity;

        // Move to next interval
        let _ = valve.request(INTERVAL_TICKS + 1, 0);

        if original_reset_capacity < maximum_capacity.get() {
            let expected = original_reset_capacity
                .saturating_add(rate_of_change)
                .min(maximum_capacity.get());
            kani::assert(
                valve.reset_capacity == expected,
                "Reset capacity should grow linearly by rate_of_change",
            );
        }
    }

    /// When a request exceeds current capacity, the throttle returns the time
    /// remaining until the next interval boundary (not a guarantee of fulfillment)
    #[kani::proof]
    fn insufficient_capacity_returns_time_to_interval_boundary() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let initial_capacity: u32 =
            kani::any_where(|i: &u32| *i > 0 && *i <= maximum_capacity.get());
        let rate_of_change: u32 = kani::any();

        let mut valve = Valve::new(initial_capacity, maximum_capacity, rate_of_change);

        // Request more than available capacity
        let request: u32 =
            kani::any_where(|r: &u32| *r > initial_capacity && *r <= maximum_capacity.get());
        let ticks_in_interval: u64 = kani::any_where(|t: &u64| *t < INTERVAL_TICKS);

        let slop = valve
            .request(ticks_in_interval, request)
            .expect("request should succeed");

        kani::assert(
            slop == INTERVAL_TICKS - ticks_in_interval,
            "Wait time should be exactly the time remaining until interval boundary",
        );
    }
}
