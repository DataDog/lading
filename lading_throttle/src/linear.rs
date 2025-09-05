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
    #[error("Capacity")]
    Capacity,
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
        loop {
            let slop: u64 = self
                .valve
                .request(self.clock.ticks_elapsed(), request.get())?;
            if slop == 0 {
                break;
            }
            self.clock.wait(slop).await;
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
            return Err(Error::Capacity);
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
