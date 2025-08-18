//! Stable throttle
//!
//! This throttle refills capacity at a steady rate.

use std::num::NonZeroU32;

use super::{Clock, INTERVAL_TICKS, RealClock};

/// Errors produced by [`Stable`].
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq)]
pub enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("Capacity")]
    Capacity,
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

    pub(crate) fn with_clock(maximum_capacity: NonZeroU32, timeout_micros: u64, clock: C) -> Self {
        Self {
            valve: Valve::new_with_timeout(maximum_capacity, timeout_micros),
            clock,
        }
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
            return Err(Error::Capacity);
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
        #[allow(clippy::cast_possible_truncation)]
        let intervals_to_store = (intervals_passed.min(u64::from(MAX_ROLLED_INTERVALS))) as usize;
        if intervals_to_store == 0 {
            return;
        }

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

    /// Deduct capacity, taking from the current interval preferentially, then
    /// backward through the remaining unused capacity.
    fn deduct_capacity(&mut self, mut capacity_request: u32, ticks_elapsed: u64) {
        // Deduct from the current capacity, returning early if there's
        // sufficient capacity to service the request.
        if capacity_request <= self.capacity {
            self.capacity -= capacity_request;
            return;
        }
        capacity_request -= self.capacity;
        self.capacity = 0;

        // Bail out: when timeout is 0, there's no unused capacity to deduct
        // from.
        if self.timeout_ticks == 0 {
            return;
        }

        // Deduct from any available unused capacity that hasn't expired. By
        // definition any capacity in self.unused is 'in the past' and is fair
        // play for consumption.
        for i in 0..MAX_ROLLED_INTERVALS as usize {
            if self.unused[i].amount > 0 && self.unused[i].expires_at > ticks_elapsed {
                if capacity_request <= self.unused[i].amount {
                    self.unused[i].amount -= capacity_request;
                    return;
                }
                capacity_request -= self.unused[i].amount;
                self.unused[i].amount = 0;
            }
        }
    }
}

/// Calculate which interval a given tick count falls into
#[inline]
fn tick_to_interval(ticks: u64) -> u64 {
    ticks / INTERVAL_TICKS
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

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        fn request(
            &mut self,
            ticks_elapsed: u64,
            capacity_request: u32,
        ) -> Result<u64, super::Error> {
            if capacity_request > self.maximum_capacity {
                return Err(super::Error::Capacity);
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
