//! Stable throttle
//!
//! This throttle refills capacity at a steady rate.

use std::{cmp, num::NonZeroU32};

use super::{Clock, RealClock};

// An 'interval' is the period in which all counters reset. The throttle makes
// no claims on units, but consider if a user intends to produce 1Mb/s the
// 'interval' is one second and each tick corresponds to one microsecond. Each
// microsecond accumulates 1 byte.
#[cfg(test)]
const INTERVAL_TICKS: u64 = 1_000_000;
// The user is only allowed to set the maximum capacity to 2**32. That means the
// maximum value that can be refilled per tick in an interval is
// 4_294.967_296. The largest fractional value is then 999_999. From this we
// construct a mask that allows us to get at the integer part of a refill in one
// tick and the fractional part. This happens to be INTERVAL_TICKS. The integer
// portion is gotten at with a division, the factional with a remainder.
const REFILL_MASK: u64 = 1_000_000;

// In order to preserve integer operations and avoid the use of
// floating point we make the following simplifications:
//
//  * the maximum refill-per-tick is 4_295,
//  * the minimum

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
/// This throttle is stable in that it will steadily refill units at a known
/// rate and does not inspect the target in any way, compare to `Predictive` in
/// that regard.
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
        // SAFETY: 1_u32 is a non-zero u32.
        let one = unsafe { NonZeroU32::new_unchecked(1_u32) };
        self.wait_for(one).await
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

    pub(crate) fn with_clock(maximum_capacity: NonZeroU32, clock: C) -> Self {
        Self {
            valve: Valve::new(maximum_capacity),
            clock,
        }
    }
}

/// The non-async interior to Stable, about which we can make proof claims. The
/// mechanical analogue isn't quite right but think of this as a poppet valve
/// for the stable throttle.
#[derive(Debug)]
struct Valve {
    /// The maximum capacity of `Valve` past which no more capacity will be
    /// added. Stored in megatick-units.
    maximum_capacity: u64,
    /// The current capacity of the `Valve`. Stored in megatick-units. Must
    /// never exceed `maximum_capacity`.
    capacity: u64,
    /// Per tick, how much capacity is added to the `Valve`. Stored in
    /// microtick-units.
    refill_per_tick: u64,
    /// The tick of the last request.
    last_tick: u64,
}

impl Valve {
    /// Create a new `Valve` instance with a maximum capacity, given in
    /// tick-units.
    fn new(maximum_capacity: NonZeroU32) -> Self {
        let refill_per_tick = u64::from(maximum_capacity.get());
        let maximum_capacity = u64::from(maximum_capacity.get()) * REFILL_MASK;
        Self {
            capacity: 0,
            maximum_capacity,
            refill_per_tick,
            last_tick: 0,
        }
    }

    /// For a given `capacity_request` and an amount of `ticks_elapsed` since
    /// the last call return how long a caller would have to wait -- in ticks --
    /// before the valve will have sufficient spare capacity to be open.
    ///
    /// Note that `ticks_elapsed` must be an absolute value.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn request(&mut self, ticks_elapsed: u64, capacity_request: u32) -> Result<u64, Error> {
        // Okay, here's the idea. At the base of `Stable` is a cell rate
        // algorithm. We have bucket that gradually fills up and when it's full
        // it doesn't fill up anymore. Callers draw down on this capacity and if
        // they draw down more than is available in the bucket they're made to
        // wait.
        if capacity_request == 0 {
            return Ok(0);
        }
        // Scale the capacity up to megatick-units.
        let capacity_request = u64::from(capacity_request) * REFILL_MASK;
        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if capacity_request > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        let ticks_diff = ticks_elapsed - self.last_tick;
        self.last_tick = ticks_elapsed;

        // Now that the preliminaries are out of the way, wake up and compute
        // how much the throttle capacity is refilled since we were last
        // called. Depending on how long ago this was we may have completely
        // filled up throttle capacity.
        let cap = (self.refill_per_tick * ticks_diff) + self.capacity;
        let refilled_capacity: u64 = cmp::min(cap, self.maximum_capacity);
        println!("[{tick}] max: {max} | cap: {cap} | refilled_capacity: {refilled_capacity} | capacity_request: {capacity_request}", max = self.maximum_capacity, tick = self.last_tick);
        self.capacity = refilled_capacity;

        // If the refilled capacity is greater or equal to the request we deduct
        // the request from capacity and return 0 slop, signaling to the user
        // that their request is a success. Else, we calculate how long the
        // caller should wait for and _do not_ adjust the capacity. User must
        // call again with the same request.
        if capacity_request <= self.capacity {
            // If the refilled capacity is greater or equal to the request we
            // respond to the caller immediately and store the spare capacity
            // for next call.
            self.capacity -= capacity_request;
            println!("immediate return");
            Ok(0)
        } else {
            // If the refill is not sufficient we calculate how many ticks will
            // need to pass before capacity is sufficient, force the client to
            // wait that amount of time. We _do not_ change the actual capacity
            // as we do not know when the caller will request again.
            let cap_diff = capacity_request - self.capacity;
            // If the slop is zero this means that there's some fractional
            // refill that is missing. This is not achievable because it's
            // impossible for the caller to wait less than a tick.
            let slop = cmp::max(1, cap_diff / self.refill_per_tick);
            println!(
                "wait || slop: {slop} | cap_diff: {cap_diff} | capacity: {capacity}",
                capacity = self.capacity
            );
            Ok(slop)
        }
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU32;

    use proptest::{collection, prelude::*};

    use crate::stable::{Valve, INTERVAL_TICKS};

    fn capacity_never_exceeds_max_in_interval_inner(
        maximum_capacity: u32,
        mut ticks_requests: Vec<(u64, NonZeroU32)>,
    ) -> Result<(), proptest::test_runner::TestCaseError> {
        let mut valve = Valve::new(NonZeroU32::new(maximum_capacity).unwrap());
        let maximum_capacity = u64::from(maximum_capacity);

        let mut ticks_elapsed: u64 = 0;
        let mut granted_requests: u64 = 0;
        let mut current_interval: u64 = 0;

        println!("\n\nTEST TEST TEST");

        let mut slop = 0;
        for (ticks_elapsed_diff, request) in ticks_requests.drain(..) {
            ticks_elapsed += ticks_elapsed_diff + slop;

            {
                let interval = ticks_elapsed / INTERVAL_TICKS;
                if current_interval < interval {
                    // We have entered into a new interval, the granted requests
                    // must be reset.
                    prop_assert!(granted_requests <= maximum_capacity,
                                     "[interval-change] Granted requests {granted_requests} exceeded the maximum capacity of the valve, {maximum_capacity}");
                    granted_requests = 0;
                    current_interval = interval;
                }
            }

            match valve.request(ticks_elapsed, request.get()) {
                Ok(0) => {
                    // The request went through right away.
                    granted_requests += u64::from(request.get());
                    slop = 0;
                }
                Ok(s) => {
                    // The request must wait for 'slop' ticks. We choose to
                    // 'wait' by adding to the slop accumulator but may or may
                    // not make the same request of the valve. No request is
                    // granted if slop is non-zero.
                    slop = s;
                }
                Err(_) => {
                    // ignored intentionally
                }
            }
            prop_assert!(granted_requests <= maximum_capacity,
                             "[end] Granted requests {granted_requests} exceeded the maximum capacity of the valve, {maximum_capacity}");
        }
        Ok(())
    }

    #[test]
    fn static_capacity_never_exceeds_max_in_interval() {
        let maximum_capacity = 490301363u32;
        let ticks_requests: Vec<(u64, NonZeroU32)> = vec![
            (0, NonZeroU32::new(1).unwrap()),
            (0, NonZeroU32::new(490301363).unwrap()),
        ];
        capacity_never_exceeds_max_in_interval_inner(maximum_capacity, ticks_requests).unwrap()
    }

    fn ticks_elapsed_and_cap_requests(max: u32) -> impl Strategy<Value = Vec<(u64, NonZeroU32)>> {
        let max = max + 100;
        collection::vec(
            (
                (0..1_000u64),
                (1..max).prop_map(|i| NonZeroU32::new(i).unwrap()),
            ),
            1..5,
        )
    }

    // The sum of capacity requests must never exceed maximum_capacity in one
    // INTERVAL_TICKS.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 10_000_000,
            max_shrink_iters: 100_000_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn capacity_never_exceeds_max_in_interval(
            maximum_capacity in (1..40_u32), // u32::MAX),
            ticks_requests in ticks_elapsed_and_cap_requests(256_u32)
        ) {
            capacity_never_exceeds_max_in_interval_inner(maximum_capacity, ticks_requests)?
        }
    }
}

#[cfg(kani)]
mod verification {
    use crate::stable::{Valve, INTERVAL_TICKS};
    use std::num::NonZeroU32;

    const MAX_TICKS_ELAPSED: u64 = INTERVAL_TICKS * 4;

    // The sum of capacity requests must never exceed maximum_capacity in one
    // INTERVAL_TICKS.
    #[kani::proof]
    #[kani::unwind(100)] // must match `iters` below
    fn capacity_never_exceeds_max_in_interval() {
        let maximum_capacity: NonZeroU32 = kani::any();
        kani::assume(maximum_capacity.get() < 160_000);

        let mut valve = Valve::new(maximum_capacity);
        let maximum_capacity = u64::from(maximum_capacity.get());

        let mut ticks_elapsed: u64 = 0;
        let mut granted_requests: u64 = 0;
        let mut current_interval: u64 = 0;

        let iters: usize = kani::any();
        kani::assume(iters < 100);

        for _ in 0..iters {
            let ticks_elapsed_diff: u64 = kani::any();
            kani::assume(ticks_elapsed_diff <= MAX_TICKS_ELAPSED);

            let request: u32 = kani::any();
            kani::assume(u64::from(request) <= maximum_capacity);

            ticks_elapsed += ticks_elapsed_diff;

            {
                let interval = ticks_elapsed / INTERVAL_TICKS;
                if current_interval < interval {
                    // We have entered into a new interval, the granted requests
                    // must be reset.
                    if granted_requests > maximum_capacity {
                        panic!("too many requests granted");
                    }
                    granted_requests = 0;
                    current_interval = interval;
                }
            }

            match valve.request(ticks_elapsed, request) {
                Ok(0) => {
                    // The request went through right away.
                    granted_requests += u64::from(request);
                }
                Ok(slop) => {
                    // The request must wait for 'slop' ticks. If adding
                    // these to our current ticks_elapsed do not push us
                    // past the interval boundary we count them in the
                    // current interval. Else, we account them to the next.
                    let interval = (ticks_elapsed + slop) / INTERVAL_TICKS;
                    ticks_elapsed += slop;
                    if current_interval < interval {
                        granted_requests += u64::from(request);
                    } else {
                        // Be sure to capacity check requests in the
                        // previous interval.
                        if granted_requests > maximum_capacity {
                            panic!("too many requests granted");
                        }
                        granted_requests = u64::from(request);
                        current_interval = interval;
                    }
                }
                Err(_) => {
                    // ignored intentionally
                }
            }
            if granted_requests > maximum_capacity {
                panic!("too many requests granted");
            }
        }
    }
}
