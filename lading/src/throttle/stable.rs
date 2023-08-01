//! Stable throttle
//!
//! This throttle refills capacity at a steady rate.
//!
//! ## Metrics
//!
//! `throttle_refills_per_tick`: Throttle capacity will refill to allow this
//! many operations per tick of the throttle's clock source.
//!

use std::{cmp, num::NonZeroU32};

use super::{Clock, RealClock};

const INTERVAL_TICKS: u64 = 1_000_000;

/// Errors produced by [`Stable`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub(crate) enum Error {
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
pub(crate) struct Stable<C = RealClock> {
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
        let slop: u64 = self.valve.request(self.clock.ticks_elapsed(), request)?;
        self.clock.wait(slop).await;
        Ok(())
    }

    pub(crate) fn with_clock(maximum_capacity: NonZeroU32, clock: C) -> Self {
        Self {
            valve: Valve::new(clock.ticks_elapsed(), u64::from(maximum_capacity.get())),
            clock,
        }
    }
}

/// The non-async interior to Stable, about which we can make proof claims. The
/// mechanical analogue isn't quite right but think of this as a poppet valve
/// for the stable throttle.
#[derive(Debug)]
struct Valve {
    /// Monotonically increasing recording of the last tick at each request.
    last_tick: u64,
    /// The capacity left in `Valve` after a request.
    spare_capacity: u64,
    /// The maximum capacity of `Valve` past which no more capacity will be
    /// added.
    maximum_capacity: u64,
    /// Per tick, how much capacity is added to the `Valve`.
    refill_per_tick: u64,
}

impl Valve {
    /// Create a new `Valve` instance
    fn new(last_tick: u64, maximum_capacity: u64) -> Self {
        // We do not want a situation where refill never happens. If the maximum
        // capacity is less than INTERVAL_TICKS we set the floor at 1.
        let refill_per_tick = cmp::max(1, maximum_capacity / INTERVAL_TICKS);
        Self {
            last_tick,
            spare_capacity: 0,
            maximum_capacity,
            refill_per_tick,
        }
    }

    /// For a given `capacity_request` and an amount of `ticks_elapsed` return
    /// how long a caller would have to wait -- in ticks -- before the valve
    /// will have sufficient spare capacity to be open.
    fn request(&mut self, ticks_elapsed: u64, capacity_request: NonZeroU32) -> Result<u64, Error> {
        let capacity_request: u64 = u64::from(capacity_request.get());
        // Okay, here's the idea. At the base of `Stable` is a cell rate
        // algorithm. We have bucket that gradually fills up and when it's full
        // it doesn't fill up anymore. Callers draw down on this capacity and if
        // they draw down more than is available in the bucket they're made to
        // wait.

        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if capacity_request > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        // Now that the preliminaries are out of the way, wake up and compute
        // how much the throttle capacity is refilled since we were last
        // called. Depending on how long ago this was we may have completely
        // filled up throttle capacity.
        let ticks_since_last_wait = ticks_elapsed.saturating_sub(self.last_tick);
        self.last_tick = ticks_elapsed;
        let refilled_capacity: u64 = cmp::min(
            ticks_since_last_wait
                .saturating_mul(self.refill_per_tick)
                .saturating_add(self.spare_capacity),
            self.maximum_capacity,
        );

        if refilled_capacity > capacity_request {
            // If the refilled capacity is greater than the request we respond
            // to the caller immediately and store the spare capacity for next
            // call.
            self.spare_capacity = refilled_capacity - capacity_request;
            Ok(0)
        } else {
            // If the refill is not sufficient we calculate how many ticks will
            // need to pass before capacity is sufficient, force the client to
            // wait that amount of time.
            self.spare_capacity = 0;
            Ok((capacity_request - refilled_capacity) / self.refill_per_tick)
        }
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU32;

    use proptest::{collection, prelude::*};

    use crate::throttle::stable::{Valve, INTERVAL_TICKS};

    fn ticks_elapsed_and_cap_requests() -> impl Strategy<Value = Vec<(u64, NonZeroU32)>> {
        collection::vec(
            (
                (0..1_000u64),
                (1..u32::MAX).prop_map(|i| NonZeroU32::new(i).unwrap()),
            ),
            1..1_000,
        )
    }

    // The recording of the last tick must always monotonically increase.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn last_tick_monotonic_increase(maximum_capacity in (1..u64::MAX), last_tick: u64,
                                        mut ticks_requests in ticks_elapsed_and_cap_requests()) {
            let mut valve = Valve::new(last_tick, maximum_capacity);

            let mut ticks_elapsed = 0;
            let mut prev_last_tick = 0;
            for (ticks_elapsed_diff, request) in ticks_requests.drain(..) {
                ticks_elapsed += ticks_elapsed_diff;
                // NOTE: If these error out it's only because the request is
                // larger than maximum_capacity, which we do not care about in
                // this test.
                let _ = valve.request(ticks_elapsed, request);

                prop_assert!(valve.last_tick >= prev_last_tick);
                prev_last_tick = valve.last_tick;
            }
        }
    }

    // Spare capacity must never exceed the maximum capacity.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn spare_capacity_never_exceed(maximum_capacity in (1..u64::MAX), last_tick: u64,
                                       mut ticks_requests in ticks_elapsed_and_cap_requests()) {
            let mut valve = Valve::new(last_tick, maximum_capacity);

            let mut ticks_elapsed = 0;
            for (ticks_elapsed_diff, request) in ticks_requests.drain(..) {
                ticks_elapsed += ticks_elapsed_diff;

                // NOTE: If these error out it's only because the request is
                // larger than maximum_capacity, which we do not care about in
                // this test.
                let _ = valve.request(ticks_elapsed, request);
                prop_assert!(valve.spare_capacity <= maximum_capacity);
            }
        }
    }

    // The sum of capacity requests must never exceed maximum_capacity in one
    // INTERVAL_TICKS.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn capacity_never_exceeds_max_in_interval(
            maximum_capacity in (1..u64::MAX), last_tick: u64,
            mut ticks_requests in ticks_elapsed_and_cap_requests()) {
            let mut valve = Valve::new(last_tick, maximum_capacity);

            let mut ticks_elapsed: u64 = 0;
            let mut granted_requests: u64 = 0;
            let mut current_interval: u64 = 0;

            for (ticks_elapsed_diff, request) in ticks_requests.drain(..) {
                ticks_elapsed += ticks_elapsed_diff;

                {
                    let interval = ticks_elapsed / INTERVAL_TICKS;
                    if current_interval < interval {
                        // We have entered into a new interval, the granted requests
                        // must be reset.
                        granted_requests = 0;
                        current_interval = interval;
                    }
                }

                match valve.request(ticks_elapsed, request) {
                    Ok(0) => {
                        // The request went through right away.
                        granted_requests += u64::from(request.get());
                    }
                    Ok(slop) => {
                        // The request must wait for 'slop' ticks. If adding
                        // these to our current ticks_elapsed do not push us
                        // past the interval boundary we count them in the
                        // current interval. Else, we account them to the next.
                        let interval = (ticks_elapsed + slop) / INTERVAL_TICKS;
                        ticks_elapsed += slop;
                        if current_interval < interval {
                            granted_requests += u64::from(request.get());
                        } else {
                            // Be sure to capacity check requests in the
                            // previous interval.
                            prop_assert!(granted_requests <= maximum_capacity);
                            granted_requests = u64::from(request.get());
                            current_interval = interval;
                        }
                        prop_assert!(granted_requests <= maximum_capacity);
                    }
                    Err(_) => {
                        // ignored intentionally
                    }
                }
            }
        }
    }
}
