//! Stable throttle
//!
//! This throttle refills capacity at a steady rate.

use std::{cmp, num::NonZeroU32};

use super::{Clock, RealClock};

const INTERVAL_TICKS: u64 = 1_000_000;

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
        let slop: u64 = self
            .valve
            .request(self.clock.ticks_elapsed(), request.get())?;
        self.clock.wait(slop).await;
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
    /// Monotonically increasing recording of the last tick at each request.
    last_tick: u64,
    /// The capacity left in `Valve` after a request.
    spare_capacity: u64,
    /// The maximum capacity of `Valve` past which no more capacity will be
    /// added.
    maximum_capacity: u64,
    /// Per tick, how much capacity is added to the `Valve`.
    refill_per_tick: f64,
}

impl Valve {
    /// Create a new `Valve` instance
    fn new(maximum_capacity: NonZeroU32) -> Self {
        let maximum_capacity = u64::from(maximum_capacity.get());
        let refill_per_tick = (maximum_capacity as f64) / (INTERVAL_TICKS as f64);
        Self {
            last_tick: 0,
            spare_capacity: 0,
            maximum_capacity,
            refill_per_tick,
        }
    }

    /// For a given `capacity_request` and an amount of `ticks_elapsed` return
    /// how long a caller would have to wait -- in ticks -- before the valve
    /// will have sufficient spare capacity to be open.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn request(&mut self, ticks_elapsed: u64, capacity_request: u32) -> Result<u64, Error> {
        let capacity_request = u64::from(capacity_request);
        // Okay, here's the idea. At the base of `Stable` is a cell rate
        // algorithm. We have bucket that gradually fills up and when it's full
        // it doesn't fill up anymore. Callers draw down on this capacity and if
        // they draw down more than is available in the bucket they're made to
        // wait.

        if capacity_request == 0 {
            return Ok(0);
        }
        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if capacity_request > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        // Now that the preliminaries are out of the way, wake up and compute
        // how much the throttle capacity is refilled since we were last
        // called. Depending on how long ago this was we may have completely
        // filled up throttle capacity.
        let ticks_since_last_wait: u64 = ticks_elapsed.saturating_sub(self.last_tick);
        self.last_tick = ticks_elapsed;

        let cap: u64 = (((ticks_since_last_wait as f64) * self.refill_per_tick).round() as u64)
            .saturating_add(self.spare_capacity);
        let refilled_capacity: u64 = cmp::min(cap, self.maximum_capacity);

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
            let cap_diff = capacity_request - refilled_capacity;
            Ok((cap_diff as f64 / self.refill_per_tick).floor() as u64)
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

#[cfg(test)]
mod test {
    use std::num::NonZeroU32;

    use proptest::{collection, prelude::*};

    use crate::stable::{Valve, INTERVAL_TICKS};

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
        fn last_tick_monotonic_increase(maximum_capacity in (1..u32::MAX),
                                        mut ticks_requests in ticks_elapsed_and_cap_requests()) {
            let mut valve = Valve::new(NonZeroU32::new(maximum_capacity).unwrap());

            let mut ticks_elapsed = 0;
            let mut prev_last_tick = 0;
            for (ticks_elapsed_diff, request) in ticks_requests.drain(..) {
                ticks_elapsed += ticks_elapsed_diff;
                // NOTE: If these error out it's only because the request is
                // larger than maximum_capacity, which we do not care about in
                // this test.
                let _ = valve.request(ticks_elapsed, request.get());

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
        fn spare_capacity_never_exceed(maximum_capacity in (1..u32::MAX),
                                       mut ticks_requests in ticks_elapsed_and_cap_requests()) {
            let mut valve = Valve::new(NonZeroU32::new(maximum_capacity).unwrap());
            let maximum_capacity = u64::from(maximum_capacity);

            let mut ticks_elapsed = 0;
            for (ticks_elapsed_diff, request) in ticks_requests.drain(..) {
                ticks_elapsed += ticks_elapsed_diff;

                // NOTE: If these error out it's only because the request is
                // larger than maximum_capacity, which we do not care about in
                // this test.
                let _ = valve.request(ticks_elapsed, request.get());
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
            maximum_capacity in (1..u32::MAX),
            mut ticks_requests in ticks_elapsed_and_cap_requests()) {
            let mut valve = Valve::new(NonZeroU32::new(maximum_capacity).unwrap());
            let maximum_capacity = u64::from(maximum_capacity);

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
                        prop_assert!(granted_requests <= maximum_capacity);
                        granted_requests = 0;
                        current_interval = interval;
                    }
                }

                match valve.request(ticks_elapsed, request.get()) {
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
                    }
                    Err(_) => {
                        // ignored intentionally
                    }
                }
                prop_assert!(granted_requests <= maximum_capacity);
            }
        }
    }
}
