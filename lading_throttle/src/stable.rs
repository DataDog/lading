//! Stable throttle
//!
//! This throttle refills capacity at a steady rate.

use std::num::NonZeroU32;

use super::{Clock, RealClock};

// An 'interval' is the period in which all counters reset. The throttle makes
// no claims on units, but consider if a user intends to produce 1Mb/s the
// 'interval' is one second and each tick corresponds to one microsecond. Each
// microsecond accumulates 1 byte.
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
    /// added.
    maximum_capacity: u32,
    /// The capacity of the `Valve`. This amount will be drawn on by every
    /// request. It is refilled to maximum at every interval roll-over.
    capacity: u32,
    /// The current interval -- multiple of `INTERVAL_TICKS` --  of time.
    interval: u64,
}

impl Valve {
    /// Create a new `Valve` instance with a maximum capacity, given in
    /// tick-units.
    fn new(maximum_capacity: NonZeroU32) -> Self {
        let maximum_capacity = maximum_capacity.get();
        Self {
            capacity: maximum_capacity,
            maximum_capacity,
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
        // seconds up and requests draw down on that bucket. When it's empty, we
        // return the number of ticks until the next interval roll-over. Callers
        // are expected to wait although nothing forces them to. Capacity is
        // only drawn on when it is immediately available.
        if capacity_request == 0 {
            return Ok(0);
        }

        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if capacity_request > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        let current_interval = ticks_elapsed / INTERVAL_TICKS;
        if current_interval > self.interval {
            // We have rolled forward into a new interval. At this point the
            // capacity is reset to maximum -- no matter how deep we are into
            // the interval -- and we record the new interval index.
            self.capacity = self.maximum_capacity;
            self.interval = current_interval;
        }

        // If the capacity is greater or equal to the request we deduct the
        // request from capacity and return 0 slop, signaling to the user that
        // their request is a success. Else, we calculate how long the caller
        // should wait until the interval rolls over. The capacity will never
        // increase in this interval so they will have to call again later.
        if capacity_request <= self.capacity {
            self.capacity -= capacity_request;
            Ok(0)
        } else {
            Ok(INTERVAL_TICKS - (ticks_elapsed % INTERVAL_TICKS))
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
        mut requests: Vec<NonZeroU32>,
    ) -> Result<(), proptest::test_runner::TestCaseError> {
        let mut valve = Valve::new(NonZeroU32::new(maximum_capacity).unwrap());
        let maximum_capacity = u64::from(maximum_capacity);

        let mut ticks_elapsed: u64 = 0;
        let mut granted_requests: u64 = 0;
        let mut interval: u64 = 0;

        let mut slop = 0;
        for request in requests.drain(..) {
            ticks_elapsed += slop;

            let current_interval = ticks_elapsed / INTERVAL_TICKS;
            if interval < current_interval {
                // We have entered into a new interval, the granted requests
                // must be reset.
                prop_assert!(granted_requests <= maximum_capacity,
                                     "[interval-change] Granted requests {granted_requests} exceeded the maximum capacity of the valve, {maximum_capacity}");
                granted_requests = 0;
                interval = current_interval;
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
        let maximum_capacity = 490_301_363u32;
        let requests: Vec<NonZeroU32> = vec![
            NonZeroU32::new(1).unwrap(),
            NonZeroU32::new(490_301_363).unwrap(),
        ];
        capacity_never_exceeds_max_in_interval_inner(maximum_capacity, requests).unwrap();
    }

    fn cap_requests(max: u32) -> impl Strategy<Value = Vec<NonZeroU32>> {
        collection::vec((1..max).prop_map(|i| NonZeroU32::new(i).unwrap()), 1..100)
    }

    // The sum of capacity requests must never exceed maximum_capacity in one
    // INTERVAL_TICKS.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000_000,
            max_shrink_iters: 1_000_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn capacity_never_exceeds_max_in_interval(
            maximum_capacity in (1..u32::MAX),
            requests in cap_requests(u32::from(u16::MAX))
        ) {
            capacity_never_exceeds_max_in_interval_inner(maximum_capacity, requests)?;
        }
    }
}

#[cfg(kani)]
mod verification {
    use crate::stable::{Valve, INTERVAL_TICKS};
    use std::num::NonZeroU32;

    // The sum of capacity requests must never exceed maximum_capacity in one
    // INTERVAL_TICKS.
    #[kani::proof]
    #[kani::unwind(100)] // must match `iters` below
    fn capacity_never_exceeds_max_in_interval() {
        let maximum_capacity: NonZeroU32 = kani::any();
        let mut valve = Valve::new(maximum_capacity);
        let maximum_capacity = maximum_capacity.get();

        let mut ticks_elapsed: u64 = 0;
        let mut granted_requests: u64 = 0;
        let mut interval: u64 = 0;

        let iters: usize = kani::any();
        kani::assume(iters < 100);

        let mut slop = 0;
        for _ in 0..iters {
            let request: NonZeroU32 = kani::any();
            kani::assume(request.get() <= maximum_capacity);

            ticks_elapsed += slop;

            let current_interval = ticks_elapsed / INTERVAL_TICKS;
            if interval < current_interval {
                // We have entered into a new interval, the granted requests
                // must be reset.
                if granted_requests > u64::from(maximum_capacity) {
                    panic!("too many requests granted");
                }
                granted_requests = 0;
                interval = current_interval;
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
            if granted_requests > u64::from(maximum_capacity) {
                panic!("too many requests granted");
            }
        }
    }
}
