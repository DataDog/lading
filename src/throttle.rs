//! A throttle for input into the target program.

// There are three signals that are obeyed in this Throttle,

use std::{cmp, num::NonZeroU32};
use tokio::time::{self, Duration, Instant};
use tracing::info;

use crate::target;

const INTERVAL_TICKS: u128 = 1_000_000;

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("Capacity")]
    Capacity,
}

// // Knuth TAOCP vol 2, 3rd edition, page 232, discussed
// // https://www.johndcook.com/blog/standard_deviation/ and
// // https://math.stackexchange.com/a/116344.
// #[derive(Default, Debug)]
// struct Welford {
//     m_n: u64,
//     m_oldM: f64,
//     m_newM: f64,
//     m_oldS: f64,
//     m_newS: f64,
// }

// impl Welford {
//     fn push(&mut self, value: f64) {
//         self.m_n += 1;
//         if self.m_n == 1 {
//             self.m_newM = value;
//             self.m_oldM = value;
//             self.m_oldS = 0.0;
//         } else {
//             self.m_newM = self.m_oldM + (value - self.m_oldM) / (self.m_n as f64);
//             self.m_newS = self.m_oldS + (value - self.m_oldM) * (value - self.m_newM);

//             self.m_oldM = self.m_newM;
//             self.m_oldS = self.m_newS;
//         }
//     }

//     fn mean(&self) -> f64 {
//         if self.m_n == 0 {
//             0.0
//         } else {
//             self.m_newM
//         }
//     }

//     fn variance(&self) -> f64 {
//         if self.m_n > 1 {
//             let shifted_m_n = self.m_n - 1;
//             self.m_newS / (shifted_m_n as f64)
//         } else {
//             0.0
//         }
//     }

//     fn stdev(&self) -> f64 {
//         self.variance().sqrt()
//     }
// }

#[derive(Debug)]
/// A throttle type.
///
/// This throttle participates in three signals:
///
/// * time-based allocation of capacity, with user draw-down of that capacity,
/// * binary back-off based on RSS consumption of the target and
/// * a 'bean counter' approach to searching for target throughput setpoint.
///
/// The last is based on a budget allocation heuristic in large political organizations:
///
/// * Given parties Consumer, Provider and Budget which Provider dictates the
///   maximum of, doles out to Consumer. Allow the initial state of Budget to be
///   X.
/// * At each negotiation interval Consumer advertises to Provider that its
///   actual consumption of budget was Y, its new desired budget is Z.
///   - If Y < X then new budget is Y, discarding Z.
///   - If Y >= X then new budget is Z * 125%.
///
/// In `Throttle` consumer does not advertise its desired budget. Instead
/// `Throttle` keeps track keep track of the actual budget consumption would
/// have been were every request satisfied and, once a second, sets the new
/// budget as the midway point between the previous budget and actual
pub(crate) struct Throttle {
    spare_capacity: u128,

    maximum_capacity: u128,
    /// The budget that was requested in the interval, whether it was satisfied
    /// or not.
    requested_budget: u128,
    projected_budget: u128,
    /// The instance the current interval began.
    interval_start: Instant,
    /// The interval number, primarily useful for testing and debugging.
    interval: u128,
    //    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
}

impl Throttle {
    pub(crate) fn new(maximum_capacity: NonZeroU32) -> Self {
        // We set the maximum capacity of the bucket, X. We say that an
        // 'interval' happens once every second. If we allow for the tick of
        // Throttle to be one per microsecond that's 1x10^6 ticks per interval.

        // let refill_per_tick = (maximum_capacity.get() as f64) / 1_000_000.0;

        // let rate_limiter = RateLimiter::direct(Quota::per_second(maximum_capacity));
        // let maximum_capacity = maximum_capacity.get();
        // let interval_actual_budget = maximum_capacity / 10;

        Self {
            maximum_capacity: maximum_capacity.get() as u128,

            //            rate_limiter,
            // interval_actual_budget,
            requested_budget: 0,
            projected_budget: maximum_capacity.get() as u128 / 2,
            interval_start: Instant::now(),
            interval: 0,
            spare_capacity: 0,
        }
    }

    #[inline]
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        // SAFETY: 1_u32 is a non-zero u32.
        let one = unsafe { NonZeroU32::new_unchecked(1_u32) };
        self.wait_for(one).await
    }

    pub(crate) async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        loop {
            if target::Meta::rss_bytes_limit_exceeded() {
                info!("RSS byte limit exceeded, backing off...");
                time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }

        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if request.get() as u128 > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        // Determine if its time for the budget to be recalculated. If we have
        // passed the second boundary since the budget anchor was set we look to see if the previous budget was used. If not, we reset to the actual used budget, if yes we

        // Wake up and compute how much the throttle capacity is refilled since
        // we were last called.
        let now = Instant::now();
        let ticks_since = now.duration_since(self.interval_start).as_micros();
        let refilled_capacity: u128 = cmp::max(
            ticks_since.wrapping_mul(f64::MAX as u128) + self.spare_capacity,
            self.projected_budget,
        );

        // Figure out if we're in the same interval or not. If we are, increase
        // interval_requested_budget. If we are not see if the client ended the
        // interval with budget remaining, adjust refill rate downward, else
        // upward.
        let current_interval = ticks_since % INTERVAL_TICKS;
        if current_interval != self.interval {
            // interval elapsed
            self.interval = current_interval;
            // If the client requested budget is lower than the projected budget
            // we set the next interval's projected budget to the requested
            // one. If it's higher, we creep up from the projected budget.
            if self.requested_budget <= self.projected_budget {
                self.projected_budget = self.requested_budget;
            } else {
                self.projected_budget = cmp::max(
                    self.projected_budget + (self.projected_budget / 4),
                    self.maximum_capacity,
                );
            }
            self.requested_budget = 0;
        }

        self.requested_budget = self.requested_budget.wrapping_add(request.get() as u128);

        let capacity_request = request.get() as u128;
        if refilled_capacity > capacity_request {
            self.spare_capacity = refilled_capacity - capacity_request;
        } else {
            let slop = capacity_request - refilled_capacity;
            time::sleep(Duration::from_micros(slop as u64)).await;
        }
        Ok(())

        // satisfy
        // the request or wait enough ticks to do so. Either way we increase the requested If we are not,

        // let seconds_elapsed = self.interval_start.elapsed().as_secs();
        // if seconds_elapsed > 0 {
        //     self.interval += seconds_elapsed;
        // }

        // Are we in the same interval or not?

        // When the refilled_capacity is greater than the user requested capacity we settle the request, adjusting capacity and budget
        // if refilled_capacity >

        // // We check whether we're within the current interval or have moved
        // // beyond it. If we are within the current interval we bump the
        // // requested_budget and then wait for the inner rate limiter,
        // // returning. This will potentially push us into the next interval but
        // // avoids the situation where we accumulate in the interior rate limiter
        // // overmuch and then burst at the start of each interval.
        // //
        // // If we are beyond `interval` then we bump the interval, recording its
        // // new instant, and fiddle with the budget based on previous requests.
        // if self.interval_start.elapsed() <= self.interval_width {
        //     self.interval += 1;
        //     self.interval_requested_budget =
        //         self.interval_actual_budget.wrapping_add(capacity.get());

        //     // Wait for the rate limiter, then check that the budget requested
        //     // has not exceeded the budget for the interval.
        //     self.rate_limiter.until_n_ready(capacity).await.unwrap();
        //     if self.interval_requested_budget > self.interval_actual_budget {
        //         return Ok(None);
        //     }
        //     unimplemented!()
        // } else {
        //     unimplemented!()
        // }

        // self.rate_limiter.until_n_ready(capacity).await.unwrap();
        // Ok(())
    }
}
