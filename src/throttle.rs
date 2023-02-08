//! A throttle for input into the target program.

use metrics::gauge;
use std::{cmp, num::NonZeroU32};
use tokio::time::{self, Duration, Instant};
use tracing::info;

use crate::target;

const INTERVAL_TICKS: u64 = 1_000_000;

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("Capacity")]
    Capacity,
}

#[derive(Debug)]
struct Clock {
    start: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Clock {
    /// Return the number of ticks since `Clock` was created.
    ///
    /// # Panics
    ///
    /// Function will panic if the number of ticks elapsed is greater than u64::MAX.
    fn ticks_elapsed(&self) -> u64 {
        let now = Instant::now();
        let ticks_since: u128 = now.duration_since(self.start).as_micros();
        if ticks_since > (u64::MAX as u128) {
            panic!("584,554 years elapsed since last call!");
        }
        ticks_since as u64
    }

    async fn wait(&self, ticks: u64) {
        time::sleep(Duration::from_micros(ticks)).await
    }
}

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
    spare_capacity: u64,
    /// The maximum capacity of `Throttle` past which no more capacity will be
    /// added.
    maximum_capacity: u64,
    /// The budget that was requested in the interval, whether it was satisfied
    /// or not.
    requested_budget: u64,
    /// The budget that was projected for the interval, a 'soft' limit. Will
    /// never be greater than `maximum_capacity`.
    projected_budget: u64,
    /// The interval number, primarily useful for testing and debugging.
    interval: u64,
    /// Per tick, how much capacity is added to the throttle.
    refill_per_tick: u64,
    /// The clock that this `Throttle` will use.
    clock: Clock,
}

impl Throttle {
    pub(crate) fn new(maximum_capacity: NonZeroU32) -> Self {
        // We set the maximum capacity of the bucket, X. We say that an
        // 'interval' happens once every second. If we allow for the tick of
        // Throttle to be one per microsecond that's 1x10^6 ticks per interval.

        let refill_per_tick = (maximum_capacity.get() as u64) / INTERVAL_TICKS;

        // let rate_limiter = RateLimiter::direct(Quota::per_second(maximum_capacity));
        // let maximum_capacity = maximum_capacity.get();
        // let interval_actual_budget = maximum_capacity / 10;

        Self {
            maximum_capacity: maximum_capacity.get() as u64,
            refill_per_tick,
            requested_budget: 0,
            projected_budget: maximum_capacity.get() as u64 / 2,
            interval: 0,
            spare_capacity: 0,
            clock: Clock::default(),
        }
    }

    #[inline]
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        // SAFETY: 1_u32 is a non-zero u32.
        let one = unsafe { NonZeroU32::new_unchecked(1_u32) };
        self.wait_for(one).await
    }

    pub(crate) async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        // Okay, here's the idea. At the base of `Throttle` is a cell rate
        // algorithm. We have bucket that gradually fills up and when it's full
        // it doesn't fill up anymore. Callers draw down on this capacity and if
        // they draw down more than is available in the bucket they're made to
        // wait.
        //
        // We augment this in two ways. The first is a binary on/off with regard
        // to target RSS limit: if the target is above the limit we hang the
        // caller for one second, else we don't. The second is a trick we play
        // with capacity. The bucket is of a certain, fixed size but we draw a
        // 'projected budget' somewhere below that capacity and hold the client
        // to that. If they don't meet the projected budget in an 'interval' --
        // one second -- their next projected budget is lower. If they go above
        // that projected budget their next interval has more. The goal is to
        // narrow in on a 'true' rate for the caller.

        gauge!("throttle_spare_capacity", self.spare_capacity as f64);
        gauge!("throttle_refills_per_tick", self.refill_per_tick as f64);
        gauge!("throttle_requested_budget", self.requested_budget as f64);
        gauge!("throttle_projected_budget", self.projected_budget as f64);

        // RSS limit signal. Thankfully very simple: a loop that polls
        // `rss_bytes_limit_exceeded` once a second.
        loop {
            if target::Meta::rss_bytes_limit_exceeded() {
                info!("RSS byte limit exceeded, backing off...");
                self.clock.wait(INTERVAL_TICKS).await;
            } else {
                break;
            }
        }

        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if request.get() as u64 > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        // Now that the preliminaries are out of the way, wake up and compute
        // how much the throttle capacity is refilled since we were last
        // called. Depending on how long ago this was we may have completely
        // filled up throttle capacity. Note we fill to the _projected_ budget
        // and not the maximum capacity.
        let ticks_since = self.clock.ticks_elapsed();
        let refilled_capacity: u64 = cmp::min(
            ticks_since
                .wrapping_mul(f64::MAX as u64)
                .wrapping_mul(self.refill_per_tick)
                .wrapping_add(self.spare_capacity),
            self.projected_budget,
        );

        // Now that capacity is refreshed it's time to adjust the projected
        // budget. This we do by determining if we've moved into a new interval
        // and, if we have, raising that budget. Note that if we skip multiple
        // intervals between calls we do not currently reduce the projected
        // budget for those 'missing' intervals, although that would be
        // interesting to do maybe.

        let current_interval = ticks_since % INTERVAL_TICKS;
        if current_interval != self.interval {
            // We are not in the same interval.
            self.interval = current_interval;
            // If the client requested budget is lower than the projected budget
            // we set the next interval's projected budget to the requested
            // one. If it's higher, we creep up from the projected budget.
            if self.requested_budget <= self.projected_budget {
                self.projected_budget = self.requested_budget;
            } else {
                self.projected_budget = cmp::min(
                    self.projected_budget + (self.projected_budget / 4),
                    self.maximum_capacity,
                );
            }
            self.requested_budget = 0;
        } else {
            // Intentionally blank. There is nothing to do in the event we are
            // in the same interval, with regard to budgets.
        }

        self.requested_budget = self.requested_budget.wrapping_add(request.get() as u64);

        let capacity_request = request.get() as u64;
        if refilled_capacity > capacity_request {
            // If the refilled capacity is greater than the request we respond
            // to the caller immediately and store the spare capacity for next
            // call.
            self.spare_capacity = refilled_capacity - capacity_request;
        } else {
            // If the refill is not sufficient we calculate how many ticks will
            // need to pass before capacity is sufficient, force the client to
            // wait that amount of time.
            self.spare_capacity = 0;
            let slop = capacity_request - refilled_capacity;
            self.clock.wait(slop).await;
        }
        Ok(())
    }
}
