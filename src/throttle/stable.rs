use std::{cmp, num::NonZeroU32};

use metrics::gauge;

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
/// This throttle is stable in that it will steadily refil units at a known rate and does not inspect the target in any way, compare to `Predictive` in that regard.
pub(crate) struct Stable<C = RealClock> {
    last_tick: u64,
    /// The capacity left in `Stable` after a user request.
    spare_capacity: u64,
    /// The maximum capacity of `Stable` past which no more capacity will be
    /// added.
    maximum_capacity: u64,
    /// Per tick, how much capacity is added to the throttle.
    refill_per_tick: u64,
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
        // Okay, here's the idea. At the base of `Stable` is a cell rate
        // algorithm. We have bucket that gradually fills up and when it's full
        // it doesn't fill up anymore. Callers draw down on this capacity and if
        // they draw down more than is available in the bucket they're made to
        // wait.

        gauge!("throttle_refills_per_tick", self.refill_per_tick as f64);

        // Fast bail-out. There's no way for this to ever be satisfied and is a
        // bug on the part of the caller, arguably.
        if u64::from(request.get()) > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        // Now that the preliminaries are out of the way, wake up and compute
        // how much the throttle capacity is refilled since we were last
        // called. Depending on how long ago this was we may have completely
        // filled up throttle capacity.
        let ticks_since_start = self.clock.ticks_elapsed();
        let ticks_since_last_wait = ticks_since_start.saturating_sub(self.last_tick);
        self.last_tick = ticks_since_start;
        let refilled_capacity: u64 = cmp::min(
            ticks_since_last_wait
                .saturating_mul(self.refill_per_tick)
                .saturating_add(self.spare_capacity),
            self.maximum_capacity,
        );

        let capacity_request = u64::from(request.get());
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
            let slop = (capacity_request - refilled_capacity) / self.refill_per_tick;
            self.clock.wait(slop).await;
        }
        Ok(())
    }
}

impl<C> Stable<C>
where
    C: Clock + Sync + Send,
{
    pub(crate) fn with_clock(maximum_capacity: NonZeroU32, clock: C) -> Self {
        // We set the maximum capacity of the bucket, X. We say that an
        // 'interval' happens once every second. If we allow for the tick of
        // Throttle to be one per microsecond that's 1x10^6 ticks per interval.

        // We do not want a situation where refill never happens. If the maximum
        // capacity is less than INTERVAL_TICKS we set the floor at 1.
        let refill_per_tick = cmp::max(1, u64::from(maximum_capacity.get()) / INTERVAL_TICKS);

        Self {
            last_tick: clock.ticks_elapsed(),
            maximum_capacity: u64::from(maximum_capacity.get()),
            refill_per_tick,
            spare_capacity: 0,
            clock,
        }
    }

    #[cfg(test)]
    fn maximum_capacity(&self) -> u64 {
        self.maximum_capacity
    }

    #[cfg(test)]
    fn requested_budget(&self) -> u64 {
        self.requested_budget
    }

    #[cfg(test)]
    fn projected_budget(&self) -> u64 {
        self.projected_budget
    }

    #[cfg(test)]
    fn interval(&self) -> u64 {
        self.interval
    }
}
