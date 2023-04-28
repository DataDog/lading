use std::{cmp, num::NonZeroU32};

use metrics::gauge;
use tracing::info;

use crate::target;

use super::{Clock, RealClock};

const INTERVAL_TICKS: u64 = 1_000_000;

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub(crate) enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("Capacity")]
    Capacity,
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
/// The last is based on a budget allocation heuristic in large political
/// organizations:
///
/// * Given parties Consumer, Provider and Budget which Provider dictates the
///   maximum of, doles out to Consumer. The initial budget is the maximum.
/// * Allow the projected budget for an interval to be X and the actual
///   requested budget for that interval to be Y.
/// * At each negotiation interval the Provider calculates a new projected budget:
///   - If Y < X then new budget is X - (X-Y)/8,
///   - If Y >= X then new budget is X + (Y-X)/4.
pub(crate) struct Predictive<C = RealClock> {
    last_tick: u64,
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
    clock: C,
}

impl<C> Predictive<C>
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
        if u64::from(request.get()) > self.maximum_capacity {
            return Err(Error::Capacity);
        }

        // Now that the preliminaries are out of the way, wake up and compute
        // how much the throttle capacity is refilled since we were last
        // called. Depending on how long ago this was we may have completely
        // filled up throttle capacity. Note we fill to the _projected_ budget
        // and not the maximum capacity.
        let ticks_since_start = self.clock.ticks_elapsed();
        let ticks_since_last_wait = ticks_since_start.saturating_sub(self.last_tick);
        self.last_tick = ticks_since_start;
        let refilled_capacity: u64 = cmp::min(
            ticks_since_last_wait
                .saturating_mul(self.refill_per_tick)
                .saturating_add(self.spare_capacity),
            self.projected_budget,
        );

        // Now that capacity is refreshed it's time to adjust the projected
        // budget. This we do by determining if we've moved into a new interval
        // and, if we have, raising that budget. Note that if we skip multiple
        // intervals between calls we do not currently reduce the projected
        // budget for those 'missing' intervals, although that would be
        // interesting to do maybe.

        let current_interval = ticks_since_start / INTERVAL_TICKS;
        if current_interval == self.interval {
            // Intentionally blank. There is nothing to do in the event we are
            // in the same interval, with regard to budgets.
        } else {
            // The new interval is begun, calculate new budgets.
            //
            // If the client requested budget is lower than the projected budget
            // we set the next interval's projected budget 1/8 down the
            // difference from the current projected budget. If it's higher we
            // creep up 1/4 up the difference. This allows us to steadily change
            // toward the setpoint without fluctuating wildly.
            if self.requested_budget <= self.projected_budget {
                let diff = self.projected_budget - self.requested_budget;
                self.projected_budget -= diff / 8;
            } else {
                let diff = self.requested_budget - self.projected_budget;
                self.projected_budget =
                    cmp::min(self.projected_budget + (diff / 4), self.maximum_capacity);
            }
            // Adjust the interval upward, set the requested budget to zero in
            // this brave new interval.
            assert!(current_interval > self.interval);
            self.interval = current_interval;
            self.requested_budget = 0;
        }

        self.requested_budget = cmp::min(
            self.requested_budget.wrapping_add(u64::from(request.get())),
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

impl<C> Predictive<C>
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
            requested_budget: 0,
            projected_budget: u64::from(maximum_capacity.get()),
            interval: 0,
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

#[cfg(test)]
mod test {
    use std::{
        num::NonZeroU32,
        sync::Mutex,
        task::{Context, Poll},
    };

    use async_trait::async_trait;
    use futures::{task::noop_waker, Future, FutureExt};
    use proptest::prelude::*;

    use super::{Clock, Predictive};

    // A test clock for Throttle
    //
    // The idea here is `wait` does nothing. Time skips forward by looping
    // through `tick_progressions`, simulating a setup with no real-time
    // deadlines. We store -- in a relaxed fashion -- the previous tick wait,
    // which we add to any progression of time. If we didn't do that waits would
    // be totally untethered from tick progress, which is not quite right.
    #[derive(Debug)]
    struct TestClockMeta {
        idx: usize,
        ticks_elapsed: u64,
    }

    #[derive(Debug)]
    struct TestClock {
        meta: Mutex<TestClockMeta>,
        tick_progressions: Vec<u8>, // time can move backward but stdlib returns 0 if that happens
    }

    impl TestClock {
        fn new(tick_progressions: Vec<u8>) -> Self {
            Self {
                meta: Mutex::new(TestClockMeta {
                    idx: 0,
                    ticks_elapsed: 0,
                }),
                tick_progressions,
            }
        }
    }

    #[async_trait]
    impl Clock for TestClock {
        fn ticks_elapsed(&self) -> u64 {
            self.meta.lock().unwrap().ticks_elapsed
        }

        async fn wait(&self, request: u64) {
            let mut meta = self.meta.lock().unwrap();

            let current_tick = self.tick_progressions[meta.idx];
            meta.ticks_elapsed += u64::from(current_tick) + request;
            meta.idx = (meta.idx + 1) % self.tick_progressions.len();
        }
    }

    fn drive<T>(future: impl Future<Output = T>) -> T {
        let waker = noop_waker();
        let mut ctx = Context::from_waker(&waker);

        let mut future = Box::pin(future);
        loop {
            match future.poll_unpin(&mut ctx) {
                Poll::Pending => continue,
                Poll::Ready(res) => return res,
            }
        }
    }

    fn ticks() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 1..5000)
    }

    fn nonempty_requests() -> impl Strategy<Value = Vec<u32>> {
        let vals = any::<u32>().prop_filter("non-zero", |x| *x != 0_u32);
        prop::collection::vec(vals, 1..10_000)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn projected_budget_always_le_max(
            tick_progressions in ticks(),
            maximum_capacity in 1_u32..u32::MAX,
            requests in nonempty_requests(),
        ) {
            let maximum_capacity = NonZeroU32::new(maximum_capacity).unwrap();
            let requests: Vec<NonZeroU32> = requests
                .into_iter()
                .filter(|x| *x > 0)
                .map(|x| NonZeroU32::new(x).unwrap())
                .collect();

            let clock = TestClock::new(tick_progressions);
            let mut throttle = Predictive::with_clock(maximum_capacity, clock);

            for request in requests {
                assert!(throttle.maximum_capacity() >= throttle.projected_budget());
                let _: Result<(), _> = drive(throttle.wait_for(request));
                assert!(throttle.maximum_capacity() >= throttle.projected_budget());
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn requested_budget_always_le_max(
            tick_progressions in ticks(),
            maximum_capacity in 1_u32..u32::MAX,
            requests in nonempty_requests(),
        ) {
            let maximum_capacity = NonZeroU32::new(maximum_capacity).unwrap();
            let requests: Vec<NonZeroU32> = requests
                .into_iter()
                .filter(|x| *x > 0)
                .map(|x| NonZeroU32::new(x).unwrap())
                .collect();

            let clock = TestClock::new(tick_progressions);
            let mut throttle = Predictive::with_clock(maximum_capacity, clock);

            for request in requests {
                assert!(throttle.maximum_capacity() >= throttle.requested_budget());
                let _: Result<(), _> = drive(throttle.wait_for(request));
                assert!(throttle.maximum_capacity() >= throttle.requested_budget());
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            max_shrink_iters: 100_000,
            .. ProptestConfig::default()
        })]
        #[test]
        fn time_advances(
            tick_progressions in ticks(),
            maximum_capacity in 1_u32..u32::MAX,
            requests in nonempty_requests(),
        ) {
            let maximum_capacity = NonZeroU32::new(maximum_capacity).unwrap();
            let requests: Vec<NonZeroU32> = requests
                .into_iter()
                .filter(|x| *x > 0)
                .map(|x| NonZeroU32::new(x).unwrap())
                .collect();

            let clock = TestClock::new(tick_progressions);
            let mut throttle = Predictive::with_clock(maximum_capacity, clock);

            let mut prev_interval = throttle.interval();

            for request in requests {
                let _: Result<(), _> = drive(throttle.wait_for(request));
                assert!(throttle.interval() >= prev_interval);
                prev_interval = throttle.interval();
            }
        }
    }
}
