//! Per-thread lock-free metrics for neper-style workloads.
//!
//! Each OS thread owns a [`ThreadMetrics`] struct containing plain `u64`
//! counters behind [`UnsafeCell`]. Workers increment via [`ThreadCounter::add`].
//! A dedicated metrics thread periodically snapshots all counters, computes deltas,
//! and submits aggregated values to the `metrics` crate via `counter!()`.
//!
//! ## Safety
//!
//! [`ThreadCounter`] is `Sync` because:
//! - Only one thread (the owner) writes to the counter.
//! - The snapshot thread only reads.
//! - Since there is a single writer, and the snapshotter does not need the
//!   precise counts every time, we avoid the use of atomics.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use metrics::counter;

/// A counter written by exactly one thread and read (snapshot) by another.
///
/// Uses plain `u64` for zero overhead on the write path.
#[repr(C, align(8))]
pub(crate) struct ThreadCounter {
    value: UnsafeCell<u64>,
}

// Safety: only one thread writes; reads are tear-free on aligned u64.
unsafe impl Sync for ThreadCounter {}
// Required for Arc<Vec<ThreadMetrics>>.
unsafe impl Send for ThreadCounter {}

impl ThreadCounter {
    pub(crate) const fn new() -> Self {
        Self {
            value: UnsafeCell::new(0),
        }
    }

    /// Increment. Must only be called from the owning thread.
    #[inline]
    pub(crate) fn add(&self, n: u64) {
        unsafe { *self.value.get() += n }
    }

    /// Read current value. May be called from any thread (snapshot reader).
    #[inline]
    pub(crate) fn get(&self) -> u64 {
        unsafe { *self.value.get() }
    }
}

/// Defines [`ThreadMetrics`], the field-name array, and the `read_all` helper
/// from a single list of field names. Add new metrics below to the macro call
macro_rules! define_thread_metrics {
    ($($name:ident),* $(,)?) => {
        /// Per-thread counters.
        /// Fields are a superset used across all modes — unused fields stay at 0.
        #[repr(C, align(128))]
        pub(crate) struct ThreadMetrics {
            $(pub(crate) $name: ThreadCounter,)*
        }

        impl ThreadMetrics {
            pub(crate) const fn new() -> Self {
                Self {
                    $($name: ThreadCounter::new(),)*
                }
            }
        }

        const FIELD_NAMES: &[&str] = &[$(stringify!($name),)*];

        fn read_all(tm: &ThreadMetrics) -> Vec<u64> {
            vec![$(tm.$name.get(),)*]
        }
    };
}

define_thread_metrics! {
    requests_sent,
    responses_received,
    bytes_written,
    bytes_read,
    connections_failed,
    connections_accepted,
    requests_received_count,
    responses_sent,
    bytes_received,
}

/// Tracks previous snapshot values and computes deltas.
struct MetricsSnapshot {
    prev: Vec<Vec<u64>>,
}

impl MetricsSnapshot {
    fn new(num_threads: usize) -> Self {
        Self {
            prev: vec![vec![0u64; FIELD_NAMES.len()]; num_threads],
        }
    }

    fn snapshot_and_submit(
        &mut self,
        thread_metrics: &[ThreadMetrics],
        labels: &[(String, String)],
    ) {
        let num_fields = FIELD_NAMES.len();
        let mut totals = vec![0u64; num_fields];
        for (i, tm) in thread_metrics.iter().enumerate() {
            let curr = read_all(tm);
            for f in 0..num_fields {
                let delta = curr[f].wrapping_sub(self.prev[i][f]);
                totals[f] += delta;
            }
            self.prev[i] = curr;
        }
        for f in 0..num_fields {
            if totals[f] > 0 {
                counter!(FIELD_NAMES[f], labels).increment(totals[f]);
            }
        }
    }
}

/// Run the metrics snapshot loop on the current thread.
///
/// Blocks until `shutdown` is set. Performs a final snapshot before returning
/// to flush any remaining deltas.
pub(crate) fn run_metrics_thread(
    thread_metrics: &[ThreadMetrics],
    labels: &[(String, String)],
    sample_period: Duration,
    shutdown: &AtomicBool,
) {
    let mut snapshot = MetricsSnapshot::new(thread_metrics.len());

    let mut total_snapshot_ns: u64 = 0;
    let mut snapshot_count: u64 = 0;

    while !shutdown.load(Relaxed) {
        std::thread::sleep(sample_period);
        let start = Instant::now();
        snapshot.snapshot_and_submit(thread_metrics, labels);
        total_snapshot_ns += u64::from(start.elapsed().subsec_nanos());
        snapshot_count += 1;
    }
    // Final snapshot to capture remaining deltas.
    snapshot.snapshot_and_submit(thread_metrics, labels);

    if snapshot_count > 0 {
        let avg_ns = total_snapshot_ns / snapshot_count;
        tracing::info!(
            avg_snapshot_ns = avg_ns,
            snapshots = snapshot_count,
            "metrics snapshot average: {avg_ns}ns over {snapshot_count} snapshots"
        );
    }
}
