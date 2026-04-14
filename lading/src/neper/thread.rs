//! OS thread lifecycle helpers for network throughput workloads
//!
//! Provides shutdown flag management, flow distribution, and thread
//! spawning/joining utilities.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::{self, JoinHandle};

/// Shared shutdown flag. Set by the async side when `lading_signal` fires.
/// Polled by OS threads in their event loops.
pub(crate) type ShutdownFlag = Arc<AtomicBool>;

/// Create a new shutdown flag, initially `false`.
pub(crate) fn new_shutdown_flag() -> ShutdownFlag {
    Arc::new(AtomicBool::new(false))
}

/// Distribute `total_flows` across `num_threads` threads.
///
/// Returns a `Vec` of length `num_threads` where entry `i` is the number of
/// flows assigned to thread `i`. The remainder is distributed round-robin to
/// the first threads.
pub(crate) fn distribute_flows(total_flows: u16, num_threads: u16) -> Vec<u16> {
    let base = total_flows / num_threads;
    let remainder = total_flows % num_threads;
    (0..num_threads)
        .map(|i| base + u16::from(i < remainder))
        .collect()
}

/// Join all thread handles.
///
/// Returns `Ok` with the collected results, or `Err` if any thread panicked.
pub(crate) fn join_all<T>(handles: Vec<JoinHandle<T>>) -> Result<Vec<T>, ()> {
    let mut results = Vec::with_capacity(handles.len());
    let mut had_panic = false;
    for handle in handles {
        match handle.join() {
            Ok(val) => results.push(val),
            Err(_) => had_panic = true,
        }
    }
    if had_panic { Err(()) } else { Ok(results) }
}

/// Spawn a named OS thread running `f`.
pub(crate) fn spawn_named<F, T>(name: &str, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .expect("failed to spawn thread")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribute_even() {
        assert_eq!(distribute_flows(4, 2), vec![2, 2]);
    }

    #[test]
    fn distribute_remainder() {
        assert_eq!(distribute_flows(5, 2), vec![3, 2]);
    }

    #[test]
    fn distribute_more_threads_than_flows() {
        assert_eq!(distribute_flows(2, 4), vec![1, 1, 0, 0]);
    }

    #[test]
    fn distribute_single_thread() {
        assert_eq!(distribute_flows(10, 1), vec![10]);
    }
}
