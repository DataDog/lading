//! Module to signal phases of execution in lading.
//!
//! Lading manages at least one sub-process, possibly two and must coordinate
//! various 'phases' of execution in addition to any error handling.
//! This component was designed as a shutdown mechanism, but is also used to signal
//! the end of warmup phase.
//! Controlling the order of a phase is the responsibility of this module.

use std::sync::Arc;
use tokio::sync::Semaphore;

#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, Ordering};

use tracing::info;

#[derive(Debug)]
/// Mechanism to control shutdown in lading.
///
/// Lading will shutdown for two reasons: the experimental time set by the user
/// has elapsed or the target sub-process has exited too soon. Everything in
/// lading that participates in controlled shutdown does so by having a clone of
/// this struct.
pub struct Phase {
    /// The mechanism by which we will 'broadcast' the current phase has been
    /// entered.
    sem: Arc<Semaphore>,

    /// The mechanism by which we will 'ack' receipt of phase change to the `Phase` creator.
    ack_sem: Arc<Semaphore>,

    /// The total number of peers, incremented only by `register`.
    peers: Arc<AtomicU32>,

    /// `true` if the current phase has been entered.
    phase_entered: bool,
}

impl Default for Phase {
    fn default() -> Self {
        Self::new()
    }
}

impl Phase {
    /// Create a new `PhaseSignal` instance. There should be only one call to this
    /// function and all subsequent instances should be created through clones.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sem: Arc::new(Semaphore::new(0)),
            ack_sem: Arc::new(Semaphore::new(0)),
            peers: Arc::new(0.into()),
            phase_entered: false,
        }
    }

    /// Receive the shutdown notice. This function will block if a notice has
    /// not already been sent.
    pub async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.phase_entered {
            return;
        }

        // We have no need of the permit that comes on the okay side, we also
        // are fine to set shutdown if the semaphore has been closed on us.
        let _ = self.sem.acquire().await;

        // Remember that the signal has been received.
        self.phase_entered = true;
    }

    /// Check if a shutdown notice has been sent without blocking.
    pub fn try_recv(&mut self) -> bool {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.phase_entered {
            return true;
        }

        // We have no need of the permit that comes on the okay side, we also
        // are fine to set shutdown if the semaphore has been closed on us.
        if let Ok(_permit) = self.sem.try_acquire() {
            // Remember that the signal has been received.
            self.phase_entered = true;

            return true;
        }

        false
    }

    /// Send the shutdown signal through to this and all derived `Shutdown`
    /// instances.
    #[tracing::instrument]
    pub fn signal(&self) {
        let fill = Semaphore::MAX_PERMITS.saturating_sub(self.sem.available_permits());
        info!(permits = fill, "signaling phase entered");
        self.sem.add_permits(fill);
    }

    /// Wait for any registered peers before returning.
    #[tracing::instrument]
    pub async fn wait_for_peers(self) {
        let peers = self.peers.load(Ordering::Acquire);
        let _ = self.ack_sem.acquire_many(peers).await;
    }

    /// Register with the `Phase` creator to avoid the call to `signal` from
    /// proceeding without the token returned here being dropped.
    #[tracing::instrument]
    pub fn register(&self) -> Token {
        // Increment the peers. We are careful to AcqRel this fetch and store
        // to avoid the parent from being unable to read the correct number of
        // peers later.
        self.peers.fetch_add(1, Ordering::AcqRel);
        Token {
            ack_sem: Arc::clone(&self.ack_sem),
        }
    }
}

impl Clone for Phase {
    fn clone(&self) -> Self {
        Self {
            ack_sem: Arc::clone(&self.ack_sem),
            peers: Arc::clone(&self.peers),
            phase_entered: self.phase_entered,
            sem: Arc::clone(&self.sem),
        }
    }
}

/// Return from [`Phase::signal`]. When this drops phase signal will be presumed
/// ack'ed whether `recv` is called or not.
#[derive(Debug)]
pub struct Token {
    /// The mechanism by which we will 'ack' receipt of phase change to the
    /// `Phase` creator.
    ack_sem: Arc<Semaphore>,
}

impl Drop for Token {
    fn drop(&mut self) {
        self.ack_sem.add_permits(1);
    }
}

#[cfg(test)]
mod test {
    use super::Phase;

    #[cfg(loom)]
    #[test]
    fn test_simple_phase_change() {
        use loom::thread;
        use tokio::runtime::Builder;

        loom::model(|| {
            // Main thread -- current thread -- will create a Phase and then
            // signal its change. Child-thread will spin up a tokio runtime and
            // recv on the phase.

            let phase = Phase::new(); // will be used to signal

            // Thread to receive phase change
            let mut rcv_phase = phase.clone();
            let rcv_thread = thread::spawn(move || {
                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                rt.block_on(async {
                    rcv_phase.recv().await;
                })
            });

            // Signal, then what for the child thread.
            phase.signal();

            rcv_thread.join().unwrap();

            // // After signaling, the phase should be entered
            // let mut phase_clone = phase.clone();
            // assert!(phase_clone.try_recv(), "Phase should be entered");
        });
    }

    #[cfg(loom)]
    #[test]
    fn test_signal_twice() {
        use loom::thread;
        use tokio::runtime::Builder;

        loom::model(|| {
            // Main thread -- current thread -- will create a Phase and then
            // signal its change. Child-thread will spin up a tokio runtime and
            // recv on the phase.

            let phase = Phase::new(); // will be used to signal

            // Thread to receive phase change
            let mut rcv_phase = phase.clone();
            let rcv_thread = thread::spawn(move || {
                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                rt.block_on(async {
                    rcv_phase.recv().await;
                })
            });

            // Signal, then what for the child thread and then signal again.
            phase.signal();
            rcv_thread.join().unwrap();
            phase.signal();

            // // After signaling, the phase should be entered
            // let mut phase_clone = phase.clone();
            // assert!(phase_clone.try_recv(), "Phase should be entered");
        });
    }

    #[cfg(loom)]
    #[test]
    fn test_signal_and_wait() {
        use loom::thread;
        use tokio::runtime::Builder;

        loom::model(|| {
            let phase = Phase::new(); // will be used to signal

            // Thread to receive phase change
            let mut rcv_handles = Vec::new();
            for _ in 0..10 {
                let mut rcv_phase = phase.clone();
                let handle = thread::spawn(move || {
                    let rt = Builder::new_current_thread().enable_all().build().unwrap();
                    rt.block_on(async {
                        let _token = rcv_phase.register();
                        rcv_phase.recv().await;
                    })
                });
                rcv_handles.push(handle);
            }

            // Thread to wait
            let wait_phase = phase.clone();
            let wait_handle = thread::spawn(move || {
                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                rt.block_on(async {
                    wait_phase.wait_for_peers().await;
                })
            });

            phase.signal();
            for handle in rcv_handles.drain(..) {
                handle.join().unwrap();
            }
            wait_handle.join().unwrap();
        });
    }
}
