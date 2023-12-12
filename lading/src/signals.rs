//! Module to signal phases of execution in lading.
//!
//! Lading manages at least one sub-process, possibly two and must coordinate
//! various 'phases' of execution in addition to any error handling.
//! This component was designed as a shutdown mechanism, but is also used to signal
//! the end of warmup phase.
//! Controlling the order of a phase is the responsibility of this module.

use std::sync::Arc;

use tokio::sync::Semaphore;
use tracing::info;

#[derive(Debug)]
/// Mechanism to control shutdown in lading.
///
/// Lading will shutdown for two reasons: the experimental time set by the user
/// has elapsed or the target sub-process has exited too soon. Everything in
/// lading that participates in controlled shutdown does so by having a clone of
/// this struct.
pub struct Phase {
    /// The interior semaphore, the mechanism by which we will 'broadcast'
    /// the current phase has been entered.
    sem: Arc<Semaphore>,

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
}

impl Clone for Phase {
    fn clone(&self) -> Self {
        Self {
            phase_entered: self.phase_entered,
            sem: Arc::clone(&self.sem),
        }
    }
}
