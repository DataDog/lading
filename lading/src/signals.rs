//! Module to control shutdown in lading.
//!
//! Lading manages at least one sub-process, possibly two and must coordinate
//! shutdown with an experimental regime in addition to the target sub-process'
//! potential failures. Controlling shutdown is the responsibility of the code
//! in this module, specifically [`Shutdown`].

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
pub struct Shutdown {
    /// The interior semaphore, the mechanism by which we will 'broadcast'
    /// shutdown.
    sem: Arc<Semaphore>,

    /// `true` if the shutdown signal has been received.
    shutdown: bool,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    /// Create a new `Shutdown` instance. There should be only one call to this
    /// function and all subsequent instances should be created through clones.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sem: Arc::new(Semaphore::new(0)),
            shutdown: false,
        }
    }

    /// Receive the shutdown notice. This function will block if a notice has
    /// not already been sent.
    pub async fn recv(&mut self) {
        // NOTE if we ever need a sync version of this function the interior of
        // this function but with `try_acquire` will work just fine.

        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return;
        }

        // We have no need of the permit that comes on the okay side, we also
        // are fine to set shutdown if the semephore has been closed on us.
        let _ = self.sem.acquire().await;

        // Remember that the signal has been received.
        self.shutdown = true;
    }

    /// Send the shutdown signal through to this and all derived `Shutdown`
    /// instances.
    #[tracing::instrument]
    pub fn signal(&self) {
        let fill = Semaphore::MAX_PERMITS.saturating_sub(self.sem.available_permits());
        info!(permits = fill, "signaling shutdown");
        self.sem.add_permits(fill);
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Self {
            shutdown: self.shutdown,
            sem: Arc::clone(&self.sem),
        }
    }
}
