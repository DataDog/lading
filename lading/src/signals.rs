//! Module to control shutdown in lading.
//!
//! Lading manages at least one sub-process, possibly two and must coordinate
//! shutdown with an experimental regime in addition to the target sub-process'
//! potential failures. Controlling shutdown is the responsibility of the code
//! in this module, specifically [`Shutdown`].

use std::sync::Arc;

use tokio::{
    sync::broadcast,
    time::{interval, Duration},
};
use tracing::{error, info};

#[derive(Debug)]
/// Errors produced by [`Shutdown`]
pub enum Error {
    /// The mechanism underlaying [`Shutdown`] failed catastrophically.
    Tokio(broadcast::error::SendError<()>),
}

#[derive(Debug)]
/// Mechanism to control shutdown in lading.
///
/// Lading will shutdown for two reasons: the experimental time set by the user
/// has elapsed or the target sub-process has exited too soon. Everything in
/// lading that participates in controlled shutdown does so by having a clone of
/// this struct.
pub struct Shutdown {
    /// The broadcast sender, signleton for all `Shutdown` instances derived
    /// from the same root `Shutdown`.
    sender: Arc<broadcast::Sender<()>>,

    /// The receive half of the channel used to listen for shutdown. One per
    /// instance.
    notify: broadcast::Receiver<()>,

    /// `true` if the shutdown signal has been received
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
        let (shutdown_snd, shutdown_rcv) = broadcast::channel(1);

        Self {
            sender: Arc::new(shutdown_snd),
            notify: shutdown_rcv,
            shutdown: false,
        }
    }

    /// Receive the shutdown notice. This function will block if a notice has
    /// not already been sent.
    pub async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        // Remember that the signal has been received.
        self.shutdown = true;
    }

    /// Send the shutdown signal through to this and all derived `Shutdown`
    /// instances. Returns the number of active intances, or error.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying tokio broadcast
    /// mechanism fails.
    pub fn signal(&self) -> Result<usize, Error> {
        self.sender.send(()).map_err(Error::Tokio)
    }

    /// Wait for all `Shutdown` instances to properly shut down. This function
    /// is safe to call from multiple instances of a `Shutdown`.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn wait(self, max_delay: Duration) {
        // Tidy up our own `notify`, avoiding a situation where we infinitely wait
        // to shut down.
        drop(self.notify);

        let mut check_pulse = interval(Duration::from_secs(1));
        let mut max_delay = interval(max_delay);
        // Move past the first delay. If we fail to avoid this 0th interval the
        // program shuts down with an error incorrectly.
        max_delay.tick().await;

        loop {
            tokio::select! {
                _ = check_pulse.tick() => {
                    let remaining: usize = self.sender.receiver_count();
                    if remaining == 0 {
                        info!("all tasks shut down");
                        return;
                    }
                    // For reasons that are obscure to me if we sleep here it's
                    // _possible_ for the runtime to fully lock up when the splunk_heck
                    // -- at least -- generator is running. See note below. This only
                    // seems to happen if we have a single-threaded runtime or a low
                    // number of worker threads available. I've reproduced the issue
                    // reliably with 2.
                    info!("waiting for {} tasks to shutdown", remaining);
                }
                _ = max_delay.tick() => {
                    let remaining: usize = self.sender.receiver_count();
                    error!("shutdown wait completing with {} remaining tasks", remaining);
                    return;
                }
            }
        }
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        let notify = self.sender.subscribe();

        Self {
            shutdown: self.shutdown,
            notify,
            sender: Arc::clone(&self.sender),
        }
    }
}
