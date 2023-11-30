//! Module to control shutdown in lading.
//!
//! Lading manages at least one sub-process, possibly two and must coordinate
//! shutdown with an experimental regime in addition to the target sub-process'
//! potential failures. Controlling shutdown is the responsibility of the code
//! in this module, specifically [`Shutdown`].

use std::sync::Arc;

use tokio::sync::broadcast;

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
