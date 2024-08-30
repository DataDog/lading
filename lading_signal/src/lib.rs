//! Module to signal phase changes in lading.
//!
//! Lading manages at least one sub-process, possibly two and must coordinate
//! various 'phases' of execution in addition to any error handling. This
//! component was designed as a shutdown mechanism, but is also used to signal
//! the end of warmup phase.
//!
//! The mechanism here has two components, a `Broadcaster` and a `Watcher`. The
//! `Broadcaster` is responsible for signaling the `Watcher` that a phase has been
//! achieved. This is a one-time event and if multiple phases are tracked
//! multiple signal mechanisms are required. The `Watcher` is responsible for
//! waiting for the signal to be sent.
//!
//! There is only one `Broadcaster` and potentially many `Watcher` instances.

#[cfg(not(loom))]
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

#[cfg(loom)]
use loom::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use tokio::sync::Notify;

/// Construct a `Watcher` and `Broadcaster` pair.
pub fn signal() -> (Watcher, Broadcaster) {
    let peers = Arc::new(AtomicU32::new(1));
    let watcher = Arc::new(Notify::new());
    let broadcaster = Arc::new(Notify::new());
    let signaled = Arc::new(AtomicBool::new(false));

    let w = Watcher {
        peers: Arc::clone(&peers),
        signal_received: false,
        signaled: Arc::clone(&signaled),
        broadcaster: Arc::clone(&broadcaster),
        watcher: Arc::clone(&watcher),
    };

    let b = Broadcaster {
        peers,
        signaled,
        broadcaster,
        watcher,
    };

    (w, b)
}

#[derive(Debug)]
/// Mechanism to notify one or more `Watcher` instances that a phase has been
/// achieved.
pub struct Broadcaster {
    /// The total number of peers subscribed to this `Broadcaster`.
    peers: Arc<AtomicU32>,
    /// Used by `Watcher` instances to wake the `Broadcaster` while it waits for
    /// `Watcher` instances to shut down.
    broadcaster: Arc<Notify>,
    /// Used by `Broadcaster` to wake `Watcher` instances when the signal is
    /// transmitted.
    watcher: Arc<Notify>,
    /// Whether the signal has been sent or not.
    signaled: Arc<AtomicBool>,
}

impl Broadcaster {
    /// Send the signal through any `Watcher` instances.
    ///
    /// Function will NOT block until all peers have ack'ed the signal.
    #[tracing::instrument(skip(self))]
    pub fn signal(self) {
        self.signaled.store(true, Ordering::SeqCst);
        self.watcher.notify_waiters();
    }

    /// Send the signal through to any `Watcher` instances.
    ///
    /// Function WILL block until all peers have ack'ed the signal.
    #[tracing::instrument(skip(self))]
    pub async fn signal_and_wait(self) {
        self.signaled.store(true, Ordering::SeqCst);
        self.broadcaster.notify_waiters();

        // Wait for all peers to drop off.
        while self.peers.load(Ordering::SeqCst) > 0 {
            // Wake the `Watchers` so that they'll drop off.
            self.broadcaster.notified().await;
        }
    }
}

#[derive(Debug)]
enum TryRecvError {
    /// The signal has been received and yet `try_recv` was called.
    SignalReceived,
}

#[derive(Debug)]
enum RegisterError {
    /// The signal has been received and yet `register` was called.
    SignalReceived,
}

#[derive(Debug)]
/// Mechanism to watch for phase changes, typically used to control shutdown.
pub struct Watcher {
    /// Used to track if the signal has been received without synchronization.
    signal_received: bool,
    /// Whether the signal has been sent or not.
    signaled: Arc<AtomicBool>,
    /// The total number of peers subscribed to the `Broadcaster`.
    peers: Arc<AtomicU32>,
    /// Used by `Watcher` instances to wake the `Broadcaster` while it waits for
    /// `Watcher` instances to shut down.
    broadcaster: Arc<Notify>,
    /// Used by `Broadcaster` to wake `Watcher` instances when the signal is
    /// transmitted.
    watcher: Arc<Notify>,
}

impl Watcher {
    /// Decrease the peer count in the `Broadcaster`, allowing the `Broadcaster` to
    /// unblock if waiting for peers.
    #[tracing::instrument(skip(self))]
    fn decrease_peer_count(&self) {
        // Why not use fetch_sub? That function overflows at the zero boundary
        // and we don't want the peer count to suddenly be u32::MAX.
        let mut old = self.peers.load(Ordering::Relaxed);
        while old > 0 {
            match self.peers.compare_exchange_weak(
                old,
                old - 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.broadcaster.notify_waiters();
                    break;
                }
                Err(x) => old = x,
            }
        }
    }

    /// Receive the shutdown notice. This function will block if a notice has
    /// not already been sent.
    ///
    /// If `recv` is called multiple times after the signal has been received
    /// this function will return immediately.
    #[tracing::instrument(skip(self))]
    pub async fn recv(mut self) {
        if self.signal_received {
            return;
        } else if self.signaled.load(Ordering::SeqCst) {
            self.decrease_peer_count();
            return;
        }

        self.broadcaster.notified().await; // Wait for signal
        self.signal_received = true;
        self.decrease_peer_count();
    }

    /// Check if a shutdown notice has been sent without blocking.
    ///
    /// If the signal has not been received returns Ok(false). If it has been
    /// received Ok(true). All calls after will return `TryRecvError::SignalReceived`.
    #[tracing::instrument(skip(self))]
    pub fn try_recv(&mut self) -> Result<bool, TryRecvError> {
        // If the shutdown signal has already been received, return with error.
        if self.signal_received {
            return Err(TryRecvError::SignalReceived);
        }

        if self.signaled.load(Ordering::SeqCst) {
            self.signal_received = true;
            self.decrease_peer_count();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Register with the `Broadcaster`, returning a new instance of `Watcher`.
    #[tracing::instrument(skip(self))]
    pub async fn register(&self) -> Result<Self, RegisterError> {
        if self.signaled.load(Ordering::SeqCst) {
            // If the shutdown signal has already been received, return with
            // error.
            return Err(RegisterError::SignalReceived);
        }

        self.peers.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            signal_received: self.signal_received,
            peers: Arc::clone(&self.peers),
            broadcaster: Arc::clone(&self.broadcaster),
            watcher: Arc::clone(&self.watcher),
            signaled: Arc::clone(&self.signaled),
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn basic_signal() {
        use loom::future::block_on;
        use loom::thread;

        use crate::signal;

        loom::model(|| {
            let (watcher, broadcaster) = signal();

            // Spawn a thread to simulate the watcher.
            let watcher_handle = thread::spawn(move || {
                block_on(watcher.recv());
            });

            // Simulate the broadcaster signaling.
            broadcaster.signal();

            watcher_handle.join().unwrap();
        });
    }
}
