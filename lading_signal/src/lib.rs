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
    Arc,
    atomic::{AtomicU32, Ordering},
};

#[cfg(loom)]
use loom::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use tokio::sync::{
    Notify,
    broadcast::{self, error},
};
use tracing::info;

#[cfg(loom)]
std::thread_local! {
    /// Loom instrumentation: When enabled, `signal_and_wait` will yield in the race
    /// window between `peers.load()` and `notified.await`. This allows loom to
    /// explore the interleaving where a watcher's `notify_waiters()` fires in this
    /// window, exposing a lost wakeup race condition that was fixed in PR#1740.
    ///
    /// This is only used in tests to prove the race exists. In production builds
    /// (`#[cfg(not(loom))]`), this is compiled out entirely.
    pub static LOOM_EXPLORE_RACE_WINDOW: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Construct a `Watcher` and `Broadcaster` pair.
#[must_use]
pub fn signal() -> (Watcher, Broadcaster) {
    // NOTE why use a broadcast channel and not a barrier to synchronize the
    // watchers/broadcaster? Or an AtomicBool and a Condvar? Well, it's a good
    // question. I think we could potentially do away with the broadcast channel
    // entirely -- we don't use it except for its reliable shutdown signal
    // semantics -- but we run up again loom 0.7's surface area. In the future
    // we can reconsider the implementation here but not at the expense of proof
    // tools.
    let (sender, receiver) = broadcast::channel(1);
    let peers = Arc::new(AtomicU32::new(1));
    let notify = Arc::new(Notify::new());

    let w = Watcher {
        peers: Arc::clone(&peers),
        receiver,
        signal_received: false,
        notify: Arc::clone(&notify),
        peer_count_decreased: false,
        registered: true,
    };

    let b = Broadcaster {
        peers,
        sender,
        notify,
    };

    (w, b)
}

#[derive(Debug)]
/// Mechanism to notify one or more `Watcher` instances that a phase has been
/// achieved.
pub struct Broadcaster {
    /// The total number of peers subscribed to this `Broadcaster`. Used by this
    /// struct to understand when all `Watcher` instances have dropped off.
    peers: Arc<AtomicU32>,
    /// Transmission point for the signal to `Watcher` instances.
    sender: broadcast::Sender<()>,
    /// Allow the `Watchers` to notify `Broadcaster` that they have logged off.
    notify: Arc<Notify>,
}

impl Broadcaster {
    /// Send the signal through any `Watcher` instances.
    ///
    /// Function will NOT block until all peers have ack'ed the signal.
    pub fn signal(self) {
        drop(self.sender);
    }

    /// Send the signal through to any `Watcher` instances.
    ///
    /// Function WILL block until all peers have ack'ed the signal.
    pub async fn signal_and_wait(self) {
        drop(self.sender);

        // Wait for all peers to drop off. The opposite to
        // `decrease_peer_count`: loop will not consume CPU until a `Watcher`
        // has signaled that it has received the transmitted signal.
        //
        // To avoid a race condition, we must: (1) register for notification,
        // (2) check the condition, (3) await. If we checked first and then
        // registered, a peer could decrement and notify between our check and
        // registration, causing us to miss the wakeup and hang forever.
        loop {
            let notified = self.notify.notified();

            let peers = self.peers.load(Ordering::SeqCst);

            #[cfg(loom)]
            if LOOM_EXPLORE_RACE_WINDOW.get() {
                loom::thread::yield_now(); // Force exploration here
            }

            if peers == 0 {
                break;
            }
            info!("Waiting for {peers} peers");

            notified.await;
        }
    }
}

/// Errors for `Watcher::try_recv`.
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum TryRecvError {
    /// The signal has been received and yet `try_recv` was called.
    #[error("signal has been received")]
    SignalReceived,
}

/// Errors for `Watcher::register`.
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum RegisterError {
    /// The signal has been received and yet `register` was called.
    #[error("signal has been received")]
    SignalReceived,
}

#[derive(Debug)]
/// Mechanism to watch for phase changes, typically used to control shutdown.
pub struct Watcher {
    /// Used to track if the signal has been received without synchronization.
    signal_received: bool,
    /// Record whether the peer count of this Watcher has been decreased.
    peer_count_decreased: bool,
    /// The total number of peers subscribed to the `Broadcaster`. Used by this
    /// struct not to observe other `Watcher` instances but to inform
    /// `Broadcaster` of the existence/lack-of of this instance.
    peers: Arc<AtomicU32>,
    /// Transmission point for the signal from `Broadcaster`.
    receiver: broadcast::Receiver<()>,
    /// Allow the `Watchers` to notify `Broadcaster` that they have logged off.
    notify: Arc<Notify>,
    /// Whether the `Broadcaster` is aware of this instance's existence and will
    /// wait via `signal_and_wait` for it to terminate.
    registered: bool,
}

impl Watcher {
    /// Decrease the peer count in the `Broadcaster`, allowing the `Broadcaster` to
    /// unblock if waiting for peers. See `Broadcaster::signal_and_wait`.
    fn decrease_peer_count(&mut self) {
        if !self.registered {
            // If this instance is not registered the `Broadcaster` will not
            // wait for its dropping. As a result this function has no work.
            return;
        }

        if self.peer_count_decreased {
            // If this instance is registered but the `Broadcaster` has already
            // been informed the peer intends to drop off, there is no work to
            // be done.
            //
            // Only set if this function is previously called.
            return;
        }

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
                    self.notify.notify_waiters();
                    break;
                }
                Err(x) => old = x,
            }
        }
        self.peer_count_decreased = true;
    }

    /// Receive the shutdown notice. This function will block if a notice has
    /// not already been sent.
    ///
    /// If `recv` is called multiple times after the signal has been received
    /// this function will return immediately.
    ///
    /// # Panics
    ///
    /// Panics if the broadcast receiver has lagged behind, indicating a catastrophic
    /// programming error in the signal coordination system.
    pub async fn recv(mut self) {
        if self.signal_received {
            // Once the signal is received if this function were called in a
            // `select!` it might drown out every other arm. May indicate the
            // semantics of this should be like `try_recv`.
            #[cfg(not(loom))]
            {
                tokio::task::yield_now().await;
            }
            return;
        }

        match self.receiver.recv().await {
            Ok(()) | Err(error::RecvError::Closed) => {
                self.decrease_peer_count();
                self.signal_received = true;
            }
            Err(error::RecvError::Lagged(_)) => {
                panic!("Catastrophic programming error: lagged behind");
            }
        }
    }

    /// Check if a shutdown notice has been sent without blocking.
    ///
    /// If the signal has not been received returns Ok(false). If it has been
    /// received Ok(true). All calls after will return `TryRecvError::SignalReceived`.
    ///
    /// # Errors
    ///
    /// Returns `TryRecvError::SignalReceived` if the signal has already been received
    /// and processed by this watcher.
    ///
    /// # Panics
    ///
    /// Panics if the broadcast receiver has lagged behind, indicating a catastrophic
    /// programming error in the signal coordination system.
    pub fn try_recv(&mut self) -> Result<bool, TryRecvError> {
        // If the shutdown signal has already been received, return with error.
        if self.signal_received {
            return Err(TryRecvError::SignalReceived);
        }

        match self.receiver.try_recv() {
            Ok(()) | Err(error::TryRecvError::Closed) => {
                self.decrease_peer_count();
                self.signal_received = true;
                Ok(true)
            }
            Err(error::TryRecvError::Empty) => Ok(false),
            Err(error::TryRecvError::Lagged(_)) => {
                panic!("Catastrophic programming error: lagged behind")
            }
        }
    }

    /// Register with the `Broadcaster`, returning a new instance of `Watcher`.
    ///
    /// # Errors
    ///
    /// Returns `RegisterError::SignalReceived` if the signal has already been received
    /// by this watcher, preventing registration of new watchers after shutdown.
    pub fn register(&self) -> Result<Self, RegisterError> {
        if self.signal_received {
            // If the shutdown signal has already been received, return with
            // error.
            return Err(RegisterError::SignalReceived);
        }

        self.peers.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            peers: Arc::clone(&self.peers),
            receiver: self.receiver.resubscribe(),
            signal_received: self.signal_received,
            notify: Arc::clone(&self.notify),
            // Do not copy existing peer count decreased state as this new peer
            // is independent.
            peer_count_decreased: false,
            registered: true,
        })
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.decrease_peer_count();
    }
}

impl Clone for Watcher {
    fn clone(&self) -> Self {
        Self {
            peers: Arc::clone(&self.peers),
            receiver: self.receiver.resubscribe(),
            signal_received: self.signal_received,
            notify: Arc::clone(&self.notify),
            // Do not copy existing peer count decreased state as this new peer
            // is independent.
            peer_count_decreased: false,
            registered: false,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(loom)]
    #[test]
    fn basic_signal() {
        use loom::{future::block_on, thread};

        use crate::signal;

        loom::model(|| {
            let (watcher, broadcaster) = signal();

            // In a thread we have a singular watcher that blocks on recv.
            let watcher_handle = thread::spawn(move || {
                block_on(watcher.recv());
            });

            // In our main thread we signal and then wait.
            block_on(broadcaster.signal_and_wait());

            // Now, assert that the watcher has received the signal and its
            // thread has shut down.
            watcher_handle.join().unwrap();
        });
    }

    #[cfg(loom)]
    #[test]
    fn basic_signal_one_unregistered() {
        use loom::{future::block_on, thread};

        use crate::signal;

        loom::model(|| {
            let (watcher, broadcaster) = signal();
            let _unregistered_watcher = watcher.clone();

            // In a thread we have a singular watcher that blocks on recv.
            let watcher_handle = thread::spawn(move || {
                block_on(watcher.recv());
            });

            // Note that _unregistered_watcher is not dropped and has not called
            // recv, but because it is not registered signal_and_wait must not
            // deadlock.

            // In our main thread we signal and then wait.
            block_on(broadcaster.signal_and_wait());

            // Now, assert that the watcher has received the signal and its
            // thread has shut down.
            watcher_handle.join().unwrap();
        });
    }

    #[cfg(loom)]
    #[test]
    fn basic_signal_only_unregistered() {
        use loom::future::block_on;

        use crate::signal;

        loom::model(|| {
            let (watcher, broadcaster) = signal();
            let _unregistered_watcher = watcher.clone();
            drop(watcher);

            // Note that _unregistered_watcher is not dropped and has not called
            // recv, but because it is not registered signal_and_wait must not
            // deadlock.

            // In our main thread we signal and then wait.
            block_on(broadcaster.signal_and_wait());
        });
    }

    #[cfg(loom)]
    #[test]
    fn multiple_watchers() {
        use loom::{future::block_on, thread};

        use crate::signal;

        loom::model(|| {
            let (watcher1, broadcaster) = signal();
            let watcher2 = watcher1.register().unwrap();

            // Create two watchers in two threads, blocking on recv.
            let watcher_handle1 = thread::spawn(move || {
                block_on(watcher1.recv());
            });
            let watcher_handle2 = thread::spawn(move || {
                block_on(watcher2.recv());
            });

            // In our main thread we signal and then wait.
            block_on(broadcaster.signal_and_wait());

            // Now, assert that the watchers have received the signal and shut
            // down.
            watcher_handle1.join().unwrap();
            watcher_handle2.join().unwrap();
        });
    }

    #[cfg(loom)]
    #[test]
    fn multiple_watchers_one_unregistered() {
        use loom::{future::block_on, thread};

        use crate::signal;

        loom::model(|| {
            let (watcher1, broadcaster) = signal();
            let watcher2 = watcher1.register().unwrap();
            let _unregistered_watcher = watcher2.clone();

            // Create two watchers in two threads, blocking on recv.
            let watcher_handle1 = thread::spawn(move || {
                block_on(watcher1.recv());
            });
            let watcher_handle2 = thread::spawn(move || {
                block_on(watcher2.recv());
            });

            // Note that _unregistered_watcher is not dropped and has not called
            // recv, but because it is not registered signal_and_wait must not
            // deadlock.

            // In our main thread we signal and then wait.
            block_on(broadcaster.signal_and_wait());

            // Now, assert that the watchers have received the signal and shut
            // down.
            watcher_handle1.join().unwrap();
            watcher_handle2.join().unwrap();
        });
    }

    #[cfg(loom)]
    #[test]
    fn try_receive_before_signal() {
        use crate::signal;

        loom::model(|| {
            let (mut watcher, broadcaster) = signal();

            // Call try_recv before signaling
            assert_eq!(watcher.try_recv().unwrap(), false);

            // Signal without blocking and then we should see that the watcher
            // gets the signal.
            broadcaster.signal();

            assert_eq!(watcher.try_recv().unwrap(), true);
        });
    }

    #[cfg(loom)]
    #[test]
    fn try_receive_after_signal() {
        use crate::signal;

        loom::model(|| {
            let (mut watcher, broadcaster) = signal();

            // Signal before calling try_recv
            broadcaster.signal();

            // Call try_recv after signaling
            assert_eq!(watcher.try_recv().unwrap(), true);

            // From this point every call to try_recv errors.
            assert!(matches!(
                watcher.try_recv(),
                Err(crate::TryRecvError::SignalReceived)
            ));
        });
    }

    #[cfg(loom)]
    #[test]
    fn register_after_signal_before_recv() {
        use crate::signal;

        loom::model(|| {
            let (mut watcher1, broadcaster) = signal();

            // Signal before attempting to register a new watcher. Registration
            // after this point fails.
            broadcaster.signal();

            // The signal is sent but it has not been received by watcher yet as we've
            // not called `recv` or similar. Registration should succeed.
            let mut watcher2 = watcher1.register().unwrap();

            // The signal should be received by both watchers and then never again.
            assert_eq!(watcher1.try_recv().unwrap(), true);
            assert_eq!(watcher2.try_recv().unwrap(), true);

            assert_eq!(
                matches!(
                    watcher1.try_recv(),
                    Err(crate::TryRecvError::SignalReceived)
                ),
                true
            );
            assert_eq!(
                matches!(
                    watcher2.try_recv(),
                    Err(crate::TryRecvError::SignalReceived)
                ),
                true
            );
        });
    }

    #[cfg(loom)]
    #[test]
    fn register_after_signal_after_recv() {
        use crate::signal;

        loom::model(|| {
            let (mut watcher1, broadcaster) = signal();

            // Signal before attempting to register a new watcher. Registration
            // after this point fails.
            broadcaster.signal();

            // Recv the signal in watcher1, assert failure on a second recv.
            // Then register watcher2.
            assert_eq!(watcher1.try_recv().unwrap(), true);
            assert_eq!(
                matches!(
                    watcher1.try_recv(),
                    Err(crate::TryRecvError::SignalReceived)
                ),
                true
            );

            // The signal is sent and received by watcher1. Registration should
            // fail.
            let result = watcher1.register();
            assert!(matches!(result, Err(crate::RegisterError::SignalReceived)));
        });
    }

    #[cfg(loom)]
    #[test]
    fn signal_without_watchers() {
        use crate::signal;

        loom::model(|| {
            let (watcher, broadcaster) = signal();

            drop(watcher);
            // Signal without any watcher waiting.
            broadcaster.signal();
        });
    }

    #[cfg(loom)]
    #[test]
    fn watcher_drops_before_signal_and_wait() {
        use crate::signal;
        use loom::{future::block_on, thread};

        loom::model(|| {
            let (watcher, broadcaster) = signal();

            // In a thread drop the watcher. While we can't directly observe the
            // number of peers in a broadcaster we can assert that the broadcaster
            // does not hang if its watchers exit.
            let watcher_handle = thread::spawn(move || {
                drop(watcher);
            });

            block_on(broadcaster.signal_and_wait());
            watcher_handle.join().unwrap();
        });
    }

    #[cfg(loom)]
    #[test]
    // This tests the same flow as `watcher_drops_before_signal_and_wait` but with an explicit
    // yield to clue loom into exploring interleavings that exposed a race condition fixed in
    // PR#1740
    fn signal_and_wait_race_proof_with_yield() {
        use crate::LOOM_EXPLORE_RACE_WINDOW;
        use crate::signal;
        use loom::{future::block_on, thread};

        loom::model(|| {
            LOOM_EXPLORE_RACE_WINDOW.set(true); // Enable instrumentation

            let (watcher, broadcaster) = signal();

            let handle = thread::spawn(move || drop(watcher));

            block_on(broadcaster.signal_and_wait()); // Calls REAL function

            handle.join().unwrap();
        });
    }
}
