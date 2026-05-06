//! Flow management for neper-style workloads.
//!
//! A "flow" is a single TCP connection managed by a thread's mio event loop.
//! This module provides the generic [`Flow`] struct, token-indexed storage
//! ([`FlowMap`]), and the [`Action`] enum that event handlers return to
//! drive flow lifecycle.

use mio::net::TcpStream;
use mio::{Interest, Registry, Token};

/// A network flow (TCP connection) managed by a thread's event loop.
pub(crate) struct Flow<S> {
    pub(crate) stream: TcpStream,
    pub(crate) token: Token,
    pub(crate) state: S,
    /// Remaining bytes for the current I/O operation.
    pub(crate) xfer: usize,
}

/// What the event loop should do after processing a flow event.
#[derive(Clone, Copy)]
pub(crate) enum Action {
    /// No change to mio registration.
    Continue,
    /// Reregister the flow with a new interest set.
    Reregister(Interest),
    /// Deregister and remove the flow.
    Remove,
}

/// Token-indexed flow storage.
///
/// Flows are stored in a `Vec` indexed by token value. Removed slots become
/// `None` and are not reused — tokens are monotonically increasing, matching
/// neper's behavior.
pub(crate) struct FlowMap<S> {
    inner: Vec<Option<Flow<S>>>,
}

impl<S> FlowMap<S> {
    pub(crate) fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Insert a flow. Grows the backing vec if needed.
    pub(crate) fn insert(&mut self, flow: Flow<S>) {
        let idx = flow.token.0;
        if idx >= self.inner.len() {
            self.inner.resize_with(idx + 1, || None);
        }
        self.inner[idx] = Some(flow);
    }

    /// Get a mutable reference to the flow at the given token.
    pub(crate) fn get_mut(&mut self, token: Token) -> Option<&mut Flow<S>> {
        self.inner.get_mut(token.0).and_then(|slot| slot.as_mut())
    }

    /// Remove and return the flow at the given token.
    pub(crate) fn remove(&mut self, token: Token) -> Option<Flow<S>> {
        self.inner.get_mut(token.0).and_then(Option::take)
    }
}

/// Apply an [`Action`] to a flow via the poll registry.
pub(crate) fn apply_action<S>(
    action: Action,
    token: Token,
    flows: &mut FlowMap<S>,
    registry: &Registry,
) {
    match action {
        Action::Continue => {}
        Action::Reregister(interest) => {
            if let Some(flow) = flows.get_mut(token) {
                let _ = registry.reregister(&mut flow.stream, flow.token, interest);
            }
        }
        Action::Remove => {
            if let Some(mut flow) = flows.remove(token) {
                let _ = registry.deregister(&mut flow.stream);
            }
        }
    }
}
