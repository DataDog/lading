//! Flow management for neper-style workloads.
//!
//! A "flow" is a single TCP connection managed by a thread's mio event loop.
//! This module provides the generic [`Flow`] struct, token-indexed storage
//! ([`FlowMap`]), and the [`Action`] enum that event handlers return to
//! drive flow lifecycle.

use std::io;

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
/// A fixed-size ring of slots. A flow lives in slot `token % capacity`, so
/// tokens can keep increasing for the whole run while the storage stays the
/// size it was given at construction. That matters for `tcp_crr`, which opens
/// a new connection, and so takes a new token, for every transaction.
///
/// Two live tokens that are congruent modulo the capacity would share a slot.
/// The map never overwrites an occupied slot, and every lookup checks the
/// occupant's token, so a collision surfaces as an [`Error`] rather than as one
/// flow silently driving another.
pub(crate) struct FlowMap<S> {
    inner: Vec<Option<Flow<S>>>,
}

/// Errors produced by [`FlowMap`] and [`apply_action`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The slot for a new flow is already held by a live flow.
    #[error(
        "flow map slot {idx} for token {token} is held by token {occupant} (capacity {capacity})"
    )]
    SlotOccupied {
        /// Slot index the new flow maps to.
        idx: usize,
        /// Token of the flow being inserted.
        token: usize,
        /// Token of the flow already in the slot.
        occupant: usize,
        /// Number of slots in the map.
        capacity: usize,
    },
    /// A lookup found a different flow in the slot the token maps to.
    #[error("flow map slot {idx} holds token {found}, expected token {expected}")]
    TokenMismatch {
        /// Slot index the token maps to.
        idx: usize,
        /// Token that was looked up.
        expected: usize,
        /// Token of the flow actually in the slot.
        found: usize,
    },
    /// The poll registry rejected an operation on a live flow.
    #[error("failed to {operation} flow token {token}: {source}")]
    Registry {
        /// The registry operation that failed.
        operation: &'static str,
        /// Token of the flow.
        token: usize,
        /// Underlying IO error.
        #[source]
        source: io::Error,
    },
}

impl<S> FlowMap<S> {
    /// Create a map with room for `flows` concurrent flows.
    ///
    /// Twice `flows` slots are reserved so a reconnecting flow's new token is
    /// unlikely to land on a slot still held by another flow. At least one slot
    /// is always reserved, so slot arithmetic never divides by zero.
    pub(crate) fn new(flows: usize) -> Self {
        Self {
            inner: Vec::with_capacity(flows.saturating_mul(2).max(1)),
        }
    }

    /// Slot index for `token`.
    fn slot(&self, token: Token) -> usize {
        token.0 % self.inner.capacity()
    }

    /// Insert a flow and return a reference to it in its slot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SlotOccupied`] if another flow holds the slot. The flow
    /// passed in is dropped, closing its socket, so the caller has nothing to
    /// clean up.
    pub(crate) fn insert(&mut self, flow: Flow<S>) -> Result<&mut Flow<S>, Error> {
        let idx = self.slot(flow.token);
        let capacity = self.inner.capacity();
        if self.inner.len() <= idx {
            self.inner.resize_with(idx + 1, || None);
        }
        let token = flow.token.0;
        match self.inner.get_mut(idx) {
            Some(slot) if slot.is_none() => Ok(slot.insert(flow)),
            Some(slot) => Err(Error::SlotOccupied {
                idx,
                token,
                occupant: slot.as_ref().map_or(token, |occupant| occupant.token.0),
                capacity,
            }),
            // `resize_with` above guarantees `idx < len`, so this arm is not
            // reached. It is still an error rather than a panic.
            None => Err(Error::SlotOccupied {
                idx,
                token,
                occupant: token,
                capacity,
            }),
        }
    }

    /// Get a mutable reference to the flow at the given token.
    ///
    /// Returns `Ok(None)` for an empty slot, which is normal: a readiness event
    /// can arrive for a flow that was removed earlier in the same poll batch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TokenMismatch`] if the slot holds a flow with a
    /// different token.
    pub(crate) fn get_mut(&mut self, token: Token) -> Result<Option<&mut Flow<S>>, Error> {
        let idx = self.slot(token);
        match self.inner.get_mut(idx).and_then(Option::as_mut) {
            None => Ok(None),
            Some(flow) if flow.token == token => Ok(Some(flow)),
            Some(flow) => Err(Error::TokenMismatch {
                idx,
                expected: token.0,
                found: flow.token.0,
            }),
        }
    }

    /// Remove and return the flow at the given token.
    ///
    /// Returns `Ok(None)` for an empty slot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TokenMismatch`] if the slot holds a flow with a
    /// different token. That flow is left in place: removing it would close a
    /// connection that belongs to someone else.
    pub(crate) fn remove(&mut self, token: Token) -> Result<Option<Flow<S>>, Error> {
        let idx = self.slot(token);
        let Some(slot) = self.inner.get_mut(idx) else {
            return Ok(None);
        };
        let found = match slot {
            None => return Ok(None),
            Some(flow) => flow.token,
        };
        if found != token {
            return Err(Error::TokenMismatch {
                idx,
                expected: token.0,
                found: found.0,
            });
        }
        Ok(slot.take())
    }
}

/// Apply an [`Action`] to a flow via the poll registry.
///
/// # Errors
///
/// Returns [`Error::Registry`] if the registry rejects the reregister or
/// deregister, or [`Error::TokenMismatch`] if the token's slot holds another
/// flow. A live, owned flow should always accept both operations, so either
/// failure means the thread's event loop can no longer be driven correctly and
/// the caller is expected to treat it as fatal for that thread.
pub(crate) fn apply_action<S>(
    action: Action,
    token: Token,
    flows: &mut FlowMap<S>,
    registry: &Registry,
) -> Result<(), Error> {
    match action {
        Action::Continue => {}
        Action::Reregister(interest) => {
            if let Some(flow) = flows.get_mut(token)? {
                registry
                    .reregister(&mut flow.stream, flow.token, interest)
                    .map_err(|source| Error::Registry {
                        operation: "reregister",
                        token: token.0,
                        source,
                    })?;
            }
        }
        Action::Remove => {
            if let Some(mut flow) = flows.remove(token)? {
                registry
                    .deregister(&mut flow.stream)
                    .map_err(|source| Error::Registry {
                        operation: "deregister",
                        token: token.0,
                        source,
                    })?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Error, Flow, FlowMap};
    use mio::Token;

    /// A connected, nonblocking mio stream. `FlowMap` stores real sockets, so
    /// the tests need one per flow.
    fn stream() -> mio::net::TcpStream {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        stream.set_nonblocking(true).unwrap();
        mio::net::TcpStream::from_std(stream)
    }

    fn flow(token: usize) -> Flow<()> {
        Flow {
            stream: stream(),
            token: Token(token),
            state: (),
            xfer: 0,
        }
    }

    /// For all tokens t: after `insert(t)`, `get_mut(t)` and `remove(t)` return the
    /// flow with token t, and the slot is empty afterwards.
    #[test]
    fn inserted_flow_is_found_by_its_token() {
        let mut map = FlowMap::new(2);
        map.insert(flow(3)).unwrap();
        assert_eq!(map.get_mut(Token(3)).unwrap().unwrap().token, Token(3));
        assert_eq!(map.remove(Token(3)).unwrap().unwrap().token, Token(3));
        assert!(map.get_mut(Token(3)).unwrap().is_none());
    }

    /// An empty slot is not an error: events can arrive for a flow removed
    /// earlier in the same poll batch.
    #[test]
    fn empty_slot_reads_as_none() {
        let mut map: FlowMap<()> = FlowMap::new(2);
        assert!(map.get_mut(Token(0)).unwrap().is_none());
        assert!(map.remove(Token(0)).unwrap().is_none());
    }

    /// For all t, k with k > 0: while t is live, looking up t + k * capacity
    /// returns `TokenMismatch`, and `remove` leaves t in place.
    #[test]
    fn colliding_token_is_rejected_and_occupant_kept() {
        let mut map = FlowMap::new(2);
        let capacity = map.inner.capacity();
        map.insert(flow(1)).unwrap();
        let stale = Token(1 + capacity);

        assert!(matches!(
            map.get_mut(stale),
            Err(Error::TokenMismatch { expected, found: 1, .. }) if expected == stale.0
        ));
        assert!(matches!(
            map.remove(stale),
            Err(Error::TokenMismatch { found: 1, .. })
        ));
        assert_eq!(map.get_mut(Token(1)).unwrap().unwrap().token, Token(1));
    }

    /// Inserting into an occupied slot fails and does not replace the occupant.
    #[test]
    fn insert_into_occupied_slot_is_rejected() {
        let mut map = FlowMap::new(2);
        let capacity = map.inner.capacity();
        map.insert(flow(0)).unwrap();

        assert!(matches!(
            map.insert(flow(capacity)),
            Err(Error::SlotOccupied { token, occupant: 0, .. }) if token == capacity
        ));
        assert_eq!(map.get_mut(Token(0)).unwrap().unwrap().token, Token(0));
    }

    /// Once a flow is removed its slot is reused by the next token that maps
    /// to it, which is what keeps `tcp_crr` from growing the map per
    /// transaction.
    #[test]
    fn removed_slot_is_reused() {
        let mut map = FlowMap::new(2);
        let capacity = map.inner.capacity();
        map.insert(flow(0)).unwrap();
        map.remove(Token(0)).unwrap();
        map.insert(flow(capacity)).unwrap();
        assert_eq!(
            map.get_mut(Token(capacity)).unwrap().unwrap().token,
            Token(capacity)
        );
        assert_eq!(map.inner.len(), 1);
    }

    /// A map built for zero flows still has a slot, so slot arithmetic cannot
    /// divide by zero.
    #[test]
    fn zero_flow_map_does_not_divide_by_zero() {
        let mut map: FlowMap<()> = FlowMap::new(0);
        assert!(map.get_mut(Token(7)).unwrap().is_none());
    }
}
