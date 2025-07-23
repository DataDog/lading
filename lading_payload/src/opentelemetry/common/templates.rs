//! Template utilities for OpenTelemetry payload generation

use prost::Message;
use rand::{Rng, prelude::*, seq::IteratorRandom};
use std::collections::BTreeMap;

/// Errors related to pool operations
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum PoolError<E> {
    /// Choice could not be made on empty container
    #[error("Choice could not be made on empty container.")]
    EmptyChoice,
    /// Generation error
    #[error("Generation error: {0}")]
    Generator(E),
}

/// A pool that stores pre-generated instances indexed by their encoded size
#[derive(Debug, Clone)]
pub(crate) struct Pool<T, G> {
    context_cap: u32,
    /// key: encoded size; val: templates with that size
    by_size: BTreeMap<usize, Vec<T>>,
    generator: G,
    len: u32,
}

impl<T, G> Pool<T, G>
where
    T: Message,
{
    /// Build an empty pool that can hold at most `context_cap` templates.
    pub(crate) fn new(context_cap: u32, generator: G) -> Self {
        Self {
            context_cap,
            by_size: BTreeMap::new(),
            generator,
            len: 0,
        }
    }

    /// Return a reference to an item from the pool.
    ///
    /// Instances returned by this function are guaranteed to be of an encoded
    /// size no greater than budget. No greater than `context_cap` instances
    /// will ever be stored in this structure.
    pub(crate) fn fetch<'a, R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<&'a T, PoolError<G::Error>>
    where
        R: Rng + ?Sized,
        G: crate::SizedGenerator<'a, Output = T>,
        G::Error: 'a,
    {
        // If we are at context cap, search by_size for templates <= budget and
        // return a random choice. If we are not at context cap, call
        // generator with the budget and then store the result
        // for future use in `by_size`.
        //
        // Size search is in the interval (0, budget].

        let upper = *budget;

        // Generate new instances until either context_cap is hit or the
        // remaining space drops below our lookup interval.
        if self.len < self.context_cap {
            let mut limit = *budget;
            match self.generator.generate(rng, &mut limit) {
                Ok(item) => {
                    let sz = item.encoded_len();
                    self.by_size.entry(sz).or_default().push(item);
                    self.len += 1;
                }
                Err(e) => return Err(PoolError::Generator(e)),
            }
        }

        let (choice_sz, choices) = self
            .by_size
            .range(..=upper)
            .choose(rng)
            .ok_or(PoolError::EmptyChoice)?;

        let choice = choices.choose(rng).ok_or(PoolError::EmptyChoice)?;
        *budget = budget.saturating_sub(*choice_sz);

        Ok(choice)
    }
}
