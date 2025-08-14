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
            if let Ok(item) = self.generator.generate(rng, &mut limit) {
                let sz = item.encoded_len();
                self.by_size.entry(sz).or_default().push(item);
                self.len += 1;
            } else {
                // Generation failed. It's possible there's an existing
                // template that fits the budget.
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

    /// Check if any template in the pool can fit within the given budget.
    /// Returns true if at least one template fits, false otherwise.
    pub(crate) fn template_fits(&self, budget: usize) -> bool {
        // Check if any existing template in the pool fits
        self.by_size.range(..=budget).next().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SizedGenerator;

    /// A test generator that creates items of specific sizes
    struct TestGenerator {
        sizes_to_generate: Vec<usize>,
        next_index: usize,
    }

    impl TestGenerator {
        fn new(sizes: Vec<usize>) -> Self {
            Self {
                sizes_to_generate: sizes,
                next_index: 0,
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct TestItem {
        size: usize,
        id: usize,
    }

    impl prost::Message for TestItem {
        fn encode_raw(&self, _buf: &mut impl bytes::BufMut) {
            // Not needed for test
        }

        fn merge_field(
            &mut self,
            _tag: u32,
            _wire_type: prost::encoding::WireType,
            _buf: &mut impl bytes::Buf,
            _ctx: prost::encoding::DecodeContext,
        ) -> Result<(), prost::DecodeError> {
            Ok(())
        }

        fn encoded_len(&self) -> usize {
            self.size
        }

        fn clear(&mut self) {
            // Not needed for test
        }
    }

    impl<'a> SizedGenerator<'a> for TestGenerator {
        type Output = TestItem;
        type Error = String;

        fn generate<R>(
            &'a mut self,
            _rng: &mut R,
            budget: &mut usize,
        ) -> Result<Self::Output, Self::Error>
        where
            R: rand::Rng + ?Sized,
        {
            if self.next_index >= self.sizes_to_generate.len() {
                return Err("No more items to generate".to_string());
            }

            let size = self.sizes_to_generate[self.next_index];
            if size > *budget {
                return Err(format!("Item size {} exceeds budget {}", size, *budget));
            }

            let item = TestItem {
                size,
                id: self.next_index,
            };
            self.next_index += 1;
            *budget -= size;

            Ok(item)
        }
    }

    #[test]
    fn pool_should_return_existing_template_when_generation_fails() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);

        // Generator that will create:
        // 1. First call: 100-byte item (succeeds)
        // 2. Second call: 500-byte item (fails with 200 budget)
        let generator = TestGenerator::new(vec![100, 500]);
        let mut pool = Pool::new(10, generator);

        // First fetch with 300 budget - should generate and store 100-byte item
        let mut budget = 300;
        let result = pool.fetch(&mut rng, &mut budget);
        assert!(result.is_ok());
        let item = result.unwrap();
        assert_eq!(item.size, 100);
        assert_eq!(budget, 200); // 300 - 100

        // Pool now has one 100-byte item (verify it can fit)
        assert!(pool.template_fits(100));

        // Second fetch with 200 budget:
        // - Pool is not at capacity (1 < 10)
        // - Tries to generate new 500-byte item (fails - exceeds budget)
        // - SHOULD check existing templates and return the 100-byte item
        // - BUG: Currently returns error without checking existing templates
        let mut budget = 200;
        let result = pool.fetch(&mut rng, &mut budget);

        // This SHOULD succeed by returning the existing 100-byte template
        assert!(
            result.is_ok(),
            "fetch() should return existing 100-byte template when generation fails"
        );
        let item = result.unwrap();
        assert_eq!(item.size, 100);
        assert_eq!(budget, 100); // 200 - 100
    }

    #[test]
    fn pool_with_no_templates_should_fail_when_generation_fails() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);

        // Generator that can only create 500-byte items
        let generator = TestGenerator::new(vec![500]);
        let mut pool = Pool::new(10, generator);

        // Pool starts empty (verify nothing fits yet)
        assert!(!pool.template_fits(100));

        // Try to fetch with 100 budget - generation will fail (500 > 100)
        let mut budget = 100;
        let result = pool.fetch(&mut rng, &mut budget);

        // This should fail because:
        // 1. Pool is empty
        // 2. Can't generate new template (500 > 100)
        // 3. No existing templates to fall back to
        assert!(
            result.is_err(),
            "fetch() should fail when pool is empty and generation fails"
        );
    }
}
