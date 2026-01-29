use super::{Handle, Pool};

/// A list of strings
///
/// It can be useful to configure lading to create strings from a fixed list
/// to control the shape of data emitted. The pool will hand out a random string
/// from the list.
#[derive(Debug)]
pub(crate) struct StringListPool {
    metric_names: Vec<String>,
}

impl StringListPool {
    /// Create a new list of strings.
    pub(crate) fn new(metric_names: Vec<String>) -> Self {
        assert!(
            !metric_names.is_empty(),
            "don't create a string list with an empty list"
        );
        Self { metric_names }
    }
}

impl Pool for StringListPool {
    fn of_size_with_handle<'a, R>(&'a self, rng: &mut R, _bytes: usize) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized,
    {
        let idx: usize = rng.random_range(0..self.metric_names.len());
        Some((&self.metric_names[idx], Handle::Index(idx)))
    }

    fn using_handle(&self, handle: Handle) -> Option<&str> {
        let idx = handle
            .as_index()
            .expect("handle for string list pool should be a string pool handle");
        self.metric_names.get(idx).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::{Pool, StringListPool};
    use rand::{SeedableRng, rngs::SmallRng};

    // Ensure that no returned string ever has a non-alphabet character.
    proptest! {
        #[test]
        fn string_is_in_list(seed: u64, mut names: Vec<String> ) {
            // Make sure the list is never empty.
            names.push("thing".to_string());

            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = StringListPool::new(names.clone());
            if let Some((s1, h)) = pool.of_size_with_handle(&mut rng, 0) {
                if let Some(s2) = pool.using_handle(h) {
                    assert_eq!(s1, s2);
                    assert!(names.iter().any(|s| s == s2));
                } else {
                    panic!("could not get string with handle");
                }
            } else {
                panic!("could not get handle from pool");
            }
        }
    }
}
