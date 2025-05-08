//! A rejection set for u32 ranges.
use std::collections::{BTreeMap, BTreeSet};

/// A `RejectSet` that excludes the full [A..=B] span when |A − B| <= eps.
/// Outside that, failures sit as singletons until more neighbors arrive.
#[derive(Debug)]
pub(crate) struct RejectSet {
    /// "Density" threshold for clustering failures into one interval.
    epsilon: u32,

    /// All individual failures not yet absorbed into an interval.
    singles: BTreeSet<u32>,

    /// Disjoint, merged intervals of rejected values. Key is the start of the
    /// interval, value the end.
    intervals: BTreeMap<u32, u32>,
}

impl RejectSet {
    /// Create a new `RejectSet`
    ///
    /// `epsilon` defines the maximum gap that this structure will consider
    /// intervals to not be "clustered".
    #[must_use]
    pub(crate) fn new(epsilon: u32) -> Self {
        Self {
            epsilon,
            singles: BTreeSet::new(),
            intervals: BTreeMap::new(),
        }
    }

    /// Returns true if `x` is present in the set.
    #[must_use]
    pub(crate) fn is_rejected(&self, x: u32) -> bool {
        if self.singles.contains(&x) {
            return true;
        }
        if let Some((&start, &end)) = self.intervals.range(..=x).next_back() {
            return x >= start && x <= end;
        }
        false
    }

    /// Record a failure at `x`.
    pub(crate) fn reject(&mut self, x: u32) {
        // Okay. We first insert into `singles` so we're sure to record `x`
        // directly. We then use `epsilon` to determine the search interval for
        // 'collison' with existing intervals, search whether both ends are
        // within `epsilon` of an existing epsilon to merge.
        //
        // We define our interval as [x-eps,x+eps]. If this proves to be
        // unintuitive we could define `x` as the midpoint of an interval
        // defined like [x-eps/2, x+eps/2] but I'd like to skip the divison if
        // we can avoid it.
        self.singles.insert(x);

        let low_bound = x.saturating_sub(self.epsilon);
        let high_bound = x.saturating_add(self.epsilon);

        // Calcualte the low, high next singletons.
        let low_single = self.singles.range(low_bound..=x).next_back().copied();
        let high_single = self.singles.range(x..=high_bound).next().copied();

        // If the two ends are close enough, form an interval from them.
        if let (Some(ls), Some(hs)) = (low_single, high_single) {
            let diff = hs.saturating_sub(ls);
            if hs >= ls && diff <= self.epsilon {
                // TODO can I avoid this Vec?
                let to_absorb: Vec<u32> = self.singles.range(ls..=hs).copied().collect();
                for v in &to_absorb {
                    self.singles.remove(v);
                }

                // merge [ls..=hs] into intervals (coalescing neighbors)
                self.insert_interval(ls, hs);
            }
        }
    }

    /// Insert an interval into the set, merging as needed.
    fn insert_interval(&mut self, mut start: u32, mut end: u32) {
        // merge with any low interval that touches
        if let Some((&lstart, &lend)) = self.intervals.range(..=start).next_back() {
            if start.saturating_sub(lend) <= self.epsilon {
                start = start.min(lstart);
                end = end.max(lend);
                self.intervals.remove(&lstart);
            }
        }
        // merge any high intervals that touch
        while let Some((&hstart, &hend)) = self.intervals.range(start..).next() {
            if hstart.saturating_sub(end) <= self.epsilon {
                start = start.min(hstart);
                end = end.max(hend);
                self.intervals.remove(&hstart);
            } else {
                break;
            }
        }
        // insert the new coalesced interval
        self.intervals.insert(start, end);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Property 1: For all x, after reject(x), is_rejected(x) == true.
    proptest! {
        #[test]
        fn prop_reject_self(eps in 0u32..10, x in 0u32..1000) {
            let mut rs = RejectSet::new(eps);
            rs.reject(x);
            prop_assert!(rs.is_rejected(x));
        }
    }

    // Property 2: For all x, y, if |x − y| ≤ epsilon, after reject(x) and
    // reject(y), all z in [min(x, y), max(x, y)] are rejected.
    proptest! {
        #[test]
        fn prop_interval_formation(eps in 1u32..10, x in 0u32..1000, offset in 0u32..10) {
            let y = x.saturating_add(offset.min(eps));
            let mut rs = RejectSet::new(eps);
            rs.reject(x);
            rs.reject(y);
            let (lo, hi) = (x.min(y), x.max(y));
            for z in lo..=hi {
                prop_assert!(rs.is_rejected(z));
            }
        }
    }

    // Property 3: For all x, y, if |x − y| > epsilon, after reject(x) and
    // reject(y), is_rejected(z) == true iff z == x or z == y.
    proptest! {
        #[test]
        fn prop_no_overabsorption(eps in 0u32..10, x in 0u32..1000, offset in 11u32..100) {
            let y = x.saturating_add(offset + eps);
            let mut rs = RejectSet::new(eps);
            rs.reject(x);
            rs.reject(y);
            for z in [x, y] {
                prop_assert!(rs.is_rejected(z));
            }
            let mid = x + ((y - x) / 2);
            if mid != x && mid != y {
                prop_assert!(!rs.is_rejected(mid));
            }
        }
    }

    // Property 4: For all x, after reject(x) twice, the set is unchanged after
    // the first.
    proptest! {
        #[test]
        fn prop_idempotency(eps in 0u32..10, x in 0u32..1000) {
            let mut rs = RejectSet::new(eps);
            rs.reject(x);
            let before = rs.is_rejected(x);
            rs.reject(x);
            let after = rs.is_rejected(x);
            prop_assert_eq!(before, after);
        }
    }

    // Property 5: For all x, y, z, if x and y form an interval, and z is within
    // epsilon of that interval, then after reject(z), the interval expands to
    // include z.
    proptest! {
        #[test]
        fn prop_interval_merging(eps in 1u32..10, x in 0u32..1000, offset in 1u32..10) {
            let y = x.saturating_add(eps);
            let z = y.saturating_add(offset.min(eps));
            let mut rs = RejectSet::new(eps);
            rs.reject(x);
            rs.reject(y);
            rs.reject(z);
            let (lo, hi) = (x.min(z), x.max(z));
            for v in lo..=hi {
                prop_assert!(rs.is_rejected(v));
            }
        }
    }

    // Property 6: Order independence. Inserting the same values in any order
    // yields the same result. This is implied by Property 6a and 6b.

    // Property 6a: Determinism. For any sequence of insertions, the result is
    // always the same for the same sequence.
    proptest! {
        #[test]
        fn prop_determinism(eps in 0u32..10, vals in proptest::collection::vec(0u32..1000, 1..10)) {
            let mut rs1 = RejectSet::new(eps);
            for &v in &vals {
                rs1.reject(v);
            }
            let mut rs2 = RejectSet::new(eps);
            for &v in &vals {
                rs2.reject(v);
            }
            for probe in 0u32..1024 {
                prop_assert_eq!(rs1.is_rejected(probe), rs2.is_rejected(probe));
            }
        }
    }

    // Property 6b: Commutativity. Inserting two elements in any order yields
    // the same result.
    proptest! {
        #[test]
        fn prop_commutativity(eps in 0u32..10, x in 0u32..1000, y in 0u32..1000) {
            let mut rs1 = RejectSet::new(eps);
            rs1.reject(x);
            rs1.reject(y);
            let mut rs2 = RejectSet::new(eps);
            rs2.reject(y);
            rs2.reject(x);
            for probe in 0u32..1024 {
                prop_assert_eq!(rs1.is_rejected(probe), rs2.is_rejected(probe));
            }
        }
    }
}
