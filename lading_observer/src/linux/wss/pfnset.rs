use procfs::process::Pfn;
use rustc_hash::FxHashMap;

/// Structure used to represent a set of page frame numbers (PFNs)
/// in a format suitable for use with the Idle Page Tracking API.
/// <https://www.kernel.org/doc/html/latest/admin-guide/mm/idle_page_tracking.html>
#[derive(Debug)]
pub(super) struct PfnSet(FxHashMap<u64, u64>);

impl PfnSet {
    pub(super) fn new() -> Self {
        Self(FxHashMap::default())
    }

    pub(super) fn insert(&mut self, pfn: Pfn) {
        *self.0.entry(pfn.0 / 64).or_default() |= 1 << (pfn.0 % 64);
    }
}

impl IntoIterator for PfnSet {
    type Item = (u64, u64);
    type IntoIter = std::collections::hash_map::IntoIter<u64, u64>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pfn_set() {
        let input = vec![0, 64, 65, 66, 128, 136, 255];
        let expected = vec![(0, 0x1), (1, 0x7), (2, 0x101), (3, 0x8000_0000_0000_0000)];

        let mut pfn_set = PfnSet::new();
        for i in input {
            pfn_set.insert(Pfn(i));
        }
        let mut output: Vec<_> = pfn_set.into_iter().collect();
        output.sort_by_key(|(k, _)| *k);

        assert_eq!(output, expected);
    }
}
