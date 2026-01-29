use super::{Handle, Pool};

#[derive(Debug, Clone)]
pub(crate) struct StringListPool {
    metric_names: Vec<String>,
}

impl StringListPool {
    pub(crate) fn new(metric_names: Vec<String>) -> Self {
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
