//! Common configuration for all lading payloads

use rand::distr::uniform::SampleUniform;
use serde::Deserialize;
use std::{cmp, fmt};

/// Range expression for configuration
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy,
{
    /// A constant T
    Constant(T),
    /// In which a T is chosen between `min` and `max`, inclusive of `max`.
    Inclusive {
        /// The minimum of the range.
        min: T,
        /// The maximum of the range.
        max: T,
    },
}

impl<T> ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy,
{
    /// Returns true if the range provided by the user is valid, false
    /// otherwise.
    pub(crate) fn valid(&self) -> (bool, &'static str) {
        match self {
            Self::Constant(_) => (true, ""),
            Self::Inclusive { min, max } => (min <= max, "min must be less than or equal to max"),
        }
    }

    pub(crate) fn start(&self) -> T {
        match self {
            ConfRange::Constant(c) => *c,
            ConfRange::Inclusive { min, .. } => *min,
        }
    }

    pub(crate) fn end(&self) -> T {
        match self {
            ConfRange::Constant(c) => *c,
            ConfRange::Inclusive { max, .. } => *max,
        }
    }
}

impl<T> ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy + SampleUniform,
{
    pub(crate) fn sample<R>(&self, rng: &mut R) -> T
    where
        R: rand::Rng + ?Sized,
    {
        match self {
            ConfRange::Constant(c) => *c,
            ConfRange::Inclusive { min, max } => rng.random_range(*min..=*max),
        }
    }
}

impl<T> fmt::Display for ConfRange<T>
where
    T: PartialEq + cmp::PartialOrd + Clone + Copy + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfRange::Constant(c) => write!(f, "{c}"),
            ConfRange::Inclusive { min, max } => {
                if min == max {
                    write!(f, "{min}")
                } else {
                    write!(f, "{min}..={max}")
                }
            }
        }
    }
}
