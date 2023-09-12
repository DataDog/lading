use std::{fmt, ops::Range};

use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};

use crate::Generator;

pub(crate) mod tags;

#[derive(Clone, Debug)]
pub(crate) enum NumValue {
    Int(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct NumValueGenerator {
    int_distr: Uniform<i64>,
}

impl NumValueGenerator {
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn new(range: Range<i64>) -> Self {
        Self {
            int_distr: Uniform::new_inclusive(range.start, range.end),
        }
    }
}

impl<'a> Generator<'a> for NumValueGenerator {
    type Output = NumValue;

    fn generate<R>(&'a self, rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        NumValue::Int(self.int_distr.sample(rng))
    }
}

impl fmt::Display for NumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(val) => write!(f, "{val}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ZeroToOne {
    One,
    Frac(u32),
}

impl Distribution<ZeroToOne> for Standard {
    fn sample<R>(&self, rng: &mut R) -> ZeroToOne
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..=1) {
            0 => ZeroToOne::One,
            1 => ZeroToOne::Frac(rng.gen()),
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for ZeroToOne {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One => write!(f, "1"),
            Self::Frac(inner) => {
                if *inner == 0 {
                    write!(f, "0")
                } else {
                    let val = 1.0 / f64::from(*inner);
                    write!(f, "{val}")
                }
            }
        }
    }
}
