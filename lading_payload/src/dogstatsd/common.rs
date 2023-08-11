use std::fmt;

use rand::{distributions::Standard, prelude::Distribution, Rng};

pub(crate) mod tags;

#[derive(Clone, Debug)]
pub(crate) enum NumValue {
    Float(f64),
    Int(i64),
}

impl Distribution<NumValue> for Standard {
    fn sample<R>(&self, rng: &mut R) -> NumValue
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..=1) {
            0 => NumValue::Float(rng.gen()),
            1 => NumValue::Int(rng.gen()),
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for NumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float(val) => write!(f, "{val}"),
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
