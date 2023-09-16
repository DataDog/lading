use std::fmt;

use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};

use crate::Generator;

use super::{ValueConf, ValueRange};

pub(crate) mod tags;

#[derive(Clone, Debug)]
pub(crate) enum NumValue {
    Int(i64),
    Float(f64),
}

#[derive(Clone, Debug)]
pub(crate) enum NumValueGenerator {
    Constant {
        float_weight: u8,
        int: i64,
        float: f64,
    },
    Uniform {
        float_weight: u8,
        int_distr: Uniform<i64>,
        float_distr: Uniform<f64>,
    },
}

impl NumValueGenerator {
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn new(conf: ValueConf) -> Self {
        match conf.range {
            ValueRange::Constant(c) => Self::Constant {
                float_weight: conf.float_weight,
                int: c,
                float: c as f64,
            },
            ValueRange::Inclusive { min, max } => Self::Uniform {
                float_weight: conf.float_weight,
                int_distr: Uniform::new_inclusive(min, max),
                float_distr: Uniform::new_inclusive(min as f64, max as f64),
            },
        }
    }
}

impl<'a> Generator<'a> for NumValueGenerator {
    type Output = NumValue;

    fn generate<R>(&'a self, rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        match self {
            Self::Constant {
                float_weight,
                int,
                float,
            } => {
                if rng.gen_ratio(u32::from(*float_weight), 256) {
                    NumValue::Float(*float)
                } else {
                    NumValue::Int(*int)
                }
            }
            Self::Uniform {
                float_weight,
                int_distr,
                float_distr,
            } => {
                if rng.gen_ratio(u32::from(*float_weight), 256) {
                    NumValue::Float(float_distr.sample(rng))
                } else {
                    NumValue::Int(int_distr.sample(rng))
                }
            }
        }
    }
}

impl fmt::Display for NumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(val) => write!(f, "{val}"),
            Self::Float(val) => write!(f, "{val}"),
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
