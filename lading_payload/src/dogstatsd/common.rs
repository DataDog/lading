use std::fmt;

use rand::{
    distributions::{OpenClosed01, Standard, Uniform},
    prelude::Distribution,
    Rng,
};

use crate::{Error, Generator};

use super::{ConfRange, ValueConf};

pub(crate) mod tags;

#[derive(Clone, Debug, Copy)]
pub enum NumValue {
    Int(i64),
    Float(f64),
}

#[derive(Clone, Debug)]
pub(crate) enum NumValueGenerator {
    Constant {
        float_probability: f32,
        int: i64,
        float: f64,
    },
    Uniform {
        float_probability: f32,
        int_distr: Uniform<i64>,
        float_distr: Uniform<f64>,
    },
}

impl NumValueGenerator {
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn new(conf: ValueConf) -> Self {
        match conf.range {
            ConfRange::Constant(c) => Self::Constant {
                float_probability: conf.float_probability,
                int: c,
                float: c as f64,
            },
            ConfRange::Inclusive { min, max } => Self::Uniform {
                float_probability: conf.float_probability,
                int_distr: Uniform::new_inclusive(min, max),
                float_distr: Uniform::new_inclusive(min as f64, max as f64),
            },
        }
    }
}

impl<'a> Generator<'a> for NumValueGenerator {
    type Output = NumValue;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let prob: f32 = OpenClosed01.sample(rng);
        match self {
            Self::Constant {
                float_probability,
                int,
                float,
            } => {
                if prob < *float_probability {
                    Ok(NumValue::Float(*float))
                } else {
                    Ok(NumValue::Int(*int))
                }
            }
            Self::Uniform {
                float_probability,
                int_distr,
                float_distr,
            } => {
                if prob < *float_probability {
                    Ok(NumValue::Float(float_distr.sample(rng)))
                } else {
                    Ok(NumValue::Int(int_distr.sample(rng)))
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
pub enum ZeroToOne {
    One,
    Frac(u32),
}

#[derive(Debug, Clone, Copy)]
pub enum ZeroToOneError {
    OutOfRange,
}

impl TryFrom<f32> for ZeroToOne {
    type Error = ZeroToOneError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        if (value - 1.0).abs() < f32::EPSILON {
            Ok(Self::One)
        } else if !(0.0..=1.0).contains(&value) {
            Err(ZeroToOneError::OutOfRange)
        } else {
            #[allow(clippy::cast_sign_loss)]
            #[allow(clippy::cast_possible_truncation)]
            Ok(Self::Frac((1.0 / value) as u32))
        }
    }
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
