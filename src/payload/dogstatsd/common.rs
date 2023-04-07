use std::{collections::HashMap, fmt};

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use crate::payload::Generator;

use super::random_strings;

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

#[derive(Clone, Copy)]
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

pub(crate) type Tags = HashMap<String, String>;

pub(crate) struct TagsGenerator {
    pub(crate) max_keys: usize,
    pub(crate) max_values: usize,
}

impl TagsGenerator {
    pub(crate) fn new(max_keys: usize, max_values: usize) -> Self {
        Self {
            max_keys,
            max_values,
        }
    }
}

impl Generator<Tags> for TagsGenerator {
    fn generate<R>(&self, mut rng: &mut R) -> Tags
    where
        R: rand::Rng + ?Sized,
    {
        let tag_keys = random_strings(self.max_keys, &mut rng);
        let tag_values = random_strings(self.max_values, &mut rng);

        let total_keys = rng.gen_range(0..self.max_keys);
        let mut tags = HashMap::new();
        for k in tag_keys.choose_multiple(&mut rng, total_keys) {
            let key = k.clone();
            let val = tag_values.choose(&mut rng).unwrap().clone();

            tags.insert(key, val);
        }
        tags
    }
}
