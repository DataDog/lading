use std::{collections::HashMap, fmt, mem};

use arbitrary::Unstructured;

const MAX_SMALLVEC: usize = 8;
const MAX_TAGS: usize = 16;
const SIZES: [usize; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
#[allow(clippy::cast_possible_truncation)]
const CHARSET_LEN: u8 = CHARSET.len() as u8;

#[derive(Hash, PartialEq, Eq)]
pub(crate) struct MetricTagStr {
    bytes: Vec<u8>,
}

impl MetricTagStr {
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_str(&self) -> &str {
        // Safety: given that CHARSET is where we derive members from
        // `self.bytes` is always valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }
}

impl fmt::Display for MetricTagStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'a> arbitrary::Arbitrary<'a> for MetricTagStr {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.arbitrary()?;
        let size = SIZES[(choice as usize) % SIZES.len()];
        let mut bytes: Vec<u8> = vec![0; size];
        u.fill_buffer(&mut bytes)?;
        bytes
            .iter_mut()
            .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
        Ok(Self { bytes })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        let empty_sz = mem::size_of::<Self>();
        let full_bytes_sz = mem::size_of::<u8>() * 128; // max in SIZES

        (empty_sz, Some(empty_sz + full_bytes_sz))
    }
}

pub(crate) enum NumValue {
    Float(f64),
    Int(i64),
}

impl fmt::Display for NumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float(val) => write!(f, "{val}"),
            Self::Int(val) => write!(f, "{val}"),
        }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for NumValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let is_float: bool = u.arbitrary()?;
        let nv = if is_float {
            Self::Float(u.arbitrary()?)
        } else {
            Self::Int(u.arbitrary()?)
        };

        Ok(nv)
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (1, Some(mem::size_of::<i64>()))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ZeroToOne {
    One,
    Frac(u32),
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

impl<'a> arbitrary::Arbitrary<'a> for ZeroToOne {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let is_one = u.arbitrary()?;
        let zto = if is_one {
            Self::One
        } else {
            Self::Frac(u.arbitrary()?)
        };
        Ok(zto)
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (
            mem::size_of::<ZeroToOne>(),
            Some(mem::size_of::<ZeroToOne>()),
        )
    }
}

pub(crate) struct Tags {
    pub(crate) inner: HashMap<MetricTagStr, MetricTagStr>,
}

impl<'a> arbitrary::Arbitrary<'a> for Tags {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let total: usize = u.arbitrary::<usize>()? % MAX_TAGS;
        let mut inner = HashMap::with_capacity(total);
        for _ in 0..total {
            let key = u.arbitrary()?;
            let val = u.arbitrary()?;
            inner.insert(key, val);
        }
        Ok(Self { inner })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let (low, upper) = MetricTagStr::size_hint(depth);
        (low * MAX_TAGS, upper.map(|u| u * MAX_TAGS))
    }
}

pub(crate) struct NonEmptyVec<T> {
    pub(crate) inner: Vec<T>,
}

impl<'a, T> arbitrary::Arbitrary<'a> for NonEmptyVec<T>
where
    T: arbitrary::Arbitrary<'a>,
{
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let total: usize = {
            let val = u.arbitrary::<usize>()? % MAX_SMALLVEC;
            if val == 0 {
                1
            } else {
                val
            }
        };

        let mut inner = Vec::with_capacity(total);
        for _ in 0..total {
            inner.push(u.arbitrary()?);
        }
        Ok(Self { inner })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let (low, upper) = T::size_hint(depth);
        (low, upper.map(|u| u * MAX_SMALLVEC))
    }
}
