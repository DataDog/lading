use std::{collections::HashMap, fmt, mem};

use arbitrary::{Arbitrary, Unstructured};

const MAX_SMALLVEC: usize = 8;
const MAX_TAGS: usize = 16;

const STRS: [&str; 64] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "10", "11",
    "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F", "20", "21",
    "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F", "30", "31",
    "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
];
// NOTE if you adjust the size of KEYS or VALUES please choose a length that
// divides equally into the bit size of `idx`, else we'll preference some keys
// and values over others. Not also that the binomial coefficient here is 12,870
// so please do take some measure to constrain cardinality further if you
// increase the space here.
const KEYS: [&str; 16] = [
    "000", "001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011", "012",
    "013", "014", "015",
];
const VALUES: [&str; 8] = ["000", "001", "002", "003", "004", "005", "006", "007"];

#[derive(Hash, PartialEq, Eq, Arbitrary)]
pub(crate) struct MetricTagKey {
    idx: u8,
}

impl fmt::Display for MetricTagKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let idx = (self.idx as usize) % KEYS.len();
        write!(f, "{}", KEYS[idx])
    }
}

#[derive(Hash, PartialEq, Eq, Arbitrary)]
pub(crate) struct MetricTagValue {
    idx: u8,
}

impl fmt::Display for MetricTagValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let idx = (self.idx as usize) % VALUES.len();
        write!(f, "{}", VALUES[idx])
    }
}

#[derive(Hash, PartialEq, Eq, Arbitrary)]
pub(crate) struct MetricTagStr {
    idx: u8,
}

impl MetricTagStr {
    pub(crate) fn len(&self) -> usize {
        self.as_str().len()
    }

    #[inline]
    pub(crate) fn as_str(&self) -> &str {
        let idx = (self.idx as usize) % STRS.len();
        STRS[idx]
    }
}

impl fmt::Display for MetricTagStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
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

#[derive(Clone, Copy, Arbitrary)]
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

pub(crate) struct Tags {
    pub(crate) inner: HashMap<MetricTagKey, MetricTagValue>,
}

impl<'a> arbitrary::Arbitrary<'a> for Tags {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let total: usize = u.int_in_range(0..=MAX_TAGS)?;
        let mut inner = HashMap::with_capacity(total);
        for _ in 0..total {
            let key = u.arbitrary()?;
            let val = u.arbitrary()?;
            inner.insert(key, val);
        }
        Ok(Self { inner })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        arbitrary::size_hint::and(
            MetricTagKey::size_hint(depth),
            MetricTagValue::size_hint(depth),
        )
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
        let total: usize = u.int_in_range(1..=MAX_SMALLVEC)?;
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
