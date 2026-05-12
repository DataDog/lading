//! Common configuration for all lading payloads

use rand::{RngExt, distr::uniform::SampleUniform};
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

/// Bit pattern of `-0.0_f32`. Used to reject negative zero, which compares
/// equal to `+0.0` under IEEE-754 numeric ordering.
const NEG_ZERO_BITS: u32 = 0x8000_0000;

/// Error returned when a value cannot be turned into a [`Probability`].
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum ProbabilityError {
    /// Value is NaN or `±∞`.
    #[error("probability must be finite, got {0}")]
    NotFinite(f32),
    /// Value has the bit pattern of `-0.0`.
    #[error("probability must not be -0.0")]
    NegativeZero,
    /// Value is below the type's compile-time lower bound.
    #[error("probability {value} is below lower bound {min}")]
    BelowMin {
        /// The lower bound encoded in the type parameter.
        min: f32,
        /// The offending value.
        value: f32,
    },
    /// Value exceeds `+1.0`.
    #[error("probability {0} exceeds 1.0")]
    AboveOne(f32),
}

/// An `f32`-valued probability with a compile-time lower bound.
///
/// The const generic `MIN_BITS` is the IEEE-754 bit pattern of the lower bound,
/// obtained at the call site via [`f32::to_bits`]. The decoded bound must be a
/// finite value in `[+0.0, +1.0]` and must not be `-0.0`; otherwise the type
/// fails to instantiate at compile time via the assertions on [`Self::MIN`].
///
/// Stored values are validated by [`Self::try_new`] against the same edge-case
/// rules and must lie in `[MIN, +1.0]`.
///
/// # Design
///
/// Rust does not yet allow floating-point types as const generic parameters on
/// stable; tracking issue `rust-lang/rust#95174` covers `#![feature(adt_const_params)]`
/// and its float-specific complications. To carry a compile-time lower bound
/// today the type must be parameterized on an integer, so callers spell the
/// bound as a `u32` bit pattern (`{ f32::to_bits(0.5) }`) and the type decodes
/// it back to `f32` inside [`Self::MIN`].
///
/// Once the const generic is an integer, the natural follow-up question is
/// whether bound checks can stay in the integer domain. They can, for the
/// values this type accepts, because of two IEEE-754 properties of the
/// `f32` ↔ `u32` round trip exposed by [`f32::to_bits`] / [`f32::from_bits`]:
///
/// * For any two values `a, b` in `[+0.0, +∞)` (i.e. non-negative, non-NaN),
///   `a <= b` iff `a.to_bits() <= b.to_bits()`. The sign bit is zero and the
///   remaining 31 bits are laid out exponent-then-mantissa, so the unsigned
///   integer ordering matches the numeric ordering.
/// * `+0.0` and `-0.0` are numerically equal under `==`/`<`/`<=` but have
///   distinct bit patterns (`0x0000_0000` vs `0x8000_0000`). Allowing `-0.0`
///   would break the order-preservation property above (`-0.0.to_bits()` is
///   the largest `u32`), so the type rejects `-0.0` explicitly.
///
/// The current implementation still compares in the `f32` domain via
/// [`PartialOrd`] for clarity, but `MIN_BITS` is stored as `u32` precisely so
/// that future revisions can move the hot-path comparison into the integer
/// domain without an API break.
///
/// # Example
///
/// ```ignore
/// use lading_payload::common::config::Probability;
///
/// type AtLeastHalf = Probability<{ f32::to_bits(0.5) }>;
/// let p = AtLeastHalf::try_new(0.75).expect("0.75 is in [0.5, 1.0]");
/// assert!((p.get() - 0.75).abs() < f32::EPSILON);
/// ```
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(into = "f32", try_from = "f32")]
pub struct Probability<const MIN_BITS: u32> {
    value: f32,
}

impl<const MIN_BITS: u32> TryFrom<f32> for Probability<MIN_BITS> {
    type Error = ProbabilityError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl<const MIN_BITS: u32> From<Probability<MIN_BITS>> for f32 {
    fn from(p: Probability<MIN_BITS>) -> Self {
        p.value
    }
}

impl<const MIN_BITS: u32> fmt::Display for Probability<MIN_BITS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<const MIN_BITS: u32> Probability<MIN_BITS> {
    /// The lower bound decoded from `MIN_BITS`.
    ///
    /// The `assert!`s here run at const-evaluation time for every
    /// monomorphization of [`Probability`], rejecting bit patterns that decode
    /// to NaN, `±∞`, `-0.0`, or values outside `[+0.0, +1.0]`.
    pub const MIN: f32 = {
        assert!(
            MIN_BITS != NEG_ZERO_BITS,
            "MIN_BITS must not encode -0.0"
        );
        let v = f32::from_bits(MIN_BITS);
        assert!(v.is_finite(), "MIN_BITS must decode to a finite f32");
        assert!(v >= 0.0, "lower bound must be >= +0.0");
        assert!(v <= 1.0, "lower bound must be <= +1.0");
        v
    };

    /// The upper bound, fixed at `+1.0`.
    pub const MAX: f32 = 1.0;

    /// Construct a [`Probability`] from `value`, validating that it lies in
    /// `[MIN, +1.0]` and is not NaN, `±∞`, or `-0.0`.
    ///
    /// # Errors
    ///
    /// Returns [`ProbabilityError`] when validation fails.
    pub fn try_new(value: f32) -> Result<Self, ProbabilityError> {
        // Force evaluation of the const-eval bound check for this
        // monomorphization. Without this reference the assertions in `MIN`
        // can be elided when no caller names `Self::MIN` directly.
        let min = Self::MIN;

        if !value.is_finite() {
            return Err(ProbabilityError::NotFinite(value));
        }
        if value.to_bits() == NEG_ZERO_BITS {
            return Err(ProbabilityError::NegativeZero);
        }
        if value < min {
            return Err(ProbabilityError::BelowMin { min, value });
        }
        if value > Self::MAX {
            return Err(ProbabilityError::AboveOne(value));
        }
        Ok(Self { value })
    }

    /// Return the stored probability value.
    #[must_use]
    pub const fn get(&self) -> f32 {
        self.value
    }
}

#[cfg(test)]
mod probability_tests {
    use super::{NEG_ZERO_BITS, Probability, ProbabilityError};

    type ZeroOrMore = Probability<{ f32::to_bits(0.0) }>;
    type AtLeastHalf = Probability<{ f32::to_bits(0.5) }>;
    type AtLeastOne = Probability<{ f32::to_bits(1.0) }>;

    #[test]
    fn min_const_matches_bit_pattern() {
        assert_eq!(ZeroOrMore::MIN.to_bits(), 0.0_f32.to_bits());
        assert_eq!(AtLeastHalf::MIN.to_bits(), 0.5_f32.to_bits());
        assert_eq!(AtLeastOne::MIN.to_bits(), 1.0_f32.to_bits());
    }

    #[test]
    fn max_const_is_one() {
        assert_eq!(ZeroOrMore::MAX.to_bits(), 1.0_f32.to_bits());
    }

    #[test]
    fn accepts_values_in_range() {
        let p = ZeroOrMore::try_new(0.5).expect("0.5 is in [0.0, 1.0]");
        assert_eq!(p.get().to_bits(), 0.5_f32.to_bits());

        let p = AtLeastHalf::try_new(0.75).expect("0.75 is in [0.5, 1.0]");
        assert_eq!(p.get().to_bits(), 0.75_f32.to_bits());
    }

    #[test]
    fn accepts_lower_bound() {
        let p = AtLeastHalf::try_new(0.5).expect("0.5 is the lower bound");
        assert_eq!(p.get().to_bits(), 0.5_f32.to_bits());
    }

    #[test]
    fn accepts_upper_bound() {
        let p = ZeroOrMore::try_new(1.0).expect("1.0 is the upper bound");
        assert_eq!(p.get().to_bits(), 1.0_f32.to_bits());
    }

    #[test]
    fn accepts_only_one_for_unit_bound() {
        let p = AtLeastOne::try_new(1.0).expect("1.0 is in [1.0, 1.0]");
        assert_eq!(p.get().to_bits(), 1.0_f32.to_bits());
    }

    #[test]
    fn rejects_below_min() {
        let err = AtLeastHalf::try_new(0.1).expect_err("0.1 < 0.5");
        match err {
            ProbabilityError::BelowMin { min, value } => {
                assert_eq!(min.to_bits(), 0.5_f32.to_bits());
                assert_eq!(value.to_bits(), 0.1_f32.to_bits());
            }
            other => panic!("expected BelowMin, got {other:?}"),
        }
    }

    #[test]
    fn rejects_above_one() {
        let err = ZeroOrMore::try_new(1.5).expect_err("1.5 > 1.0");
        assert!(matches!(err, ProbabilityError::AboveOne(_)));
    }

    #[test]
    fn rejects_nan() {
        let err = ZeroOrMore::try_new(f32::NAN).expect_err("NaN is not finite");
        assert!(matches!(err, ProbabilityError::NotFinite(_)));
    }

    #[test]
    fn rejects_positive_infinity() {
        let err = ZeroOrMore::try_new(f32::INFINITY).expect_err("+inf is not finite");
        assert!(matches!(err, ProbabilityError::NotFinite(_)));
    }

    #[test]
    fn rejects_negative_infinity() {
        let err = ZeroOrMore::try_new(f32::NEG_INFINITY).expect_err("-inf is not finite");
        assert!(matches!(err, ProbabilityError::NotFinite(_)));
    }

    #[test]
    fn rejects_negative_zero() {
        let neg_zero = f32::from_bits(NEG_ZERO_BITS);
        let err = ZeroOrMore::try_new(neg_zero).expect_err("-0.0 is rejected");
        assert!(matches!(err, ProbabilityError::NegativeZero));
    }

    #[test]
    fn rejects_negative_nonzero() {
        // -0.1 is finite and not -0.0, so it fails the lower-bound check.
        let err = ZeroOrMore::try_new(-0.1).expect_err("-0.1 < 0.0");
        assert!(matches!(err, ProbabilityError::BelowMin { .. }));
    }

    #[test]
    fn serde_round_trip_json() {
        let p = AtLeastHalf::try_new(0.75).expect("0.75 in [0.5, 1.0]");
        let json = serde_json::to_string(&p).expect("serialize");
        assert_eq!(json, "0.75");
        let back: AtLeastHalf = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get().to_bits(), 0.75_f32.to_bits());
    }

    #[test]
    fn serde_deserialize_accepts_valid() {
        let p: AtLeastHalf = serde_json::from_str("0.5").expect("0.5 is the lower bound");
        assert_eq!(p.get().to_bits(), 0.5_f32.to_bits());
    }

    #[test]
    fn serde_deserialize_rejects_below_min() {
        let err = serde_json::from_str::<AtLeastHalf>("0.1").expect_err("0.1 < 0.5");
        // The serde error wraps our ProbabilityError's Display output.
        assert!(
            err.to_string().contains("below lower bound"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn serde_deserialize_rejects_above_one() {
        let err = serde_json::from_str::<ZeroOrMore>("1.5").expect_err("1.5 > 1.0");
        assert!(
            err.to_string().contains("exceeds 1.0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn serde_yaml_round_trip() {
        let p = ZeroOrMore::try_new(0.25).expect("0.25 in [0.0, 1.0]");
        let yaml = serde_yaml::to_string(&p).expect("serialize");
        let back: ZeroOrMore = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(back.get().to_bits(), 0.25_f32.to_bits());
    }

    #[test]
    fn display_matches_inner_f32() {
        let p = AtLeastHalf::try_new(0.75).expect("0.75 in [0.5, 1.0]");
        assert_eq!(format!("{p}"), format!("{}", 0.75_f32));
    }

    #[test]
    fn display_propagates_format_specifiers() {
        let p = AtLeastHalf::try_new(0.5).expect("0.5 is the lower bound");
        assert_eq!(format!("{p:.3}"), format!("{:.3}", 0.5_f32));
    }
}
