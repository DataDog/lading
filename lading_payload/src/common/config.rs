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
const NEG_ZERO_AS_BITS: u32 = 0x8000_0000;

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
/// The const generic `MIN_AS_BITS` is the IEEE-754 bit pattern of the lower bound,
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
/// [`PartialOrd`] for clarity, but `MIN_AS_BITS` is stored as `u32` precisely so
/// that future revisions can move the hot-path comparison into the integer
/// domain without an API break.
///
/// # Example
///
/// ```
/// use lading_payload::common::config::Probability;
///
/// type AtLeastHalf = Probability<{ f32::to_bits(0.5) }>;
/// let p = AtLeastHalf::try_new(0.75).expect("0.75 is in [0.5, 1.0]");
/// assert_eq!(p.get(), 0.75);
/// ```
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(into = "f32", try_from = "f32")]
pub struct Probability<const MIN_AS_BITS: u32> {
    value: f32,
}

impl<const MIN_AS_BITS: u32> TryFrom<f32> for Probability<MIN_AS_BITS> {
    type Error = ProbabilityError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl<const MIN_AS_BITS: u32> From<Probability<MIN_AS_BITS>> for f32 {
    fn from(p: Probability<MIN_AS_BITS>) -> Self {
        p.value
    }
}

impl<const MIN_AS_BITS: u32> fmt::Display for Probability<MIN_AS_BITS> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<const MIN_AS_BITS: u32> Probability<MIN_AS_BITS> {
    /// The lower bound decoded from `MIN_AS_BITS`.
    ///
    /// The `assert!`s here run at const-evaluation time for every
    /// monomorphization of [`Probability`], rejecting bit patterns that decode
    /// to NaN, `±∞`, `-0.0`, or values outside `[+0.0, +1.0]`.
    pub const MIN: f32 = {
        assert!(
            MIN_AS_BITS != NEG_ZERO_AS_BITS,
            "MIN_AS_BITS must not encode -0.0"
        );
        let v = f32::from_bits(MIN_AS_BITS);
        assert!(v.is_finite(), "MIN_AS_BITS must decode to a finite f32");
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
        if value.to_bits() == NEG_ZERO_AS_BITS {
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
    use super::{NEG_ZERO_AS_BITS, Probability, ProbabilityError};
    use proptest::prelude::*;

    type ZeroOrMore = Probability<{ f32::to_bits(0.0) }>;
    type AtLeastHalf = Probability<{ f32::to_bits(0.5) }>;
    type AtLeastOne = Probability<{ f32::to_bits(1.0) }>;

    // ===== Unit tests: constants =====

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
    fn accepts_only_one_for_unit_bound() {
        let p = AtLeastOne::try_new(1.0).expect("1.0 is in [1.0, 1.0]");
        assert_eq!(p.get().to_bits(), 1.0_f32.to_bits());
    }

    #[test]
    fn rejects_negative_zero() {
        let neg_zero = f32::from_bits(NEG_ZERO_AS_BITS);
        let err = ZeroOrMore::try_new(neg_zero).expect_err("-0.0 is rejected");
        assert!(matches!(err, ProbabilityError::NegativeZero));
    }

    // ===== Unit tests: wire-format pins =====

    #[test]
    fn serde_round_trip_json() {
        let p = AtLeastHalf::try_new(0.75).expect("0.75 in [0.5, 1.0]");
        let json = serde_json::to_string(&p).expect("serialize");
        assert_eq!(json, "0.75");
        let back: AtLeastHalf = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get().to_bits(), 0.75_f32.to_bits());
    }

    #[test]
    fn serde_round_trip_yaml_format() {
        let p = AtLeastHalf::try_new(0.75).expect("0.75 in [0.5, 1.0]");
        let yaml = serde_yaml::to_string(&p).expect("serialize");
        assert_eq!(yaml, "0.75\n");
        let back: AtLeastHalf = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(back.get().to_bits(), 0.75_f32.to_bits());
    }

    // ===== Property-test strategies =====

    fn valid_value_strategy(min: f32) -> BoxedStrategy<f32> {
        // proptest's `RangeInclusive<f32>` panics on degenerate ranges, so
        // produce a single-value strategy when `min == MAX`.
        if min >= 1.0 {
            Just(1.0_f32).boxed()
        } else {
            (min..=1.0_f32)
                .prop_filter("not -0.0", |v| v.to_bits() != NEG_ZERO_AS_BITS)
                .boxed()
        }
    }

    fn below_min_strategy(min: f32) -> impl Strategy<Value = f32> {
        (f32::MIN..min).prop_filter("finite, not -0.0", |v| {
            v.is_finite() && v.to_bits() != NEG_ZERO_AS_BITS
        })
    }

    fn above_one_strategy() -> impl Strategy<Value = f32> {
        (1.0_f32..f32::MAX).prop_filter("> 1.0 and finite", |v| *v > 1.0 && v.is_finite())
    }

    fn non_finite_strategy() -> impl Strategy<Value = f32> {
        prop_oneof![
            Just(f32::NAN),
            Just(f32::INFINITY),
            Just(f32::NEG_INFINITY),
        ]
    }

    // ===== Property-test helpers (generic over MIN_AS_BITS) =====

    fn check_accepts_in_range<const MIN_AS_BITS: u32>(v: f32) {
        let p = Probability::<MIN_AS_BITS>::try_new(v)
            .expect("v should be valid by construction");
        assert_eq!(p.get().to_bits(), v.to_bits());
    }

    fn check_rejects_below_min<const MIN_AS_BITS: u32>(v: f32) {
        let err = Probability::<MIN_AS_BITS>::try_new(v).expect_err("v should be below MIN");
        match err {
            ProbabilityError::BelowMin { min, value } => {
                assert_eq!(min.to_bits(), Probability::<MIN_AS_BITS>::MIN.to_bits());
                assert_eq!(value.to_bits(), v.to_bits());
            }
            other => panic!("expected BelowMin, got {other:?}"),
        }
    }

    fn check_display_matches<const MIN_AS_BITS: u32>(v: f32) {
        let p = Probability::<MIN_AS_BITS>::try_new(v).expect("valid v");
        assert_eq!(format!("{p}"), format!("{v}"));
    }

    fn check_display_precision<const MIN_AS_BITS: u32>(v: f32, n: usize) {
        let p = Probability::<MIN_AS_BITS>::try_new(v).expect("valid v");
        assert_eq!(format!("{p:.n$}"), format!("{v:.n$}"));
    }

    fn check_serde_json_round_trip<const MIN_AS_BITS: u32>(v: f32) {
        let p = Probability::<MIN_AS_BITS>::try_new(v).expect("valid v");
        let json = serde_json::to_string(&p).expect("serialize");
        let back: Probability<MIN_AS_BITS> =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get().to_bits(), v.to_bits());
    }

    fn check_serde_yaml_round_trip<const MIN_AS_BITS: u32>(v: f32) {
        let p = Probability::<MIN_AS_BITS>::try_new(v).expect("valid v");
        let yaml = serde_yaml::to_string(&p).expect("serialize");
        let back: Probability<MIN_AS_BITS> =
            serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(back.get().to_bits(), v.to_bits());
    }

    fn check_serde_json_rejects_below_min<const MIN_AS_BITS: u32>(v: f32) {
        let json = serde_json::to_string(&v).expect("serialize raw f32");
        let err = serde_json::from_str::<Probability<MIN_AS_BITS>>(&json)
            .expect_err("v < MIN");
        assert!(
            err.to_string().contains("below lower bound"),
            "unexpected error: {err}"
        );
    }

    fn check_serde_yaml_rejects_below_min<const MIN_AS_BITS: u32>(v: f32) {
        let yaml = serde_yaml::to_string(&v).expect("serialize raw f32");
        let err = serde_yaml::from_str::<Probability<MIN_AS_BITS>>(&yaml)
            .expect_err("v < MIN");
        assert!(
            err.to_string().contains("below lower bound"),
            "unexpected error: {err}"
        );
    }

    // ===== Property tests: input validation =====

    proptest! {
        #[test]
        fn accepts_any_value_in_range_zero_or_more(
            v in valid_value_strategy(ZeroOrMore::MIN),
        ) {
            check_accepts_in_range::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn accepts_any_value_in_range_at_least_half(
            v in valid_value_strategy(AtLeastHalf::MIN),
        ) {
            check_accepts_in_range::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn accepts_any_value_in_range_at_least_one(
            v in valid_value_strategy(AtLeastOne::MIN),
        ) {
            check_accepts_in_range::<{ f32::to_bits(1.0) }>(v);
        }

        #[test]
        fn rejects_any_value_below_min_zero_or_more(
            v in below_min_strategy(ZeroOrMore::MIN),
        ) {
            check_rejects_below_min::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn rejects_any_value_below_min_at_least_half(
            v in below_min_strategy(AtLeastHalf::MIN),
        ) {
            check_rejects_below_min::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn rejects_any_value_below_min_at_least_one(
            v in below_min_strategy(AtLeastOne::MIN),
        ) {
            check_rejects_below_min::<{ f32::to_bits(1.0) }>(v);
        }

        #[test]
        fn rejects_any_value_above_one(v in above_one_strategy()) {
            let err = ZeroOrMore::try_new(v).expect_err("v > 1.0");
            prop_assert!(matches!(err, ProbabilityError::AboveOne(_)));
        }

        #[test]
        fn rejects_any_non_finite(v in non_finite_strategy()) {
            let err = ZeroOrMore::try_new(v).expect_err("v is not finite");
            prop_assert!(matches!(err, ProbabilityError::NotFinite(_)));
        }
    }

    // ===== Property tests: Display =====

    proptest! {
        #[test]
        fn display_matches_inner_f32_for_valid_values_zero_or_more(
            v in valid_value_strategy(ZeroOrMore::MIN),
        ) {
            check_display_matches::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn display_matches_inner_f32_for_valid_values_at_least_half(
            v in valid_value_strategy(AtLeastHalf::MIN),
        ) {
            check_display_matches::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn display_matches_inner_f32_for_valid_values_at_least_one(
            v in valid_value_strategy(AtLeastOne::MIN),
        ) {
            check_display_matches::<{ f32::to_bits(1.0) }>(v);
        }

        #[test]
        fn display_propagates_precision_zero_or_more(
            v in valid_value_strategy(ZeroOrMore::MIN),
            n in 0_usize..=10,
        ) {
            check_display_precision::<{ f32::to_bits(0.0) }>(v, n);
        }

        #[test]
        fn display_propagates_precision_at_least_half(
            v in valid_value_strategy(AtLeastHalf::MIN),
            n in 0_usize..=10,
        ) {
            check_display_precision::<{ f32::to_bits(0.5) }>(v, n);
        }

        #[test]
        fn display_propagates_precision_at_least_one(
            v in valid_value_strategy(AtLeastOne::MIN),
            n in 0_usize..=10,
        ) {
            check_display_precision::<{ f32::to_bits(1.0) }>(v, n);
        }
    }

    // ===== Property tests: serde round-trips =====

    proptest! {
        #[test]
        fn serde_json_round_trips_for_valid_values_zero_or_more(
            v in valid_value_strategy(ZeroOrMore::MIN),
        ) {
            check_serde_json_round_trip::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn serde_json_round_trips_for_valid_values_at_least_half(
            v in valid_value_strategy(AtLeastHalf::MIN),
        ) {
            check_serde_json_round_trip::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn serde_json_round_trips_for_valid_values_at_least_one(
            v in valid_value_strategy(AtLeastOne::MIN),
        ) {
            check_serde_json_round_trip::<{ f32::to_bits(1.0) }>(v);
        }

        #[test]
        fn serde_yaml_round_trips_for_valid_values_zero_or_more(
            v in valid_value_strategy(ZeroOrMore::MIN),
        ) {
            check_serde_yaml_round_trip::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn serde_yaml_round_trips_for_valid_values_at_least_half(
            v in valid_value_strategy(AtLeastHalf::MIN),
        ) {
            check_serde_yaml_round_trip::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn serde_yaml_round_trips_for_valid_values_at_least_one(
            v in valid_value_strategy(AtLeastOne::MIN),
        ) {
            check_serde_yaml_round_trip::<{ f32::to_bits(1.0) }>(v);
        }
    }

    // ===== Property tests: serde rejections =====

    proptest! {
        #[test]
        fn serde_json_deserialize_rejects_below_min_zero_or_more(
            v in below_min_strategy(ZeroOrMore::MIN),
        ) {
            check_serde_json_rejects_below_min::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn serde_json_deserialize_rejects_below_min_at_least_half(
            v in below_min_strategy(AtLeastHalf::MIN),
        ) {
            check_serde_json_rejects_below_min::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn serde_json_deserialize_rejects_below_min_at_least_one(
            v in below_min_strategy(AtLeastOne::MIN),
        ) {
            check_serde_json_rejects_below_min::<{ f32::to_bits(1.0) }>(v);
        }

        #[test]
        fn serde_yaml_deserialize_rejects_below_min_zero_or_more(
            v in below_min_strategy(ZeroOrMore::MIN),
        ) {
            check_serde_yaml_rejects_below_min::<{ f32::to_bits(0.0) }>(v);
        }

        #[test]
        fn serde_yaml_deserialize_rejects_below_min_at_least_half(
            v in below_min_strategy(AtLeastHalf::MIN),
        ) {
            check_serde_yaml_rejects_below_min::<{ f32::to_bits(0.5) }>(v);
        }

        #[test]
        fn serde_yaml_deserialize_rejects_below_min_at_least_one(
            v in below_min_strategy(AtLeastOne::MIN),
        ) {
            check_serde_yaml_rejects_below_min::<{ f32::to_bits(1.0) }>(v);
        }

        #[test]
        fn serde_json_deserialize_rejects_above_one(v in above_one_strategy()) {
            let json = serde_json::to_string(&v).expect("serialize");
            let err = serde_json::from_str::<ZeroOrMore>(&json).expect_err("v > 1.0");
            prop_assert!(
                err.to_string().contains("exceeds 1.0"),
                "unexpected error: {}", err
            );
        }

        #[test]
        fn serde_yaml_deserialize_rejects_above_one(v in above_one_strategy()) {
            let yaml = serde_yaml::to_string(&v).expect("serialize");
            let err = serde_yaml::from_str::<ZeroOrMore>(&yaml).expect_err("v > 1.0");
            prop_assert!(
                err.to_string().contains("exceeds 1.0"),
                "unexpected error: {}", err
            );
        }
    }
}
